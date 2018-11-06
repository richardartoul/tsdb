// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	defaultM3DBNamespace = ident.StringID("defaultM3DBNamespace")
	defaultSeriesID      = ident.StringID("default_m3db_id")
	defaultNumShardsM3DB = 100
)

func main() {
	var (
		cli                  = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		benchCmd             = cli.Command("bench", "run benchmarks")
		benchWriteCmd        = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath    = benchWriteCmd.Flag("out", "set the output path").Default("benchout").String()
		benchWriteNumMetrics = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("10000").Int()
		storageEngine        = benchWriteCmd.Flag("engine", "storage engine to use for persisting the data").Default("tsdb").String()
		benchSamplesFile     = benchWriteCmd.Arg("file", "input file with samples data, default is ("+filepath.Join("..", "testdata", "20kseries.json")+")").Default(filepath.Join("..", "testdata", "20kseries.json")).String()
		listCmd              = cli.Command("ls", "list db blocks")
		listCmdHumanReadable = listCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		listPath             = listCmd.Arg("db path", "database path (default is "+filepath.Join("benchout", "storage")+")").Default(filepath.Join("benchout", "storage")).String()
	)

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case benchWriteCmd.FullCommand():
		wb := &writeBenchmark{
			outPath:       *benchWriteOutPath,
			numMetrics:    *benchWriteNumMetrics,
			samplesFile:   *benchSamplesFile,
			storageEngine: *storageEngine,
		}
		wb.run()
	case listCmd.FullCommand():
		db, err := tsdb.Open(*listPath, nil, nil, nil)
		if err != nil {
			exitWithError(err)
		}
		printBlocks(db.Blocks(), listCmdHumanReadable)
	}
	flag.CommandLine.Set("log.level", "debug")
}

type writeBenchmark struct {
	outPath       string
	samplesFile   string
	cleanup       bool
	numMetrics    int
	storageEngine string

	tsdbStorage   *tsdb.DB
	m3dbStorage   storage.Database
	m3dbNamespace storage.Namespace

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func (b *writeBenchmark) useM3DB() bool {
	return b.storageEngine == "m3db"
}

func (b *writeBenchmark) run() {
	if b.useM3DB() {
		fmt.Println("Running with M3DB engine")
	} else {
		fmt.Println("Running with tsdb engine")
	}
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	if b.useM3DB() {
		var shards []shard.Shard
		for i := 0; i < defaultNumShardsM3DB; i++ {
			shards = append(shards, shard.NewShard(uint32(i)).SetState(shard.Available))
		}
		shardSet, err := sharding.NewShardSet(shards, sharding.DefaultHashFn(defaultNumShardsM3DB))
		if err != nil {
			exitWithError(err)
		}

		ropts := retention.NewOptions().
			SetRetentionPeriod(15 * 24 * 60 * 60 * 1000 * time.Millisecond).
			SetBlockSize(2 * time.Hour).
			SetBufferPast(time.Hour).
			SetBufferFuture(time.Hour)

		md, err := namespace.NewMetadata(defaultM3DBNamespace,
			namespace.NewOptions().
				SetBootstrapEnabled(true).
				SetFlushEnabled(true).
				SetWritesToCommitLog(true).
				SetCleanupEnabled(true).
				SetRepairEnabled(false).
				SetRetentionOptions(ropts).
				SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true)))
		if err != nil {
			exitWithError(err)
		}

		fsOpts := fs.NewOptions()
		pm, err := fs.NewPersistManager(fsOpts)
		if err != nil {
			exitWithError(err)
		}

		opts := storage.NewOptions().
			SetSeriesCachePolicy(series.CacheRecentlyRead).
			SetNamespaceInitializer(namespace.NewStaticInitializer([]namespace.Metadata{md})).
			SetRepairEnabled(false).
			SetPersistManager(pm).
			SetCommitLogOptions(commitlog.NewOptions().
				SetBacklogQueueSize(400000))

		db, err := storage.NewDatabase(shardSet, opts)
		if err != nil {
			exitWithError(err)
		}

		err = db.Open()
		if err != nil {
			exitWithError(err)
		}
		b.m3dbStorage = db
		namespace, ok := db.Namespace(defaultM3DBNamespace)
		if !ok {
			exitWithError(fmt.Errorf("could not retrieve default m3db namespace"))
		}

		b.m3dbNamespace = namespace

	} else {
		st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
			WALFlushInterval:  200 * time.Millisecond,
			RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
			BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
		})
		if err != nil {
			exitWithError(err)
		}
		b.tsdbStorage = st
	}

	var (
		metrics     []labels.Labels
		m3dbMetrics []ident.TagsIterator
	)
	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		metrics, err = readPrometheusLabels(f, b.numMetrics)
		if err != nil {
			exitWithError(err)
		}

		for _, metric := range metrics {
			var m3dbMetric []ident.Tag
			for _, tag := range metric {
				m3dbMetric = append(m3dbMetric, ident.Tag{Name: ident.StringID(tag.Name), Value: ident.StringID(tag.Value)})
			}
			m3dbMetrics = append(m3dbMetrics, ident.NewTagsIterator(ident.NewTags(m3dbMetric...)))
		}
	})

	var total uint64
	var err error

	dur := measureTime("ingestScrapes", func() {
		b.startProfiling()
		total, err = b.ingestScrapes(metrics, m3dbMetrics, 3000)
		if err != nil {
			exitWithError(err)
		}
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	measureTime("stopStorage", func() {
		if b.useM3DB() {
			if err := b.m3dbStorage.Close(); err != nil {
				exitWithError(err)
			}
		} else {
			if err := b.tsdbStorage.Close(); err != nil {
				exitWithError(err)
			}
		}
		if err := b.stopProfiling(); err != nil {
			exitWithError(err)
		}
	})
}

const timeDelta = 30000

func (b *writeBenchmark) ingestScrapes(lbls []labels.Labels, m3dbMetrics []ident.TagsIterator, scrapeCount int) (uint64, error) {
	var mu sync.Mutex
	var total uint64

	for i := 0; i < scrapeCount; i += 100 {
		var (
			wg  sync.WaitGroup
			now = time.Now().UnixNano()
		)

		if b.useM3DB() {
			lbls := m3dbMetrics
			for len(lbls) > 0 {
				l := 1000
				if len(lbls) < 1000 {
					l = len(lbls)
				}
				batch := lbls[:l]
				lbls = lbls[l:]

				wg.Add(1)
				go func() {
					n, err := b.ingestScrapesShard(nil, batch, 100, int64(timeDelta+now))
					if err != nil {
						// exitWithError(err)
						fmt.Println(" err", err)
					}
					mu.Lock()
					total += n
					mu.Unlock()
					wg.Done()
				}()
			}
		} else {
			lbls := lbls
			for len(lbls) > 0 {
				l := 1000
				if len(lbls) < 1000 {
					l = len(lbls)
				}
				batch := lbls[:l]
				lbls = lbls[l:]

				wg.Add(1)
				go func() {
					n, err := b.ingestScrapesShard(batch, nil, 100, int64(timeDelta*i))
					if err != nil {
						// exitWithError(err)
						fmt.Println(" err", err)
					}
					mu.Lock()
					total += n
					mu.Unlock()
					wg.Done()
				}()
			}
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(metrics []labels.Labels, m3dbMetrics []ident.TagsIterator, scrapeCount int, baset int64) (uint64, error) {
	ts := baset
	total := uint64(0)

	if b.useM3DB() {
		type sample struct {
			tags  ident.TagsIterator
			value int64
			ref   *uint64
		}

		scrape := make([]*sample, 0, len(metrics))

		for _, m := range m3dbMetrics {
			scrape = append(scrape, &sample{
				tags:  m,
				value: 123456789,
			})
		}

		for i := 0; i < scrapeCount; i++ {
			ts += timeDelta
			id := ident.StringID(string(i))
			//
			for _, s := range scrape {
				s.value += 1000
				err := b.m3dbStorage.WriteTagged(
					nil, defaultM3DBNamespace, id, s.tags, time.Unix(0, ts), float64(s.value), xtime.Millisecond, nil)
				if err != nil {
					panic(err)
				}

				total++
			}
		}

	} else {
		type sample struct {
			labels labels.Labels
			value  int64
			ref    *uint64
		}

		scrape := make([]*sample, 0, len(metrics))

		for _, m := range metrics {
			scrape = append(scrape, &sample{
				labels: m,
				value:  123456789,
			})
		}

		for i := 0; i < scrapeCount; i++ {
			app := b.tsdbStorage.Appender()
			ts += timeDelta

			for _, s := range scrape {
				s.value += 1000

				if s.ref == nil {
					ref, err := app.Add(s.labels, ts, float64(s.value))
					if err != nil {
						panic(err)
					}
					s.ref = &ref
				} else if err := app.AddFast(*s.ref, ts, float64(s.value)); err != nil {

					if errors.Cause(err) != tsdb.ErrNotFound {
						panic(err)
					}

					ref, err := app.Add(s.labels, ts, float64(s.value))
					if err != nil {
						panic(err)
					}
					s.ref = &ref
				}

				total++
			}
			if err := app.Commit(); err != nil {
				return total, err
			}
		}
	}

	return total, nil
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(fmt.Errorf("bench: could not start CPU profile: %v", err))
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *writeBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func mapToLabels(m map[string]interface{}, l *labels.Labels) {
	for k, v := range m {
		*l = append(*l, labels.Label{Name: k, Value: v.(string)})
	}
}

func readPrometheusLabels(r io.Reader, n int) ([]labels.Labels, error) {
	scanner := bufio.NewScanner(r)

	var mets []labels.Labels
	hashes := map[uint64]struct{}{}
	i := 0

	for scanner.Scan() && i < n {
		m := make(labels.Labels, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			m = append(m, labels.Label{Name: split[0], Value: split[1]})
		}
		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(m)
		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}
		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	return mets, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printBlocks(blocks []*tsdb.Block, humanReadable *bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable *bool) string {
	if *humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}
