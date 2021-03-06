package tsdbutil

import (
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
)

type Sample interface {
	T() int64
	V() float64
}

func ChunkFromSamples(s []Sample) chunks.Meta {
	mint, maxt := int64(0), int64(0)

	if len(s) > 0 {
		mint, maxt = s[0].T(), s[len(s)-1].T()
	}

	c := chunkenc.NewXORChunk()
	ca, _ := c.Appender()

	for _, s := range s {
		ca.Append(s.T(), s.V())
	}
	return chunks.Meta{
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   c,
	}
}

// PopulatedChunk creates a chunk populated with samples every second starting at minTime
func PopulatedChunk(numSamples int, minTime int64) chunks.Meta {
	samples := make([]Sample, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = sample{minTime + int64(i*1000), 1.0}
	}
	return ChunkFromSamples(samples)
}
