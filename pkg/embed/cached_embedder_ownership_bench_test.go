package embed

import (
	"context"
	"fmt"
	"testing"
)

// ownershipBenchEmbedder supplies deterministic vectors without network cost,
// isolating cache ownership overhead at production-sized dimensions.
type ownershipBenchEmbedder struct {
	mockEmbedder
	dimensions int
}

func (m *ownershipBenchEmbedder) Dimensions() int { return m.dimensions }

func (m *ownershipBenchEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	v := make([]float32, m.dimensions)
	v[0], v[len(v)-1] = float32(len(text)), 1
	return v, nil
}

func (m *ownershipBenchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vs := make([][]float32, len(texts))
	for i, text := range texts {
		vs[i], _ = m.Embed(ctx, text)
	}
	return vs, nil
}

func BenchmarkCachedEmbeddingOwnership(b *testing.B) {
	for _, dimensions := range []int{1024, 4096} {
		for _, mode := range []string{"single_hit", "batch16_hit", "batch16_duplicate", "batch16_half_miss"} {
			b.Run(fmt.Sprintf("dims%d/%s", dimensions, mode), func(b *testing.B) {
				cached := NewCachedEmbedder(&ownershipBenchEmbedder{dimensions: dimensions}, 256)
				b.Cleanup(func() {
					if err := cached.Close(); err != nil {
						b.Fatal(err)
					}
				})
				ctx := context.Background()
				texts := make([]string, 16)
				for i := range texts {
					texts[i] = fmt.Sprintf("repeated query %d", i)
					if mode == "batch16_duplicate" {
						texts[i] = "repeated query"
					}
				}
				if _, err := cached.EmbedBatch(ctx, texts); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode == "single_hit" {
						v, err := cached.Embed(ctx, texts[0])
						if err != nil || len(v) != dimensions || v[0] != float32(len(texts[0])) || v[dimensions-1] != 1 {
							b.Fatalf("invalid single result: %v", err)
						}
						continue
					}
					if mode == "batch16_half_miss" {
						for j := 8; j < len(texts); j++ {
							texts[j] = fmt.Sprintf("new query %d %d", i, j)
						}
					}
					vs, err := cached.EmbedBatch(ctx, texts)
					if err != nil || len(vs) != len(texts) {
						b.Fatalf("invalid batch: %v", err)
					}
					for j, v := range vs {
						if len(v) != dimensions || v[0] != float32(len(texts[j])) || v[dimensions-1] != 1 {
							b.Fatal("invalid batch vector")
						}
					}
				}
			})
		}
	}
}
