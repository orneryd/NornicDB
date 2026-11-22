package embed

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestNOR381CachedVectorsBelongToEachResult exercises sequential caller reuse;
// it introduces no concurrent mutation of a shared vector.
func TestNOR381CachedVectorsBelongToEachResult(t *testing.T) {
	for _, route := range []string{"single", "batch", "typed_single", "typed_batch", "typed_fallback_single", "typed_fallback_batch"} {
		for _, mutateHit := range []bool{false, true} {
			phase := "miss"
			if mutateHit {
				phase = "hit"
			}
			t.Run(route+"/"+phase, func(t *testing.T) {
				var base Embedder = &mockEmbedder{}
				typed := route == "typed_single" || route == "typed_batch"
				if typed {
					base = &typedMockEmbedder{}
				}
				cached := NewCachedEmbedder(base, 4)
				t.Cleanup(func() { require.NoError(t, cached.Close()) })
				call := func() []float32 {
					var v []float32
					var err error
					switch route {
					case "single":
						v, err = cached.Embed(context.Background(), "same text")
					case "batch":
						var vs [][]float32
						vs, err = cached.EmbedBatch(context.Background(), []string{"same text"})
						if err == nil {
							require.Len(t, vs, 1)
							v = vs[0]
						}
					case "typed_single", "typed_fallback_single":
						v, err = cached.EmbedWithInputType(context.Background(), "same text", InputTypeQuery)
					default:
						var vs [][]float32
						vs, err = cached.EmbedBatchWithInputType(context.Background(), []string{"same text"}, InputTypeQuery)
						if err == nil {
							require.Len(t, vs, 1)
							v = vs[0]
						}
					}
					require.NoError(t, err)
					require.Len(t, v, 3)
					return v
				}
				first := call()
				expected := append([]float32(nil), first...)
				exposed := first
				if mutateHit {
					exposed = call()
					require.Equal(t, expected, exposed)
				}
				exposed[0] = 12345
				next := call()
				assert.Equal(t, expected, next, "caller mutation leaked into retained cache")
				stats := cached.Stats()
				require.Equal(t, uint64(1), stats.Misses)
				require.GreaterOrEqual(t, stats.Hits, uint64(1))
			})
		}
	}
}

func TestNOR381DuplicateCachedBatchResultsAreIndependent(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "plain"
		if typed {
			name = "typed"
		}
		t.Run(name, func(t *testing.T) {
			cached := NewCachedEmbedder(&typedMockEmbedder{}, 4)
			t.Cleanup(func() { require.NoError(t, cached.Close()) })
			call := func(texts []string) [][]float32 {
				var vs [][]float32
				var err error
				if typed {
					vs, err = cached.EmbedBatchWithInputType(context.Background(), texts, InputTypeDocument)
				} else {
					vs, err = cached.EmbedBatch(context.Background(), texts)
				}
				require.NoError(t, err)
				return vs
			}
			expected := append([]float32(nil), call([]string{"same text"})[0]...)
			pair := call([]string{"same text", "same text"})
			require.Len(t, pair, 2)
			require.Equal(t, expected, pair[0])
			require.Equal(t, expected, pair[1])
			pair[0][0] = 12345
			assert.Equal(t, expected, pair[1], "one duplicate result changed its sibling")
			assert.Equal(t, expected, call([]string{"same text"})[0], "duplicate result changed retained cache")
		})
	}
}
