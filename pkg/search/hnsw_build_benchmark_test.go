package search

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// cpuOnlyHNSWBuild runs the production construction entry point on the CPU
// kernel, whatever NORNICDB_HNSW_BUILD_GPU_ENABLED says.
func cpuOnlyHNSWBuild(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator) (*HNSWIndex, hnswBuildStats, error) {
	config.UseGPUBuild = false
	return buildHNSWWithOptionalGPU(ctx, dimensions, config, lookup, total, iter, nil)
}

func hnswBuildFixture(n int, identical bool) (*VectorIndex, []hnswBuildPair) {
	vi := NewVectorIndex(2048)
	rng := rand.New(rand.NewSource(86))
	for i := 0; i < n; i++ {
		v := make([]float32, 2048)
		for j := range v {
			if identical {
				v[j] = 1
			} else {
				v[j] = rng.Float32()*2 - 1
			}
		}
		_ = vi.Add(fmt.Sprintf("v%05d", i), v)
	}
	pairs := hnswOrderedVectorIndexPairs(vi, nil)
	// Stable insertion order makes comparisons independent of map iteration.
	for i := range pairs {
		pairs[i].id = fmt.Sprintf("v%05d", i)
		pairs[i].vec, _ = vi.getVectorRef(pairs[i].id)
	}
	return vi, pairs
}

func BenchmarkHNSWBuild2048(b *testing.B) {
	for _, n := range []int{2000, 5300} {
		for _, identical := range []bool{true, false} {
			for _, lexical := range []bool{false, true} {
				b.Run(fmt.Sprintf("n%d/identical%t/lexical%t", n, identical, lexical), func(b *testing.B) {
					vi, pairs := hnswBuildFixture(n, identical)
					for i := range pairs {
						if identical {
							v := make([]float32, 2048)
							v[0] = 1
							_ = vi.Add(pairs[i].id, v)
							pairs[i].vec, _ = vi.getVectorRef(pairs[i].id)
						}
						if lexical && i < 256 {
							pairs[i].hasHint = true
							pairs[i].hint = LexicalSeedHint{ID: pairs[i].id, Rank: uint32(i), Signature: uint64(i) * 0x9e3779b185ebca87}
						}
					}
					config := HNSWConfigFromEnv()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := cpuOnlyHNSWBuild(b.Context(), 2048, config, vi.getVectorRef, n, hnswPairSliceIterator(pairs))
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
func BenchmarkHNSWSearch2048(b *testing.B) {
	vi, pairs := hnswBuildFixture(5300, false)
	idx, _, err := cpuOnlyHNSWBuild(b.Context(), 2048, DefaultHNSWConfig(), vi.getVectorRef, len(pairs), hnswPairSliceIterator(pairs))
	if err != nil {
		b.Fatal(err)
	}
	query := vector.Normalize(pairs[42].vec)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Search(b.Context(), query, 20, -1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Opt-in diagnostic: executes only on the remote test host. Benchmarks use the
// benchmark queue; this records CPU/heap attribution for one construction.
func TestHNSWBuildCPUProfile(t *testing.T) {
	if os.Getenv("NORNICDB_HNSW_PROFILE") == "" {
		t.Skip("opt-in CPU profile")
	}
	runtime.GOMAXPROCS(2)
	vi, pairs := hnswBuildFixture(2000, true)
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(f))
	_, stats, err := cpuOnlyHNSWBuild(context.Background(), 2048, DefaultHNSWConfig(), vi.getVectorRef, len(pairs), hnswPairSliceIterator(pairs))
	pprof.StopCPUProfile()
	require.NoError(t, f.Close())
	require.NoError(t, err)
	t.Logf("build duration: %s", stats.Duration)
	out, err := exec.Command("go", "tool", "pprof", "-top", "-nodecount=20", os.Args[0], path).CombinedOutput()
	require.NoError(t, err, string(out))
	t.Log(string(out))
}
