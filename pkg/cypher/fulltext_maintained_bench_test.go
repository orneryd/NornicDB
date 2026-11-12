package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The original NOR-148 application data is unavailable. This generated,
// in-memory workload preserves the reported public top-k/count/eligibility
// query shapes and long correspondence bodies. It is not application-suite
// qualification. Every invocation reaches execution with result caching off.
func BenchmarkNOR148MaintainedPublic(b *testing.B) {
	for _, size := range []int{400, 4000, 16000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			exec, _ := fulltextMaintainedFixture(b, size)
			benchmarkNOR148Queries(b, exec, size/40, size/80)
		})
	}
}

func benchmarkNOR148Queries(b *testing.B, exec *StorageExecutor, matches, eligible int) {
	params := map[string]any{"query": "hotel torrey pines services", "limit": int64(10)}
	for _, tc := range []struct{ name, query string }{
		{"top10", "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit"},
		{"count", "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN count(node) AS n"},
		{"eligible", "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score WHERE coalesce(node.current, true) RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			first, err := exec.Execute(b.Context(), tc.query, params)
			require.NoError(b, err)
			require.NotEmpty(b, first.Rows)
			if tc.name == "count" {
				require.Equal(b, int64(matches), first.Rows[0][0])
			} else {
				expected := min(10, matches)
				if tc.name == "eligible" {
					expected = min(10, eligible)
				}
				require.Len(b, first.Rows, expected)
			}
			// Warm the corpus and term statistics before measuring steady repeated
			// reads. Separate read-state benchmarks include acquisition work.
			second, err := exec.Execute(b.Context(), tc.query, params)
			require.NoError(b, err)
			require.Equal(b, first.Rows, second.Rows)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(b.Context(), tc.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Hold the indexed corpus and result size fixed while unrelated storage grows.
// This separates the removed whole-database reconstruction from the necessary
// cost of scoring more indexed documents or returning more matching rows.
func BenchmarkNOR148UnrelatedCorpusGrowth(b *testing.B) {
	for _, total := range []int{400, 4000, 16000} {
		b.Run(fmt.Sprintf("nodes=%d", total), func(b *testing.B) {
			exec, store := fulltextMaintainedFixture(b, 400)
			extra := make([]*storage.Node, total-400)
			text := strings.Repeat("routine correspondence meeting attachment ", 48)
			for i := range extra {
				extra[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("unrelated%d", i)), Labels: []string{"Unrelated"}, Properties: map[string]any{"text": text}}
			}
			require.NoError(b, store.BulkCreateNodes(b.Context(), extra))
			benchmarkNOR148Queries(b, exec, 10, 5)
		})
	}
}

// Measure the first and second corpus reads separately with a warm parser.
// A steady-state gain must not hide an expensive first reuse. Fixture/service
// construction and the first read of a reused corpus are outside timing.
func BenchmarkNOR148CorpusReadState(b *testing.B) {
	for _, size := range []int{400, 4000} {
		for _, state := range []struct {
			name       string
			priorReads int
		}{{"cold", 0}, {"first-reuse", 1}} {
			b.Run(fmt.Sprintf("nodes=%d/%s", size, state.name), func(b *testing.B) {
				exec, store := fulltextMaintainedFixture(b, size)
				benchmarkFulltextCorpusReadState(b, exec, store, state.priorReads, int64(size/40))
			})
		}
	}
}

// benchmarkFulltextCorpusReadState owns service reset, the untimed prior read
// and the same timed public query for each body-diversity control.
func benchmarkFulltextCorpusReadState(b *testing.B, exec *StorageExecutor, store *fulltextCountingStore, priorReads int, expected int64) {
	query := "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN count(node) AS n"
	_, err := exec.Execute(b.Context(), query, nil)
	require.NoError(b, err)
	if service := exec.search(); service != nil {
		require.NoError(b, service.Close())
	}
	labels := pprof.Labels("phase", "fulltext-corpus-read", "workload", b.Name())
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		service := search.NewService(store, search.DefaultVectorDimensions, nil)
		exec.SetSearchService(service)
		for j := 0; j < priorReads; j++ {
			_, err := exec.Execute(b.Context(), query, nil)
			require.NoError(b, err)
		}
		var got *ExecuteResult
		pprof.Do(b.Context(), labels, func(ctx context.Context) {
			b.StartTimer()
			got, err = exec.Execute(ctx, query, nil)
			b.StopTimer()
		})
		require.NoError(b, err)
		require.Equal(b, expected, got.Rows[0][0])
		require.NoError(b, service.Close())
	}
}

func BenchmarkFulltextDistinctBodyCorpusReadState(b *testing.B) {
	for _, state := range []struct {
		name       string
		priorReads int
	}{{"cold", 0}, {"first-reuse", 1}} {
		b.Run(state.name, func(b *testing.B) {
			exec, store := fulltextMaintainedFixture(b, 4000, fulltextDistinctBodies)
			benchmarkFulltextCorpusReadState(b, exec, store, state.priorReads, 100)
		})
	}
}

// Distinct term sets exercise corpus reuse without a repeated statistics-cache
// hit. All four sets match the same generated documents; result caching is off.
func BenchmarkNOR148DistinctTerms(b *testing.B) {
	for _, size := range []int{400, 4000, 16000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			exec, _ := fulltextMaintainedFixture(b, size)
			query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN count(node) AS n"
			terms := []string{"hotel torrey", "pines services", "hotel services", "torrey pines"}
			params := map[string]any{}
			for _, termSet := range terms {
				params["query"] = termSet
				got, err := exec.Execute(b.Context(), query, params)
				require.NoError(b, err)
				require.Equal(b, int64(size/40), got.Rows[0][0])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params["query"] = terms[i%len(terms)]
				if _, err := exec.Execute(b.Context(), query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
