package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestC71OrderedWithWindowPreservesRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "c71")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		for _, statement := range []string{
			"CREATE RANGE INDEX c71_rank FOR (n:C71Indexed) ON (n.rank)",
			"UNWIND range(1, 100) AS i CREATE (:C71Base:C71Other {rank: i})",
			"UNWIND range(1, 20) AS i CREATE (:C71Base:C71Indexed {rank: 21-i, id: i})",
			"CALL db.awaitIndexes()",
		} {
			_, err := exec.Execute(t.Context(), statement, nil)
			require.NoError(t, err, statement)
		}
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
			indexed     bool
		}{
			{"ascending", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n ORDER BY n.rank LIMIT 3 RETURN n.rank", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, true},
			{"descending", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n ORDER BY n.rank DESC LIMIT 3 RETURN n.rank", [][]interface{}{{int64(20)}, {int64(19)}, {int64(18)}}, true},
			{"alias", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n.rank AS r ORDER BY r LIMIT 3 RETURN r", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, false},
			{"renamed node", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n AS m ORDER BY m.rank LIMIT 3 RETURN m.rank", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, false},
			{"skip", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n ORDER BY n.rank SKIP 2 LIMIT 3 RETURN n.rank", [][]interface{}{{int64(3)}, {int64(4)}, {int64(5)}}, false},
			{"multiple projections", "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n, n.rank AS r ORDER BY n.rank LIMIT 3 RETURN r", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, false},
			{"descending without bound", "MATCH (n:C71Base:C71Indexed) WITH n ORDER BY n.rank DESC LIMIT 3 RETURN n.rank", [][]interface{}{{int64(20)}, {int64(19)}, {int64(18)}}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.want, result.Rows, tc.query)
				require.Equal(t, tc.indexed, exec.LastHotPathTrace().OuterIndexTopK, tc.query)
			})
		}
		t.Run("seeded rows retain global window", func(t *testing.T) {
			result, err := exec.Execute(t.Context(), "UNWIND [1, 2] AS batch MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n ORDER BY n.rank LIMIT 3 RETURN n.rank", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}, {int64(2)}}, result.Rows)
		})
		t.Run("partial index preserves null rows", func(t *testing.T) {
			_, err := exec.Execute(t.Context(), "CREATE (:C71Base:C71Indexed {id: 999})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), "MATCH (n:C71Base:C71Indexed) WITH n ORDER BY n.rank LIMIT 25 RETURN n.rank", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 21)
			for i := 0; i < 20; i++ {
				require.Equal(t, int64(i+1), result.Rows[i][0])
			}
			require.Nil(t, result.Rows[20][0])
			require.False(t, exec.LastHotPathTrace().OuterIndexTopK)
		})
		t.Run("tied key keeps secondary order", func(t *testing.T) {
			_, err := exec.Execute(t.Context(), "CREATE (:C71Base:C71Indexed {rank: 1, id: 0})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), "MATCH (n:C71Base:C71Indexed) WHERE n.rank > 0 WITH n ORDER BY n.rank, n.id LIMIT 3 RETURN n.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}, {int64(20)}, {int64(19)}}, result.Rows)
			require.True(t, exec.LastHotPathTrace().OuterIndexTopK)
		})
		t.Run("transaction sees pending node", func(t *testing.T) {
			_, err := exec.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(t.Context(), "ROLLBACK", nil) })
			_, err = exec.Execute(t.Context(), "CREATE (:C71Base:C71Indexed {rank: 0, id: 0})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(t.Context(), "MATCH (n:C71Base:C71Indexed) WHERE n.rank >= 0 WITH n ORDER BY n.rank LIMIT 3 RETURN n.rank", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}, {int64(1)}, {int64(1)}}, result.Rows)
		})
	})
}

func BenchmarkC71OrderedWithPage(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "c71_bench")
	setup := NewStorageExecutor(store)
	for _, statement := range []string{
		"CREATE RANGE INDEX c71_bench_rank FOR (n:C71BenchIndexed) ON (n.rank)",
		"UNWIND range(1, 10000) AS i CREATE (:C71BenchBase:C71BenchIndexed {rank: 10001-i})",
	} {
		if _, err := setup.Execute(b.Context(), statement, nil); err != nil {
			b.Fatal(err)
		}
	}
	query := "MATCH (n:C71BenchBase:C71BenchIndexed) WHERE n.rank > 0 WITH n ORDER BY n.rank LIMIT 100 RETURN n.rank"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		result, err := exec.Execute(b.Context(), query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 || result.Rows[0][0] != int64(1) || result.Rows[99][0] != int64(100) {
			b.Fatalf("unexpected ordered page: %v", result.Rows)
		}
	}
}
