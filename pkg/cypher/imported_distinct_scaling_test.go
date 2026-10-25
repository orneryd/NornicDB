package cypher

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const importedDistinctPrefix = "WITH [x IN range(1,$n) | {id:x, pad:$pad}] AS big "

func TestImportedDistinctScaling(t *testing.T) {
	queries := []struct{ name, query string }{
		{"control", "UNWIND range(0,$n*2-1) AS i WITH DISTINCT i%100 AS k, i%7 AS h RETURN count(*) AS rows"},
		{"outer", importedDistinctPrefix + "UNWIND range(0,$n*2-1) AS i WITH DISTINCT i%100 AS k, i%7 AS h RETURN count(*) AS rows"},
		{"imported", importedDistinctPrefix + "CALL (big) { UNWIND range(0,size(big)*2-1) AS i WITH DISTINCT i%100 AS k, i%7 AS h RETURN count(*) AS rows } RETURN rows"},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			var elapsed [2]time.Duration
			for index, size := range []int{500, 2000} {
				for repeat := 0; repeat < 2; repeat++ {
					store := storage.NewMemoryEngine()
					executor := NewStorageExecutor(store)
					start := time.Now()
					result, err := executor.Execute(context.Background(), query.query, map[string]interface{}{"n": int64(size), "pad": strings.Repeat("x", 200)})
					duration := time.Since(start)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(700)}}, result.Rows)
					require.NoError(t, store.Close())
					if elapsed[index] == 0 || duration < elapsed[index] {
						elapsed[index] = duration
					}
				}
				t.Logf("n=%d min=%s", size, elapsed[index])
			}
			growth := float64(elapsed[1]) / float64(elapsed[0])
			t.Logf("growth for 4x rows: %.2fx", growth)
			require.Less(t, growth, 9.0, "four times the input must grow near-linearly")
		})
	}
}

func TestImportedDistinctListArgument(t *testing.T) {
	for _, query := range []string{
		"UNWIND range(0,999) AS i WITH i%100 AS k, i%7 AS h RETURN count(DISTINCT [k,h]) AS rows",
		importedDistinctPrefix + "CALL (big) { UNWIND range(0,size(big)*2-1) AS i WITH i%100 AS k, i%7 AS h RETURN count(DISTINCT [k,h]) AS rows } RETURN rows",
	} {
		t.Run(query, func(t *testing.T) {
			store := storage.NewMemoryEngine()
			defer store.Close()
			result, err := NewStorageExecutor(store).Execute(context.Background(), query, map[string]interface{}{"n": int64(500), "pad": strings.Repeat("x", 200)})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(700)}}, result.Rows)
		})
	}
}

func BenchmarkImportedDistinctScaling(b *testing.B) {
	query := importedDistinctPrefix + "CALL (big) { UNWIND range(0,size(big)*2-1) AS i WITH DISTINCT i%100 AS k, i%7 AS h RETURN count(*) AS rows } RETURN rows"
	for _, size := range []int{500, 2000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			store := storage.NewMemoryEngine()
			defer store.Close()
			// Reuse parsing work without letting the warm-up cache the result.
			executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			params := map[string]interface{}{"n": int64(size), "pad": strings.Repeat("x", 200)}
			_, err := executor.Execute(context.Background(), query, params)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := executor.Execute(context.Background(), query, params)
				require.NoError(b, err)
				require.Equal(b, [][]interface{}{{int64(700)}}, result.Rows)
			}
		})
	}
}
