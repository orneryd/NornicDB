package cypher_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Varying a literal keeps this a compilation workload as well as execution.
// Every size is positive and the CALL always has one input row.
func BenchmarkTransactionalCallBatchCompile(b *testing.B) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, base.Close()) })
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := "CALL { RETURN 1 AS value } IN TRANSACTIONS OF " + strconv.Itoa(i+1) + " ROWS RETURN value"
		if _, err := exec.Execute(ctx, query, map[string]interface{}{"nonce": i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransactionalCallChainPlanning(b *testing.B) {
	for _, sample := range []struct{ name, boundary string }{
		{"contiguous", ""},
		{"match", "MATCH (n:BatchChainBenchmark) "},
	} {
		b.Run(sample.name, func(b *testing.B) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, base.Close()) })
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
			ctx := context.Background()
			query := "CALL { CREATE (:BatchChainBenchmark {v:99}) } IN TRANSACTIONS OF 1 ROWS " + sample.boundary + "CALL { CREATE (:BatchChainBenchmark {v:1}) } IN TRANSACTIONS OF 1 ROWS"
			reset := "MATCH (n:BatchChainBenchmark) DETACH DELETE n"
			_, err = exec.Execute(ctx, query, nil)
			require.NoError(b, err)
			result, err := exec.Execute(ctx, "MATCH (n:BatchChainBenchmark) RETURN count(n)", nil)
			require.NoError(b, err)
			require.Equal(b, [][]interface{}{{int64(2)}}, result.Rows)
			_, err = exec.Execute(ctx, reset, nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				params := map[string]interface{}{"nonce": index}
				if _, err := exec.Execute(ctx, query, params); err != nil {
					b.Fatal(err)
				}
				if _, err := exec.Execute(ctx, reset, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
