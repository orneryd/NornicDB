package cypher

import (
	pageFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkCDCQueryPage reads one db.cdc.query page of 500 and of 5,000
// changes, auto-commit and inside an open explicit transaction.
func BenchmarkCDCQueryPage(b *testing.B) {
	for _, events := range []int{500, 5000} {
		base, err := pageFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
		require.NoError(b, err)
		b.Cleanup(func() { _ = base.Close() })
		store := storage.NewNamespacedEngine(base, "test")
		capture, ok := storage.ChangeCaptureOf(store)
		require.True(b, ok)
		capture.SetChangeCapture("test", storage.ChangeCaptureFull)
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		current, err := exec.Execute(ctx, "CALL db.cdc.current() YIELD id RETURN id", nil)
		require.NoError(b, err)
		start := current.Rows[0][0].(string)
		_, err = exec.Execute(ctx, "UNWIND range(1, $n) AS i CREATE (:Item {i: i})", map[string]interface{}{"n": events})
		require.NoError(b, err)
		params := map[string]interface{}{"from": start}
		const query = "CALL db.cdc.query($from) YIELD id RETURN count(*)"

		for _, explicit := range []bool{false, true} {
			mode := "autocommit"
			if explicit {
				mode = "transaction"
			}
			b.Run(fmt.Sprintf("events=%d/%s", events, mode), func(b *testing.B) {
				reader := NewStorageExecutor(store)
				if explicit {
					_, err := reader.Execute(ctx, "BEGIN", nil)
					require.NoError(b, err)
					defer func() { _, _ = reader.Execute(ctx, "ROLLBACK", nil) }()
				}
				result, err := reader.Execute(ctx, query, params)
				require.NoError(b, err)
				require.Equal(b, int64(events), result.Rows[0][0])
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := reader.Execute(ctx, query, params)
					if err != nil {
						b.Fatal(err)
					}
					gdsBenchmarkResult = result
				}
			})
		}
	}
}
