package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestPipelineCancellationWALAbortAndCleanup(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			dir := t.TempDir()
			wal, err := storage.NewWAL(dir, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			engine := storage.NewNamespacedEngine(storage.NewWALEngine(base, wal), "pipeline_wal_deadline")
			executor := NewStorageExecutor(engine)
			readers := base.ActiveReaders()
			if explicit {
				_, err = executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			_, err = executor.Execute(ctx, "CREATE (:DeadlineWALWrite) WITH 1 AS seed UNWIND range(1,1000000000) AS x RETURN sum(x+seed)", nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			if explicit {
				_, err = executor.handleRollback()
				require.NoError(t, err)
			}
			require.Less(t, time.Since(deadline), 250*time.Millisecond, "abort marker and rollback must complete before returning")
			require.Equal(t, readers, base.ActiveReaders())
			result, err := NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n:DeadlineWALWrite) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			require.NoError(t, wal.Sync())
			entries, err := storage.ReadWALEntries(filepath.Join(dir, "wal.log"))
			require.NoError(t, err)
			begins, aborts, commits := 0, 0, 0
			for _, entry := range entries {
				switch entry.Operation {
				case storage.OpTxBegin:
					begins++
				case storage.OpTxAbort:
					aborts++
				case storage.OpTxCommit:
					commits++
				}
			}
			require.Positive(t, begins)
			require.Equal(t, begins, aborts)
			require.Zero(t, commits)
		})
	}
}
