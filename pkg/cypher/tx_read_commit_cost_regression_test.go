package cypher

// Regression guard for the reported #1548 read-in-transaction cost: BEGIN, an
// empty-label read and COMMIT used to allocate a cache-capacity map set per
// commit (~2.4 MiB) because the read-only commit's cache scope was unknown.

import (
	"context"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// txReadCommitEngine is the reported harness: the test memory engine exposing a
// real WAL, with transaction commits routed through CommitContext.
type txReadCommitEngine struct {
	*storage.MemoryEngine
	wal *storage.WAL
}

func (e *txReadCommitEngine) GetWAL() *storage.WAL { return e.wal }

func (e *txReadCommitEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	return tx.CommitContext(ctx)
}

// TestExplicitTransactionReadCommitDoesNotRebuildStorageCaches runs the public
// statement sequence and bounds its allocation. The pre-fix cost was ~2.55 MB
// per commit; the fixed cost is a small fixed amount per commit, well under the
// bound, and the bound sits far below one rebuilt cache-capacity map set.
func TestExplicitTransactionReadCommitDoesNotRebuildStorageCaches(t *testing.T) {
	engine := newTestMemoryEngine(t)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(&txReadCommitEngine{MemoryEngine: engine, wal: wal}, "nor180_cost"))
	ctx := context.Background()
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })

	statements := []string{"BEGIN", "MATCH (n:NOR180Cost) RETURN count(n) AS c", "COMMIT"}
	run := func() {
		for _, statement := range statements {
			result, err := exec.Execute(ctx, statement, nil)
			require.NoErrorf(t, err, "%s", statement)
			require.NotNilf(t, result, "%s", statement)
			if statement == statements[1] {
				require.Len(t, result.Rows, 1)
				require.Equal(t, int64(0), result.Rows[0][0], "fresh label must read an empty count")
			}
		}
	}

	// Warm up so one-time setup is outside the measurement.
	for i := 0; i < 3; i++ {
		run()
	}

	const iterations = 40
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < iterations; i++ {
		run()
	}
	runtime.ReadMemStats(&after)

	perCommit := (after.TotalAlloc - before.TotalAlloc) / iterations
	t.Logf("allocated %d bytes per BEGIN/read/COMMIT sequence", perCommit)
	require.Less(t, perCommit, uint64(256<<10),
		"an explicit read commit allocates %d bytes per sequence, which is a per-commit cache rebuild", perCommit)
}
