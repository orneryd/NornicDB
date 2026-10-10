package storage

// Temporary reproduction for the write-behind flush hang observed with very
// large generations. Delete once the trigger is understood.

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteBehind_LargeGenerationFlushSpansBadgerBatches(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	const n = 25000
	for i := 0; i < n; i++ {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		_, err = tx.CreateNode(&Node{
			ID:         NodeID("test:zz" + strconv.Itoa(i)),
			Labels:     []string{"ZZ"},
			Properties: nil,
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())
}

// Sync-path control: one explicit transaction creating the same 30K nodes,
// write-behind disabled entirely.
func TestWriteBehind_SyncLargeCommitControl(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const n = 25000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for i := 0; i < n; i++ {
		_, err = tx.CreateNode(&Node{
			ID:         NodeID("test:zz" + strconv.Itoa(i)),
			Labels:     []string{"ZZ"},
			Properties: nil,
		})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}
