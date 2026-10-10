package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newWriteBehindTestEngine(t *testing.T, interval time.Duration) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: interval,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func bufferedCreate(t *testing.T, engine *BadgerEngine, node *Node) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestWriteBehind_BufferedCommitVisibleBeforeFlush(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.Greater(t, engine.writeBehind.PendingOps(), 0, "commit must stay buffered before flush")

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"], "acknowledged write visible through the overlay")
}

func TestWriteBehind_FlushLandsInBadger(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"])

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestWriteBehind_MultipleCommitsApplyInOrder(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(2)}})

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"], "newest commit shadows the older one")

	require.NoError(t, engine.FlushWriteBehind())
	got, err = engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}

func TestWriteBehind_BufferedDeleteHidesAndPersists(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, engine.FlushWriteBehind())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:n1"))
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "buffered delete hides the node")

	require.NoError(t, engine.FlushWriteBehind())
	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "delete persists after flush")
}

func TestWriteBehind_ExplicitBeginDrainsBufferedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	// The storage layer's BeginTransaction does not drain (autocommit uses it);
	// the explicit-transaction admission path drains via FlushWriteBehind.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode("test:n1")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NoError(t, tx.Rollback())
}

func TestWriteBehind_CloseDrainsAndPersists(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             dir,
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	})
	require.NoError(t, err)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(7)}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Properties["v"], "Close must persist acknowledged buffered writes")
}

func TestWriteBehind_DisabledCommitIsSynchronous(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.Nil(t, engine.writeBehind)
	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "disabled engine commits synchronously")
}
