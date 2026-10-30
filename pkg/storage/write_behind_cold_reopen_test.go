package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestColdBufferedWriteAfterReopen: the first buffered commit after a reopen
// loads its namespace's MVCC state cold, which persists the recovered
// sequence through a physical commit of its own. It must not do that while
// it excludes physical commits (the commit gate): the commit finishes.
func TestColdBufferedWriteAfterReopen(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	open := func() *BadgerEngine {
		b, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	b := open()
	_, err := b.CreateNode(&Node{ID: "cold:a", Properties: map[string]any{"value": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, b.Close())
	b = open()
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	b.SetBufferedCommitWAL(wal)
	done := make(chan error, 1)
	go func() {
		tx, err := b.BeginTransaction()
		if err == nil {
			err = tx.SetNamespace("cold")
		}
		if err == nil {
			err = tx.SetImplicit(true)
		}
		if err == nil {
			err = tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "cold:a", Properties: map[string]any{"value": int64(2)}}, Policy: ClientNodeEdits})
		}
		if err == nil {
			err = tx.Commit()
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the first buffered commit after a reopen did not finish: it waits on the commit gate it holds")
	}
}
