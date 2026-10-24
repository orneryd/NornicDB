package storage

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR2RebuildWriteRebuildSameWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := reviewBindingEngine(t, t.TempDir())
	// Persist a schema with no remaining live rows. The supported complete WAL
	// records must carry these already-persisted names independently of that store.
	for _, item := range []struct{ id, name string }{{"rebuild:warm-beta", "beta"}, {"rebuild:warm-alpha", "alpha"}} {
		_, err := source.CreateNode(&Node{ID: NodeID(item.id), Properties: map[string]any{item.name: int64(0)}})
		require.NoError(t, err)
		require.NoError(t, source.DeleteNode(NodeID(item.id)))
	}
	walDir := t.TempDir()
	w, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	journal := NewWALEngine(source, w)
	t.Cleanup(func() { _ = journal.Close() })
	for _, item := range []struct{ id, name string }{{"rebuild:a", "alpha"}, {"rebuild:b", "beta"}} {
		tx, e := source.BeginTransaction()
		require.NoError(t, e)
		require.NoError(t, tx.SetNamespace("rebuild"))
		require.NoError(t, tx.SetImplicit(true))
		_, e = tx.CreateNode(&Node{ID: NodeID(item.id), Properties: map[string]any{item.name: int64(1)}})
		require.NoError(t, e)
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())
		require.NoError(t, source.FlushWriteBehind())
	}
	require.NoError(t, w.Close())
	rebuilt := reviewBindingEngine(t, t.TempDir())
	first, _, err := RecoverIntoEngine(rebuilt, walDir, "")
	require.NoError(t, err)
	t.Logf("first rebuild: %s", first.Summary())
	require.Zero(t, first.Failed)
	require.Equal(t, 2, first.Applied)
	for _, item := range []struct{ id, name string }{{"rebuild:a", "alpha"}, {"rebuild:b", "beta"}} {
		n, e := rebuilt.GetNode(NodeID(item.id))
		require.NoError(t, e)
		require.Equal(t, map[string]any{item.name: int64(1)}, n.Properties)
	}
	assertPropertyDictionaryBijection(t, rebuilt)
	sourceAlpha, _ := source.propKeyDict.lookupID("rebuild", "alpha")
	rebuiltAlpha, _ := rebuilt.propKeyDict.lookupID("rebuild", "alpha")
	t.Logf("source alpha=%d rebuilt alpha=%d", sourceAlpha, rebuiltAlpha)
	require.NotEqual(t, sourceAlpha, rebuiltAlpha, "fixture forces a legitimate allocation-order change")
	// A normal synchronous no-property write initializes lazy namespace MVCC.
	// This isolates binding ownership from the separately proven develop gate
	// deadlock; it allocates no property names and leaves no additional live row.
	_, err = rebuilt.CreateNode(&Node{ID: "rebuild:mvcc-warm"})
	require.NoError(t, err)
	require.NoError(t, rebuilt.DeleteNode("rebuild:mvcc-warm"))
	continuation, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	continuedJournal := NewWALEngine(rebuilt, continuation)
	t.Cleanup(func() { _ = continuedJournal.Close() })
	tx, err := rebuilt.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("rebuild"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateNode(&Node{ID: "rebuild:a", Properties: map[string]any{"alpha": int64(2)}}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, rebuilt.FlushWriteBehind())
	require.NoError(t, continuation.Close())
	entries, err := ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for _, entry := range entries {
		record, e := decodeBufferedWALRecord(entry)
		require.NoError(t, e)
		require.NotEmpty(t, record.PropertyBindings)
		require.Empty(t, record.PropKeys)
		t.Logf("seq=%d tx=%s complete bindings=%+v", entry.Sequence, record.TxID, legacyBindingEntries(t, record.PropertyBindings))
	}
	secondStore := reviewBindingEngine(t, t.TempDir())
	second, _, err := RecoverIntoEngine(secondStore, walDir, "")
	require.NoError(t, err)
	t.Logf("second rebuild: %s errors=%+v", second.Summary(), second.Errors)
	require.Zero(t, second.Failed, "each new-format record owns a complete source binding table")
	require.Equal(t, 3, second.Applied)
	node, err := secondStore.GetNode("rebuild:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"alpha": int64(2)}, node.Properties)
	assertPropertyDictionaryBijection(t, secondStore)
}
