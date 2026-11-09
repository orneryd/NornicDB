package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR2RebuildWriteRebuildSameWAL(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := reviewBindingEngine(t, t.TempDir(), nil)
	// Persist a schema with no remaining live rows. The supported complete WAL
	// records must carry these already-persisted names independently of that store.
	for _, item := range []struct{ id, name string }{{"rebuild:warm-beta", "beta"}, {"rebuild:warm-alpha", "alpha"}} {
		_, err := source.CreateNode(t.Context(), &Node{ID: NodeID(item.id), Properties: map[string]any{item.name: int64(0)}})
		require.NoError(t, err)
		require.NoError(t, source.DeleteNode(t.Context(), NodeID(item.id)))
	}
	walDir := t.TempDir()
	w, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	journal := fixtureowner.Own(t, NewWALEngine(source, w))
	for _, item := range []struct{ id, name string }{{"rebuild:a", "alpha"}, {"rebuild:b", "beta"}} {
		tx, e := source.BeginTransaction(t.Context())
		require.NoError(t, e)
		require.NoError(t, tx.SetNamespace("rebuild"))
		require.NoError(t, tx.SetImplicit(true))
		_, e = tx.CreateNode(t.Context(), &Node{ID: NodeID(item.id), Properties: map[string]any{item.name: int64(1)}})
		require.NoError(t, e)
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())
		require.NoError(t, source.FlushWriteBehind(context.Background()))
	}
	sourceAlpha, _ := source.propKeyDict.lookupID("rebuild", "alpha")
	// End the original engine and journal lifetime before another engine
	// appends commits to this WAL. Its applied markers cannot cover that writer.
	require.NoError(t, journal.Close())
	rebuilt := reviewBindingEngine(t, t.TempDir(), nil)
	first, _, err := RecoverIntoEngine(t.Context(), rebuilt, walDir, "")
	require.NoError(t, err)
	t.Logf("first rebuild: %s", first.Summary())
	require.Zero(t, first.Failed)
	require.Equal(t, 2, first.Applied)
	for _, item := range []struct{ id, name string }{{"rebuild:a", "alpha"}, {"rebuild:b", "beta"}} {
		n, e := rebuilt.GetNode(t.Context(), NodeID(item.id))
		require.NoError(t, e)
		require.Equal(t, map[string]any{item.name: int64(1)}, n.Properties)
	}
	assertPropertyDictionaryBijection(t, rebuilt)
	rebuiltAlpha, _ := rebuilt.propKeyDict.lookupID("rebuild", "alpha")
	t.Logf("source alpha=%d rebuilt alpha=%d", sourceAlpha, rebuiltAlpha)
	require.NotEqual(t, sourceAlpha, rebuiltAlpha, "fixture forces a legitimate allocation-order change")
	// A normal synchronous no-property write initializes lazy namespace MVCC.
	// This isolates binding ownership from the separately proven develop gate
	// deadlock; it allocates no property names and leaves no additional live row.
	_, err = rebuilt.CreateNode(t.Context(), &Node{ID: "rebuild:mvcc-warm"})
	require.NoError(t, err)
	require.NoError(t, rebuilt.DeleteNode(t.Context(), "rebuild:mvcc-warm"))
	continuation, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	continuedJournal := fixtureowner.Own(t, NewWALEngine(rebuilt, continuation))
	tx, err := rebuilt.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("rebuild"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "rebuild:a", Properties: map[string]any{"alpha": int64(2)}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, rebuilt.FlushWriteBehind(context.Background()))
	require.NoError(t, continuedJournal.Close())
	entries, err := ReadWALEntriesFromDir(t.Context(), walDir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for _, entry := range entries {
		record, e := decodeBufferedWALRecord(entry)
		require.NoError(t, e)
		require.NotEmpty(t, record.PropertyBindings)
		require.Empty(t, record.PropKeys)
		t.Logf("seq=%d tx=%s complete bindings=%+v", entry.Sequence, record.TxID, legacyBindingEntries(t, record.PropertyBindings))
	}
	secondStore := reviewBindingEngine(t, t.TempDir(), nil)
	second, _, err := RecoverIntoEngine(t.Context(), secondStore, walDir, "")
	require.NoError(t, err)
	t.Logf("second rebuild: %s errors=%+v", second.Summary(), second.Errors)
	require.Zero(t, second.Failed, "each new-format record owns a complete source binding table")
	require.Equal(t, 3, second.Applied)
	node, err := secondStore.GetNode(t.Context(), "rebuild:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"alpha": int64(2)}, node.Properties)
	assertPropertyDictionaryBijection(t, secondStore)
}
