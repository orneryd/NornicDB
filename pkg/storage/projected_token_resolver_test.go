package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedTokenResolverKeepsSnapshotAndNamespace(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Projected: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "alpha:one", Labels: []string{"Doc"}, Properties: map[string]any{"name": "alpha"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "beta:one", Labels: []string{"Doc"}, Properties: map[string]any{"other": int64(1)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "beta:two", Labels: []string{"Doc"}, Properties: map[string]any{"name": "beta"}})
	require.NoError(t, err)

	txn, readTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	defer engine.db.endRead(readTs)
	defer txn.Discard()
	resolver := &projectedTokenResolver{b: engine, txn: txn}
	for _, id := range []NodeID{"alpha:one", "beta:two", "alpha:one"} {
		want, found, err := engine.readStoredPropertyToken(txn, namespaceForNodeID(id), "name")
		require.NoError(t, err)
		require.True(t, found)
		got, found, err := resolver.resolve(id, "name")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, want, got)
	}
	_, found, err := resolver.resolve("alpha:one", "new")
	require.NoError(t, err)
	require.False(t, found)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "alpha:two", Labels: []string{"Doc"}, Properties: map[string]any{"new": "later"}})
	require.NoError(t, err)
	_, found, err = resolver.resolve("alpha:one", "new")
	require.NoError(t, err)
	require.False(t, found, "the held read view must not see a later binding")
	current, currentReadTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	defer engine.db.endRead(currentReadTs)
	defer current.Discard()
	_, found, err = (&projectedTokenResolver{b: engine, txn: current}).resolve("alpha:two", "new")
	require.NoError(t, err)
	require.True(t, found)
}
