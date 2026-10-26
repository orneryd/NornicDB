package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionRevealLabelCacheKeyDoesNotAliasStoredLabels(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	const label = "Doc"
	const unusualLabel = "\x00reveal:Doc"
	_, err = engine.CreateNode(&Node{ID: "test:hidden", Labels: []string{label}, Properties: map[string]interface{}{"name": "hidden"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:other", Labels: []string{unusualLabel}, Properties: map[string]interface{}{"name": "other"}})
	require.NoError(t, err)
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:hidden", true))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	collect := func(label string, reveal bool) []NodeID {
		cleanup := engine.BeginQueryRevealScope(reveal)
		defer cleanup()
		var ids []NodeID
		require.NoError(t, tx.StreamNodesByLabelProjected(label, nil, func(node *Node) error { ids = append(ids, node.ID); return nil }))
		return ids
	}
	require.Equal(t, []NodeID{"test:other"}, collect(unusualLabel, false))
	require.Equal(t, []NodeID{"test:hidden"}, collect(label, true))
	require.Empty(t, collect(label, false))
	require.Equal(t, []NodeID{"test:other"}, collect(unusualLabel, true))
}
