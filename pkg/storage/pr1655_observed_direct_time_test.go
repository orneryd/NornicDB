package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPR1655ObservedDirectUpdateKeepsMissingTimeDefaults(t *testing.T) {
	engine := newTestEngine(t)
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := engine.CreateNode(&Node{ID: "test:a", CreatedAt: created, UpdatedAt: created})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:blind", CreatedAt: created, UpdatedAt: created})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:blind", Properties: map[string]any{"v": "changed"}}}))
	blind, err := engine.GetNode("test:blind")
	require.NoError(t, err)
	require.True(t, created.Equal(blind.CreatedAt))
	require.False(t, blind.UpdatedAt.IsZero())
	before, err := engine.GetNode("test:a")
	require.NoError(t, err)
	after := &Node{ID: before.ID, Properties: map[string]any{"v": "changed"}}
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{Before: before, After: after}))
	stored, err := engine.GetNode(before.ID)
	require.NoError(t, err)
	require.True(t, created.Equal(stored.CreatedAt), "observed direct update must preserve missing CreatedAt just like the existing direct API")
	require.False(t, stored.UpdatedAt.IsZero(), "observed direct update must initialize missing UpdatedAt just like the existing direct API")
}

func TestPR1655ExplicitAndAuthoritativeUpdatesKeepZeroTimes(t *testing.T) {
	for _, policy := range []EntityImagePolicy{ClientNodeEdits, StoredNodeImages} {
		engine := newTestEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a"})
		require.NoError(t, err)
		before, err := engine.GetNode("test:a")
		require.NoError(t, err)
		tx, err := BeginEntityTransaction(engine, CommitModeExplicit)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID}, Policy: policy}))
		require.NoError(t, tx.Commit())
		stored, err := engine.GetNode(before.ID)
		require.NoError(t, err)
		require.True(t, stored.CreatedAt.IsZero())
		require.True(t, stored.UpdatedAt.IsZero())
	}
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	before, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID}, Policy: StoredNodeImages}))
	stored, err := engine.GetNode(before.ID)
	require.NoError(t, err)
	require.True(t, stored.CreatedAt.IsZero())
	require.True(t, stored.UpdatedAt.IsZero())
}
