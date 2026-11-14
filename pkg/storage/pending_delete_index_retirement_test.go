package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexLookupHidesPendingSupersessionAfterGenerationRetires(t *testing.T) {
	for _, mutation := range []string{"deleted", "unlabelled"} {
		t.Run(mutation, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddPropertyIndex("user_id", "User", []string{"id"}))
			require.NoError(t, schema.AddCompositeIndex("user_group_id", "User", []string{"group", "id"}))
			bufferedCreate(t, engine, &Node{ID: "test:kept", Labels: []string{"User"}, Properties: map[string]any{"id": int64(2), "group": int64(0)}})
			_, err := engine.writeBehind.rotate(t.Context())
			require.NoError(t, err)
			engine.writeBehind.drainOne(true, nil)
			require.NoError(t, engine.writeBehind.LastErr())
			bufferedCreate(t, engine, &Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"id": int64(1), "group": int64(0)}})
			_, err = engine.writeBehind.rotate(t.Context())
			require.NoError(t, err)
			bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
				if mutation == "deleted" {
					return tx.DeleteGraphNode(t.Context(), "test:u1", CascadeEdges)
				}
				return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:u1", Properties: map[string]any{"id": int64(1), "group": int64(0)}}})
			})
			// Retire only the older create; the newer acknowledged delete stays in
			// the active generation, so the committed posting must remain hidden.
			// A relabel to no labels similarly leaves no pending index members.
			engine.writeBehind.drainOne(true, nil)
			require.NoError(t, engine.writeBehind.LastErr())
			require.Equal(t, mutation == "deleted", engine.writeBehind.NodeDeleted("test:u1"))
			ids, err := schema.PropertyIndexLookup(t.Context(), "User", "id", int64(1))
			require.NoError(t, err)
			require.Empty(t, ids, "an empty value-membership index still owns pending deletions")
			ids, err = schema.PropertyIndexLookupAnyLabel(t.Context(), "id", int64(1))
			require.NoError(t, err)
			require.Empty(t, ids)
			ids, err = schema.PropertyIndexTopK(t.Context(), "User", "id", 1, false)
			require.NoError(t, err)
			require.Equal(t, []NodeID{"test:kept"}, ids)
			ids, err = schema.PropertyIndexAllNonNil(t.Context(), "User", "id", false)
			require.NoError(t, err)
			require.Equal(t, []NodeID{"test:kept"}, ids)
			indexes, err := schema.SeekableCompositeIndexesForLabel(t.Context(), "User")
			require.NoError(t, err)
			var pair *CompositeIndex
			for _, index := range indexes {
				if index.Name == "user_group_id" {
					pair = index
				}
			}
			require.NotNil(t, pair)
			ids, err = schema.CompositeIndexLookup(t.Context(), pair, []any{int64(0), int64(1)}, true)
			require.NoError(t, err)
			require.Empty(t, ids)
			ids, err = schema.CompositeIndexLookup(t.Context(), pair, []any{int64(0)}, false)
			require.NoError(t, err)
			require.Equal(t, []NodeID{"test:kept"}, ids)
			_, err = engine.writeBehind.rotate(t.Context())
			require.NoError(t, err)
			engine.writeBehind.drainOne(true, nil)
			require.NoError(t, engine.writeBehind.LastErr())
			ids, err = schema.PropertyIndexLookup(t.Context(), "User", "id", int64(1))
			require.NoError(t, err)
			require.Empty(t, ids)
		})
	}
}
