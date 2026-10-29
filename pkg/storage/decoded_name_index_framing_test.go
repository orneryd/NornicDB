package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodedNameIndexPrefixExtents(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: false})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := NewNamespacedEngine(engine, "index_names")
	start, err := store.CreateNode(&Node{ID: "start"})
	require.NoError(t, err)
	end, err := store.CreateNode(&Node{ID: "end"})
	require.NoError(t, err)
	for index, name := range []string{"Prefix", "Prefix\x00nested", "OrdinaryControl"} {
		require.NoError(t, store.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("edge%d", index)), StartNode: start, EndNode: end, Type: name, Properties: map[string]any{"key": int64(index + 7)}}))
	}
	for _, name := range []string{"Prefix", "Prefix\x00nested", "OrdinaryControl"} {
		t.Run(name, func(t *testing.T) {
			t.Run("materialized", func(t *testing.T) {
				edges, err := store.GetEdgesByType(name)
				require.NoError(t, err)
				require.Len(t, edges, 1)
				require.Equal(t, name, edges[0].Type)
			})
			t.Run("stored", func(t *testing.T) {
				edges, err := engine.GetStoredEdgesByType(name)
				require.NoError(t, err)
				require.Len(t, edges, 1)
				require.Equal(t, name, edges[0].Type)
			})
			t.Run("stream", func(t *testing.T) {
				var types []string
				err := StreamEdgesByType(context.Background(), store, name, func(edge *Edge) error { types = append(types, edge.Type); return nil })
				require.NoError(t, err)
				require.Equal(t, []string{name}, types)
			})
			t.Run("snapshot", func(t *testing.T) {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("index_names"))
				edges, err := tx.GetEdgesByType(name)
				require.NoError(t, err)
				require.Len(t, edges, 1)
				require.Equal(t, name, edges[0].Type)
			})
		})
	}
	t.Run("unique admission", func(t *testing.T) {
		require.NoError(t, engine.GetSchemaForNamespace("index_names").AddConstraint(Constraint{Name: "exact_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "Prefix", Properties: []string{"key"}}))
		edge := &Edge{ID: "unique-allowed", StartNode: start, EndNode: end, Type: "Prefix", Properties: map[string]any{"key": int64(8)}}
		require.NoError(t, store.CreateEdge(edge), "the same property on the longer type does not conflict")
		edge.ID = "unique-rejected"
		require.Error(t, store.CreateEdge(edge), "a duplicate on the exact type still conflicts")
	})
	t.Run("temporal admission", func(t *testing.T) {
		for index, name := range []string{"Window", "Window\x00nested"} {
			require.NoError(t, store.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("window%d", index)), StartNode: start, EndNode: end, Type: name, Properties: map[string]any{"key": int64(index + 7), "valid_from": time.Unix(0, 0), "valid_to": time.Unix(10, 0)}}))
		}
		require.NoError(t, engine.GetSchemaForNamespace("index_names").AddConstraint(Constraint{Name: "exact_window", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "Window", Properties: []string{"key", "valid_from", "valid_to"}}))
		edge := &Edge{ID: "window-allowed", StartNode: start, EndNode: end, Type: "Window", Properties: map[string]any{"key": int64(8), "valid_from": time.Unix(5, 0), "valid_to": time.Unix(15, 0)}}
		require.NoError(t, store.CreateEdge(edge), "another type does not own this interval")
		edge.ID = "window-rejected"
		require.Error(t, store.CreateEdge(edge), "the exact type's interval still conflicts")
	})
}

func TestDecodedNameCountDeltaRestore(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: false})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := NewNamespacedEngine(engine, "fold_names")
	end, err := store.CreateNode(&Node{ID: "end"})
	require.NoError(t, err)
	name := "FoldName\x00nested"
	for index := 0; index < 3; index++ {
		start, err := store.CreateNode(&Node{ID: NodeID(fmt.Sprintf("start%d", index)), Labels: []string{name}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("edge%d", index)), StartNode: start, EndNode: end, Type: name}))
	}
	backup := filepath.Join(t.TempDir(), "exact-names.backup")
	require.NoError(t, engine.Backup(backup))
	for _, phase := range []string{"committed deltas", "restored and folded"} {
		if phase == "restored and folded" {
			require.NoError(t, engine.Restore(backup))
		}
		t.Run(phase, func(t *testing.T) {
			for _, carrier := range []struct {
				name             string
				scoped, unscoped func(string) (int64, error)
			}{
				{"label", store.NodeCountByLabel, engine.NodeCountByLabel},
				{"type", store.EdgeCountByType, engine.EdgeCountByType},
			} {
				t.Run(carrier.name, func(t *testing.T) {
					for _, reader := range []struct {
						name string
						read func(string) (int64, error)
					}{
						{"scoped", carrier.scoped}, {"unscoped", carrier.unscoped},
					} {
						t.Run(reader.name, func(t *testing.T) {
							count, err := reader.read(name)
							require.NoError(t, err)
							require.EqualValues(t, 3, count, "public backup/restore folding preserves the whole identity")
						})
						t.Run(reader.name+" distinct prefix", func(t *testing.T) {
							count, err := reader.read("FoldName")
							require.NoError(t, err)
							require.Zero(t, count)
						})
					}
				})
			}
		})
	}
}
