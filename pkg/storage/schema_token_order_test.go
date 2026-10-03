package storage

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaTokenOrder(t *testing.T) {
	schema := NewSchemaManager()
	require.Empty(t, schema.OrderTokens(nil, false))
	require.NoError(t, schema.RegisterTokens([]string{"Bb", "Aa", "Bb"}, []string{"Yy", "Xx", "Yy"}))
	require.NoError(t, schema.RegisterTokens([]string{"Aa"}, []string{"Xx"}))
	require.Equal(t, []string{"Bb", "Aa", "LegacyA", "LegacyZ"}, schema.OrderTokens([]string{"LegacyZ", "Aa", "LegacyA", "Bb"}, false))
	require.Equal(t, []string{"Yy", "Xx"}, schema.OrderTokens([]string{"Xx", "Yy"}, true))
	definition := schema.ExportDefinition()
	restored := NewSchemaManager()
	require.NoError(t, restored.ReplaceFromDefinition(definition))
	definition.LabelTokens[0] = "changed"
	require.Equal(t, []string{"Bb", "Aa"}, restored.OrderTokens([]string{"Aa", "Bb"}, false))
	definition = restored.ExportDefinition()
	definition.RelationshipTypeTokens[0] = "changed"
	require.Equal(t, []string{"Yy", "Xx"}, restored.OrderTokens([]string{"Xx", "Yy"}, true))
	legacy := NewSchemaManager()
	require.NoError(t, legacy.ReplaceFromDefinition(&SchemaDefinition{Version: 1}))
	require.Equal(t, []string{"Aa", "Bb"}, legacy.OrderTokens([]string{"Bb", "Aa"}, false))
}

func TestSchemaTokenOrderPersistenceFailure(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterTokens([]string{"Bb"}, []string{"Yy"}))
	sentinel := errors.New("token persistence failed")
	schema.SetPersister(func(*SchemaDefinition) error { return sentinel })
	require.ErrorIs(t, schema.RegisterTokens([]string{"Aa"}, []string{"Xx"}), sentinel)
	require.Equal(t, []string{"Bb"}, schema.ExportDefinition().LabelTokens)
	require.Equal(t, []string{"Yy"}, schema.ExportDefinition().RelationshipTypeTokens)
	require.NoError(t, schema.RegisterTokens([]string{"Bb"}, []string{"Yy"}))
	schema.SetPersister(func(*SchemaDefinition) error { return nil })
	require.NoError(t, schema.RegisterTokens([]string{"Aa"}, []string{"Xx"}))
	require.Equal(t, []string{"Bb", "Aa"}, schema.OrderTokens([]string{"Aa", "Bb"}, false))
}

func TestSchemaTokenOrderConcurrent(t *testing.T) {
	schema := NewSchemaManager()
	var workers sync.WaitGroup
	failures := make(chan error, 12)
	for worker := 0; worker < 12; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 10; iteration++ {
				if err := schema.RegisterTokens([]string{"Bb", "Aa"}, []string{"Yy", "Xx"}); err != nil {
					failures <- err
					return
				}
				if len(schema.OrderTokens([]string{"Aa", "Bb"}, false)) != 2 {
					failures <- errors.New("concurrent token listing lost a name")
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Equal(t, []string{"Bb", "Aa"}, schema.ExportDefinition().LabelTokens)
	require.Equal(t, []string{"Yy", "Xx"}, schema.ExportDefinition().RelationshipTypeTokens)
}

func TestSchemaTokenOrderReloadAndNamespaceIsolation(t *testing.T) {
	path := t.TempDir()
	engine, err := NewBadgerEngine(path)
	require.NoError(t, err)
	first := NewNamespacedEngine(engine, "first")
	second := NewNamespacedEngine(engine, "second")
	require.NoError(t, first.BulkCreateNodes([]*Node{{ID: "b", Labels: []string{"Bb"}}, {ID: "a", Labels: []string{"Aa"}}}))
	require.NoError(t, second.BulkCreateNodes([]*Node{{ID: "a", Labels: []string{"Aa"}}, {ID: "b", Labels: []string{"Bb"}}}))
	require.NoError(t, first.BulkCreateEdges([]*Edge{{ID: "y", StartNode: "b", EndNode: "a", Type: "Yy"}, {ID: "x", StartNode: "a", EndNode: "b", Type: "Xx"}}))
	require.NoError(t, first.DeleteEdge("y"))
	require.NoError(t, first.DeleteEdge("x"))
	require.NoError(t, first.DeleteNode("b"))
	require.NoError(t, engine.Close())
	engine, err = NewBadgerEngine(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	first = NewNamespacedEngine(engine, "first")
	second = NewNamespacedEngine(engine, "second")
	_, err = first.CreateNode(&Node{ID: "new_b", Labels: []string{"Bb"}})
	require.NoError(t, err)
	require.NoError(t, first.CreateEdge(&Edge{ID: "new_y", StartNode: "new_b", EndNode: "a", Type: "Yy"}))
	require.Equal(t, []string{"Bb", "Aa"}, first.GetSchema().OrderTokens([]string{"Aa", "Bb"}, false))
	require.Equal(t, []string{"Aa", "Bb"}, second.GetSchema().OrderTokens([]string{"Bb", "Aa"}, false))
	require.Equal(t, []string{"Yy", "Xx"}, first.GetSchema().OrderTokens([]string{"Xx", "Yy"}, true))
}
