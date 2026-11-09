package storage

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaTokenOrder(t *testing.T) {
	schema := NewSchemaManager()
	require.Empty(t, schema.OrderTokens(nil, false))
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Bb", "Aa", "Bb"}, []string{"Yy", "Xx", "Yy"}))
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Aa"}, []string{"Xx"}))
	require.Equal(t, []string{"Bb", "Aa", "LegacyA", "LegacyZ"}, schema.OrderTokens([]string{"LegacyZ", "Aa", "LegacyA", "Bb"}, false))
	require.Equal(t, []string{"Yy", "Xx"}, schema.OrderTokens([]string{"Xx", "Yy"}, true))
	definition := schemaDefinitionForTest(t, schema)
	restored := NewSchemaManager()
	require.NoError(t, restored.ReplaceFromDefinition(context.Background(), definition))
	definition.LabelTokens[0] = "changed"
	require.Equal(t, []string{"Bb", "Aa"}, restored.OrderTokens([]string{"Aa", "Bb"}, false))
	definition = schemaDefinitionForTest(t, restored)
	definition.RelationshipTypeTokens[0] = "changed"
	require.Equal(t, []string{"Yy", "Xx"}, restored.OrderTokens([]string{"Xx", "Yy"}, true))
	legacy := NewSchemaManager()
	require.NoError(t, legacy.ReplaceFromDefinition(context.Background(), &SchemaDefinition{Version: 1}))
	require.Equal(t, []string{"Aa", "Bb"}, legacy.OrderTokens([]string{"Bb", "Aa"}, false))
}

func TestSchemaTokenOrderPersistenceFailure(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Bb"}, []string{"Yy"}))
	sentinel := errors.New("token persistence failed")
	schema.SetPersister(func(*SchemaDefinition) error { return sentinel })
	require.ErrorIs(t, schema.RegisterTokens(t.Context(), []string{"Aa"}, []string{"Xx"}), sentinel)
	require.Equal(t, []string{"Bb"}, schemaDefinitionForTest(t, schema).LabelTokens)
	require.Equal(t, []string{"Yy"}, schemaDefinitionForTest(t, schema).RelationshipTypeTokens)
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Bb"}, []string{"Yy"}))
	schema.SetPersister(func(*SchemaDefinition) error { return nil })
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Aa"}, []string{"Xx"}))
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
				if err := schema.RegisterTokens(t.Context(), []string{"Bb", "Aa"}, []string{"Yy", "Xx"}); err != nil {
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
	require.Equal(t, []string{"Bb", "Aa"}, schemaDefinitionForTest(t, schema).LabelTokens)
	require.Equal(t, []string{"Yy", "Xx"}, schemaDefinitionForTest(t, schema).RelationshipTypeTokens)
}

func TestSchemaTokenOrderReloadAndNamespaceIsolation(t *testing.T) {
	path := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	first := NewNamespacedEngine(engine, "first")
	second := NewNamespacedEngine(engine, "second")
	require.NoError(t, first.BulkCreateNodes(t.Context(), []*Node{{ID: "b", Labels: []string{"Bb"}}, {ID: "a", Labels: []string{"Aa"}}}))
	require.NoError(t, second.BulkCreateNodes(t.Context(), []*Node{{ID: "a", Labels: []string{"Aa"}}, {ID: "b", Labels: []string{"Bb"}}}))
	require.NoError(t, first.BulkCreateEdges(t.Context(), []*Edge{{ID: "y", StartNode: "b", EndNode: "a", Type: "Yy"}, {ID: "x", StartNode: "a", EndNode: "b", Type: "Xx"}}))
	require.NoError(t, first.DeleteEdge(t.Context(), "y"))
	require.NoError(t, first.DeleteEdge(t.Context(), "x"))
	require.NoError(t, first.DeleteNode(t.Context(), "b"))
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	first = NewNamespacedEngine(engine, "first")
	second = NewNamespacedEngine(engine, "second")
	_, err = first.CreateNode(t.Context(), &Node{ID: "new_b", Labels: []string{"Bb"}})
	require.NoError(t, err)
	require.NoError(t, first.CreateEdge(t.Context(), &Edge{ID: "new_y", StartNode: "new_b", EndNode: "a", Type: "Yy"}))
	require.Equal(t, []string{"Bb", "Aa"}, first.GetSchema().OrderTokens([]string{"Aa", "Bb"}, false))
	require.Equal(t, []string{"Aa", "Bb"}, second.GetSchema().OrderTokens([]string{"Bb", "Aa"}, false))
	require.Equal(t, []string{"Yy", "Xx"}, first.GetSchema().OrderTokens([]string{"Xx", "Yy"}, true))
}
