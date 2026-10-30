package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallDbSchemaVisualization(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Company"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r1", Type: "WORKS_AT", StartNode: "n1", EndNode: "n2"}))
	result, err := exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	virtualNodes, ok := result.Rows[0][0].([]*storage.Node)
	require.True(t, ok)
	require.Len(t, virtualNodes, 2)
	byLabel := make(map[string]*storage.Node)
	for _, node := range virtualNodes {
		require.Len(t, node.Labels, 1)
		byLabel[node.Labels[0]] = node
		require.Equal(t, node.Labels[0], node.Properties["name"])
		require.Equal(t, []string{}, node.Properties["indexes"])
		require.Equal(t, []string{}, node.Properties["constraints"])
	}
	virtualEdges, ok := result.Rows[0][1].([]*storage.Edge)
	require.True(t, ok)
	require.Len(t, virtualEdges, 1)
	require.Equal(t, "WORKS_AT", virtualEdges[0].Type)
	require.Equal(t, "WORKS_AT", virtualEdges[0].Properties["name"])
	require.Equal(t, byLabel["Person"].ID, virtualEdges[0].StartNode)
	require.Equal(t, byLabel["Company"].ID, virtualEdges[0].EndNode)
	count, err := store.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	_, err = exec.Execute(ctx, "CREATE INDEX person_schema_viz FOR (n:Person) ON (n.value, n.other)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT person_schema_unique FOR (n:Person) REQUIRE n.key IS UNIQUE", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	for _, node := range result.Rows[0][0].([]*storage.Node) {
		if node.Labels[0] == "Person" {
			require.Equal(t, []string{"value,other"}, node.Properties["indexes"])
			constraints := node.Properties["constraints"].([]string)
			require.Len(t, constraints, 1)
			require.Contains(t, constraints[0], "person_schema_unique")
			require.Contains(t, constraints[0], "IS UNIQUE")
		}
	}
}

func TestCallDbSchemaVisualizationEmptyAndMultipleLabels(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	result, err := exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	require.Equal(t, []*storage.Node{}, result.Rows[0][0])
	require.Equal(t, []*storage.Edge{}, result.Rows[0][1])
	_, err = exec.Execute(ctx, "CREATE (:StartA:StartB)-[:LINK]->(:EndA:EndB)", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "CALL db.schema.visualization()", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows[0][0], 4)
	require.Len(t, result.Rows[0][1], 4)
}

type visualizationErrorEngine struct {
	storage.Engine
	failNodes bool
	err       error
}

func (engine visualizationErrorEngine) AllNodes() ([]*storage.Node, error) {
	if engine.failNodes {
		return nil, engine.err
	}
	return engine.Engine.AllNodes()
}

func (engine visualizationErrorEngine) AllEdges() ([]*storage.Edge, error) {
	return nil, engine.err
}

func TestCallDbSchemaVisualizationStorageErrors(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for _, failNodes := range []bool{true, false} {
		failure := errors.New("visualization storage failure")
		exec := NewStorageExecutor(visualizationErrorEngine{Engine: store, failNodes: failNodes, err: failure})
		_, err := exec.callDbSchemaVisualization()
		require.ErrorIs(t, err, failure)
	}
}
