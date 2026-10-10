package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type projectedTraversalCountingEngine struct {
	storage.Engine
	startNodeVisits int
}

func (engine *projectedTraversalCountingEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	reader, ok := engine.Engine.(storage.ProjectedLabelNodeReader)
	if !ok {
		return storage.ErrNotImplemented
	}
	return reader.StreamNodesByLabelProjected(label, properties, func(node *storage.Node) error {
		engine.startNodeVisits++
		return visit(node)
	})
}

func TestTraversalLimitStopsStreamingStartNodes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "traversal")
	for index := 0; index < 40; index++ {
		id := storage.NodeID(fmt.Sprintf("person-%02d", index))
		_, err := store.CreateNode(&storage.Node{
			ID:         id,
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": string(id)},
		})
		require.NoError(t, err)
	}
	for index := 0; index < 40; index++ {
		err := store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("knows-%02d", index)),
			StartNode: storage.NodeID(fmt.Sprintf("person-%02d", index)),
			EndNode:   storage.NodeID(fmt.Sprintf("person-%02d", (index+1)%40)),
			Type:      "KNOWS",
		})
		require.NoError(t, err)
	}
	probe := &projectedTraversalCountingEngine{Engine: store}
	result, err := NewStorageExecutor(probe).Execute(context.Background(),
		"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 2", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, 2, probe.startNodeVisits, "LIMIT should stop the start-node scan after two matching anchors")

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	txStore := &transactionStorageWrapper{tx: tx, underlying: store, namespace: "traversal", separator: ":"}
	txProbe := &projectedTraversalCountingEngine{Engine: txStore}
	txResult, err := NewStorageExecutor(txProbe).executeMatchWithRelationshipsWithPathSeeded(
		context.Background(), "(a:Person)-[:KNOWS]->(b:Person)", "",
		[]returnItem{{expr: "a.name"}, {expr: "b.name"}}, nil, nil, "", 2,
	)
	require.NoError(t, err)
	require.Len(t, txResult.Rows, 2)
	require.Equal(t, 2, txProbe.startNodeVisits, "snapshot traversal LIMIT should stop the transaction reader after two matching anchors")
}
