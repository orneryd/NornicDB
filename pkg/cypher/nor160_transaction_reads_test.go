// Author: Codex, NOR-160 implementation sub-agent, 2026-10-26.
package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNOR160TransactionNamespaceReadsOwnProperties(t *testing.T) {
	engine := newTestMemoryEngine(t)
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := writer.CreateNode(&storage.Node{ID: id, Labels: []string{"Held"}, Properties: map[string]any{"value": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, writer.CreateEdge(&storage.Edge{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b", Type: "COHELD", Properties: map[string]any{"value": int64(1)}}))
	require.NoError(t, writer.Commit())
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("tenant"))
	t.Cleanup(func() { _ = tx.Rollback() })
	w := &transactionStorageWrapper{tx: tx, underlying: engine, namespace: "tenant", separator: ":"}
	reads := []struct {
		name string
		read func() ([]*storage.Edge, error)
	}{
		{"point", func() ([]*storage.Edge, error) { edge, err := w.GetEdge("e"); return []*storage.Edge{edge}, err }},
		{"stored point", func() ([]*storage.Edge, error) { edge, err := w.GetStoredEdge("e"); return []*storage.Edge{edge}, err }},
		{"outgoing", func() ([]*storage.Edge, error) { return w.GetOutgoingEdges("a") }},
		{"incoming", func() ([]*storage.Edge, error) { return w.GetIncomingEdges("b") }},
		{"incident", func() ([]*storage.Edge, error) { return w.IncidentEdges("a") }},
		{"stored outgoing", func() ([]*storage.Edge, error) { return w.GetStoredOutgoingEdges("a") }},
		{"stored incoming", func() ([]*storage.Edge, error) { return w.GetStoredIncomingEdges("b") }},
		{"type", func() ([]*storage.Edge, error) { return w.GetEdgesByType("COHELD") }},
		{"all", w.AllEdges},
	}
	for pass := 0; pass < 2; pass++ {
		for _, read := range reads {
			edges, err := read.read()
			require.NoError(t, err, read.name)
			require.Len(t, edges, 1, read.name)
			require.Equal(t, storage.EdgeID("e"), edges[0].ID)
			require.Equal(t, storage.NodeID("a"), edges[0].StartNode)
			require.Equal(t, storage.NodeID("b"), edges[0].EndNode)
			require.Equal(t, "tenant", edges[0].Database)
			require.Equal(t, int64(1), edges[0].Properties["value"], read.name)
			edges[0].Properties["value"] = int64(99)
		}
		require.NoError(t, w.StreamEdgesByType(context.Background(), "COHELD", func(edge *storage.Edge) error {
			require.Equal(t, int64(1), edge.Properties["value"])
			edge.Properties["value"] = int64(99)
			return nil
		}))
	}
	edge, err := w.GetEdge("e")
	require.NoError(t, err)
	edge.Properties["value"] = int64(2)
	require.NoError(t, w.UpdateEdge(edge))
	for _, read := range reads {
		edges, err := read.read()
		require.NoError(t, err, read.name)
		require.Len(t, edges, 1)
		require.Equal(t, int64(2), edges[0].Properties["value"], read.name)
	}
}

func TestNOR160HistoricalNamespaceReadsOwnProperties(t *testing.T) {
	// Historical (VisibleAt) storage reads are caller-owned, so namespace
	// conversion only rewrites identity; mutating a result never changes a
	// later read of the same version.
	engine := newTestMemoryEngine(t)
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err := writer.CreateNode(&storage.Node{ID: id, Labels: []string{"Held"}, Properties: map[string]any{"value": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, writer.CreateEdge(&storage.Edge{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b", Type: "COHELD", Properties: map[string]any{"value": int64(1)}}))
	require.NoError(t, writer.Commit())
	version := storage.MVCCVersion{CommitTimestamp: time.Now().Add(time.Second), CommitSequence: ^uint64(0)}
	w := &transactionStorageWrapper{underlying: engine, namespace: "tenant", separator: ":"}
	for _, read := range []func() ([]*storage.Edge, error){
		func() ([]*storage.Edge, error) { return w.GetOutgoingEdgesVisibleAt("a", version) },
		func() ([]*storage.Edge, error) { return w.GetIncomingEdgesVisibleAt("b", version) },
		func() ([]*storage.Edge, error) { return w.GetEdgesByTypeVisibleAt("COHELD", version) },
		func() ([]*storage.Edge, error) { return w.GetEdgesBetweenVisibleAt("a", "b", version) },
	} {
		edges, err := read()
		require.NoError(t, err)
		require.Len(t, edges, 1)
		require.Equal(t, storage.EdgeID("e"), edges[0].ID)
		edges[0].Properties["value"] = int64(99)
		again, err := read()
		require.NoError(t, err)
		require.Len(t, again, 1)
		require.Equal(t, int64(1), again[0].Properties["value"])
	}
}
