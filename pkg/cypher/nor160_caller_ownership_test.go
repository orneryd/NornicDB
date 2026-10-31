// Author: Codex, NOR-160 namespace read API ownership, 29 October 2026.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNOR160CallerOwnedNamespaceReadAPIs(t *testing.T) {
	pointNode := func(read func(*transactionStorageWrapper) (*storage.Node, error)) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) { return read(w) }
	}
	pointEdge := func(read func(*transactionStorageWrapper) (*storage.Edge, error)) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) { return read(w) }
	}
	nodes := func(read func(*transactionStorageWrapper) ([]*storage.Node, error)) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) { return read(w) }
	}
	edges := func(read func(*transactionStorageWrapper) ([]*storage.Edge, error)) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) { return read(w) }
	}
	streamNodes := func(stored bool, properties []string) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) {
			var result []*storage.Node
			visit := func(node *storage.Node) error { result = append(result, node); return nil }
			var err error
			if stored {
				err = w.StreamStoredNodesByLabelProjected("Held", properties, visit)
			} else {
				err = w.StreamNodesByLabelProjected("Held", properties, visit)
			}
			return result, err
		}
	}
	streamEdges := func(stored bool) func(*transactionStorageWrapper) (any, error) {
		return func(w *transactionStorageWrapper) (any, error) {
			var result []*storage.Edge
			visit := func(edge *storage.Edge) error { result = append(result, edge); return nil }
			var err error
			if stored {
				err = w.StreamStoredEdgesByType(context.Background(), "T", visit)
			} else {
				err = w.StreamEdgesByType(context.Background(), "T", visit)
			}
			return result, err
		}
	}
	cases := []struct {
		name string
		read func(*transactionStorageWrapper) (any, error)
	}{
		{"GetNode", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) { return w.GetNode("a") })},
		{"GetQueryNode", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) { return w.GetQueryNode("a") })},
		{"GetStoredNode", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) { return w.GetStoredNode("a") })},
		{"GetNodeProjected/nil", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) { return w.GetNodeProjected("a", nil) })},
		{"GetNodeProjected/selected", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) {
			return w.GetNodeProjected("a", []string{"payload"})
		})},
		{"GetNodesByLabel", nodes(func(w *transactionStorageWrapper) ([]*storage.Node, error) { return w.GetNodesByLabel("Held") })},
		{"GetStoredNodesByLabel", nodes(func(w *transactionStorageWrapper) ([]*storage.Node, error) { return w.GetStoredNodesByLabel("Held") })},
		{"GetFirstNodeByLabel", pointNode(func(w *transactionStorageWrapper) (*storage.Node, error) { return w.GetFirstNodeByLabel("Held") })},
		{"AllNodes", nodes((*transactionStorageWrapper).AllNodes)},
		{"GetAllNodes", func(w *transactionStorageWrapper) (any, error) { return w.GetAllNodes(), nil }},
		{"BatchGetNodes", func(w *transactionStorageWrapper) (any, error) {
			// This base's batch method forwards the underlying engine's IDs;
			// capability/routing changes remain #1644's separate contract.
			return w.BatchGetNodes([]storage.NodeID{"test:a", "test:b"})
		}},
		{"StreamNodesByLabelProjected/nil", streamNodes(false, nil)},
		{"StreamNodesByLabelProjected/selected", streamNodes(false, []string{"payload"})},
		{"StreamStoredNodesByLabelProjected/nil", streamNodes(true, nil)},
		{"StreamStoredNodesByLabelProjected/selected", streamNodes(true, []string{"payload"})},
		{"StreamNodesWithOptions", func(w *transactionStorageWrapper) (any, error) {
			var result []*storage.Node
			err := w.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{Label: "Held", QueryNodes: true}, func(node *storage.Node) error { result = append(result, node); return nil })
			return result, err
		}},
		{"GetEdge", pointEdge(func(w *transactionStorageWrapper) (*storage.Edge, error) { return w.GetEdge("e") })},
		{"GetStoredEdge", pointEdge(func(w *transactionStorageWrapper) (*storage.Edge, error) { return w.GetStoredEdge("e") })},
		{"GetOutgoingEdges", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetOutgoingEdges("a") })},
		{"GetIncomingEdges", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetIncomingEdges("b") })},
		{"GetStoredOutgoingEdges", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetStoredOutgoingEdges("a") })},
		{"GetStoredIncomingEdges", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetStoredIncomingEdges("b") })},
		{"IncidentEdges", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.IncidentEdges("a") })},
		{"OutgoingEdgeHeaders", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) {
			result, answered, err := w.OutgoingEdgeHeaders("a")
			require.True(t, answered)
			return result, err
		})},
		{"IncomingEdgeHeaders", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) {
			result, answered, err := w.IncomingEdgeHeaders("b")
			require.True(t, answered)
			return result, err
		})},
		{"GetEdgesBetween", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetEdgesBetween("a", "b") })},
		{"GetEdgeBetween", func(w *transactionStorageWrapper) (any, error) { return w.GetEdgeBetween("a", "b", "T"), nil }},
		{"MatchEdgesBetween", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) {
			return w.MatchEdgesBetween("a", "b", "T", []string{"payload"}, func(*storage.Edge) bool { return true })
		})},
		{"GetEdgesByType", edges(func(w *transactionStorageWrapper) ([]*storage.Edge, error) { return w.GetEdgesByType("T") })},
		{"AllEdges", edges((*transactionStorageWrapper).AllEdges)},
		{"StreamEdgesByType", streamEdges(false)},
		{"StreamStoredEdgesByType", streamEdges(true)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			writer, err := engine.BeginTransaction()
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"test:a", "test:b"} {
				_, err := writer.CreateNode(&storage.Node{
					ID: id, Labels: []string{"Held"},
					Properties:      map[string]any{"payload": map[string]any{"list": []any{map[string]any{"value": "original"}, []byte{1, 2}}}},
					ChunkEmbeddings: [][]float32{{1, 2, 3}}, NamedEmbeddings: map[string][]float32{"original": {4, 5, 6}},
					EmbedMeta: map[string]any{"chunk_count": int64(1), "nested": map[string]any{"list": []any{"original", []byte{1, 2}}}},
				})
				require.NoError(t, err)
			}
			require.NoError(t, writer.CreateEdge(&storage.Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]any{"payload": map[string]any{"list": []any{map[string]any{"value": "original"}, []byte{1, 2}}}}}))
			require.NoError(t, writer.Commit())
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			t.Cleanup(func() { _ = tx.Rollback() })
			w := &transactionStorageWrapper{tx: tx, underlying: engine, namespace: "test", separator: ":"}
			_, err = w.GetNode("a")
			require.NoError(t, err)
			_, err = w.GetEdge("e")
			require.NoError(t, err)
			for pass := 0; pass < 3; pass++ {
				result, err := tc.read(w)
				require.NoError(t, err)
				require.NotEmpty(t, result)
				want := readownership.Capture(result)
				readownership.Mutate(result)
				again, err := tc.read(w)
				require.NoError(t, err)
				require.Equal(t, want, readownership.Capture(again), "namespace %s returned a retained mutable reference", tc.name)
			}
		})
	}
}
