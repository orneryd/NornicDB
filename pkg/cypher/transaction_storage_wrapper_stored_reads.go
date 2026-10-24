package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The transaction wrapper owns a semantic view. Its stored capability must
// preserve that view instead of unwrapping to committed storage.
var _ storage.StoredEntityReader = (*transactionStorageWrapper)(nil)

func (w *transactionStorageWrapper) GetStoredNode(id storage.NodeID) (*storage.Node, error) {
	tx := w.transaction()
	if tx == nil {
		return storage.GetStoredNode(w.underlying, id)
	}
	node, err := tx.GetStoredNode(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return node, err
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	tx := w.transaction()
	if tx == nil {
		return storage.GetStoredEdge(w.underlying, id)
	}
	edge, err := tx.GetStoredEdge(w.prefixEdgeID(id))
	if err != nil || w.namespace == "" {
		return edge, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) GetStoredOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	tx := w.transaction()
	if tx == nil {
		return storage.GetStoredOutgoingEdges(w.underlying, id)
	}
	edges, err := tx.GetStoredOutgoingEdges(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	tx := w.transaction()
	if tx == nil {
		return storage.GetStoredIncomingEdges(w.underlying, id)
	}
	edges, err := tx.GetStoredIncomingEdges(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredNodesByLabel(label string) ([]*storage.Node, error) {
	tx := w.transaction()
	if tx == nil {
		return storage.GetStoredNodesByLabel(w.underlying, label)
	}
	nodes, err := tx.GetStoredNodesByLabel(label)
	if err != nil || w.namespace == "" {
		return nodes, err
	}
	return w.toUserNamespacedNodes(nodes), nil
}

func (w *transactionStorageWrapper) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjected(label, properties, true, visit)
}

func (w *transactionStorageWrapper) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return w.streamEdgesByType(ctx, edgeType, true, visit)
}
