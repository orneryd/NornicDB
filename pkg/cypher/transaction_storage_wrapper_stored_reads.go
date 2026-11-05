package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The transaction wrapper owns a semantic view. Its stored capability must
// preserve that view instead of unwrapping to committed storage.
var _ storage.StoredEntityReader = (*transactionStorageWrapper)(nil)

func (w *transactionStorageWrapper) GetStoredNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetStoredNode(ctx, w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return node, err
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetStoredEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	edge, err := w.tx.GetStoredEdge(ctx, w.prefixEdgeID(id))
	if err != nil || w.namespace == "" {
		return edge, err
	}
	return w.toUserEdge(edge), nil
}

// GetStoredOutgoingEdges is GetOutgoingEdges' stored read: the same read exit
// over the transaction's physically stored adjacency instead of its live,
// pending-inclusive view.
func (w *transactionStorageWrapper) GetStoredOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return w.adjacencyEdges(ctx, adjacencyRead{kind: storage.TransactionReadOutgoing, node: id, stored: true})
}

// GetStoredIncomingEdges is GetStoredOutgoingEdges for the incoming side.
func (w *transactionStorageWrapper) GetStoredIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return w.adjacencyEdges(ctx, adjacencyRead{kind: storage.TransactionReadIncoming, node: id, stored: true})
}

func (w *transactionStorageWrapper) GetStoredNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := w.tx.GetStoredNodesByLabel(ctx, label)
	return projectTransactionRead(ctx, w, nodes, err, readScanned)
}

func (w *transactionStorageWrapper) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjected(ctx, label, properties, true, visit)
}

func (w *transactionStorageWrapper) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return w.streamEdgesByType(ctx, edgeType, true, visit)
}
