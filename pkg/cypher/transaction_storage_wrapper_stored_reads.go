package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The transaction wrapper owns a semantic view. Its stored capability must
// preserve that view instead of unwrapping to committed storage.
var _ storage.StoredEntityReader = (*transactionStorageWrapper)(nil)

func (w *transactionStorageWrapper) GetStoredNode(id storage.NodeID) (*storage.Node, error) {
	node, err := storage.ReadWithCapability(w.tx, func(reader storage.StoredEntityReader) (*storage.Node, error) {
		return reader.GetStoredNode(w.prefixNodeID(id))
	}, nil)
	if err != nil || w.namespace == "" {
		return node, err
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, err := storage.ReadWithCapability(w.tx, func(reader storage.StoredEntityReader) (*storage.Edge, error) {
		return reader.GetStoredEdge(w.prefixEdgeID(id))
	}, nil)
	if err != nil || w.namespace == "" {
		return edge, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) GetStoredOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := storage.ReadWithCapability(w.tx, func(reader storage.StoredEntityReader) ([]*storage.Edge, error) {
		return reader.GetStoredOutgoingEdges(w.prefixNodeID(id))
	}, nil)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := storage.ReadWithCapability(w.tx, func(reader storage.StoredEntityReader) ([]*storage.Edge, error) {
		return reader.GetStoredIncomingEdges(w.prefixNodeID(id))
	}, nil)
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredNodesByLabel(label string) ([]*storage.Node, error) {
	return w.GetStoredNodesByLabelWithContext(context.Background(), label)
}

// GetStoredNodesByLabelWithContext carries the caller through validation and label materialization.
func (w *transactionStorageWrapper) GetStoredNodesByLabelWithContext(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := storage.ReadWithCapability(w.tx, func(reader storage.StoredEntityReader) ([]*storage.Node, error) {
		return storage.GetStoredNodesByLabelWithContext(ctx, reader, label)
	}, nil)
	if err != nil || w.namespace == "" {
		return nodes, err
	}
	return w.toUserNamespacedNodes(nodes), nil
}

func (w *transactionStorageWrapper) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return w.StreamStoredNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamStoredNodesByLabelProjectedWithContext carries the caller through validation and label materialization.
func (w *transactionStorageWrapper) StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjectedWithContext(ctx, label, properties, true, visit)
}

func (w *transactionStorageWrapper) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return w.streamEdgesByType(ctx, edgeType, true, visit)
}

func (*transactionStorageWrapper) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.NativeStoredReadPolicy()
}
func (w *transactionStorageWrapper) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if w.tx == nil {
		return nil, storage.ErrNotImplemented
	}
	if w.separator != "" && w.separator != ":" {
		return nil, storage.ErrNotImplemented
	}
	return storage.ReadStoredInNamespace(ctx, w.tx, w.namespace, r)
}
