package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StreamEdgesByType streams the transaction's view of one relationship type
// (storage.EdgeTypeStreamer): its snapshot overlaid with its pending writes,
// converted to user-facing IDs per visited edge, so constraint validation
// inside a transaction never collects the type into a slice.
func (w *transactionStorageWrapper) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return w.streamEdgesByType(ctx, edgeType, false, visit)
}

func (w *transactionStorageWrapper) streamEdgesByType(ctx context.Context, edgeType string, stored bool, visit func(*storage.Edge) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	if w.tx == nil {
		if stored {
			return storage.StreamStoredEdgesByType(ctx, w.underlying, edgeType, visit)
		}
		return storage.StreamEdgesByType(ctx, w.underlying, edgeType, visit)
	}
	prefix := w.namespace + w.separator
	stream := w.tx.StreamEdgesByType
	if stored {
		stream = w.tx.StreamStoredEdgesByType
	}
	return stream(ctx, edgeType, func(edge *storage.Edge) error {
		if edge == nil {
			return nil
		}
		if w.namespace == "" {
			return visit(edge)
		}
		if !strings.HasPrefix(string(edge.ID), prefix) {
			return nil
		}
		// The transaction already hands out copies; strip prefixes in place
		// on a shallow copy, as StreamNodesByLabelProjected does.
		out := *edge
		out.ID = w.unprefixEdgeID(out.ID)
		out.StartNode = w.unprefixNodeID(out.StartNode)
		out.EndNode = w.unprefixNodeID(out.EndNode)
		out.Database = w.namespace
		return visit(&out)
	})
}

var _ storage.EdgeTypeStreamer = (*transactionStorageWrapper)(nil)
