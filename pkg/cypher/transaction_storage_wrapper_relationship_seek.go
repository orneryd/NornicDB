package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.RelationshipPropertySeeker = (*transactionStorageWrapper)(nil)

// RelationshipPropertySeeksAvailable reports the active transaction's capability.
func (w *transactionStorageWrapper) RelationshipPropertySeeksAvailable() bool {
	if w.tx != nil {
		return w.tx.RelationshipPropertySeeksAvailable()
	}
	p, ok := w.underlying.(storage.RelationshipPropertySeeker)
	return ok && p.RelationshipPropertySeeksAvailable()
}

// SeekRelationships preserves the transaction read view and returns local IDs,
// following the same adapter boundary as StreamEdgesByType.
func (w *transactionStorageWrapper) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	if w.tx == nil {
		if p, ok := w.underlying.(storage.RelationshipPropertySeeker); ok {
			return p.SeekRelationships(ctx, relType, properties, values)
		}
		return nil, false, nil
	}
	edges, answered, err := w.tx.SeekRelationships(ctx, relType, properties, values)
	if err != nil || !answered || w.namespace == "" {
		return edges, answered, err
	}
	var out []*storage.Edge
	for _, edge := range edges {
		if !strings.HasPrefix(string(edge.ID), w.namespace+w.separator) {
			continue
		}
		copy := *edge
		copy.ID = w.unprefixEdgeID(edge.ID)
		copy.StartNode = w.unprefixNodeID(edge.StartNode)
		copy.EndNode = w.unprefixNodeID(edge.EndNode)
		out = append(out, &copy)
	}
	return out, true, nil
}
