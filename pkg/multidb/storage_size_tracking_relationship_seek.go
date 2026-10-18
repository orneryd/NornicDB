package multidb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Preserve the wrapped database's optional read capability without changing
// size accounting or write admission.
func (t *sizeTrackingEngine) RelationshipPropertySeeksAvailable() bool {
	p, ok := t.Engine.(storage.RelationshipPropertySeeker)
	return ok && p.RelationshipPropertySeeksAvailable()
}

func (t *sizeTrackingEngine) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	p, ok := t.Engine.(storage.RelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationships(ctx, relType, properties, values)
}

var _ storage.RelationshipPropertySeeker = (*sizeTrackingEngine)(nil)
