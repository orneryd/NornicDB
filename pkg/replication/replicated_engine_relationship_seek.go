package replication

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Reads use the local applied view, just like the embedded Engine's other reads.
// Keep both forms so replication can wrap either physical or database storage.
func (e *ReplicatedEngine) RelationshipPropertySeeksAvailable() bool {
	p, ok := e.Engine.(storage.RelationshipPropertySeeker)
	return ok && p.RelationshipPropertySeeksAvailable()
}

func (e *ReplicatedEngine) ScopedRelationshipPropertySeeksAvailable() bool {
	p, ok := e.Engine.(storage.ScopedRelationshipPropertySeeker)
	return ok && p.ScopedRelationshipPropertySeeksAvailable()
}

func (e *ReplicatedEngine) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	p, ok := e.Engine.(storage.RelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationships(ctx, relType, properties, values)
}

func (e *ReplicatedEngine) SeekRelationshipsInNamespace(ctx context.Context, namespace, relType string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	p, ok := e.Engine.(storage.ScopedRelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationshipsInNamespace(ctx, namespace, relType, properties, values)
}

var (
	_ storage.RelationshipPropertySeeker       = (*ReplicatedEngine)(nil)
	_ storage.ScopedRelationshipPropertySeeker = (*ReplicatedEngine)(nil)
)
