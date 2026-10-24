package storage

import "context"

// Transparent wrappers preserve the immediate inner read view. In particular,
// forwarding a local seek never unwraps a NamespacedEngine or a transaction.
func (w *WALEngine) RelationshipPropertySeeksAvailable() bool {
	p, ok := w.engine.(RelationshipPropertySeeker)
	return ok && p.RelationshipPropertySeeksAvailable()
}

func (w *WALEngine) ScopedRelationshipPropertySeeksAvailable() bool {
	p, ok := w.engine.(ScopedRelationshipPropertySeeker)
	return ok && p.ScopedRelationshipPropertySeeksAvailable()
}

func (w *WALEngine) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	p, ok := w.engine.(RelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationships(ctx, relType, properties, values)
}

func (w *WALEngine) SeekRelationshipsInNamespace(ctx context.Context, namespace, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	p, ok := w.engine.(ScopedRelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationshipsInNamespace(ctx, namespace, relType, properties, values)
}

func (t *TracedEngine) RelationshipPropertySeeksAvailable() bool {
	p, ok := t.Engine.(RelationshipPropertySeeker)
	return ok && p.RelationshipPropertySeeksAvailable()
}

func (t *TracedEngine) ScopedRelationshipPropertySeeksAvailable() bool {
	p, ok := t.Engine.(ScopedRelationshipPropertySeeker)
	return ok && p.ScopedRelationshipPropertySeeksAvailable()
}

func (t *TracedEngine) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	p, ok := t.Engine.(RelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationships(ctx, relType, properties, values)
}

func (t *TracedEngine) SeekRelationshipsInNamespace(ctx context.Context, namespace, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	p, ok := t.Engine.(ScopedRelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	return p.SeekRelationshipsInNamespace(ctx, namespace, relType, properties, values)
}

var (
	_ ScopedRelationshipPropertySeeker = (*BadgerEngine)(nil)
	_ ScopedRelationshipPropertySeeker = (*MemoryEngine)(nil)
	_ ScopedRelationshipPropertySeeker = (*WALEngine)(nil)
	_ ScopedRelationshipPropertySeeker = (*TracedEngine)(nil)
	_ RelationshipPropertySeeker       = (*WALEngine)(nil)
	_ RelationshipPropertySeeker       = (*TracedEngine)(nil)
	_ RelationshipPropertySeeker       = (*NamespacedEngine)(nil)
	_ RelationshipPropertySeeker       = (*BadgerTransaction)(nil)
)
