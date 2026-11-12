package storage

import (
	"context"
	"errors"
	"strings"
)

// RelationshipPropertyEqualityLookup answers a fixed-hop pattern whose
// relationship carries equality properties a declared relationship uniqueness
// or relationship key constraint indexes, such as
//
//	MATCH (a)-[r:PD_RELATION {id: $id}]->(b)
//
// from the relationship equality index the constraint's own validation
// maintains (badger_relationship_unique_index.go), instead of enumerating every
// relationship of the type. Candidates come from the index postings and each is
// read back and re-checked, so a value changed or a relationship deleted since
// its posting is never reported. handled is false when the index cannot answer
// this lookup (no retained or buildable index, write-behind or decay-filtered
// views, unkeyable values, no namespace); the caller then keeps its complete
// scan. Callers still evaluate the whole pattern on what a lookup returns.
type RelationshipPropertyEqualityLookup interface {
	LookupEdgesByPropertyEquality(ctx context.Context, relationshipType string, properties []string, values []interface{}) (edges []*Edge, handled bool, err error)
}

// relationshipPropertyEqualityIndexEngine is the engine-level counterpart of
// RelationshipPropertyEqualityLookup. The namespace is explicit because one
// engine retains every namespace's equality indexes.
type relationshipPropertyEqualityIndexEngine interface {
	LookupEdgesByPropertyEqualityInNamespace(ctx context.Context, namespace, relationshipType string, properties []string, values []interface{}) (edges []*Edge, handled bool, err error)
	BuildRelationshipPropertyEqualityIndex(ctx context.Context, namespace, relationshipType string, properties []string) error
}

// relationshipPropertyEqualityLookupUnder resolves the engine that owns the
// relationship equality index through the transparent durability and tracing
// decorators between a request and it. A namespace boundary, a transaction and a
// tiered composite store each keep their own visibility semantics, so each
// reports no lookup here rather than another layer's answer.
func relationshipPropertyEqualityLookupUnder(engine Engine) (relationshipPropertyEqualityIndexEngine, bool) {
	for engine != nil {
		if provider, ok := engine.(relationshipPropertyEqualityIndexEngine); ok {
			return provider, true
		}
		switch engine.(type) {
		case *NamespacedEngine, *CompositeEngine:
			return nil, false
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := unwrapper.GetInnerEngine()
		if inner == nil || inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}

// relationshipPropertyEqualityIndexOwnerFor resolves declaration-time derived
// index preparation, rather than a query read. Transparent request wrappers may
// precede the database namespace; only that first namespace is crossed. A
// composite or nested namespace keeps its own owner. Pending transaction images
// are not published here: accepted writes maintain the derived index normally.
func relationshipPropertyEqualityIndexOwnerFor(engine Engine) (relationshipPropertyEqualityIndexEngine, bool) {
	for engine != nil {
		if namespaced, ok := engine.(*NamespacedEngine); ok {
			return relationshipPropertyEqualityLookupUnder(namespaced.inner)
		}
		if _, composite := engine.(*CompositeEngine); composite {
			return nil, false
		}
		if provider, ok := engine.(relationshipPropertyEqualityIndexEngine); ok {
			return provider, true
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := unwrapper.GetInnerEngine()
		if inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}

// relationshipEdgeCarriesValues reports whether edge is a relationship of the
// looked-up type whose indexed properties still equal the looked-up values. It
// is the read-back check of one index posting, and the filter a transaction's
// own pending images must satisfy.
func relationshipEdgeCarriesValues(edge *Edge, relationshipType string, properties []string, values []interface{}) bool {
	if edge == nil || edge.Type != relationshipType || len(properties) != len(values) {
		return false
	}
	for i, property := range properties {
		if !compareValues(edge.Properties[property], values[i]) {
			return false
		}
	}
	return true
}

// LookupEdgesByPropertyEqualityInNamespace answers one exact-value relationship
// lookup from the retained index of namespace, building that index from the
// stored relationships of the type when it is not retained yet. Only matching
// relationship bodies are read.
func (b *BadgerEngine) LookupEdgesByPropertyEqualityInNamespace(ctx context.Context, namespace, relationshipType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if b.writeBehind != nil || b.decayEnabled || namespace == "" || relationshipType == "" || len(properties) == 0 || len(properties) != len(values) {
		return nil, false, nil
	}
	looked := make(map[string]interface{}, len(properties))
	for i, property := range properties {
		looked[property] = values[i]
	}
	keys, supported, err := relationshipConstraintValueKeys(ctx, properties, looked, false)
	if err != nil {
		return nil, false, err
	}
	if !supported {
		return nil, false, nil
	}
	key := relationshipUniqueIndexKey{namespace, relationshipType, relationshipPropertyKey(properties)}
	set := &b.relationshipUniqueIndexes
	if err := set.mu.LockContext(ctx); err != nil {
		return nil, false, err
	}
	defer set.mu.Unlock()
	idx, building, err := b.relationshipUniqueIndexLocked(ctx, key, properties)
	if err != nil {
		set.active.Store(len(set.entries) > 0)
		return nil, false, err
	}
	if building {
		defer func() { set.active.Store(len(set.entries) > 0) }()
	}
	ids, err := idx.index.LookupFull(ctx, keys...)
	if err != nil {
		return nil, false, err
	}
	if building {
		b.publishRelationshipUniqueIndexLocked(idx)
	}
	edges := make([]*Edge, 0, len(ids))
	prefix := namespace + ":"
	for _, candidate := range ids {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		id := EdgeID(candidate)
		if !strings.HasPrefix(string(id), prefix) {
			continue
		}
		edge, err := b.GetEdge(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		// The posting is a candidate, not evidence: the stored image decides,
		// so a relationship whose value changed or that was deleted since its
		// posting is not reported.
		if relationshipEdgeCarriesValues(edge, relationshipType, properties, values) {
			edges = append(edges, edge)
		}
	}
	return edges, true, nil
}

// BuildRelationshipPropertyEqualityIndex builds and publishes the index one
// (namespace, relationship type, property set) answers with, from the stored
// relationships of the type. The statement that declares the relationship
// constraint calls it, so a match after the declaration is an exact-value
// lookup rather than the type's first read paying for the build.
func (b *BadgerEngine) BuildRelationshipPropertyEqualityIndex(ctx context.Context, namespace, relationshipType string, properties []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.writeBehind != nil || b.decayEnabled || namespace == "" || relationshipType == "" || len(properties) == 0 {
		return nil
	}
	key := relationshipUniqueIndexKey{namespace, relationshipType, relationshipPropertyKey(properties)}
	set := &b.relationshipUniqueIndexes
	if err := set.mu.LockContext(ctx); err != nil {
		return err
	}
	defer set.mu.Unlock()
	if set.entries[key] != nil {
		return nil
	}
	idx, building, err := b.relationshipUniqueIndexLocked(ctx, key, properties)
	if err != nil {
		set.active.Store(len(set.entries) > 0)
		return err
	}
	if building {
		b.publishRelationshipUniqueIndexLocked(idx)
	}
	set.active.Store(len(set.entries) > 0)
	return nil
}

// PrepareRelationshipPropertyEqualityIndexForEngine builds the relationship
// equality index a new relationship constraint answers reads from, the way a
// node constraint's owned property index is filled from the stored nodes
// (#875). An engine that keeps no such index (a transaction, a tiered composite
// store, write-behind or decay-filtered views, a namespace-less store) keeps its
// complete scan, so this is a no-op there rather than an error.
func PrepareRelationshipPropertyEqualityIndexForEngine(ctx context.Context, engine Engine, constraint Constraint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if engine == nil || constraint.EffectiveEntityType() != ConstraintEntityRelationship {
		return nil
	}
	if constraint.Type != ConstraintUnique && constraint.Type != ConstraintRelationshipKey {
		return nil
	}
	if constraint.Label == "" || len(constraint.Properties) == 0 {
		return nil
	}
	provider, ok := relationshipPropertyEqualityIndexOwnerFor(engine)
	if !ok {
		return nil
	}
	return provider.BuildRelationshipPropertyEqualityIndex(ctx, namespaceForEngine(engine), constraint.Label, constraint.Properties)
}
