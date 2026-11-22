package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipPropertySeedEdge resolves a one-hop match whose relationship
// carries inline equality properties a declared relationship RANGE index covers, such as
//
//	MATCH (a)-[r:PD_RELATION {id: $id}]->(b)
//
// to the relationship those values select, read from the relationship equality
// index (storage.RelationshipPropertyEqualityLookup) instead of enumerating the
// type's relationships (relationshipTypeSeedNodes). none reports that the
// indexed values select no relationship, so the pattern matches nothing; ok is
// false for every other shape, no covering index, or a lookup the index
// cannot answer, and the type index keeps its existing role.
//
// The relationship is handed to the traversal as a pinned relationship, so its
// type, direction, inline properties, endpoint admission and the WHERE keep
// their existing owners: this only replaces where the pattern's relationship
// comes from.
func (e *StorageExecutor) relationshipPropertySeedEdge(ctx context.Context, match *TraversalMatch) (edge *storage.Edge, none, ok bool, err error) {
	relationship := match.Relationship
	if match.IsChained || relationship.VariableLength || relationship.MinHops != 1 || relationship.MaxHops != 1 {
		return nil, false, false, nil
	}
	// A pattern value this match must test itself (no inline equality value to
	// index by) is left to the type index.
	if len(relationship.Types) != 1 || len(relationship.Properties) == 0 {
		return nil, false, false, nil
	}
	// The index holds every stored relationship of the type, not one point in
	// time; a temporal viewport keeps the traversal's own relationship reads.
	if viewport, active := TemporalViewportFromContext(ctx); active && viewport.Enabled() {
		return nil, false, false, nil
	}
	store := e.getStorage(ctx)
	lookup, indexed := store.(storage.RelationshipPropertyEqualityLookup)
	if !indexed {
		return nil, false, false, nil
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false, false, nil
	}
	relationshipType := relationship.Types[0]
	properties, indexed, err := schema.RelationshipEqualityIndexProperties(ctx, relationshipType, relationship.Properties)
	if err != nil {
		return nil, false, false, err
	}
	if !indexed {
		return nil, false, false, nil
	}
	values := make([]interface{}, len(properties))
	for i, property := range properties {
		values[i] = relationship.Properties[property]
	}
	edges, handled, err := lookup.LookupEdgesByPropertyEquality(ctx, relationshipType, properties, values)
	if err != nil {
		return nil, false, false, err
	}
	if !handled {
		return nil, false, false, nil
	}
	switch len(edges) {
	case 0:
		return nil, true, true, nil
	case 1:
		return edges[0], false, true, nil
	default:
		// The pinned-edge path consumes one relationship. Ordinary indexes may
		// select several: preserve complete pattern evaluation instead of choosing
		// one or imposing uniqueness that the declaration does not require.
		return nil, false, false, nil
	}
}
