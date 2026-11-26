package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipPropertySeedEdge resolves a one-hop match whose relationship
// has mandatory equality properties a declared relationship RANGE index covers, such as
//
//	MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id = $id
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
func (e *StorageExecutor) relationshipPropertySeedEdge(ctx context.Context, match *TraversalMatch, whereClause string) (edge *storage.Edge, none, ok bool, err error) {
	relationship := match.Relationship
	if match.IsChained || relationship.VariableLength || relationship.MinHops != 1 || relationship.MaxHops != 1 {
		return nil, false, false, nil
	}
	if len(relationship.Types) != 1 {
		return nil, false, false, nil
	}
	if len(relationship.Properties) == 0 && (relationship.Variable == "" || whereClause == "") {
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
	// Inline equality keeps its established path. For a WHERE equality, only
	// mandatory top-level conjuncts on this relationship may supply a seed.
	// The original WHERE still validates the pinned edge and other predicates.
	valuesByProperty := relationship.Properties
	if len(valuesByProperty) == 0 && relationship.Variable != "" {
		// Pattern properties can be an allocated empty map. Derived WHERE
		// values must never turn into inline pattern predicates.
		valuesByProperty = e.relationshipWhereEqualityValues(ctx, relationship.Variable, whereClause)
	}
	if len(valuesByProperty) == 0 {
		return nil, false, false, nil
	}
	relationshipType := relationship.Types[0]
	properties, indexed, err := schema.RelationshipEqualityIndexProperties(ctx, relationshipType, valuesByProperty)
	if err != nil {
		return nil, false, false, err
	}
	if !indexed {
		return nil, false, false, nil
	}
	values := make([]interface{}, len(properties))
	for i, property := range properties {
		values[i] = valuesByProperty[property]
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

// relationshipWhereEqualityValues extracts only stable, mandatory top-level
// relationship-property equalities. The caller may use these values to narrow
// a candidate source; the original WHERE remains the final predicate owner.
func (e *StorageExecutor) relationshipWhereEqualityValues(ctx context.Context, variable, whereClause string) map[string]interface{} {
	if variable == "" || whereClause == "" {
		return nil
	}
	var values map[string]interface{}
	for _, term := range splitTopLevelAndConjuncts(ctx, whereClause) {
		clause := unwrapOuterParens(term)
		scan, comparison := scanComparisonChain(clause)
		if !comparison || scan.count != 1 {
			continue
		}
		left, right := scan.operand(clause, 0), scan.operand(clause, 1)
		leftBase, _, leftProperty := splitPostfixPropertyAccess(left)
		rightBase, _, rightProperty := splitPostfixPropertyAccess(right)
		if !(leftProperty && leftBase == variable || rightProperty && rightBase == variable) || !chainConjunctIsStable(ctx, term) {
			continue
		}
		property, value, eligible := e.parseSimpleIndexedEquality(ctx, variable, term)
		if !eligible {
			continue
		}
		if values == nil {
			values = make(map[string]interface{})
		}
		values[property] = value
	}
	return values
}
