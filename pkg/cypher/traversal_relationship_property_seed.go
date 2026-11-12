package cypher

import (
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipPropertySeedEdge resolves a one-hop match whose relationship
// carries inline equality properties a declared relationship uniqueness or
// relationship key constraint indexes, such as
//
//	MATCH (a)-[r:PD_RELATION {id: $id}]->(b)
//
// to the relationship those values select, read from the relationship equality
// index (storage.RelationshipPropertyEqualityLookup) instead of enumerating the
// type's relationships (relationshipTypeSeedNodes). none reports that the
// indexed values select no relationship, so the pattern matches nothing; ok is
// false for every other shape, no indexed constraint, or a lookup the index
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
	constraints, err := schema.GetConstraintsForLabels(ctx, []string{relationshipType}, storage.ConstraintUnique, storage.ConstraintRelationshipKey)
	if err != nil {
		return nil, false, false, err
	}
	properties, constrained, err := relationshipConstraintIndexProperties(ctx, constraints, relationshipType, relationship.Properties)
	if err != nil {
		return nil, false, false, err
	}
	if !constrained {
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
		// Two relationships of one type carrying one constrained value contradict
		// the constraint this index serves. Keep the complete pattern evaluation
		// rather than choosing one of them.
		return nil, false, false, nil
	}
}

// relationshipConstraintIndexProperties chooses the shortest complete unique
// key: every eligible key selects at most one relationship. Property-order ties
// are lexical, so selection is deterministic without allocating/sorting candidates.
func relationshipConstraintIndexProperties(ctx context.Context, constraints []storage.Constraint, relationshipType string, patternProperties map[string]interface{}) ([]string, bool, error) {
	var selected []string
	for _, constraint := range constraints {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if constraint.EffectiveEntityType() != storage.ConstraintEntityRelationship || constraint.Label != relationshipType ||
			(constraint.Type != storage.ConstraintUnique && constraint.Type != storage.ConstraintRelationshipKey) || len(constraint.Properties) == 0 {
			continue
		}
		bound := true
		for _, property := range constraint.Properties {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if patternProperties[property] == nil {
				bound = false
				break
			}
		}
		if bound && (selected == nil || len(constraint.Properties) < len(selected) || len(constraint.Properties) == len(selected) && slices.Compare(constraint.Properties, selected) < 0) {
			selected = constraint.Properties
		}
	}
	return selected, selected != nil, nil
}
