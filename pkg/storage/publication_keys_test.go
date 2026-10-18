package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Compare dependency sets with the original sorted-schema implementation.
// Order is deliberately ignored: only shared keys determine publication order.
func TestPublicationKeysPreserveSchemaDependencies(t *testing.T) {
	schema := NewSchemaManager()
	for _, c := range []Constraint{
		{Name: "node", Type: ConstraintUnique, Label: "A", Properties: []string{"id"}},
		{Name: "nodekey", Type: ConstraintNodeKey, Label: "B", Properties: []string{"id"}},
		{Name: "exists", Type: ConstraintExists, Label: "A", Properties: []string{"other"}},
		{Name: "edge", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "A", Properties: []string{"id"}},
		{Name: "edgekey", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "B", Properties: []string{"id", "tenant"}},
	} {
		require.NoError(t, schema.AddConstraint(c))
	}
	nodes := []*Node{nil, {Labels: []string{"A", "A"}, Properties: map[string]interface{}{"id": "old"}}, {Labels: []string{"B"}, Properties: map[string]interface{}{"id": "new"}}, {Labels: []string{"other"}, Properties: map[string]interface{}{"id": "new"}}, {Labels: []string{"A"}}}
	edges := []*Edge{nil, {Type: "A", Properties: map[string]interface{}{"id": "old"}}, {Type: "B", Properties: map[string]interface{}{"id": "new", "tenant": int64(1)}}, {Type: "other", Properties: map[string]interface{}{"id": "new"}}, {Type: "B", Properties: map[string]interface{}{"id": "missing tenant"}}}
	for _, old := range nodes {
		for _, written := range nodes {
			require.ElementsMatch(t, referenceNodePublicationKeys(nil, schema, "db:n", old, written), appendNodePublicationKeys(nil, schema, "db:n", old, written))
		}
	}
	for _, old := range edges {
		for _, written := range edges {
			require.ElementsMatch(t, referenceEdgePublicationKeys(nil, schema, "db:e", old, written), appendEdgePublicationKeys(nil, schema, "db:e", old, written))
		}
	}
}

func BenchmarkPublicationNodeKeys(b *testing.B) {
	schema := NewSchemaManager()
	if err := schema.AddConstraint(Constraint{Name: "unique", Type: ConstraintUnique, Label: "A", Properties: []string{"id"}}); err != nil {
		b.Fatal(err)
	}
	node := &Node{Labels: []string{"A"}, Properties: map[string]interface{}{"id": "value"}}
	for _, tc := range []struct {
		name    string
		collect func([]any, *SchemaManager, NodeID, *Node, *Node) []any
	}{
		{"original", referenceNodePublicationKeys}, {"current", appendNodePublicationKeys},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = tc.collect(nil, schema, "db:n", node, node)
			}
		})
	}
}

func referenceNodePublicationKeys(keys []any, schema *SchemaManager, id NodeID, old, written *Node) []any {
	keys = append(keys, id)
	if schema == nil || !schema.HasUniqueValueTracking() {
		return keys
	}
	for _, node := range []*Node{old, written} {
		if node == nil {
			continue
		}
		for _, c := range schema.GetConstraintsForLabels(node.Labels) {
			if c.EffectiveEntityType() != ConstraintEntityNode || len(c.Properties) != 1 || (c.Type != ConstraintUnique && c.Type != ConstraintNodeKey) {
				continue
			}
			value, ok := node.Properties[c.Properties[0]]
			if !ok || !hasLabel(node.Labels, c.Label) {
				continue
			}
			if key, keyed := uniqueConstraintValueKey(value); keyed {
				keys = append(keys, uniqueConstraintLockKey{label: c.Label, property: c.Properties[0], value: key})
			}
		}
	}
	return keys
}

// referenceEdgePublicationKeys appends the keys a publication of an edge write
// is ordered on: the edge, and the relationship-uniqueness values old and
// written hold.
func referenceEdgePublicationKeys(keys []any, schema *SchemaManager, id EdgeID, old, written *Edge) []any {
	keys = append(keys, id)
	if schema == nil {
		return keys
	}
	for _, edge := range []*Edge{old, written} {
		if edge == nil || len(edge.Properties) == 0 {
			continue
		}
		for _, c := range schema.GetConstraintsForLabels([]string{edge.Type}) {
			if c.EffectiveEntityType() != ConstraintEntityRelationship || (c.Type != ConstraintUnique && c.Type != ConstraintRelationshipKey) {
				continue
			}
			values, ok := relationshipConstraintValueKeys(c.Properties, edge.Properties, true)
			if !ok || len(values) == 0 {
				continue
			}
			var key interface{} = values[0]
			if len(values) > 1 {
				key = NewCompositeKey(values...).Hash
			}
			keys = append(keys, uniqueConstraintLockKey{label: "\x00relationship:" + edge.Type, property: relationshipPropertyKey(c.Properties), value: key})
		}
	}
	return keys
}
