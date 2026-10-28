package adminimport

// Control coverage and a benchmark for the canonical constraint renderer.
//
// exportSchemaCypher renders two constraint origins through one private
// request: a stored general constraint and a stored property-type constraint.
// The controls below pin the property-type origin (node, relationship,
// quoting, default entity) and the rejection of a general constraint of type
// PROPERTY_TYPE, and BenchmarkSchemaConstraintRendering exercises the shapes
// the export always renders - an ordinary node unique constraint, a compound
// node key, a property-type constraint on a node and a property-type
// constraint on a relationship - so before/after samples measure the same
// workload inputs against the maintained API.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRenderConstraintStatement_RendersPropertyTypeOrigin(t *testing.T) {
	cases := []struct {
		name   string
		input  storage.PropertyTypeConstraint
		expect string
	}{
		{
			name: "node property type defaults to the node entity",
			input: storage.PropertyTypeConstraint{
				Name: "person_age_type", Label: "Person",
				Property: "age", ExpectedType: storage.PropertyTypeInteger,
			},
			expect: "CREATE CONSTRAINT `person_age_type` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`age` IS :: INTEGER",
		},
		{
			name: "relationship property type uses the relationship pattern and variable",
			input: storage.PropertyTypeConstraint{
				Name: "rel_since_type", Label: "KNOWS", EntityType: storage.ConstraintEntityRelationship,
				Property: "since", ExpectedType: storage.PropertyTypeDateTime,
			},
			expect: "CREATE CONSTRAINT `rel_since_type` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() REQUIRE r.`since` IS :: DATETIME",
		},
		{
			name: "node property type with a union expected type",
			input: storage.PropertyTypeConstraint{
				Name: "person_score_type", Label: "Person", EntityType: storage.ConstraintEntityNode,
				Property: "score",
				ExpectedType: storage.PropertyType(
					string(storage.PropertyTypeString) + storage.PropertyTypeUnionSeparator + string(storage.PropertyTypeFloat)),
			},
			expect: "CREATE CONSTRAINT `person_score_type` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`score` IS :: STRING | FLOAT",
		},
		{
			name: "property type keeps source-stage quoting for name, label and property",
			input: storage.PropertyTypeConstraint{
				Name: "odd name", Label: "Person Label",
				Property: "first name", ExpectedType: storage.PropertyTypeString,
			},
			expect: "CREATE CONSTRAINT `odd name` IF NOT EXISTS FOR (n:`Person Label`) REQUIRE n.`first name` IS :: STRING",
		},
		{
			name: "property type origin still renders an empty expected type unconditionally",
			input: storage.PropertyTypeConstraint{
				Name: "person_empty_type", Label: "Person", Property: "nickname",
			},
			expect: "CREATE CONSTRAINT `person_empty_type` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`nickname` IS :: ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderConstraintStatement(constraintStatementRequest{typeConstraint: &tc.input})
			require.True(t, ok)
			require.Equal(t, tc.expect, got)
		})
	}
}

func TestRenderConstraintStatement_RejectsGeneralPropertyTypeOrigin(t *testing.T) {
	// A general constraint of type PROPERTY_TYPE has no expected type in the
	// general payload, so the general origin keeps rejecting it even when the
	// remaining fields look renderable.
	cases := []struct {
		name  string
		input storage.Constraint
	}{
		{
			name:  "bare general property type",
			input: storage.Constraint{Name: "x", Label: "Person", Type: storage.ConstraintPropertyType},
		},
		{
			name: "node general property type with a property",
			input: storage.Constraint{
				Name: "person_age_type", Label: "Person",
				Type: storage.ConstraintPropertyType, Properties: []string{"age"},
			},
		},
		{
			name: "relationship general property type with a property",
			input: storage.Constraint{
				Name: "rel_since_type", Label: "KNOWS", EntityType: storage.ConstraintEntityRelationship,
				Type: storage.ConstraintPropertyType, Properties: []string{"since"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderConstraintStatement(constraintStatementRequest{general: tc.input})
			require.False(t, ok)
			require.Empty(t, got)
		})
	}
}

func constraintRenderBenchmarkWorkload() []storage.Constraint {
	return []storage.Constraint{
		{
			Name: "person_email_unique", Label: "Person",
			Type: storage.ConstraintUnique, Properties: []string{"email"},
		},
		{
			Name: "person_natural_key", Label: "Person",
			Type: storage.ConstraintNodeKey, Properties: []string{"country", "passportNumber"},
		},
	}
}

func propertyTypeRenderBenchmarkWorkload() []storage.PropertyTypeConstraint {
	return []storage.PropertyTypeConstraint{
		{
			Name: "person_age_type", Label: "Person",
			Property: "age", ExpectedType: storage.PropertyTypeInteger,
		},
		{
			Name: "rel_since_type", Label: "KNOWS", EntityType: storage.ConstraintEntityRelationship,
			Property: "since", ExpectedType: storage.PropertyTypeDateTime,
		},
	}
}

func BenchmarkSchemaConstraintRendering(b *testing.B) {
	constraints := constraintRenderBenchmarkWorkload()
	propertyTypes := propertyTypeRenderBenchmarkWorkload()
	requests := make([]constraintStatementRequest, 0, len(constraints)+len(propertyTypes))
	for _, constraint := range constraints {
		requests = append(requests, constraintStatementRequest{general: constraint})
	}
	for i := range propertyTypes {
		requests = append(requests, constraintStatementRequest{typeConstraint: &propertyTypes[i]})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, request := range requests {
			renderConstraintStatement(request)
		}
	}
}
