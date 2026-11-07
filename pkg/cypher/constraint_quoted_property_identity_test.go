package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateConstraintQuotedQualifiedPropertyIdentity covers the property
// identity that CREATE CONSTRAINT ... FOR (n:L) REQUIRE n.<property> IS UNIQUE
// registers and enforces. The property reference is one symbolic name: a dot
// inside backticks and a doubled backtick escape belong to the name, so the
// schema must hold exactly the quoted name and the constraint must enforce
// uniqueness on the property the written name denotes. An unquoted qualified
// reference is the control: n.code stays qualifier plus property code. For
// every form a duplicate value is rejected with ConstraintValidationFailed and
// is not stored, while distinct values are admitted.
//
// The contract observations use assert so one run reports both the registered
// property and the enforcement outcome; the setup steps use require.
//
// The parser selection is process-wide, so each parser runs in its own process
// (see withCypherParsers).
func TestCreateConstraintQuotedQualifiedPropertyIdentity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			constraint string // the CREATE CONSTRAINT statement under test
			label      string
			property   string // the exact property name the schema must hold
			reference  string // the same property written in a node pattern
		}{
			{
				name:       "backticked property holding a dot is one property",
				constraint: "CREATE CONSTRAINT quoted_property_dot FOR (n:QuotedDot) REQUIRE n.`external.id` IS UNIQUE",
				label:      "QuotedDot",
				property:   "external.id",
				reference:  "`external.id`",
			},
			{
				name:       "escaped backtick property keeps the escaped name",
				constraint: "CREATE CONSTRAINT escaped_property FOR (n:EscapedTick) REQUIRE n.`ext``id` IS UNIQUE",
				label:      "EscapedTick",
				property:   "ext`id",
				reference:  "`ext``id`",
			},
			{
				name:       "dot and escaped backtick retain one identity",
				constraint: "CREATE CONSTRAINT escaped_dot FOR (n:EscapedDot) REQUIRE n.`external``.id` IS UNIQUE",
				label:      "EscapedDot",
				property:   "external`.id",
				reference:  "`external``.id`",
			},
			{
				name:       "parenthesized quoted property retains identity",
				constraint: "CREATE CONSTRAINT wrapped_dot FOR (n:WrappedDot) REQUIRE (n.`external.id`) IS UNIQUE",
				label:      "WrappedDot",
				property:   "external.id",
				reference:  "`external.id`",
			},
			{
				name:       "parentheses inside a quoted property remain data",
				constraint: "CREATE CONSTRAINT punctuation_property FOR (n:PunctuationProperty) REQUIRE (n.`external(id)`) IS UNIQUE",
				label:      "PunctuationProperty",
				property:   "external(id)",
				reference:  "`external(id)`",
			},
			{
				name:       "quoted FOR variable retains its identity",
				constraint: "CREATE CONSTRAINT bound_variable FOR (`n``.x`:BoundVariable) REQUIRE `n``.x`.`external.id` IS UNIQUE",
				label:      "BoundVariable",
				property:   "external.id",
				reference:  "`external.id`",
			},
			{
				name:       "ordinary qualified property stays qualifier and property",
				constraint: "CREATE CONSTRAINT plain_property FOR (n:PlainQualified) REQUIRE n.code IS UNIQUE",
				label:      "PlainQualified",
				property:   "code",
				reference:  "code",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "constraint_quoted_property"))
				ctx := context.Background()

				_, err := exec.Execute(ctx, tc.constraint, nil)
				require.NoError(t, err, tc.constraint)

				constraints, err := exec.storage.GetSchema().GetConstraints(ctx)
				require.NoError(t, err)
				require.Len(t, constraints, 1, "the statement registers one uniqueness constraint")
				require.Equal(t, tc.label, constraints[0].Label)
				assert.Equal(t, tc.property, constraints[0].Property,
					"the schema must hold the property the written reference denotes")

				// The node side of the same identity: the create and the read
				// back use the reference the constraint was written with, so a
				// duplicate here is a duplicate on the constrained property.
				create := fmt.Sprintf("CREATE (:%s {%s: 'same'})", tc.label, tc.reference)
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err, create)
				read := fmt.Sprintf("MATCH (n:%s) WHERE n.%s = 'same' RETURN count(n) AS c", tc.label, tc.reference)
				assert.EqualValues(t, 1, countOf(t, exec, ctx, read),
					"the created node carries the property %q", tc.property)

				_, err = exec.Execute(ctx, create, nil)
				if assert.Error(t, err, "a second node with the same %q value must be rejected", tc.property) {
					requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", create)
				}
				assert.EqualValues(t, 1, countOf(t, exec, ctx, fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS c", tc.label)),
					"the rejected duplicate must not be stored")

				distinct := fmt.Sprintf("CREATE (:%s {%s: 'other'})", tc.label, tc.reference)
				_, err = exec.Execute(ctx, distinct, nil)
				require.NoError(t, err, distinct)
				assert.EqualValues(t, 2, countOf(t, exec, ctx, fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS c", tc.label)),
					"a distinct value remains admissible")

				// Recreating the constraint must also validate existing data
				// against the exact property, without leaving a failed index.
				_, err = exec.Execute(ctx, "DROP CONSTRAINT "+constraints[0].Name, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, tc.constraint, nil)
				require.NoError(t, err, "distinct existing values must backfill successfully")
				require.True(t, exec.storage.GetSchema().MaintainsPropertyIndex(tc.label, tc.property))
				_, err = exec.Execute(ctx, create, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintValidationFailed", create)
				require.EqualValues(t, 2, countOf(t, exec, ctx, fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS c", tc.label)))
				_, err = exec.Execute(ctx, "DROP CONSTRAINT "+constraints[0].Name, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, tc.constraint, nil)
				requireErrorStatus(t, err, "Neo.DatabaseError.Schema.ConstraintCreationFailed", tc.constraint)
				require.Empty(t, exec.storage.GetSchema().GetAllConstraints())
				require.False(t, exec.storage.GetSchema().MaintainsPropertyIndex(tc.label, tc.property))
				require.EqualValues(t, 3, countOf(t, exec, ctx, fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS c", tc.label)),
					"constraint backfill refusal preserves the existing nodes")
			})
		}
	})
}
