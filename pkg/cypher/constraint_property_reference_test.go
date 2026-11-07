package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstraintPropertyReferenceUsesQuotedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		input, qualifier, property string
		valid                      bool
	}{
		{"n.name", "n", "name", true},
		{"n.`external.id`", "n", "external.id", true},
		{"(n.`external.id`)", "n", "external.id", true},
		{"n.`external``id`", "n", "external`id", true},
		{"`n.dot`.`external.id`", "n.dot", "external.id", true},
		{"n.`  `", "n", "  ", true},
		{"n", "", "", false},
		{"n.name.other", "", "", false},
		{"n.weird-name", "", "", false},
		{"n.$property", "", "", false},
		{"n.name, n.other", "", "", false},
		{"n.`unterminated", "", "", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			qualifier, property, valid := parseConstraintQualifiedProperty(tc.input)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.qualifier, qualifier)
			require.Equal(t, tc.property, property)
		})
	}
}

// Constraint createStatement syntax admits an optional enclosing group, while
// one item of an index ON list must already be a direct variable.property.
// Both consumers reuse the property reader; they retain their own admission.
func TestSchemaPropertyReadersRetainTheirGroupingAdmission(t *testing.T) {
	_, property, valid := parseConstraintQualifiedProperty("(n.name)")
	require.True(t, valid)
	require.Equal(t, "name", property)
	require.False(t, qualifiedIndexPropertiesWellFormed("(n.name)"))
	require.True(t, qualifiedIndexPropertiesWellFormed("n.name"))
}

func TestConstraintContractPropertyQualifierMustMatchTarget(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, predicate := range []string{"IS UNIQUE", "IS :: STRING"} {
			t.Run(predicate, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				query := fmt.Sprintf("CREATE CONSTRAINT bound_contract FOR (n:BoundContract) REQUIRE { other.`external.id` %s }", predicate)
				_, err := exec.Execute(ctx, query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				schema := exec.storage.GetSchema()
				require.Empty(t, schema.GetAllConstraints())
				require.Empty(t, schema.GetAllPropertyTypeConstraints())
				contracts, err := schema.GetAllConstraintContracts(ctx)
				require.NoError(t, err)
				require.Empty(t, contracts)
				_, err = exec.Execute(ctx, strings.Replace(query, "other.", "n.", 1), nil)
				require.NoError(t, err, "the same primitive with its declared qualifier remains supported")
				contracts, err = schema.GetAllConstraintContracts(ctx)
				require.NoError(t, err)
				require.Len(t, contracts, 1)
			})
		}
	})
}
