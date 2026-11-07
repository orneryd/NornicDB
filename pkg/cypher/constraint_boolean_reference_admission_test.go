package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConstraintContractComputedPredicatesRemainBoolean(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, expression := range []string{"1 IN [1]", "size(n.tags) IN [1,2]", "size(n.tags) IS NOT NULL"} {
			t.Run(expression, func(t *testing.T) {
				exec, _ := newCypherTestExecutor(t, "computed_contract_predicate")
				entry, constraint, propertyType, err := exec.parseConstraintContractEntry(t.Context(), expression, "n", storage.ConstraintEntityNode, "ComputedPredicate", "computed_predicate", 0)
				require.NoError(t, err)
				require.Equal(t, storage.ConstraintContractKindBooleanNode, entry.Kind)
				require.Equal(t, expression, entry.Expression)
				require.Nil(t, constraint)
				require.Nil(t, propertyType)
				// The runtime predicate compiler owns supported boolean grammar.
				// These expressions were unsupported before this change and must
				// still reject before publishing a contract or native constraint.
				ddl := "CREATE CONSTRAINT computed_predicate FOR (n:ComputedPredicate) REQUIRE { " + expression + " }"
				_, err = exec.Execute(t.Context(), ddl, nil)
				require.ErrorContains(t, err, "unsupported node predicate", ddl)
				require.Empty(t, exec.storage.GetSchema().GetAllConstraints())
				contracts, err := exec.storage.GetSchema().GetAllConstraintContracts(t.Context())
				require.NoError(t, err)
				require.Empty(t, contracts)

			})
		}
	})
}
