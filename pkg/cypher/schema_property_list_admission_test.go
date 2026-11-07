package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaPropertyListsRejectEmptyMembers(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, statement := range []string{
			"CREATE CONSTRAINT empty_member FOR (n:ListAdmission) REQUIRE (%s) IS UNIQUE",
			"CREATE CONSTRAINT empty_member FOR (n:ListAdmission) REQUIRE (%s) IS NODE KEY",
			"CREATE CONSTRAINT empty_member FOR (n:ListAdmission) REQUIRE { (%s) IS NODE KEY }",
			"CREATE INDEX empty_member FOR (n:ListAdmission) ON (%s)",
		} {
			for _, members := range []string{"n.a,", ",n.a", "n.a,,n.b", "n.a, /* trailing */"} {
				query := fmt.Sprintf(statement, members)
				t.Run(query, func(t *testing.T) {
					exec, _ := newCypherTestExecutor(t, "schema_empty_member")
					schema := exec.storage.GetSchema()
					indexesBefore := schema.GetIndexes()
					_, err := exec.Execute(t.Context(), query, nil)
					require.Error(t, err, query)
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
					require.Empty(t, schema.GetAllConstraints())
					require.ElementsMatch(t, indexesBefore, schema.GetIndexes())
					contracts, err := schema.GetAllConstraintContracts(t.Context())
					require.NoError(t, err)
					require.Empty(t, contracts)
				})
			}
		}
	})
}
