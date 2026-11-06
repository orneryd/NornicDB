package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCardinalityDirectionIgnoresQuotedEndpointArrows(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			pattern   string
			direction string
		}{
			{"incoming with arrow in right label", "()<-[r:`K|T `]-(n:`right->name`)", "INCOMING"},
			{"outgoing with arrow in left label", "(n:`left<-name`)-[r:`K|T `]->()", "OUTGOING"},
			{"undirected with arrow in left label", "(n:`left<-name`)-[r:`K|T `]-()", ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, store := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT quoted_arrow FOR "+tc.pattern+" REQUIRE MAX COUNT 2", nil)
				if tc.direction == "" {
					requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", tc.pattern)
					require.Empty(t, store.GetSchema().GetAllConstraints(), "rejected DDL must not publish a constraint")
					return
				}
				require.NoError(t, err)
				constraints := store.GetSchema().GetAllConstraints()
				require.Len(t, constraints, 1)
				require.Equal(t, storage.ConstraintCardinality, constraints[0].Type)
				require.Equal(t, "K|T ", constraints[0].Label)
				require.Equal(t, tc.direction, constraints[0].Direction)
				require.Equal(t, 2, constraints[0].MaxCount)
			})
		}
	})
}

func TestSchemaConsumersDecodeEscapedForVariables(t *testing.T) {
	entity, variable, name, ok := parseConstraintContractForClause("()-[`r``x`:`K|T `]-()")
	require.True(t, ok)
	require.Equal(t, storage.ConstraintEntityRelationship, entity)
	require.Equal(t, "r`x", variable)
	require.Equal(t, "K|T ", name)
	withCypherParsers(t, func(t *testing.T) {
		for _, requirement := range []string{
			"REQUIRE (`r``x`.id) IS RELATIONSHIP KEY",
			"REQUIRE { (`r``x`.id) IS RELATIONSHIP KEY }",
		} {
			t.Run(requirement, func(t *testing.T) {
				exec, store := newTestExecutor(t)
				_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT escaped_variable FOR ()-[`r``x`:`K|T `]-() "+requirement, nil)
				require.NoError(t, err)
				constraints := store.GetSchema().GetAllConstraints()
				require.Len(t, constraints, 1)
				require.Equal(t, storage.ConstraintRelationshipKey, constraints[0].Type)
				require.Equal(t, "K|T ", constraints[0].Label)
				require.Equal(t, []string{"id"}, constraints[0].Properties)
			})
		}
	})
}
