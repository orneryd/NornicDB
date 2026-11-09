package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/internal/testutil"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

func TestGraphQLCallScopeDesignConsumers(t *testing.T) {
	for _, mutation := range []bool{false, true} {
		route := "query/"
		if mutation {
			route = "mutation/"
		}
		for _, tc := range callscopetest.DesignCases() {
			t.Run(route+tc.Name, func(t *testing.T) {
				db := testDB(t)
				resolver := NewResolver(db, testutil.DatabaseManager(t, db))
				input := models.CypherInput{Statement: tc.Query}
				var result *models.CypherResult
				var err error
				if mutation {
					result, err = (&mutationResolver{resolver}).mutationExecuteCypher(context.Background(), input)
				} else {
					result, err = (&queryResolver{resolver}).queryCypher(context.Background(), input)
				}
				require.NoError(t, err)
				require.Equal(t, tc.Columns, result.Columns)
				require.Equal(t, tc.Rows, result.Rows)
			})
		}
	}
}

func TestGraphQLCallScopeTransitionConsumers(t *testing.T) {
	for _, mutation := range []bool{false, true} {
		t.Run(fmt.Sprintf("mutation=%v", mutation), func(t *testing.T) {
			// Cases create their own inputs and never count stored fixture
			// nodes, so one database per route preserves their independence.
			db := testDB(t)
			resolver := NewResolver(db, testutil.DatabaseManager(t, db))
			for _, tc := range callscopetest.TransitionCases() {
				t.Run(tc.Name, func(t *testing.T) {
					input := models.CypherInput{Statement: tc.Query}
					var result *models.CypherResult
					var err error
					if mutation {
						result, err = (&mutationResolver{resolver}).mutationExecuteCypher(context.Background(), input)
					} else {
						result, err = (&queryResolver{resolver}).queryCypher(context.Background(), input)
					}
					if tc.Code != "" {
						require.Error(t, err)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, tc.Code, code)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.Columns, result.Columns)
					require.ElementsMatch(t, tc.Rows, result.Rows)
					if tc.Name == "delete_retained" {
						remaining, err := (&queryResolver{resolver}).queryCypher(context.Background(), models.CypherInput{Statement: "MATCH (n:R73Delete) RETURN count(n) AS remaining"})
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(0)}}, remaining.Rows)
					}
				})
			}
		})
	}
}
