package bolt

import (
	"context"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoltNestedCypherUpdateStats(t *testing.T) {
	for _, tc := range nestedstats.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			run := func(query string) neo4jdriver.ResultSummary {
				session := newStatementFramingSession(t)
				ctx := context.Background()
				post := func(statement string) neo4jdriver.ResultSummary {
					result, err := session.Run(ctx, statement, tc.Params)
					require.NoError(t, err)
					summary, err := result.Consume(ctx)
					require.NoError(t, err)
					return summary
				}
				if tc.Setup != "" {
					post(tc.Setup)
				}
				return post(query)
			}
			direct, nested := run(tc.Direct), run(tc.Query)
			            counters := func(s neo4jdriver.ResultSummary) []int {
                c:=s.Counters()
                return []int{c.NodesCreated(),c.NodesDeleted(),c.RelationshipsCreated(),c.RelationshipsDeleted(),c.PropertiesSet(),c.LabelsAdded(),c.LabelsRemoved(),c.IndexesAdded(),c.IndexesRemoved(),c.ConstraintsAdded(),c.ConstraintsRemoved()}
            }
            require.Equal(t,counters(direct),counters(nested))
            require.Equal(t,direct.Counters().ContainsUpdates(),nested.Counters().ContainsUpdates())
			require.Equal(t, direct.StatementType(), nested.StatementType())
		})
	}
}
