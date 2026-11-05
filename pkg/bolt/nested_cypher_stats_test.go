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
			require.Equal(t, nestedCounterValues(direct), nestedCounterValues(nested))
			require.Equal(t, direct.Counters().ContainsUpdates(), nested.Counters().ContainsUpdates())
			require.Equal(t, direct.StatementType(), nested.StatementType())
		})
	}
}

func TestBoltNestedCypherReadTransactionAndRollback(t *testing.T) {
	// The existing session adapter accepts nested writes in a READ-mode BEGIN.
	// This regression pins that behavior; aggregation must not change promotion.
	ctx := context.Background()
	session := newStatementFramingSession(t, neo4jdriver.AccessModeRead)
	tx, err := session.BeginTransaction(ctx)
	require.NoError(t, err)
	result, err := tx.Run(ctx, "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", map[string]any{"body": "CREATE (:NestedRollback) RETURN 1 AS value"})
	require.NoError(t, err)
	summary, err := result.Consume(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, summary.Counters().NodesCreated())
	require.NoError(t, tx.Rollback(ctx))
	result, err = session.Run(ctx, "MATCH (n:NestedRollback) RETURN count(n)", nil)
	require.NoError(t, err)
	require.True(t, result.Next(ctx))
	require.EqualValues(t, 0, result.Record().Values[0])
	summary, err = result.Consume(ctx)
	require.NoError(t, err)
	require.False(t, summary.Counters().ContainsUpdates())
}

func TestBoltNestedCypherExplicitStats(t *testing.T) {
	for _, tc := range nestedstats.Cases() {
		if tc.Name == "inTransactions" {
			continue
		} // Requires auto-commit.
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			session := newStatementFramingSession(t)
			if tc.Setup != "" {
				r, err := session.Run(ctx, tc.Setup, tc.Params)
				require.NoError(t, err)
				_, err = r.Consume(ctx)
				require.NoError(t, err)
			}
			run := func(query string) neo4jdriver.ResultSummary {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				r, err := tx.Run(ctx, query, tc.Params)
				require.NoError(t, err)
				summary, err := r.Consume(ctx)
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))
				return summary
			}
			direct, nested := run(tc.Direct), run(tc.Query)
			require.Equal(t, nestedCounterValues(direct), nestedCounterValues(nested))
			require.Equal(t, direct.StatementType(), nested.StatementType())
		})
	}
}

func nestedCounterValues(s neo4jdriver.ResultSummary) []int {
	c := s.Counters()
	return []int{c.NodesCreated(), c.NodesDeleted(), c.RelationshipsCreated(), c.RelationshipsDeleted(), c.PropertiesSet(), c.LabelsAdded(), c.LabelsRemoved(), c.IndexesAdded(), c.IndexesRemoved(), c.ConstraintsAdded(), c.ConstraintsRemoved()}
}
