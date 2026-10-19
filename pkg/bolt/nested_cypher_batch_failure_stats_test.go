package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
)

func TestBoltNestedCypherBatchFailureStats(t *testing.T) {
	for _, tc := range nestedstats.BatchFailureCases() {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			session := newStatementFramingSession(t)
			if tc.Setup != "" {
				result, err := session.Run(ctx, tc.Setup, tc.Params)
				require.NoError(t, err)
				_, err = result.Consume(ctx)
				require.NoError(t, err)
			}
			tx, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			result, err := tx.Run(ctx, tc.Query, tc.Params)
			if err == nil {
				summary, consumeErr := result.Consume(ctx)
				require.Error(t, consumeErr)
				require.Nil(t, summary)
			}
			require.NoError(t, tx.Rollback(ctx))
			read, err := session.Run(ctx, "MATCH (n:BatchFailureStats) RETURN count(n)", nil)
			require.NoError(t, err)
			require.True(t, read.Next(ctx))
			require.EqualValues(t, 0, read.Record().Values[0])
			summary, err := read.Consume(ctx)
			require.NoError(t, err)
			require.False(t, summary.Counters().ContainsUpdates())
			require.Equal(t, 0, summary.Counters().NodesCreated())
		})
	}
}
