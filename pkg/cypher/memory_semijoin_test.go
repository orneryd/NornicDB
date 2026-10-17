package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// EXISTS / COUNT run as a semi-join over one node looked up by property, or
// over one relationship from a bound node, count what the semi-join holds
// while it runs: the candidate nodes and the set that drops duplicates, or
// the bound node's relationships (#1343).
func TestSemiJoinsCountWhatTheyHold(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	_, err := executor.Execute(ctx, "UNWIND range(1, 20000) AS i CREATE (:Item {p: 1})", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (h:Hub {name: 'hub'}) WITH h UNWIND range(1, 20000) AS i CREATE (h)-[:R]->(:Leaf)", nil)
	require.NoError(t, err)

	for _, test := range []struct{ name, query string }{
		{"node lookup", "RETURN COUNT { MATCH (n:Item {p: 1}) } AS c"},
		{"bound relationship", "MATCH (h:Hub {name: 'hub'}) RETURN COUNT { (h)-[:R]->(:Leaf) } AS c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(20000)}}, result.Rows)

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 512 << 10})
			_, err = executor.Execute(ctx, test.query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			requirePoolsEmpty(t, "nornic")
		})
	}
}

// A tracker a test takes from the pool starts with the limits configured
// now and no failure, whatever statement it last served.
func TestTestMemoryTrackerIsReset(t *testing.T) {
	resetTransactionMemory(t)
	stale := transactionMemoryTrackers.Get().(*transactionMemoryTracker)
	stale.limit, stale.databaseLimit, stale.globalLimit = 1, 1, 1
	stale.failure.Store(&transactionMemoryFailure{err: errors.New("a failure of an earlier statement")})
	transactionMemoryTrackers.Put(stale)

	tracker := newTestMemoryTracker("pooled")
	t.Cleanup(tracker.end)
	require.Zero(t, tracker.limit)
	require.Zero(t, tracker.databaseLimit)
	require.Zero(t, tracker.globalLimit)
	require.NoError(t, tracker.err())
}
