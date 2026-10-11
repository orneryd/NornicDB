package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// In a Cypher 25 statement a null transaction id is an id no transaction
// has (Neo4j 2026.09): SHOW lists nothing for it and TERMINATE reports it
// not found; Cypher 5 keeps Neo4j 5.26's TypeError. Cypher 25 also has the
// currentQueryProgress column, last.
func TestCypher25ShowTransactions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_show_transactions"))
	ctx := context.Background()
	for _, query := range []string{"CYPHER 25 SHOW TRANSACTIONS null", "CYPHER 25 SHOW TRANSACTIONS ['x', null]"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Empty(t, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "CYPHER 25 TERMINATE TRANSACTIONS null", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, nil, "Transaction not found."}}, result.Rows)
	for _, query := range []string{"CYPHER 5 SHOW TRANSACTIONS null", "CYPHER 5 TERMINATE TRANSACTIONS null", "CYPHER 25 SHOW TRANSACTIONS [null, 1]"} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	}

	result, err = exec.Execute(ctx, "CYPHER 25 SHOW TRANSACTIONS YIELD currentQueryProgress RETURN count(*) >= 0 AS ok", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	_, err = exec.Execute(ctx, "CYPHER 5 SHOW TRANSACTIONS YIELD currentQueryProgress RETURN *", nil)
	require.Error(t, err)

	tx := runningTransactions.begin(ctx, "cypher25tx")
	defer runningTransactions.end(tx)
	for _, version := range []string{"5", "25"} {
		result, err = exec.executeShowTransactions(withCypherVersion(ctx, "CYPHER "+version+" RETURN 1"), "SHOW TRANSACTIONS")
		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)
		require.Equal(t, len(result.Columns), len(result.Rows[0]))
		require.Equal(t, version == "25", result.Columns[len(result.Columns)-1] == "currentQueryProgress")
	}
}
