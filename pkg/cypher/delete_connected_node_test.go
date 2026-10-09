package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A non-DETACH DELETE of a node that relationships still connect fails at
// commit, not at the DELETE: a later clause of the transaction may delete
// those relationships first (Neo4j 5.26.30, #907).
func TestDeleteConnectedNodeIsCheckedAtCommit(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_connected"))
	ctx := context.Background()
	run := func(query string) (*ExecuteResult, error) { return exec.Execute(ctx, query, nil) }

	_, err := run("CREATE (:D {id: 1})-[:R]->(:D {id: 2}), (:D {id: 3})-[:R]->(:D {id: 4}), (:D {id: 5})-[:R]->(:D {id: 6}), (:D {id: 7})-[:R]->(:D {id: 8})")
	require.NoError(t, err)

	for _, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (n:D {id: 1}) OPTIONAL MATCH (n)-[r]-() DELETE n DELETE r RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (n:D {id: 3})-[r]-() DELETE n, r RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH (n:D {id: 5}) DELETE n WITH 1 AS one MATCH (:D {id: 6})-[r]-() DELETE r RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
	} {
		result, err := run(testCase.query)
		require.NoError(t, err, testCase.query)
		require.Equal(t, testCase.want, result.Rows, testCase.query)
	}
	result, err := run("MATCH (n:D) RETURN n.id AS id ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}, {int64(7)}, {int64(8)}}, result.Rows)

	// Still connected at commit: the statement fails and changes nothing.
	_, err = run("MATCH (n:D {id: 7}) DELETE n RETURN count(*) AS c")
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)
	result, err = run("MATCH (n:D {id: 7})-[r]->() RETURN count(r) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// In an explicit transaction the check is at COMMIT, after every statement.
func TestDeleteConnectedNodeExplicitTransaction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_connected_tx"))
	ctx := context.Background()
	run := func(query string) (*ExecuteResult, error) { return exec.Execute(ctx, query, nil) }
	_, err := run("CREATE (:D {id: 1})-[:R]->(:D {id: 2}), (:D {id: 3})-[:R]->(:D {id: 4})")
	require.NoError(t, err)

	for _, query := range []string{"BEGIN", "MATCH (n:D {id: 1}) DELETE n", "MATCH (:D {id: 2})-[r]-() DELETE r", "COMMIT"} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	for _, query := range []string{"BEGIN", "MATCH (n:D {id: 3}) DELETE n"} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	// Deleted for MATCH, still the end of its relationship.
	result, err := run("MATCH (n:D {id: 3}) RETURN count(n) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	result, err = run("MATCH (:D {id: 4})<-[r]-() RETURN count(r) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = run("COMMIT")
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)

	result, err = run("MATCH (n:D) RETURN n.id AS id ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}}, result.Rows)
}
