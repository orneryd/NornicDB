package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A name that ends in a keyword isn't the keyword: reason or season before
// MATCH or CREATE isn't MERGE's ON, nonoptional before MATCH isn't OPTIONAL,
// friends before WITH isn't ENDS, undetach before DELETE isn't DETACH, and a
// script ending in autocommit doesn't end with COMMIT (keywordsBefore).
func TestNameEndingInAKeywordIsNoKeyword(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "word_boundary"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {k: 'a'}), (:D)-[:R]->(:D)", nil)
	require.NoError(t, err)
	for _, test := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL db.labels() YIELD label AS reason MATCH (t:P) RETURN reason, t.k", [][]interface{}{{"D", "a"}, {"P", "a"}}},
		{"WITH 1 AS season MATCH (t:P) RETURN season, t.k", [][]interface{}{{int64(1), "a"}}},
		{"UNWIND [1] AS person MATCH (t:P) RETURN person, t.k", [][]interface{}{{int64(1), "a"}}},
		{"WITH 1 AS xon CREATE (t:Q) RETURN xon", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS nonoptional MATCH (t:Missing) RETURN nonoptional", [][]interface{}{}},
		{"WITH 'x' AS friends WITH friends + 'y' AS v RETURN v", [][]interface{}{{"xy"}}},
		{"WITH 'x' AS s WHERE s ENDS WITH 'x' RETURN s", [][]interface{}{{"x"}}},
		// MERGE's own ON CREATE and ON MATCH.
		{"MERGE (m:M {k: 1}) ON CREATE SET m.c = 1 ON MATCH SET m.m = 1 RETURN m.c, m.m", [][]interface{}{{int64(1), nil}}},
		{"MERGE (m:M {k: 1}) ON CREATE SET m.c = 2 ON MATCH SET m.m = 1 RETURN m.c, m.m", [][]interface{}{{int64(1), int64(1)}}},
	} {
		result, err := exec.Execute(ctx, test.query, nil)
		require.NoError(t, err, test.query)
		require.ElementsMatch(t, test.want, result.Rows, test.query)
	}

	_, err = exec.Execute(ctx, "MATCH (a:D)-[:R]->() WITH a, 1 AS undetach DELETE a", nil)
	require.Error(t, err, "a node with relationships needs DETACH DELETE")

	_, _, ok := splitTransactionScriptTailAction("RETURN 1 AS autocommit")
	require.False(t, ok)
	body, action, ok := splitTransactionScriptTailAction("RETURN 1 AS x COMMIT")
	require.True(t, ok)
	require.Equal(t, "RETURN 1 AS x", body)
	require.Equal(t, "COMMIT", action)
}
