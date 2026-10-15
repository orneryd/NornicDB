package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// On the system database a SHOW command's tail filters, orders, pages and
// projects the command's columns and reads no graph, as Neo4j 5.26.30
// applies it there: another clause after YIELD, a COUNT / EXISTS / COLLECT
// subquery, a pattern comprehension and a pattern expression are each
// Neo4j's SyntaxError, through the one statement policy.
func TestSystemShowTailError(t *testing.T) {
	for statement, message := range map[string]string{
		"SHOW DATABASES YIELD name RETURN name, COUNT { (n) } AS c":                  "The COUNT expression is not valid on SHOW commands.",
		"SHOW DATABASES WHERE EXISTS { (n) }":                                        "The EXISTS expression is not valid on SHOW commands.",
		"SHOW DATABASES YIELD name RETURN size(COLLECT { MATCH (n) RETURN n }) AS c": "The COLLECT expression is not valid on SHOW commands.",
		"SHOW DATABASES YIELD name RETURN size([(a)-->(b) | a]) AS c":                "Pattern comprehensions are not valid on SHOW commands.",
		"SHOW DATABASES YIELD name WHERE ()-->() RETURN name":                        "Pattern expressions are not valid on SHOW commands.",
		"SHOW DATABASES YIELD name RETURN shortestPath((a)-[*]->(b)) AS p":           "Pattern expressions are not valid on SHOW commands.",
		"SHOW DATABASES YIELD name WITH name RETURN name":                            "Invalid input 'WITH'",
		"SHOW DATABASES YIELD name OPTIONAL MATCH (n) RETURN name":                   "Invalid input 'OPTIONAL'",
		"SHOW DATABASES YIELD name CREATE (:X) RETURN name":                          "Invalid input 'CREATE'",
		"EXPLAIN SHOW DATABASES YIELD name RETURN name, COUNT { (n) } AS c;":         "The COUNT expression is not valid on SHOW commands.",
	} {
		err := systemDatabaseStatementError(statement)
		require.Error(t, err, statement)
		require.Contains(t, err.Error(), message, statement)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err), statement)
	}
	for _, statement := range []string{
		"SHOW DATABASES",
		"SHOW DATABASES;",
		"SHOW DATABASES YIELD name ORDER BY name SKIP 0 LIMIT 5 WHERE name <> '' RETURN name",
		"SHOW DATABASES YIELD name RETURN name, [x IN [1, 2] | x] AS xs, count(*) AS c",
		"SHOW USERS YIELD user WHERE user STARTS WITH 'a' RETURN user",
		"SHOW TRANSACTIONS YIELD transactionId WHERE transactionId = 'x' TERMINATE TRANSACTIONS transactionId",
	} {
		require.NoError(t, systemDatabaseStatementError(statement), statement)
	}
}

// errorStatusCode is the Neo4j status code an error carries, or "".
func errorStatusCode(err error) string {
	var coded interface{ BoltErrorCode() string }
	for current := err; current != nil; {
		if c, ok := current.(interface{ BoltErrorCode() string }); ok {
			coded = c
			break
		}
		unwrapper, ok := current.(interface{ Unwrap() error })
		if !ok {
			break
		}
		current = unwrapper.Unwrap()
	}
	if coded == nil {
		return ""
	}
	return coded.BoltErrorCode()
}

// A pattern comprehension needs a relationship, as Cypher's grammar says:
// [(n) | …] is a SyntaxError on both parsers, wherever it stands (in a SHOW
// tail too), and one with a relationship runs on both.
func TestSingleNodePatternComprehensionIsASyntaxError(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	_, err := exec.Execute(ctx, "CREATE (:U {x: 1})-[:R]->(:U {x: 2})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"RETURN [(n:U) | n.x] AS xs",
		"RETURN [(n:U) WHERE n.x > 1 | n.x] AS xs",
		"SHOW PROCEDURES YIELD name WITH name LIMIT 1 RETURN [(u:U) | u.x] AS xs",
		"SHOW PROCEDURES YIELD name RETURN name, [(u:U) | u.x] AS xs LIMIT 1",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err), query)
	}
	for query, want := range map[string]interface{}{
		"RETURN [(a:U)-->(b) | b.x] AS xs":                                              []interface{}{int64(2)},
		"SHOW PROCEDURES YIELD name WITH name LIMIT 1 RETURN [(a:U)-->(b) | b.x] AS xs": []interface{}{int64(2)},
		"RETURN [x IN [1, 2] | x] AS xs":                                                []interface{}{int64(1), int64(2)},
		"RETURN [(1)] AS xs":                                                            []interface{}{int64(1)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}
