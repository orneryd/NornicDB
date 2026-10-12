package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Statements Neo4j 5.26.30 fails to compile, which NornicDB ran or failed
// only on a row (#461, #523, #504, #217, #528): an arrow head that ends no
// relationship, a '<' where an operand starts, and text after a DROP's name.
// The valid look-alikes keep working.
func TestMalformedOperandsAreSyntaxErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "malformed_operands"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:MF {id: 1, v: 2})-[:R]->(:MF {id: 2, v: -3})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:MF) SET n.m = 'x' -> [{matched: 0}]",
		"MATCH (n:MF) WHERE n.id = 99 RETURN labels(n), properties(n) -> [{labels: []}]",
		"RETURN 1 -> 2 AS v",
		"MATCH (a)-[r->(b) RETURN a",
		"MERGE (:MF {v: <expr>})",
		"UNWIND [1] AS i MERGE (:MF {v: <expr>})",
		"MATCH (n:MF) WHERE n.v = <x> RETURN n",
		"CREATE CONSTRAINT okf_x IF NOT EXISTS FOR (n:<Label>) REQUIRE n.k IS UNIQUE",
		"CREATE FULLTEXT INDEX okf_c IF NOT EXISTS FOR (n:<Label>) ON EACH [n.title]",
		"DROP INDEX does_not_exist -> SyntaxError",
		"DROP INDEX vidx / DROP INDEX ftidx",
		"DROP CONSTRAINT nope extra",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	for query, want := range map[string][][]interface{}{
		"MATCH (a:MF)-->(b) RETURN b.id":                                   {{int64(2)}},
		"MATCH (a:MF)-[r:R]->(b) RETURN b.id":                              {{int64(2)}},
		"MATCH (b:MF)<--(a) RETURN a.id":                                   {{int64(1)}},
		"MATCH (b:MF)<-[:R]-(a) RETURN a.id":                               {{int64(1)}},
		"MATCH (n:MF) WHERE n.v<0 RETURN n.id":                             {{int64(2)}},
		"MATCH (n:MF) WHERE n.v <-1 RETURN n.id":                           {{int64(2)}},
		"MATCH (n:MF) WHERE n.v <> 2 RETURN n.id":                          {{int64(2)}},
		"RETURN '->' + '<x>' AS s // a -> b and {v: <x>}":                  {{"-><x>"}},
		"RETURN 1 /* -> <c> */ AS v":                                       {{int64(1)}},
		"MATCH (n:MF {id: 1}) RETURN [x IN [1, 5] WHERE x < n.v | x] AS v": {{[]interface{}{int64(1)}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	// One reader for DROP INDEX / DROP CONSTRAINT names.
	for rest, want := range map[string]struct {
		name     string
		ifExists bool
		ok       bool
	}{
		"idx":              {"idx", false, true},
		"idx IF EXISTS":    {"idx", true, true},
		"IF EXISTS idx":    {"idx", true, true},
		"`a b`":            {"a b", false, true},
		"`a b` if exists":  {"a b", true, true},
		"IF":               {"IF", false, true},
		"idx -> x":         {"", false, false},
		"idx IF EXISTS no": {"", false, false},
		"":                 {"", false, false},
	} {
		name, ifExists, ok := parseDropSchemaName(rest)
		require.Equal(t, want.ok, ok, rest)
		require.Equal(t, want.name, name, rest)
		require.Equal(t, want.ifExists, ifExists, rest)
	}
	_, err = exec.Execute(ctx, "DROP INDEX missing_idx IF EXISTS", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "DROP INDEX missing_idx", nil)
	requireStatusCode(t, err, "Neo.DatabaseError.Schema.IndexDropFailed")
}

// A '<' before a '-' is an incoming arrow only as <-- or <-[; otherwise it
// compares with a negative operand, as in Neo4j 5.26.30 (n.v <-1 is
// n.v < -1; NornicDB matched nothing, or failed "Cannot subtract").
func TestLessThanNegativeOperand(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "less_than_negative"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:LT {id: 1, v: 2}), (:LT {id: 2, v: -3})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (n:LT) WHERE n.v <-1 RETURN collect(n.id) AS ids":    {{[]interface{}{int64(2)}}},
		"MATCH (n:LT) WHERE n.v<-1.5 RETURN collect(n.id) AS ids":   {{[]interface{}{int64(2)}}},
		"MATCH (n:LT) RETURN n.id AS id, n.v <-1 AS lt ORDER BY id": {{int64(1), false}, {int64(2), true}},
		"RETURN 1 <-1 AS a, 1 <- 1 AS b, -2 <-1 AS c":               {{false, false, true}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	require.True(t, incomingArrowAt("(a)<--(b)", 3))
	require.True(t, incomingArrowAt("(a)<-[r]-(b)", 3))
	require.False(t, incomingArrowAt("a <-1", 2))
}

// The two operand checks skip strings and comments, whichever route hands
// them the text.
func TestOperandChecksSkipStringsAndComments(t *testing.T) {
	for _, query := range []string{
		"RETURN 1 // x -> y",
		"RETURN 1 /* {v: <x>} -> */",
		"RETURN '->' AS a, \"{v: <x>}\" AS b",
		"MATCH (a)-->(b), (c)-[:R]->(d) RETURN a",
	} {
		require.NoError(t, validateArrowOperators(query), query)
		require.NoError(t, validatePlaceholderOperands(query), query)
	}
	require.Error(t, validateArrowOperators("RETURN 1 /* ok */ -> 2"))
	require.Error(t, validatePlaceholderOperands("RETURN 1 // ok\n, <x>"))
}

// Each DELETE target is one expression: a word after a complete operand is
// Neo4j 5.26.30's "Invalid input" (DETACH DELETE n COMMIT deleted n and
// ignored the rest, #741); the valid target forms keep working.
func TestDeleteTargetIsOneExpression(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_targets"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DT {id: 1})-[:R]->(:DT {id: 2}), (:DT {id: 6})-[:R]->(:DT {id: 7}), (:DT {id: 3}), (:DT {id: 4}), (:DT {id: 5})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:DT {id: 3}) DETACH DELETE n COMMIT",
		"MATCH (n:DT {id: 3}) DELETE n m",
		// The syntax error comes before the missing parameter, as in Neo4j (#858).
		"UNWIND $ids AS id MATCH (n) WHERE n.id = id AND (n:Code OR n:Document) DETACH DELETE n DetachDelete n Eager Apply",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	for _, query := range []string{
		"MATCH (n:DT {id: 3}) DETACH DELETE n",
		"MATCH (a:DT {id: 6})-[r:R]->(b) DELETE r",
		"MATCH (n:DT) WHERE n.id IN [6, 7] DELETE n",
		"MATCH (n:DT) WHERE n.id IN [4, 5] WITH collect(n) AS ns DETACH DELETE ns[0], ns[1]",
		"MATCH p = (a:DT {id: 1})-->(b) DETACH DELETE p",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	result, err := exec.Execute(ctx, "MATCH (n:DT) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	for target, word := range map[string]string{"n COMMIT": "COMMIT", "a.b c": "c", "f(x) y": "y"} {
		got, ok := trailingWordAfterOperand(target)
		require.True(t, ok, target)
		require.Equal(t, word, got, target)
	}
	for _, target := range []string{"n", "n.list[0]", "head(collect(n))", "CASE WHEN x THEN a ELSE b END", "x IS NULL", "'a b'"} {
		_, ok := trailingWordAfterOperand(target)
		require.False(t, ok, target)
	}
}
