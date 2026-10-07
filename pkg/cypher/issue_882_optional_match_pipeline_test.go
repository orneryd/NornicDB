package cypher

// MATCH … OPTIONAL MATCH runs only in the clause pipeline (#898): the legacy
// compound OPTIONAL MATCH handler and its helpers are gone. The behaviours
// their tests pinned are checked here through Execute, with Neo4j 5.26.30's
// answers, and a form the pipeline declines is a SyntaxError.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIssue882OptionalMatchRunsInThePipeline(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			for _, setup := range []string{
				"CREATE INDEX person_id FOR (p:Person) ON (p.id)",
				"CREATE (:Person {id: 1, team: 'a'}), (:Person {id: 2, team: 'b'}), (:Person {id: 3, tags: [1, 2]})",
				"CREATE (:T {name: 'T1'})-[:R]->(:C {name: 'C1'}), (:T {name: 'T2'})",
				"CREATE (:T {name: 'T3'})-[:BLOCKED_BY]->(:C {name: 'C3', met: false}), (:T {name: 'T4'})-[:BLOCKED_BY]->(:C {name: 'C4', met: true})",
			} {
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err, setup)
			}
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				// The seed keeps the pattern's own properties when the WHERE uses
				// an indexed property (#821).
				{"MATCH (p:Person {team: 'a'}) WHERE p.id IN [1, 2] OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id AS id ORDER BY id", [][]interface{}{{int64(1)}}},
				{"MATCH (p:Person {tags: [1, 2]}) WHERE p.id IN [1, 3] OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id AS id ORDER BY id", [][]interface{}{{int64(3)}}},
				{"MATCH (p:Person {team: null}) WHERE p.id IN [1, 2, 3] OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.id AS id ORDER BY id", [][]interface{}{}},
				// count() skips the null target of an unmatched OPTIONAL MATCH.
				{"MATCH (t:T) WHERE t.name IN ['T1', 'T2'] OPTIONAL MATCH (t)-[:R]->(c:C) WITH t, count(c) AS n RETURN t.name AS name, n ORDER BY name", [][]interface{}{{"T1", int64(1)}, {"T2", int64(0)}}},
				// The OPTIONAL MATCH's WHERE sees its target variable.
				{"MATCH (t:T) WHERE t.name IN ['T3', 'T4'] OPTIONAL MATCH (t)-[:BLOCKED_BY]->(c:C) WHERE c.met = false RETURN t.name AS t, c.name AS c ORDER BY t", [][]interface{}{{"T3", "C3"}, {"T4", nil}}},
			} {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
		})
	}
}

// The OPTIONAL MATCH plan binds the MATCH's lists and paths as values, and
// a statement the pipeline cannot run is a SyntaxError. Expected rows and
// codes are Neo4j 5.26.30's.
func TestIssue882OptionalMatchPlanValuesAndRejections(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'}), (:P {name: 'c'})-[:KNOWS]->(:P {name: 'd'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:P) OPTIONAL MATCH (a)-[r:KNOWS*1..2]->(b) RETURN a.name, size(r) AS hops, [x IN r | type(x)] AS types ORDER BY a.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"a", int64(1), []interface{}{"KNOWS"}},
		{"b", nil, nil},
		{"c", int64(1), []interface{}{"KNOWS"}},
		{"d", nil, nil},
	}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH ()-[:KNOWS]->(), ()-[:KNOWS]->() OPTIONAL MATCH (x:P {name: 'd'}) RETURN x.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"d"}, {"d"}}, result.Rows)

	for _, tc := range []struct{ query, code string }{
		// UNWIND needs AS.
		{"MATCH (a:P) OPTIONAL MATCH (a)-->(b) UNWIND a.name RETURN 1", "Neo.ClientError.Statement.SyntaxError"},
		// An OPTIONAL MATCH clause's own errors reach the caller.
		{"MATCH (a:P) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.name AND true RETURN a.name", "Neo.ClientError.Statement.TypeError"},
		{"MATCH (a:P) OPTIONAL MATCH (a)-[:KNOWS*1..2]->(b {k: 1 / 0}) RETURN a.name", "Neo.ClientError.Statement.ArithmeticError"},
	} {
		_, err := exec.Execute(ctx, tc.query, nil)
		require.Error(t, err, tc.query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, tc.code, code, tc.query)
	}
}

// executeMatch has no executor for an embedded OPTIONAL MATCH with a
// relationship pattern; it rejects it instead of dropping the relationship
// variable.
func TestIssue882EmbeddedOptionalMatchIsRejected(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "MATCH (n:T) OPTIONAL MATCH (n)-[r:R]->(c) RETURN n, r, c", getParamsFromContext(ctx))
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}
