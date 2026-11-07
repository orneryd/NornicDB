package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// conditionalFrameExecutor creates the two rows the conditional-arm cases read
// and returns the executor that runs them.
func conditionalFrameExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "conditional_frame"))
	_, err := exec.Execute(context.Background(), "CREATE (:FrameCondition {name: 'Ann', age: 40}), (:FrameCondition {name: 'Bob', age: 20})", nil)
	require.NoError(t, err)
	return exec
}

// A conditional's arm is compiled in its own frame: the clauses before the
// conditional, the enclosing subquery's imports and its sibling arms' absence.
// Its columns are the admitted compile-only candidate's own, so an arm that
// reads a variable from that frame names its columns instead of reporting the
// variable as undefined. Each case runs on both parsers and both normalization
// settings through the public Execute path.
func TestConditionalArmsCompileInTheirFrame(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := conditionalFrameExecutor(t)
		l := func(values ...interface{}) []interface{} { return values }
		for _, tc := range []struct {
			name    string
			query   string
			columns []string
			rows    [][]interface{}
		}{
			{
				name:    "named imports",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN p.name AS k ELSE RETURN p.name + '!' AS k } RETURN p.name AS n, k ORDER BY n",
				columns: []string{"n", "k"},
				rows:    [][]interface{}{l("Ann", "Ann"), l("Bob", "Bob!")},
			},
			{
				name:    "all imports",
				query:   "MATCH (p:FrameCondition) CALL (*) { WHEN p.age > 35 THEN RETURN p.name AS k ELSE RETURN p.name + '!' AS k } RETURN k ORDER BY k",
				columns: []string{"k"},
				rows:    [][]interface{}{l("Ann"), l("Bob!")},
			},
			{
				name:    "no imports",
				query:   "CALL () { WHEN 1 = 1 THEN RETURN 'a' AS k ELSE RETURN 'b' AS k } RETURN k",
				columns: []string{"k"},
				rows:    [][]interface{}{l("a")},
			},
			{
				name:    "several columns read the frame",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN p.name AS x, p.age AS y ELSE RETURN p.name AS x, p.age * 2 AS y } RETURN x, y ORDER BY x",
				columns: []string{"x", "y"},
				rows:    [][]interface{}{l("Ann", int64(40)), l("Bob", int64(40))},
			},
			{
				name:    "three arms",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN 'old' AS k WHEN p.age < 25 THEN RETURN 'young' AS k ELSE RETURN 'mid' AS k } RETURN k ORDER BY k",
				columns: []string{"k"},
				rows:    [][]interface{}{l("old"), l("young")},
			},
			{
				name:    "next after a conditional",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN p.name AS a ELSE RETURN p.name AS a NEXT RETURN a AS b } RETURN b ORDER BY b",
				columns: []string{"b"},
				rows:    [][]interface{}{l("Ann"), l("Bob")},
			},
			{
				name:    "nested conditional in the ancestor's else arm",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN p.name AS k ELSE { WHEN p.name = 'Bob' THEN RETURN p.name + '?' AS k ELSE RETURN p.name AS k } } RETURN k ORDER BY k",
				columns: []string{"k"},
				rows:    [][]interface{}{l("Ann"), l("Bob?")},
			},
			{
				name:    "count subquery",
				query:   "MATCH (p:FrameCondition) RETURN p.name AS n, COUNT { WHEN p.age > 35 THEN RETURN 1 AS x ELSE RETURN 1 AS x } AS c ORDER BY n",
				columns: []string{"n", "c"},
				rows:    [][]interface{}{l("Ann", int64(1)), l("Bob", int64(1))},
			},
			{
				name:    "exists subquery",
				query:   "MATCH (p:FrameCondition) RETURN p.name AS n, EXISTS { WHEN p.age > 35 THEN RETURN 1 AS x ELSE RETURN 1 AS x } AS e ORDER BY n",
				columns: []string{"n", "e"},
				rows:    [][]interface{}{l("Ann", true), l("Bob", true)},
			},
			{
				name:    "aggregate arms",
				query:   "MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN RETURN count(p) AS c ELSE RETURN count(p) AS c } RETURN c",
				columns: []string{"c"},
				// Only the selected arm executes, including its aggregate.
				rows: [][]interface{}{l(int64(1)), l(int64(1))},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), "CYPHER 25 "+tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.columns, result.Columns)
				require.ElementsMatch(t, tc.rows, result.Rows, tc.query)
			})
		}
	})
}

// An arm that reads a name its frame does not bind is a terminal error from the
// candidate's scope owner, and it happens before the statement's effects.
func TestConditionalArmInvalidScopeHasNoEffect(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := conditionalFrameExecutor(t)
		_, err := exec.Execute(context.Background(),
			"CYPHER 25 MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN CREATE (:FrameMutation) RETURN nope AS k ELSE CREATE (:FrameMutation) RETURN 1 AS k } RETURN k", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nope")
		count, err := exec.Execute(context.Background(), "MATCH (m:FrameMutation) RETURN count(m) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, count.Rows)
	})
}

// The conditional's own diagnostics stay the conditional owner's: arms that
// disagree and an arm that names an unknown procedure fail as before.
func TestConditionalArmDiagnostics(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := conditionalFrameExecutor(t)
		_, err := exec.Execute(context.Background(), "CYPHER 25 WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y", nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		require.Contains(t, err.Error(), "same return column names")

		_, err = exec.Execute(context.Background(),
			"CYPHER 25 MATCH (p:FrameCondition) CALL (p) { WHEN p.age > 35 THEN CALL no.such.procedure() ELSE RETURN 1 AS c } RETURN c", nil)
		requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureNotFound")
	})
}

// The regional read is the columns owner's compile-only view: the columns of
// the clause that ends the region in the admitted bound product, and a region
// that owns no clause is an error rather than an empty header.
func TestRegionalStatementColumnsReadsTheBoundProduct(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := conditionalFrameExecutor(t)
		query := "MATCH (p:FrameCondition) RETURN p.name AS n"
		region := cyphertext.Span{Start: strings.Index(query, "RETURN"), End: len(query)}
		columns, err := exec.regionalStatementColumns(context.Background(), query, region)
		require.NoError(t, err)
		require.Equal(t, []string{"n"}, columns)

		_, err = exec.regionalStatementColumns(context.Background(), query, cyphertext.Span{Start: 0, End: 0})
		require.Error(t, err)

		_, err = exec.regionalStatementColumns(context.Background(), "RETURN nope AS x", cyphertext.Span{Start: 0, End: 13})
		require.Error(t, err)
		require.Contains(t, err.Error(), "nope")
	})
}

// Lowering is unchanged: a conditional still becomes the CASE guard and the
// guarded CALL subquery, and NEXT still becomes the nested CALL.
func TestConditionalLoweringKeepsItsExecutionForm(t *testing.T) {
	exec := conditionalFrameExecutor(t)
	rewritten, rewrite, err := desugarQueryStructure(context.Background(),
		"MATCH (p) CALL (p) { WHEN p.age > 35 THEN RETURN 1 AS k ELSE RETURN 2 AS k }", exec)
	require.NoError(t, err)
	require.NotNil(t, rewrite)
	require.Contains(t, rewritten, "UNWIND [CASE WHEN p.age > 35 THEN 1 ELSE 2 END] AS "+whenBranchVariable)
	require.Contains(t, rewritten, "FILTER "+whenBranchVariable+" = 1 CALL (*) { RETURN 1 AS k } RETURN k UNION ALL FILTER "+whenBranchVariable+" = 2 CALL (*) { RETURN 2 AS k")

	nexted, _, err := desugarQueryStructure(context.Background(), "RETURN 1 AS a NEXT RETURN a + 1 AS b", exec)
	require.NoError(t, err)
	require.Equal(t, "CALL (*) { RETURN 1 AS a } RETURN a + 1 AS b", nexted)
}

// conditionalFrameQuery is a pure in-memory fixture for profiling the
// compile-only candidate views: a statement-level conditional with the given
// number of arms. Every arm returns the same column name, so the arm column
// check passes and what is measured is the views plus their compilation. salt
// only varies the first condition, so a caller can choose between reusing one
// statement text and giving every iteration its own.
func conditionalFrameQuery(branches, salt int) string {
	var query strings.Builder
	fmt.Fprintf(&query, "WHEN %d = %d THEN RETURN 0 AS v ELSE ", salt, salt)
	for index := 1; index < branches; index++ {
		fmt.Fprintf(&query, "WHEN %d = %d THEN RETURN %d AS v ELSE ", index, index, index)
	}
	fmt.Fprintf(&query, "RETURN %d AS v", branches-1)
	return query.String()
}

// BenchmarkConditionalFrameCandidateViews is a profiling fixture only. It
// builds the compile-only candidate view of every arm of conditionals with 2,
// 16, 64 and 256 arms through the rewrite, executing nothing. The statement
// text is the same on every iteration, so the columns owner's program cache
// serves the compilation and the measured cost is the walk that builds each
// view: the source-view work that cache does not cover.
func BenchmarkConditionalFrameCandidateViews(b *testing.B) {
	for _, branches := range []int{2, 16, 64, 256} {
		b.Run(fmt.Sprintf("branches=%d", branches), func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "conditional_frame_bench"))
			query := conditionalFrameQuery(branches, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, _, err := desugarQueryStructure(context.Background(), query, exec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkConditionalFrameCandidateViewsCold measures the same views when
// every iteration has its own statement text, so the columns owner's cache
// misses and the arm programs' compilation is part of the cost.
func BenchmarkConditionalFrameCandidateViewsCold(b *testing.B) {
	for _, branches := range []int{2, 16, 64, 256} {
		b.Run(fmt.Sprintf("branches=%d", branches), func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "conditional_frame_bench_cold"))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				query := conditionalFrameQuery(branches, index)
				if _, _, err := desugarQueryStructure(context.Background(), query, exec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
