package cypher

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// testCallBodySource admits one CALL body's own source for the readers that take
// a retained body. Admission is the test input boundary: every reader below
// takes the product it returned, never the text.
func testCallBodySource(t *testing.T, source string) queryBody {
	t.Helper()
	body := readQueryBody(t.Context(), source, 0, len(source))
	require.Equal(t, queryBodyRead, body.status, source)
	require.NotEmpty(t, body.branches, source)
	return body
}

// An unscoped CALL body's imports are its retained branches' leading WITH
// items: each UNION branch decides for itself, a branch that imports nothing
// adds no name, and collecting is the caller's choice.
func TestUnscopedCallImportsReadRetainedBranches(t *testing.T) {
	outer := func(name string) bool { return name == "v" }
	for _, testCase := range []struct {
		body      string
		importing bool
		names     []string
	}{
		{"RETURN 1 AS x", false, nil},
		{"WITH v RETURN v AS x", true, []string{"v"}},
		{"WITH *, v RETURN v AS x", true, []string{"v"}},
		{"WITH 2 AS x RETURN x", false, nil},
		{"WITH v, size([1, 2]) AS n RETURN n AS x", true, []string{"v", "n"}},
		{"WITH v RETURN v AS x UNION RETURN 2 AS x", true, []string{"v"}},
		{"RETURN 2 AS x UNION WITH v RETURN v AS x", true, []string{"v"}},
		{"WITH 2 AS x RETURN x UNION WITH 3 AS y RETURN y", false, nil},
	} {
		names, importing, err := unscopedCallImports(t.Context(), testCallBodySource(t, testCase.body), outer, true)
		require.NoError(t, err, testCase.body)
		require.Equal(t, testCase.importing, importing, testCase.body)
		require.Equal(t, testCase.names, names, testCase.body)
	}
	names, importing, err := unscopedCallImports(t.Context(), testCallBodySource(t, "WITH v RETURN v AS x"), outer, false)
	require.NoError(t, err)
	require.True(t, importing)
	require.Empty(t, names)
}

// The canonical branch reader reports a cancelled statement's own error rather
// than an import verdict.
func TestUnscopedCallImportsReportCancellation(t *testing.T) {
	body := testCallBodySource(t, "WITH v RETURN v AS x")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := unscopedCallImports(ctx, body, func(string) bool { return true }, true)
	require.ErrorIs(t, err, context.Canceled)
}

// A branch's WITH and UNWIND bindings are read from their grammar tokens, so a
// binding whose AS is separated by a newline or a comment declares the same name
// a printed binding does. A name the clause reads before that binding (or only
// in its tail) is not a declaration: the branch keeps the outer value there.
func TestBranchDeclaresVariableReadsBindingTokens(t *testing.T) {
	for _, testCase := range []struct {
		branch, variable string
		declares         bool
	}{
		{"WITH 2 AS n RETURN n", "n", true},
		{"WITH 2 AS\nn RETURN n", "n", true},
		{"WITH 2 AS /*declared*/ n RETURN n", "n", true},
		{"WITH 2 AS `n` RETURN `n`", "n", true},
		{"UNWIND [1] AS n RETURN n", "n", true},
		{"UNWIND [1] AS\nn RETURN n", "n", true},
		{"UNWIND [1] AS /*bound*/ n RETURN n", "n", true},
		// Read before the binding: the outer value stays visible.
		{"WITH n + 1 AS n RETURN n", "n", false},
		{"UNWIND [n] AS n RETURN n", "n", false},
		{"WITH n AS m RETURN m", "n", false},
		// Mentioned only after the clause's bindings.
		{"WITH 1 AS x WHERE n IS NOT NULL RETURN x", "n", false},
		{"WITH 1 AS x ORDER BY n RETURN x", "n", false},
		{"WITH 1 AS x SKIP n RETURN x", "n", false},
		// A pattern the branch declares itself is still decided by the pattern.
		{"MATCH (n)-->(m) RETURN m", "n", true},
		{"MATCH (m) WHERE m.id = n.id RETURN m", "n", false},
	} {
		require.Equal(t, testCase.declares, branchDeclaresVariable(t.Context(), testRetainedBranches(t, testCase.branch)[0], testCase.variable), testCase.branch)
	}
	require.Equal(t, []string{"n"}, unscopedBranchLocals(t.Context(), testRetainedBranches(t, "UNWIND [1] AS /*bound*/ n RETURN n")[0], []string{"n", "k"}))
}

// The public path reaches the same readers: an unscoped body imports the names
// its leading WITH lists, each UNION branch decides for itself, and a binding
// whose AS is separated by a newline or a comment declares the branch's own
// variable. A name the branch reads before binding it keeps the outer value.
func TestUnscopedCallImportControls(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CQ {id: 1})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"WITH 1 AS v CALL { WITH v RETURN v AS x } RETURN x", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS v CALL { WITH *, v RETURN v AS x } RETURN x", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS v CALL { RETURN 2 AS x UNION WITH v RETURN v AS x } RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"WITH 1 AS v CALL { WITH v RETURN v AS x UNION RETURN 2 AS x } RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(2)}}},
		// The branch declares the outer name itself, with AS trivia.
		{"MATCH (n:CQ {id: 1}) CALL { UNWIND [7] AS n RETURN n AS x } RETURN x", [][]interface{}{{int64(7)}}},
		{"MATCH (n:CQ {id: 1}) CALL { UNWIND [7] AS\nn RETURN n AS x } RETURN x", [][]interface{}{{int64(7)}}},
		{"MATCH (n:CQ {id: 1}) CALL { UNWIND [7] AS /*declared*/ n RETURN n AS x } RETURN x", [][]interface{}{{int64(7)}}},
		{"MATCH (n:CQ {id: 1}) CALL { WITH 7 AS /*declared*/ n RETURN n AS x } RETURN x", [][]interface{}{{int64(7)}}},
		// Each UNION branch keeps its own key: the first binds its own n, the
		// second reads the outer one.
		{"MATCH (n:CQ {id: 1}) CALL { UNWIND [7] AS /*declared*/ n RETURN n AS x UNION RETURN n.id AS x } RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(7)}}},
		// The implicit import still reaches a name the branch only reads.
		{"MATCH (n:CQ {id: 1}) CALL { UNWIND [n.id] AS m RETURN m AS x } RETURN x", [][]interface{}{{int64(1)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	// An importing WITH must be a list of plain references: an alias or a tail
	// is Neo4j's InvalidVariableImport, on the first branch, and a body whose
	// only clause is that WITH is rejected where the CALL runs.
	for _, query := range []string{
		"MATCH (n:CQ {id: 1}) CALL { WITH n AS n RETURN n.id AS x } RETURN x",
		"MATCH (n:CQ {id: 1}) CALL { WITH n WHERE n IS NOT NULL RETURN n.id AS x } RETURN x",
		"WITH 1 AS v CALL { WITH v AS w RETURN w AS x } RETURN x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.ErrorContains(t, err, "Importing WITH must contain only simple references to outside variables", query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	_, err = exec.Execute(ctx, "WITH 1 AS v CALL { WITH v } RETURN v", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}

// Whole-statement syntax admission precedes semantic import validation. An
// unfinished outer RETURN reports the actual EOF, without admitting a repaired
// CALL-body prefix as a different statement.
func TestUnscopedImportFormsOnRefusedBodies(t *testing.T) {
	exec := newGh648Executor(t)
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"CALL { WITH nope RETURN nope AS x } RETURN",
			"CALL { WITH [nope] AS l RETURN l } RETURN",
			"CALL { WITH nope RETURN nope UNION RETURN 2 AS x } RETURN",
			"WITH 1 AS v CALL { WITH v AS w RETURN w AS x } RETURN",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := exec.Execute(t.Context(), query, nil)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				require.ErrorContains(t, err, fmt.Sprintf("line 1:%d unexpected input", len(query)))
			})
		}
	})
}

// A rejected UNION branch cannot erase a valid earlier importing branch and
// turn its bound outer variable into an undefined-variable diagnostic.
func TestUnscopedRefusedUnionKeepsEarlierBranchImports(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, query := range []string{
		"WITH 1 AS v CALL { WITH v RETURN v AS x UNION RETURN } RETURN x",
		"WITH 1 AS v CALL { RETURN 1 AS x UNION WITH v RETURN v AS x UNION RETURN } RETURN x",
	} {
		_, err := exec.Execute(t.Context(), query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		require.NotContains(t, err.Error(), "Variable `v` not defined", "v is imported by a valid branch: %s", query)
	}
}
