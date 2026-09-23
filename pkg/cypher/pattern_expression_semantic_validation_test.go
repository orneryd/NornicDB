package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPatternPredicatesRequireExistingBindings(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, query := range []string{
		"MATCH (n) WHERE (n)-[r]->() RETURN n",
		"MATCH (n) WHERE (n)-[]->(a) RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSemanticPatternDetail(t, err, "UndefinedVariable")
	}
	_, err := exec.Execute(ctx, "MATCH (n) WHERE (n) RETURN n", nil)
	requireSemanticPatternDetail(t, err, "InvalidArgumentType")
}

func TestPatternExpressionsAreRejectedOutsideWhere(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, query := range []string{
		"MATCH (n) RETURN (n)-[]->()",
		"MATCH (n) WITH (n)-[]->() AS connected RETURN connected",
		"MATCH (n) SET n.value = head(nodes(head((n)-[:REL]->()))).value",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSemanticPatternDetail(t, err, "UnexpectedSyntax")
	}
}

func TestPatternComprehensionsRemainValidProjectionExpressions(t *testing.T) {
	require.False(t, containsIllegalProjectedPatternExpression("[p = (n)-->() | p]"))
	require.False(t, containsIllegalProjectedPatternExpression("[x IN nodes(p) | size([(x)-->(:Y) | 1])]"))
	require.True(t, containsIllegalProjectedPatternExpression("(n)-[]->()"))
}

func TestWithProjectionRequiresUniqueNamedExpressions(t *testing.T) {
	requireSemanticPatternDetail(t, validateWithProjectionSemantics("WITH 1 AS a, 2 AS a RETURN a"), "ColumnNameConflict")
	requireSemanticPatternDetail(t, validateWithProjectionSemantics("MATCH (a) WITH a, count(*) RETURN a"), "NoExpressionAlias")
	require.NoError(t, validateWithProjectionSemantics("MATCH (a) WITH a, count(*) AS count RETURN a, count"))
}

func TestVariableLengthPatternComprehensionCorrelatesBoundEndpoints(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A)-[:T]->(:B)", nil)
	require.NoError(t, err)
	direct, err := exec.Execute(ctx, "MATCH p = (a:A)-[*]->(b:B) RETURN a, b, p", nil)
	require.NoError(t, err)
	require.Len(t, direct.Rows, 1)
	directComprehension, err := exec.Execute(ctx, "MATCH (a:A), (b:B) RETURN [p = (a)-[*]->(b) | p] AS paths", nil)
	require.NoError(t, err)
	require.Len(t, directComprehension.Rows, 1)
	require.Len(t, directComprehension.Rows[0][0], 1)

	result, err := exec.Execute(ctx, `
		MATCH (a:A), (b:B)
		WITH [p = (a)-[*]->(b) | p] AS paths, count(a) AS c
		RETURN paths, c
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, int64(1), result.Rows[0][1])
	paths, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.Len(t, paths, 1)
	path := requireReturnedPath(t, paths[0])
	require.Len(t, path.Nodes, 2)
	require.Len(t, path.Relationships, 1)
}

func requireSemanticPatternDetail(t *testing.T, err error, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, detail, semanticError.Detail)
}
