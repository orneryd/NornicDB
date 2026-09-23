package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhereRejectsPropertyAccessOnPath(t *testing.T) {
	executor := &StorageExecutor{}
	err := executor.validateMatchSemanticScopes(`
		MATCH (node)
		MATCH path = (node)-[*]->()
		WHERE path.name = 'value'
		RETURN path
	`)
	requireSemanticDetail(t, err, "InvalidArgumentType")
}

func TestWhereRejectsAggregation(t *testing.T) {
	executor := &StorageExecutor{}
	err := executor.validateMatchSemanticScopes(`
		MATCH (node)
		WHERE count(node) > 1
		RETURN node
	`)
	requireSemanticDetail(t, err, "InvalidAggregation")
}

func TestWithOrderBySemanticScopeTracksPriorHorizons(t *testing.T) {
	executor := &StorageExecutor{}
	requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(
		"WITH 1 AS a, 2 AS b, 3 AS c WITH a, b WITH a ORDER BY a, c RETURN a",
	), "UndefinedVariable")
	requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(
		"MATCH (a) WITH a, a AS b WITH a ORDER BY c RETURN a",
	), "UndefinedVariable")
	requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(
		"MATCH (a) WITH a, a AS b WITH a ORDER BY d DESC RETURN a",
	), "UndefinedVariable")
	require.NoError(t, executor.validateMatchSemanticScopes(
		"MATCH (a) WITH a, a AS b WITH a ORDER BY b RETURN a",
	))
}

func TestWithOrderByCannotIntroduceAggregation(t *testing.T) {
	executor := &StorageExecutor{}
	for _, query := range []string{
		"MATCH (n) WITH n.num1 AS foo ORDER BY count(1) RETURN foo",
		"MATCH (n) WITH n.num1 AS foo ORDER BY n.name, max(n.num2) DESC RETURN foo",
	} {
		requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(query), "InvalidAggregation")
	}
}

func TestWithOrderByAggregationUsesProjectedGroupingScope(t *testing.T) {
	executor := &StorageExecutor{}
	requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(
		"MATCH (a) WITH a.num2 % 3 AS mod, min(a.num + a.num2) AS minimum ORDER BY sum(a.num + a.num2) RETURN mod",
	), "UndefinedVariable")
	requireSemanticPatternDetail(t, executor.validateMatchSemanticScopes(
		"MATCH (me)--(you) WITH me.age + you.age AS ages, count(*) AS cnt ORDER BY me.age + you.age + count(*) RETURN ages",
	), "AmbiguousAggregationExpression")
	require.NoError(t, executor.validateMatchSemanticScopes(
		"MATCH (a) WITH a.num2 % 3 AS mod, sum(a.num + a.num2) AS total ORDER BY sum(a.num + a.num2) RETURN mod",
	))
}
