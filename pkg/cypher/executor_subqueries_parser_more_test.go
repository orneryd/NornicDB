package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubqueryParserHelpers_MoreBranches(t *testing.T) {
	parts := splitTopLevelAndConjuncts("a = 1 AND b = '(x AND y)' AND c = [1, {k: 'A AND B'}] AND `weird` = 1")
	require.Equal(t, []string{"a = 1", "b = '(x AND y)'", "c = [1, {k: 'A AND B'}]", "`weird` = 1"}, parts)

	parts = splitTopLevelAndConjuncts("'x AND y'")
	require.Equal(t, []string{"'x AND y'"}, parts)

	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("`a` = " + `"x=y"`)
	require.True(t, ok)
	require.Equal(t, "`a`", lhs)
	require.Equal(t, `"x=y"`, rhs)

	lhs, rhs, ok = splitTopLevelEqualityCallSubquery("m = {k: [1,2,3]}")
	require.True(t, ok)
	require.Equal(t, "m", lhs)
	require.Equal(t, "{k: [1,2,3]}", rhs)

	// Top-level equality should ignore '=' inside nested containers/quoted strings.
	lhs, rhs, ok = splitTopLevelEqualityCallSubquery("x = [1, {k: 'a=b'}, 3]")
	require.True(t, ok)
	require.Equal(t, "x", lhs)
	require.Equal(t, "[1, {k: 'a=b'}, 3]", rhs)

	lhs, rhs, ok = splitTopLevelEqualityCallSubquery(`cfg = {"k":[1,2,3],"s":"a=b"}`)
	require.True(t, ok)
	require.Equal(t, "cfg", lhs)
	require.Equal(t, `{"k":[1,2,3],"s":"a=b"}`, rhs)

	lhs, rhs, ok = splitTopLevelEqualityCallSubquery("arr = [func(1), {x: [2,3]}]")
	require.True(t, ok)
	require.Equal(t, "arr", lhs)
	require.Equal(t, "[func(1), {x: [2,3]}]", rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("(x = y)")
	require.False(t, ok)

	_, _, ok = splitTopLevelEqualityCallSubquery("`a=b`")
	require.False(t, ok)

	_, _, ok = splitTopLevelEqualityCallSubquery("a = ")
	require.False(t, ok)

	require.True(t, containsStandaloneIdentifier("x + y + z", "y"))
	require.False(t, containsStandaloneIdentifier("node.y = 1", "y"))
	require.False(t, containsStandaloneIdentifier("'y'", "y"))
	require.False(t, containsStandaloneIdentifier("yy + 1", "y"))
}
