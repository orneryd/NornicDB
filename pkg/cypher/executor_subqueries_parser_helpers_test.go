package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallSubqueryStringParsers(t *testing.T) {
	parts := splitTopLevelAndConjuncts("a = 1 AND b = 'x AND y' AND c = [1,2]")
	require.Equal(t, []string{"a = 1", "b = 'x AND y'", "c = [1,2]"}, parts)

	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("a = b")
	require.True(t, ok)
	require.Equal(t, "a", lhs)
	require.Equal(t, "b", rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("a = {k: 'v=v'}")
	require.True(t, ok)
	_, _, ok = splitTopLevelEqualityCallSubquery("a")
	require.False(t, ok)

	idx := indexTopLevelKeywordCallSubquery("x = 'ORDER BY y' ORDER BY n.name", "ORDER BY")
	require.GreaterOrEqual(t, idx, 0)
	require.Equal(t, "ORDER BY n.name", "x = 'ORDER BY y' ORDER BY n.name"[idx:])

	idx = indexTopLevelKeywordCallSubquery("RETURN n", "ORDER BY")
	require.Equal(t, -1, idx)
}

func TestCallSubqueryIdentifierHelpers(t *testing.T) {
	require.True(t, containsStandaloneIdentifier("x = n AND y = 1", "n"))
	require.False(t, containsStandaloneIdentifier("obj.n = 1", "n"))
	require.False(t, containsStandaloneIdentifier("'n'", "n"))
	require.False(t, containsStandaloneIdentifier("prefix_n", "n"))

	expanded := expandMapMemberAccess("WITH m RETURN m.name, m.missing", "m", map[string]interface{}{"name": "Alice"})
	require.Equal(t, "WITH m RETURN 'Alice', null", expanded)

	require.Equal(t, "RETURN x", expandMapMemberAccess("RETURN x", "", map[string]interface{}{"k": "v"}))
}

func TestSplitTopLevelResultModifiersCallSubquery(t *testing.T) {
	proj, mods := splitTopLevelResultModifiersCallSubquery("n.name AS name ORDER BY name SKIP 1 LIMIT 2")
	require.Equal(t, "n.name AS name", proj)
	require.Equal(t, "ORDER BY name SKIP 1 LIMIT 2", mods)

	proj, mods = splitTopLevelResultModifiersCallSubquery("n.name AS name")
	require.Equal(t, "n.name AS name", proj)
	require.Empty(t, mods)
}

func TestCallSubqueryWriteAndIdentifierChecks(t *testing.T) {
	require.True(t, callSubqueryQueryIsWrite("MATCH (n) CREATE (m)"))
	require.True(t, callSubqueryQueryIsWrite("MATCH (n) SET n.x = 1"))
	require.False(t, callSubqueryQueryIsWrite("MATCH (n) RETURN n"))

	require.True(t, isSimpleIdentifier("name_1"))
	require.True(t, isSimpleIdentifier("`quoted`"))
	require.False(t, isSimpleIdentifier("1bad"))
	require.False(t, isSimpleIdentifier("bad-char"))
}

func TestCallSubqueryImportNullGuardTerm(t *testing.T) {
	require.True(t, isCallSubqueryImportNotNullGuardTerm("seed IS NOT NULL", "seed"))
	require.True(t, isCallSubqueryImportNotNullGuardTerm("`seed` is not null", "seed"))
	require.False(t, isCallSubqueryImportNotNullGuardTerm("seed IS NULL", "seed"))
	require.False(t, isCallSubqueryImportNotNullGuardTerm("other IS NOT NULL", "seed"))
}
