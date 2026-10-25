package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	require.True(t, cyphertext.FindIdentifier("x = n AND y = 1", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("obj.n = 1", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("'n'", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("prefix_n", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)

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
	// This eligibility predicate receives decoded identities. Source tokens
	// are admitted and decoded by the shared identifier reader first.
	decoded, _, valid := parseIdentifierToken("`quoted`")
	require.True(t, valid)
	require.True(t, isSimpleIdentifier(decoded))
	require.False(t, isSimpleIdentifier("`quoted`"), "literal backticks in a decoded name are not a plain name")
	require.False(t, isSimpleIdentifier("1bad"))
	require.False(t, isSimpleIdentifier("bad-char"))
}

func TestCallSubqueryImportNullGuardTerm(t *testing.T) {
	for _, term := range []string{"seed IS NOT NULL", "`seed` is not null"} {
		got, ok := cyphertext.FilterImportConjuncts(term, "seed", cyphertext.IdentifierUnqualifiedFold, splitTopLevelAndConjuncts)
		require.True(t, ok)
		require.Empty(t, got)
	}
	_, ok := cyphertext.FilterImportConjuncts("seed IS NULL", "seed", cyphertext.IdentifierUnqualifiedFold, splitTopLevelAndConjuncts)
	require.False(t, ok)
	got, ok := cyphertext.FilterImportConjuncts("other IS NOT NULL", "seed", cyphertext.IdentifierUnqualifiedFold, splitTopLevelAndConjuncts)
	require.True(t, ok)
	require.Equal(t, "other IS NOT NULL", got)
}
