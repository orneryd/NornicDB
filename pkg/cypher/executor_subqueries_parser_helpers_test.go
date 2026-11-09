package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallSubqueryStringParsers(t *testing.T) {
	parts := splitTopLevelAndConjuncts(t.Context(), "a = 1 AND b = 'x AND y' AND c = [1,2]")
	require.Equal(t, []string{"a = 1", "b = 'x AND y'", "c = [1,2]"}, parts)
}

func TestCallSubqueryIdentifierHelpers(t *testing.T) {
	require.True(t, cyphertext.FindIdentifier("x = n AND y = 1", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("obj.n = 1", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("'n'", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("prefix_n", "n", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)

}

func TestCallSubqueryIdentifierChecks(t *testing.T) {
	require.True(t, isSimpleIdentifier("name_1"))
	require.True(t, isSimpleIdentifier("`quoted`"))
	require.False(t, isSimpleIdentifier("1bad"))
	require.False(t, isSimpleIdentifier("bad-char"))
}

func TestCallSubqueryImportNullGuardTerm(t *testing.T) {
	splitTopLevelAndConjuncts := func(text string) []string { return splitTopLevelAndConjuncts(t.Context(), text) }
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
