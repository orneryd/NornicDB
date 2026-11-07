package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubqueryParserHelpers_MoreBranches(t *testing.T) {
	parts := splitTopLevelAndConjuncts(t.Context(), "a = 1 AND b = '(x AND y)' AND c = [1, {k: 'A AND B'}] AND `weird` = 1")
	require.Equal(t, []string{"a = 1", "b = '(x AND y)'", "c = [1, {k: 'A AND B'}]", "`weird` = 1"}, parts)

	parts = splitTopLevelAndConjuncts(t.Context(), "'x AND y'")
	require.Equal(t, []string{"'x AND y'"}, parts)

	require.True(t, cyphertext.FindIdentifier("x + y + z", "y", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("node.y = 1", "y", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("'y'", "y", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
	require.False(t, cyphertext.FindIdentifier("yy + 1", "y", 0, cyphertext.IdentifierUnqualifiedFold) >= 0)
}
