package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWhitespaceNeighborConsumers(t *testing.T) {
	require.Equal(t, byte('a'), previousNonSpace("a \t", 3))
	require.Equal(t, byte('a'), nextNonSpace(" \ta", 0))
	require.Zero(t, previousNonSpace("  ", 2))
	require.Zero(t, nextNonSpace("a  ", 1))
	require.Equal(t, byte('/'), nextNonSpace(" /* comment */ a", 0), "comments remain text for whitespace-only readers")
	require.Zero(t, previousNonSpace("\x00", 1))
	require.True(t, caseHasPreviousToken("\x00", 1), "NUL is present even though the byte view uses zero")
	require.False(t, caseHasPreviousToken(" \t", 2))
	require.True(t, casePreviousWordIn("CASE \t", 6, "case"))
	require.True(t, casePreviousWordIn("   ", 3, ""), "empty-word membership remains caller owned")
	require.False(t, casePreviousWordIn("   ", 3, "case"))
}
