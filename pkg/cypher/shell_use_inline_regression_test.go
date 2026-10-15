package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShellUseInlineStatementBoundary(t *testing.T) {
	for _, tc := range []struct{ input, command, remaining string }{
		{":USE report RETURN 'two  spaces' AS value", ":USE report", "RETURN 'two  spaces' AS value"},
		{":use report RETURN 1\nRETURN 2", ":use report", "RETURN 1\nRETURN 2"},
		{":USE report\nRETURN 1", ":USE report", "RETURN 1"},
		{":USE report", ":USE report", ""},
		{":USE\freport RETURN 1", ":USE\freport", "RETURN 1"},
		{":USE\u00a0report RETURN 1", ":USE\u00a0report", "RETURN 1"},
	} {
		command, remaining, ok := nextShellCommand(tc.input)
		require.True(t, ok)
		require.Equal(t, tc.command, command)
		require.Equal(t, tc.remaining, remaining)
	}
}
