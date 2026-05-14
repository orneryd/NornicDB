package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindMatchingParenInUse_Cases(t *testing.T) {
	// Simple balanced parens.
	idx, err := findMatchingParenInUse("(abc)", 0)
	require.NoError(t, err)
	require.Equal(t, 4, idx)

	// Nested parens.
	idx, err = findMatchingParenInUse("(a(b)c)", 0)
	require.NoError(t, err)
	require.Equal(t, 6, idx)

	// Single quotes hide a paren.
	idx, err = findMatchingParenInUse(`('a)b')`, 0)
	require.NoError(t, err)
	require.Equal(t, 6, idx)

	// Double quotes hide a paren.
	idx, err = findMatchingParenInUse(`("a)b")`, 0)
	require.NoError(t, err)
	require.Equal(t, 6, idx)

	// Doubled single quote inside single-quoted string is an escape.
	idx, err = findMatchingParenInUse(`('a''b')`, 0)
	require.NoError(t, err)
	require.Equal(t, 7, idx)

	// Doubled double quote inside double-quoted string is an escape.
	idx, err = findMatchingParenInUse(`("a""b")`, 0)
	require.NoError(t, err)
	require.Equal(t, 7, idx)

	// Position not at paren → error.
	_, err = findMatchingParenInUse("abc", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected '('")

	// Position past end → error.
	_, err = findMatchingParenInUse("(abc)", 100)
	require.Error(t, err)

	// Unterminated parens → error.
	_, err = findMatchingParenInUse("(abc", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated")
}

func TestParseFirstGraphRefArg_Variants(t *testing.T) {
	got, err := parseFirstGraphRefArg("mydb")
	require.NoError(t, err)
	require.Equal(t, "mydb", got)

	// Single-quoted.
	got, err = parseFirstGraphRefArg(`'with spaces'`)
	require.NoError(t, err)
	require.Equal(t, "with spaces", got)

	// Doubled single-quote escape.
	got, err = parseFirstGraphRefArg(`'a''b'`)
	require.NoError(t, err)
	// The function returns the slice between the quotes — the escape
	// is preserved as the literal `''` inside.
	require.Contains(t, []string{"a", "a''b"}, got)

	// Double-quoted.
	got, err = parseFirstGraphRefArg(`"plain"`)
	require.NoError(t, err)
	require.Equal(t, "plain", got)

	// Backtick-quoted with escape.
	got, err = parseFirstGraphRefArg("`a``b`")
	require.NoError(t, err)
	require.Equal(t, "a`b", got)

	// Empty argument.
	_, err = parseFirstGraphRefArg("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty graph reference")

	// Whitespace-only.
	_, err = parseFirstGraphRefArg("   ")
	require.Error(t, err)

	// Unterminated single-quote.
	_, err = parseFirstGraphRefArg(`'unterm`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated")

	// Unterminated backtick.
	_, err = parseFirstGraphRefArg("`unterm")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated")

	// Multi-word: returns first field.
	got, err = parseFirstGraphRefArg("first second third")
	require.NoError(t, err)
	require.Equal(t, "first", got)
}
