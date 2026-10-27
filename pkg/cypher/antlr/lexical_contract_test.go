package antlr

import (
	"testing"

	antlrgo "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// The generated lexer vocabulary is a derivative representation. Check its
// whitespace/name transitions against the shared primitives, keeping raw text.
func TestSharedLexicalContract(t *testing.T) {
	for _, r := range []rune{'\t', '\n', '\v', '\f', '\r', ' ', '\u0085', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000', 'é', '界', '𐐀', '_', '$', '1'} {
		query := "x" + string(r) + "y"
		lexer := NewCypherLexer(antlrgo.NewInputStream(query))
		first := lexer.NextToken()
		if cyphertext.IsWhitespace(r) {
			require.Equal(t, "x", first.GetText())
			require.Equal(t, CypherLexerWS, lexer.NextToken().GetTokenType())
			require.Equal(t, "y", lexer.NextToken().GetText())
		} else {
			require.True(t, cyphertext.NamePart(r))
			require.Equal(t, query, first.GetText())
		}
	}
}

// The generated vocabulary stays an independent oracle. Grouped numbers are
// lowered before grammar admission; source grouping/invalid-tail facts are
// exercised in cyphertext and public numeric-admission tests.
func TestSharedNumericTokenContract(t *testing.T) {
	for _, text := range []string{"123", "1.5", ".5", "1e2", "1E-2", "0x1F", "0XfF", "0o77", "1.5f", "1d"} {
		number, ok := cyphertext.ScanNumber(text, 0)
		require.True(t, ok)
		require.False(t, number.Invalid)
		require.Equal(t, len(text), number.End)
		lexer := NewCypherLexer(antlrgo.NewInputStream(text))
		require.Equal(t, text, lexer.NextToken().GetText())
	}
	number, _ := cyphertext.ScanNumber("1..2", 0)
	require.Equal(t, 1, number.End)
	lexer := NewCypherLexer(antlrgo.NewInputStream("1..2"))
	require.Equal(t, "1", lexer.NextToken().GetText())
	require.Equal(t, "..", lexer.NextToken().GetText())
}
