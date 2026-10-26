package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSharedScannersConsumeCompleteFragments pins the consumption contract of
// the shared lexical scanners: every scanner returns the offset just past the
// whole fragment it skipped (quote with escapes or doubled quotes, comment to
// its line end or block close, bracket group to its close) and never stops in
// the middle of one.
func TestSharedScannersConsumeCompleteFragments(t *testing.T) {
	t.Run("queryCommentEnd", func(t *testing.T) {
		for _, tc := range []struct {
			text string
			want int
		}{
			{"// x", len("// x")},
			{"// x\rY", len("// x")},
			{"// x\nY", len("// x")},
			{"// x\r\nY", len("// x")},
			{"/* a * b */ Y", len("/* a * b */")},
			{"/* a / b */ Y", len("/* a / b */")},
			{"/x", -1},
			{"//", len("//")},
		} {
			require.Equal(t, tc.want, queryCommentEnd(tc.text, 0), "%q", tc.text)
		}
	})

	t.Run("queryGapEnd", func(t *testing.T) {
		text := "x /* c */\t// d\r\n  y"
		require.Equal(t, len(text)-1, queryGapEnd(text, 1))
		// A quote is not a gap: queryGapEnd returns the index itself.
		require.Equal(t, 0, queryGapEnd("'// not a comment'", 0))
	})

	t.Run("quoted spans", func(t *testing.T) {
		// Backslash escapes stay inside a string; doubled string delimiters
		// close one token and begin another.
		require.Equal(t, len(`'a\'b'`), skipCypherQuotedText(`'a\'b'`, 0, '\''))
		require.Equal(t, len("'a'"), skipCypherQuotedText(`'a''b'`, 0, '\''))
		require.Equal(t, len(`"a\"b"`), skipCypherQuotedText(`"a\"b"`, 0, '"'))
		// A backtick name escapes a backtick by doubling; a backslash does not
		// escape, so the first backtick closes the name.
		require.Equal(t, len("`a``b`"), skipCypherQuotedText("`a``b`", 0, '`'))
		require.Equal(t, 4, skipCypherQuotedText("`a\\`b`", 0, '`'))
	})

	t.Run("forEachListOperand", func(t *testing.T) {
		var visits []int
		text := "' IN ' IN [1, 2] /* IN */ IN [3] // IN\n"
		err := forEachListOperand(text, func(start, in int) error {
			visits = append(visits, in)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []int{strings.Index(text, "IN [1"), strings.Index(text, "IN [3]")}, visits)
	})

	t.Run("canonical delimiter owner", func(t *testing.T) {
		text := "([a /* ] */ {k: ')'}])"
		close := cyphertext.FindMatchingDelimiter(text, 0, '(', ')', cyphertext.OpeningDelimiter)
		require.Equal(t, len(text)-1, close)
		require.Equal(t, len("[a /* ] */ ']']")-1, cyphertext.FindMatchingDelimiter("[a /* ] */ ']']", 0, '[', ']', cyphertext.OpeningDelimiter))
		// The ']' inside the quoted string cannot close the group.
		require.Equal(t, -1, cyphertext.FindMatchingDelimiter("[a /* ] */ ']'", 0, '[', ']', cyphertext.OpeningDelimiter))
	})

	t.Run("keyword scanners skip fragments", func(t *testing.T) {
		query := "RETURN 'WITH' AS `x//y`, /* RETURN */ 1 // WITH\rWITH 2 AS z RETURN z"
		want := strings.Index(query, "WITH 2")
		require.Equal(t, want, keywordIndexFrom(query, "WITH", 0, defaultKeywordScanOpts(acceptedBraceView(query))), "cached-default")
		opts := defaultKeywordScanOpts(acceptedBraceView(query))
		opts.SkipParens = false
		require.Equal(t, want, keywordIndexFrom(query, "WITH", 0, opts), "general")
	})

	t.Run("lastLiveByte", func(t *testing.T) {
		s := "RETURN 1 // WITH\nWITH 2 AS z RETURN z"
		require.Equal(t, strings.Index(s, "1 "), lastLiveByte(s, strings.Index(s, "WITH 2")))
		// A // inside a string is part of the string, not a comment.
		u := "RETURN 'http://x.test/a' AS u"
		require.Equal(t, len(u)-1, lastLiveByte(u, len(u)))
		// A block comment hides its text; the byte before it stays live.
		b := "RETURN 1 /* WITH */ WITH 2 AS z RETURN z"
		require.Equal(t, strings.Index(b, "1 "), lastLiveByte(b, strings.Index(b, "WITH 2")))
	})

	t.Run("name check never reads comment text", func(t *testing.T) {
		// The word before a candidate clause keyword is live code, not the last
		// word of a preceding comment: the WITH after the line comment is a
		// clause, not a name, on every scanner path.
		for _, query := range []string{
			"RETURN 1 // WITH\nWITH 2 AS z RETURN z",
			"RETURN 1 // set\nWITH 2 AS z RETURN z",
			"RETURN 1 /* WITH */ WITH 2 AS z RETURN z",
		} {
			want := strings.Index(query, "WITH 2")
			require.Equal(t, want, keywordIndexFrom(query, "WITH", 0, defaultKeywordScanOpts(acceptedBraceView(query))), "cached-default %q", query)
			opts := defaultKeywordScanOpts(acceptedBraceView(query))
			opts.SkipParens = false
			require.Equal(t, want, keywordIndexFrom(query, "WITH", 0, opts), "general %q", query)
		}
	})
}
