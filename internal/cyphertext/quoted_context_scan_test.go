package cyphertext

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuotedContextChunkBoundaries(t *testing.T) {
	for _, text := range []string{
		"'short'", "`name`", "'" + strings.Repeat("a", 255) + "\\'z'", "`" + strings.Repeat("a", 255) + "``z`",
		"'" + strings.Repeat("a", 254) + "\\\\z'", "'unfinished", "`unfinished", "", "identifier",
	} {
		end, closed := ScanQuotedText(text, 0)
		actual, actualClosed, err := ScanQuotedTextContext(t.Context(), text, 0)
		require.NoError(t, err)
		require.Equal(t, end, actual)
		require.Equal(t, closed, actualClosed)
		require.Zero(t, testing.AllocsPerRun(100, func() { ScanQuotedText(text, 0) }))
		require.Zero(t, testing.AllocsPerRun(100, func() { ScanQuotedTextContext(context.Background(), text, 0) }))
	}
}

func BenchmarkQuotedScanContextControl(b *testing.B) {
	for _, tc := range []struct{ name, text string }{
		{"short", "'ordinary'"}, {"long", "'" + strings.Repeat("x", 8192) + "'"}, {"backtick", "`" + strings.Repeat("a``b", 2048) + "`"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.text)))
			for i := 0; i < b.N; i++ {
				end, closed := ScanQuotedText(tc.text, 0)
				if !closed || end != len(tc.text) {
					b.Fatalf("invalid quoted result %d %v", end, closed)
				}
			}
		})
	}
}

func BenchmarkInterpolatedScanContextControl(b *testing.B) {
	text := "'before { 'nested' } after'"
	close := strings.LastIndexByte(text, '}')
	expressionEnd := func(int) int { return close }
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		end, closed := ScanInterpolatedText(text, 0, expressionEnd)
		if !closed || end != len(text) {
			b.Fatalf("invalid interpolated result %d %v", end, closed)
		}
	}
}

// Exact expected extents independently pin both entry points at every residue
// around several windows, including escape/doubled-delimiter lookahead.
func TestQuotedWindowGrammarExtents(t *testing.T) {
	for padding := 0; padding <= 1025; padding++ {
		prefix := strings.Repeat("a", padding)
		for _, tc := range []struct {
			text   string
			closed bool
			end    int
		}{
			{"'" + prefix + "\\'z'", true, padding + 5},
			{"'" + prefix + "\\\\z'", true, padding + 5},
			{"\"" + prefix + "\\\"z\"", true, padding + 5},
			{"`" + prefix + "``z`", true, padding + 5},
			{"`" + prefix + "\\`", true, padding + 3},
			{"'" + prefix + "\\", false, padding + 2},
			{"`" + prefix + "``", false, padding + 3},
			{"'" + prefix + "''tail", true, padding + 2},
		} {
			for _, offset := range []int{0, 3} {
				text := strings.Repeat(" ", offset) + tc.text
				end, closed := ScanQuotedText(text, offset)
				require.Equal(t, offset+tc.end, end, "padding=%d text=%q", padding, text)
				require.Equal(t, tc.closed, closed)
				end, closed, err := ScanQuotedTextContext(t.Context(), text, offset)
				require.NoError(t, err)
				require.Equal(t, offset+tc.end, end)
				require.Equal(t, tc.closed, closed)
			}
		}
	}
}

func TestInterpolatedScanKeepsExpressionOwner(t *testing.T) {
	text := "'before { 'nested' } after'"
	calls := 0
	end, closed := ScanInterpolatedText(text, 0, func(start int) int {
		calls++
		require.Equal(t, 8, start)
		return strings.LastIndexByte(text, '}')
	})
	require.Equal(t, len(text), end)
	require.True(t, closed)
	require.Equal(t, 1, calls)
	end, closed = ScanInterpolatedText(text, 0, func(int) int { return -1 })
	require.Equal(t, len(text), end)
	require.False(t, closed)
}
