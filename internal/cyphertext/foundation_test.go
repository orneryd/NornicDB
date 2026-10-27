package cyphertext

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoundationSourceMap(t *testing.T) {
	for _, test := range []struct{ original, accepted string }{
		{"PASS\u200bWORD\u200c MATCH", "PASSWORD MATCH"},
		{"\ufeffRETURN\u00a0１", "RETURN １"},
		{"RETURN （1，2）", "RETURN (1,2)"},
		{"MATCH (a) — [r] — ﹥ (b) RETURN a", "MATCH (a) - [r] - > (b) RETURN a"},
		{"RETURN 'PASS\u200bWORD', `a\u200bb` // PASS\u200bWORD", "RETURN 'PASS\u200bWORD', `a\u200bb` // PASS\u200bWORD"},
	} {
		source := NormalizeSource(test.original)
		require.Equal(t, test.accepted, source.Text)
		require.Equal(t, test.original, source.Original)
		require.Equal(t, source.Text, NormalizeSource(source.Text).Text)
		previousOriginal, previousAccepted := 0, 0
		for _, change := range source.Edits {
			require.Equal(t, source.Original[previousOriginal:change.Original.Start], source.Text[previousAccepted:change.Accepted.Start])
			require.Greater(t, change.Original.End, change.Original.Start)
			previousOriginal, previousAccepted = change.Original.End, change.Accepted.End
		}
		require.Equal(t, source.Original[previousOriginal:], source.Text[previousAccepted:])
	}
	source := NormalizeSource("\ufeffPASS\u200bWORD\u200c MATCH")
	span, ok := source.OriginalSpan(Span{0, 8}, SourceExtent)
	require.True(t, ok)
	require.Equal(t, Span{3, 14}, span)
	require.Equal(t, 3, source.PasswordBoundary())
	span, ok = source.OriginalSpan(Span{0, len(source.Text)}, SourceExtent)
	require.True(t, ok)
	require.Equal(t, Span{3, len(source.Original)}, span, "leading deletion is outside a right-biased token start")
	ctx, held := WithSource(context.Background(), source.Original)
	require.Same(t, held, SourceFor(ctx, held.Text))
}

func TestFoundationIdentifierExtent(t *testing.T) {
	for _, test := range []struct {
		text, value   string
		end           int
		quoted, valid bool
	}{
		{"café\u00a0x", "café", len("café"), false, true},
		{"界$9,", "界$9", len("界$9"), false, true},
		{"` a``b ` rest", " a`b ", len("` a``b `"), true, true},
		{"`open", "", 0, false, false},
		{"\u00a0x", "", 0, false, false},
		{"9name", "", 0, false, false},
	} {
		name, ok := ScanIdentifier(test.text, 0)
		require.Equal(t, test.valid, ok)
		require.Equal(t, test.value, name.Value)
		require.Equal(t, test.end, name.End)
		require.Equal(t, test.quoted, name.Quoted)
	}
	for _, test := range []struct {
		text  string
		start int
		want  Identifier
		valid bool
	}{
		{"x café\u00a0tail", 2, Identifier{Start: 2, End: 2 + len("café"), Value: "café"}, true},
		{"x `a``b` tail", 2, Identifier{Start: 2, End: 8, Value: "a`b", Quoted: true}, true},
		{"x `` tail", 2, Identifier{Start: 2, End: 4, Quoted: true}, true},
		{"x `open", 2, Identifier{Start: 2, End: 2}, false},
		{"é", -1, Identifier{Start: -1, End: -1}, false},
		{"é", 1, Identifier{Start: 1, End: 1}, false},
		{"é", 2, Identifier{Start: 2, End: 2}, false},
		{"é", 3, Identifier{Start: 3, End: 3}, false},
	} {
		name, valid := ScanIdentifier(test.text, test.start)
		require.Equal(t, test.valid, valid)
		require.Equal(t, test.want, name)
	}
	require.True(t, IsIdentifier("café$9"))
	require.False(t, IsIdentifier("café\u00a0x"))
	require.False(t, IsIdentifier("`café`"))
}

func TestFoundationNumericExtent(t *testing.T) {
	for _, test := range []struct {
		text, token, tail string
		base              int
		invalid           bool
	}{
		{"1..2", "1", "1", 10, false},
		{".5e-2+x", ".5e-2", ".5e-2", 10, false},
		{"1_000.5_0e+1_0f,", "1_000.5_0e+1_0f", "1_000.5_0e+1_0f", 10, false},
		{"0x_1F_E,", "0x_1F_E", "0x_1F_E", 16, false},
		{"0O1_7,", "0O1_7", "0O1_7", 8, false},
		{"1e+,", "1e+", "1e+", 10, true},
		{"1__0,", "1__0", "1__0", 10, true},
		{"0xg,", "0x", "0xg", 16, true},
		{"1café,", "1", "1café", 10, true},
		{"5.", "5", "5", 10, true},
	} {
		t.Run(test.text, func(t *testing.T) {
			number, ok := ScanNumber(test.text, 0)
			require.True(t, ok)
			require.Equal(t, test.token, test.text[:number.End])
			require.Equal(t, test.tail, test.text[:number.TailEnd])
			require.Equal(t, test.base, number.Base)
			require.Equal(t, test.invalid, number.Invalid)
		})
	}
	require.False(t, NumberStartsAt("a1", 1))
	require.False(t, NumberStartsAt("$1", 1))
	require.False(t, NumberStartsAt("1..2", 2))
}

func TestFoundationWholeString(t *testing.T) {
	for _, text := range []string{"'a''b'", `"a""b"`, "'open"} {
		require.False(t, IsWholeString(text))
	}
	for _, text := range []string{"'a\\'b'", `"a\\b"`, "''"} {
		require.True(t, IsWholeString(text))
	}
}

func TestFoundationSourceComposition(t *testing.T) {
	source := NormalizeSource("R\u00a0PASS\u200bWORD 'x'")
	rewritten := source.Rewrite("RETURN PASSWORD 'x'", []SourceEdit{{Original: Span{0, 1}, Accepted: Span{0, 6}}})
	window := rewritten.Window(7, len(rewritten.Text))
	require.Equal(t, "PASSWORD 'x'", window.Text)
	span, ok := window.OriginalSpan(Span{0, 8}, SourceExtent)
	require.True(t, ok)
	require.Equal(t, Span{3, 14}, span)
	set := &CredentialSet{}
	set.RecordSource(window, 9, 12, "x", true)
	require.Len(t, set.Spans, 1)
	require.Equal(t, "'x'", source.Original[set.Spans[0].Start:set.Spans[0].End])
}

func TestFoundationStatementSpans(t *testing.T) {
	data, err := os.ReadFile("testdata/statement_spans.json")
	require.NoError(t, err)
	var cases []struct {
		Source     string   `json:"source"`
		Statements []string `json:"statements"`
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	for _, test := range cases {
		var actual []string
		for _, span := range StatementSpans(test.Source) {
			actual = append(actual, test.Source[span.Start:span.End])
		}
		require.Equal(t, test.Statements, actual)
	}
}

func TestFoundationNormalizationClasses(t *testing.T) {
	data, err := os.ReadFile("testdata/normalization.json")
	require.NoError(t, err)
	var cases []struct{ Input, Accepted, Character, Kind string }
	require.NoError(t, json.Unmarshal(data, &cases))
	for _, test := range cases {
		require.Equal(t, test.Accepted, NormalizeSource(test.Input).Text)
		for _, position := range []int{0, 4, 8} {
			original := "PASSWORD"[:position] + test.Character + "PASSWORD"[position:]
			replacement := test.Accepted
			if test.Kind == "arrow" {
				replacement = test.Character
			}
			accepted := "PASSWORD"[:position] + replacement + "PASSWORD"[position:]
			source := NormalizeSource(original)
			require.Equal(t, accepted, source.Text)
			span, ok := source.OriginalSpan(Span{0, len(source.Text)}, SourceExtent)
			require.True(t, ok)
			expected := Span{0, len(original)}
			if test.Kind == "deletion" && position == 0 {
				expected.Start = len(test.Character)
			}
			if test.Kind == "deletion" && position == 8 {
				expected.End -= len(test.Character)
			}
			require.Equal(t, expected, span)
		}
	}
}
