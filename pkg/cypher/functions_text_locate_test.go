package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// text.locate finds a cited quote's span in a text: words in order without
// case, any whitespace, JSON escape or reply marker between them, typographic
// quotes and dashes alike, markdown only around a word and inside the span, a
// copied line label dropped; the cited line, within a tolerance, picks among
// occurrences, and a line that isn't the text's is no match; the span is in
// code points.
func TestTextLocate(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "locate"))
	ctx := context.Background()
	text := "Héllo there.\n" + // line 1
		"The tenant’s rent is due on the *first* day.\n" + // line 2
		"> A late fee — 5% — applies after [ten days](https://x.example).\n" + // line 3
		"filler\nfiller\n" + // lines 4-5
		"The rent is due again.\n" // line 6; line 7 is empty
	locateIn := func(text, quote string, options map[string]interface{}) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "RETURN text.locate($text, $quote, $options) AS span", map[string]interface{}{"text": text, "quote": quote, "options": options})
		require.NoError(t, err, quote)
		return result.Rows[0][0]
	}
	locate := func(quote string, options map[string]interface{}) interface{} {
		t.Helper()
		return locateIn(text, quote, options)
	}
	span := func(start, end int) map[string]interface{} {
		return map[string]interface{}{"start": int64(start), "end": int64(end)}
	}
	substringOf := func(text string, value interface{}) string {
		t.Helper()
		s, isSpan := value.(map[string]interface{})
		require.True(t, isSpan, "a span, not %v", value)
		return string([]rune(text)[s["start"].(int64):s["end"].(int64)])
	}
	substring := func(value interface{}) string {
		t.Helper()
		return substringOf(text, value)
	}

	require.Equal(t, "Héllo", substring(locate("HÉLLO", nil)), "code points, without case")
	require.Equal(t, span(0, 5), locate("héllo", nil))
	require.Equal(t, "tenant’s rent is due on the *first*", substring(locate("tenant's  rent\\nis due on the first", nil)), "markup around the last word is in the span")
	require.Equal(t, "A late fee — 5% — applies after [ten days](https://x.example)", substring(locate("A late fee - 5% - applies after ten days", nil)))
	require.Equal(t, "late fee", substring(locate("3: late fee", nil)), "a copied line label")
	require.Equal(t, "> A late", substring(locate("> A late", nil)), "a > in the quote is literal")

	// The cited line picks among occurrences, within the tolerance.
	require.Equal(t, 2, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(2)})))
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(6)})))
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(5)})))
	require.Equal(t, 2, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(4)})), "the lower line first on a tie")
	require.Nil(t, locate("Héllo", map[string]interface{}{"line": int64(6)}), "beyond the tolerance")
	require.Equal(t, 1, lineOf(text, locate("Héllo", map[string]interface{}{"line": int64(6), "tolerance": int64(5)})))
	// A line that isn't one of the text's is no match.
	require.Nil(t, locate("Héllo", map[string]interface{}{"line": int64(0)}))
	require.Nil(t, locate("Héllo", map[string]interface{}{"line": int64(-1)}))
	require.Nil(t, locate("rent is due again", map[string]interface{}{"line": int64(8)}))
	require.Equal(t, 6, lineOf(text, locate("rent is due again", map[string]interface{}{"line": int64(7)})), "the empty last line is a line")
	// from / to bound the occurrence.
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"from": int64(30)})))
	require.Nil(t, locate("rent is due", map[string]interface{}{"to": int64(20)}))
	require.Nil(t, locate("rent is due", map[string]interface{}{"from": int64(40), "to": int64(20)}))

	require.Nil(t, locate("not in the text", nil))
	require.Nil(t, locate("   ", nil))
	require.Nil(t, locate("12: ", nil))

	// Without case, İ is i, as by its lower-case mapping; ı, ſ, the
	// Kelvin sign and ß keep Go's folding, which already agrees.
	turkish := "line one\nline two\nİstanbul I ı ſ K Straße"
	require.Equal(t, "İstanbul", substringOf(turkish, locateIn(turkish, "istanbul", nil)))
	require.Equal(t, "İstanbul", substringOf(turkish, locateIn(turkish, "ISTANBUL", nil)))
	require.Equal(t, span(18, 19), locateIn(turkish, "I", map[string]interface{}{"line": int64(3)}), "İ, the first at the line's start")
	require.Equal(t, span(18, 19), locateIn(turkish, "i", map[string]interface{}{"line": int64(3)}))
	require.Equal(t, "ſ", substringOf(turkish, locateIn(turkish, "s K", nil))[:len("ſ")])

	// Markdown only around a word, never inside one.
	columns := "Database columns file_name and filename."
	require.Equal(t, span(31, 39), locateIn(columns, "filename", map[string]interface{}{"line": int64(1)}))
	require.Nil(t, locateIn("A_B and user_id and a*b", "AB", nil))
	require.Nil(t, locateIn("A_B and user_id and a*b", "userid", nil))
	require.Nil(t, locateIn("A_B and user_id and a*b", "ab", nil))
	require.Equal(t, "a*b", substringOf("A_B and user_id and a*b", locateIn("A_B and user_id and a*b", "a*b", nil)))
	// A > in the quote matches a reply marker in the text.
	require.Equal(t, span(0, 12), locateIn("Invoice\n> 42", "Invoice > 42", map[string]interface{}{"line": int64(1)}))
	require.Equal(t, "resign\r\n> this.", substringOf("We resign\r\n> this.", locateIn("We resign\r\n> this.", "resign this.", nil)), "a quoted sentence running on after a reply marker")
	// The span includes markup around its first and last words.
	require.Equal(t, "**Charges & Payments**", substringOf("See **Charges & Payments** below", locateIn("See **Charges & Payments** below", "Charges & Payments", nil)))
	require.Equal(t, "[Google Review](https://g.example/r)", substringOf("A [Google Review](https://g.example/r).", locateIn("A [Google Review](https://g.example/r).", "Google Review", nil)))
	require.Equal(t, "__bold__ and _it_", substringOf("x __bold__ and _it_ y", locateIn("x __bold__ and _it_ y", "bold and it", nil)))

	// A JSON file's body is one line whose breaks are two-character escapes;
	// they match whitespace in the quote, real or escaped, and the span
	// covers them.
	jsonText := `{\n  "body": "UK most of\nthe time.\n\nTara Brazil\n\nAdministrator director. I do not"\n}`
	for quote, want := range map[string]string{
		"most of the time":             `most of\nthe time`,
		"most of\nthe time":            `most of\nthe time`,
		"Tara Brazil\n\nAdministrator": `Tara Brazil\n\nAdministrator`,
		`director.\nI do not`:          `director. I do not`,
		"the time. Tara Brazil":        `the time.\n\nTara Brazil`,
	} {
		require.Equal(t, want, substringOf(jsonText, locateIn(jsonText, quote, map[string]interface{}{"line": int64(1)})), quote)
	}

	result, err := exec.Execute(ctx, "RETURN text.locate(null, 'x') AS a, text.locate('x', null) AS b, text.locate('ab', 'b', null) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, nil, span(1, 2)}}, result.Rows)
	for _, query := range []string{"RETURN text.locate(1, 'x')", "RETURN text.locate('x', 1)", "RETURN text.locate('x', 'x', 1)", "RETURN text.locate('x', 'x', {line: 'a'})", "RETURN text.locate('x')"} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}

// text.locate's cost follows the part searched: a quote near the end of a
// long text, bounded by from and to, is found without reading the text
// before from as part of the match.
func TestTextLocateLongText(t *testing.T) {
	var builder strings.Builder
	for builder.Len() < 500_000 {
		builder.WriteString("Filler words on a line of text.\n")
	}
	builder.WriteString("The quoted passage is here.\n")
	text := builder.String()
	length := len([]rune(text))
	start, end, found := locateQuote(text, "quoted passage", textLocateOptions{tolerance: textLocateTolerance, from: length - 100, to: -1})
	require.True(t, found)
	require.Equal(t, "quoted passage", string([]rune(text)[start:end]))
	lines := strings.Count(text, "\n")
	_, _, found = locateQuote(text, "quoted passage", textLocateOptions{hasLine: true, line: lines, tolerance: textLocateTolerance, from: 0, to: -1})
	require.True(t, found)
}

// lineOf is the 1-based line a span starts on.
func lineOf(text string, value interface{}) int {
	start := int(value.(map[string]interface{})["start"].(int64))
	line := 1
	for i, r := range []rune(text) {
		if i == start {
			break
		}
		if r == '\n' {
			line++
		}
	}
	return line
}
