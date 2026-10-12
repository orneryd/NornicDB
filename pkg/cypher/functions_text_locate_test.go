package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// text.locate finds a cited quote's span in a text, loosely, as a reader
// copies it (Personal Documents R5): case and whitespace runs, typographic
// quotes and dashes, markdown emphasis and links, reply markers, JSON
// escapes and a copied line label don't matter; the cited line, within a
// tolerance, picks among occurrences; the span is in code points.
func TestTextLocate(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "locate"))
	ctx := context.Background()
	text := "Héllo there.\n" + // line 1
		"The tenant’s rent is due on the *first* day.\n" + // line 2
		"> A late fee — 5% — applies after [ten days](https://x.example).\n" + // line 3
		"filler\nfiller\n" + // lines 4-5
		"The rent is due again.\n" // line 6
	locate := func(quote string, options map[string]interface{}) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "RETURN text.locate($text, $quote, $options) AS span", map[string]interface{}{"text": text, "quote": quote, "options": options})
		require.NoError(t, err, quote)
		return result.Rows[0][0]
	}
	span := func(start, end int) map[string]interface{} {
		return map[string]interface{}{"start": int64(start), "end": int64(end)}
	}
	codePoints := []rune(text)
	substring := func(value interface{}) string {
		s := value.(map[string]interface{})
		return string(codePoints[s["start"].(int64):s["end"].(int64)])
	}

	require.Equal(t, "Héllo", substring(locate("HÉLLO", nil)), "code points, without case")
	require.Equal(t, span(0, 5), locate("héllo", nil))
	require.Equal(t, "tenant’s rent is due on the *first", substring(locate("tenant's  rent\\nis due on the first", nil)))
	require.Equal(t, "A late fee — 5% — applies after [ten days", substring(locate("A late fee - 5% - applies after ten days", nil)))
	require.Equal(t, "late fee", substring(locate("3: late fee", nil)), "a copied line label")
	require.Equal(t, "A late", substring(locate("> A late", nil)), "a reply marker in the quote")

	// The cited line picks among occurrences, within the tolerance.
	require.Equal(t, 2, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(2)})))
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(6)})))
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"line": int64(5)})))
	require.Nil(t, locate("Héllo", map[string]interface{}{"line": int64(6)}), "beyond the tolerance")
	require.Equal(t, 1, lineOf(text, locate("Héllo", map[string]interface{}{"line": int64(6), "tolerance": int64(5)})))
	// from / to bound the occurrence.
	require.Equal(t, 6, lineOf(text, locate("rent is due", map[string]interface{}{"from": int64(30)})))
	require.Nil(t, locate("rent is due", map[string]interface{}{"to": int64(20)}))

	// A JSON file's body is one line whose breaks are two-character escapes;
	// they match whitespace in the quote, real or escaped, and the span
	// covers them (Personal Documents R5).
	jsonText := `{\n  "body": "UK most of\nthe time.\n\nTara Brazil\n\nAdministrator director. I do not"\n}`
	jsonRunes := []rune(jsonText)
	for quote, want := range map[string]string{
		"most of the time":             `most of\nthe time`,
		"most of\nthe time":            `most of\nthe time`,
		"Tara Brazil\n\nAdministrator": `Tara Brazil\n\nAdministrator`,
		`director.\nI do not`:          `director. I do not`,
		"the time. Tara Brazil":        `the time.\n\nTara Brazil`,
	} {
		result, err := exec.Execute(ctx, "RETURN text.locate($text, $quote, {line: 1}) AS span", map[string]interface{}{"text": jsonText, "quote": quote})
		require.NoError(t, err, quote)
		found, isSpan := result.Rows[0][0].(map[string]interface{})
		require.True(t, isSpan, quote)
		require.Equal(t, want, string(jsonRunes[found["start"].(int64):found["end"].(int64)]), quote)
	}

	require.Nil(t, locate("not in the text", nil))
	require.Nil(t, locate("   ", nil))
	result, err := exec.Execute(ctx, "RETURN text.locate(null, 'x') AS a, text.locate('x', null) AS b, text.locate('ab', 'b', null) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, nil, span(1, 2)}}, result.Rows)
	for _, query := range []string{"RETURN text.locate(1, 'x')", "RETURN text.locate('x', 1)", "RETURN text.locate('x', 'x', 1)", "RETURN text.locate('x', 'x', {line: 'a'})", "RETURN text.locate('x')"} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
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
