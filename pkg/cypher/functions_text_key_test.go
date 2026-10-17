package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// textKeyCases are texts and their keys, by textKey's steps.
var textKeyCases = []struct{ text, key string }{
	{"", ""},
	{"acme 42", "acme 42"},
	{"  ACME   Corp\t", "acme corp"},
	// 1. NFKC: full-width forms and the ideographic space, a ligature,
	// compatibility digits and numbers, KELVIN SIGN and ANGSTROM SIGN,
	// mathematical bold digits; U+3007 is a letter number, not a decimal
	// digit.
	{"ＡＣＭＥ\u3000４２", "acme 42"},
	{"ﬁle", "file"},
	{"①②", "12"},
	{"x²", "x2"},
	{"¼", "1\u20444"},
	{"Ⅻ", "xii"},
	{"\u212a", "k"},
	{"\u212b", "\u00e5"},
	{"\u3007", "\u3007"},
	{"\U0001d7cf\U0001d7d0", "12"},
	// 2. full lower case: İ keeps its dot as U+0307, as str.lower() does.
	{"İstanbul", "i\u0307stanbul"},
	{"MÜLLER", "müller"},
	// 3. ß and final sigma.
	{"Straße", "strasse"},
	{"STRASSE", "strasse"},
	{"ẞ", "ss"},
	{"ΟΔΟΣ", "οδοσ"},
	{"οδος", "οδοσ"},
	{"Σίσυφος", "σίσυφοσ"},
	// 4. NFKC after lowering: a Greek capital iota with dialytika and tonos,
	// written with COMBINING GREEK DIALYTIKA TONOS too, and GREEK SMALL
	// LETTER IOTA WITH DIALYTIKA AND OXIA (canonically U+0390).
	{"\u03aa\u0301", "\u0390"},
	{"\u0399\u0344", "\u0390"},
	{"\u0390", "\u0390"},
	{"\u1fd3", "\u0390"},
	// Combining marks: composed, reordered, or kept when nothing composes.
	{"A\u030a", "\u00e5"},
	{"e\u0301", "\u00e9"},
	{"a\u0307\u0323", "\u1ea1\u0307"},
	{"\u0301", "\u0301"},
	// 5. decimal digits of any script: Arabic-Indic, Extended Arabic-Indic,
	// Devanagari, Thai, NKo, Adlam.
	{"٢٠٢٦", "2026"},
	{"۲۰۲۶", "2026"},
	{"२०२६", "2026"},
	{"๑๒", "12"},
	{"\u07c1", "1"},
	{"\U0001e951", "1"},
	{"No.٤٢", "no.42"},
	// 6. and 7. whitespace runs as one space, trimmed. ZERO WIDTH SPACE and
	// MONGOLIAN VOWEL SEPARATOR are format characters, not whitespace.
	{"a\u00a0\u00a0b", "a b"},
	{"a\t\n\r b", "a b"},
	{"a\u2028b\u2029c", "a b c"},
	{"\u1680a\u0085b\u001cc\u000bd\u3000", "a b c d"},
	{"a\u202f\u205f\u2000\u200ab", "a b"},
	{"\u001f\u001e\u001d", ""},
	{"a\u200bb", "a\u200bb"},
	{"a\u180eb", "a\u180eb"},
	{"  MÜLLER\u00a0 Straße ١", "müller strasse 1"},
}

// nornicdb.text.key in statements: the steps' keys, null for null, and
// toLower's type error for a value that isn't a string; in a projection, a
// WHERE and a SET alike.
func TestTextKeyFunction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "textkey"))
	ctx := context.Background()
	for _, c := range textKeyCases {
		result, err := exec.Execute(ctx, "RETURN nornicdb.text.key($text) AS key", map[string]interface{}{"text": c.text})
		require.NoError(t, err, c.text)
		require.Equal(t, [][]interface{}{{c.key}}, result.Rows, "%+q", c.text)
	}
	for _, c := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN nornicdb.text.key(null) AS v", nil},
		{"RETURN nornicdb.text.key('') AS v", ""},
		{"RETURN nornicdb.text.key('  Ａ  b ') AS v", "a b"},
		{"UNWIND ['A b', 'a\u00a0 B', 'ａ\tｂ'] AS s RETURN collect(DISTINCT nornicdb.text.key(s)) AS v", []interface{}{"a b"}},
	} {
		result, err := exec.Execute(ctx, c.query, nil)
		require.NoError(t, err, c.query)
		require.Equal(t, [][]interface{}{{c.want}}, result.Rows, c.query)
	}

	_, err := exec.Execute(ctx, "CREATE (:TextKey {name: 'ＭÜＬＬＥＲ  ١'}), (:TextKey {name: 'Other'})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:TextKey) WHERE nornicdb.text.key(n.name) = 'müller 1' RETURN n.name AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"ＭÜＬＬＥＲ  ١"}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (n:TextKey) SET n.key = nornicdb.text.key(n.name) RETURN n.key AS v ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"müller 1"}, {"other"}}, result.Rows)

	for _, value := range []interface{}{int64(1), 1.5, true, []interface{}{"a"}, map[string]interface{}{"a": "b"}} {
		_, keyErr := exec.Execute(ctx, "RETURN nornicdb.text.key($v) AS v", map[string]interface{}{"v": value})
		_, lowerErr := exec.Execute(ctx, "RETURN toLower($v) AS v", map[string]interface{}{"v": value})
		require.Error(t, keyErr, "%v", value)
		require.Error(t, lowerErr, "%v", value)
		keyCode, _ := nornicerrors.Neo4jStatus(keyErr)
		lowerCode, _ := nornicerrors.Neo4jStatus(lowerErr)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", keyCode, "%v", value)
		require.Equal(t, lowerCode, keyCode, "%v", value)
	}
	for _, query := range []string{"RETURN nornicdb.text.key(1) AS v", "RETURN nornicdb.text.key() AS v", "RETURN nornicdb.text.key('a', 'b') AS v"} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}

	result, err = exec.Execute(ctx, "SHOW FUNCTIONS YIELD name, category, signature, isBuiltIn WHERE name = 'nornicdb.text.key' RETURN name, category, signature, isBuiltIn", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"nornicdb.text.key", "String", "nornicdb.text.key(input :: STRING) :: STRING", true}}, result.Rows)
}

func TestTextKeyCases(t *testing.T) {
	for _, c := range textKeyCases {
		require.Equal(t, c.key, textKey(c.text), "%+q", c.text)
		require.Equal(t, c.key, textKey(c.key), "a key is its own key: %+q", c.key)
	}
}

// fullLowerKey is steps 2 and 3 as toLower lowers (x/text's full Unicode
// lower case, SpecialCasing without a language), then ß as ss and ς as σ.
func fullLowerKey(lower cases.Caser, text string) string {
	return strings.NewReplacer("ß", "ss", "ς", "σ").Replace(lower.String(text))
}

// lowerKey's simple mapping with U+0130's full one is the full lower case
// followed by step 3 for every code point, alone and in a word (where a
// final sigma is ς before step 3).
func TestTextKeyLowerMatchesFullLowerCase(t *testing.T) {
	lower := cases.Lower(language.Und)
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		for _, text := range []string{string(r), "A" + string(r) + "Σ", "Σ" + string(r)} {
			if got, want := lowerKey(text), fullLowerKey(lower, text); got != want {
				t.Fatalf("lowerKey(%+q) = %+q, full lower case %+q", text, got, want)
			}
		}
	}
	require.Equal(t, "plain", lowerKey("plain"))
	require.Equal(t, "a\xffb", lowerKey("A\xffB"), "a byte that isn't UTF-8 stays")
}

// pythonSpace is Python's str.isspace() (the \s of a str pattern): the code
// points of bidirectional class WS, B or S or category Zs, as Python 3.13
// (Unicode 15.1.0) lists them.
func pythonSpace(r rune) bool {
	switch {
	case 0x09 <= r && r <= 0x0d, 0x1c <= r && r <= 0x20, r == 0x85, r == 0xa0, r == 0x1680,
		0x2000 <= r && r <= 0x200a, r == 0x2028, r == 0x2029, r == 0x202f, r == 0x205f, r == 0x3000:
		return true
	}
	return false
}

func TestTextKeySpaceIsPythonSpace(t *testing.T) {
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if pythonSpace(r) != keySpace(r) {
			t.Fatalf("keySpace(U+%04X) = %v", r, keySpace(r))
		}
	}
}

// referenceDigits is every decimal digit beyond ASCII and its ASCII digit,
// counted up from the start of each contiguous run of category Nd.
func referenceDigits() map[rune]byte {
	digits := map[rune]byte{}
	run := 0
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !unicode.Is(unicode.Nd, r) {
			run = 0
			continue
		}
		if r >= utf8.RuneSelf {
			digits[r] = byte('0' + run%10)
		}
		run++
	}
	return digits
}

// Every decimal digit run is a whole number of zero-to-nine runs, so a
// digit's value is its distance from its zero, as unicodedata.decimal gives
// it.
func TestTextKeyDecimalDigits(t *testing.T) {
	t.Logf("Unicode versions: unicode %s, x/text norm %s, x/text cases %s", unicode.Version, norm.Version, cases.UnicodeVersion)
	digits := referenceDigits()
	require.Equal(t, len(digits), 10*len(keyDigitZeros))
	for r := rune(0); r <= unicode.MaxRune; r++ {
		want, isDigit := digits[r]
		if got, gotDigit := keyDigit(r); got != want || gotDigit != isDigit {
			t.Fatalf("keyDigit(U+%04X) = %q, %v; want %q, %v", r, got, gotDigit, want, isDigit)
		}
	}
	for _, c := range []struct {
		r    rune
		want byte
	}{
		{0x660, '0'}, {0x669, '9'}, {0x6f4, '4'}, {0x966, '0'}, {0x96f, '9'}, {0xe51, '1'}, {0xff10, '0'},
		{0xff19, '9'}, {0x1d7ce, '0'}, {0x1d7d8, '0'}, {0x1d7ff, '9'}, {0x104a3, '3'}, {0x1e950, '0'}, {0x11f59, '9'},
	} {
		got, isDigit := keyDigit(c.r)
		require.True(t, isDigit, "U+%04X", c.r)
		require.Equal(t, c.want, got, "U+%04X", c.r)
	}
	for _, r := range []rune{'0', '9', 'a', 0xb2, 0x2460, 0x3007, 0x2155, 0x10ffff} {
		_, isDigit := keyDigit(r)
		require.False(t, isDigit, "U+%04X", r)
	}
}

// textKeyReference is textKey as its steps' library calls compute them,
// slowly: NFKC, toLower's full lower case, ß and ς replaced, NFKC, digits
// from referenceDigits, Python's whitespace (pythonSpace) folded and
// trimmed.
func textKeyReference(lower cases.Caser, digits map[rune]byte, text string) string {
	text = norm.NFKC.String(fullLowerKey(lower, norm.NFKC.String(text)))
	var out strings.Builder
	for _, r := range text {
		if digit, isDigit := digits[r]; isDigit {
			out.WriteByte(digit)
		} else if pythonSpace(r) {
			out.WriteByte(' ')
		} else {
			out.WriteRune(r)
		}
	}
	return strings.Join(strings.FieldsFunc(out.String(), func(r rune) bool { return r == ' ' }), " ")
}

// textKey equals its reference for every code point, alone and between
// letters and spaces, and for random texts of the characters each step
// changes.
func TestTextKeyMatchesReference(t *testing.T) {
	lower, digits := cases.Lower(language.Und), referenceDigits()
	check := func(text string) {
		if got, want := textKey(text), textKeyReference(lower, digits, text); got != want {
			t.Fatalf("textKey(%+q) = %+q, reference %+q", text, got, want)
		}
	}
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if utf8.ValidRune(r) {
			check(string(r))
			check(" Ab" + string(r) + "\u00a0 Σ")
		}
	}
	pool := []rune("aZ09 \t\n\u000b\u001c\u0085\u00a0\u1680\u2000\u2028\u3000\u200bßẞΣσςΪ\u0301\u0308\u0344\u0307\u0323İıIＡ４٤२\U0001d7cf①¼ﬁÅ\u030aé")
	random := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		runes := make([]rune, random.IntN(12))
		for j := range runes {
			runes[j] = pool[random.IntN(len(pool))]
		}
		check(string(runes))
	}
}

// An ASCII text that is already a key, or a text beyond ASCII that is, is
// returned without allocating; an ASCII text that isn't allocates once.
func TestTextKeyAllocations(t *testing.T) {
	var sink string
	for _, text := range []string{"", "acme corp 42", "a.b-c/d", "αθήνα 2026", "müller strasse 1"} {
		require.Zero(t, testing.AllocsPerRun(100, func() { sink = textKey(text) }), "%+q", text)
	}
	for _, text := range []string{"ACME  Corp", " acme", "acme\t42"} {
		require.Equal(t, 1.0, testing.AllocsPerRun(100, func() { sink = textKey(text) }), "%+q", text)
	}
	_ = sink
}

// textKeyOracleFile holds vectors the key's reference implementation
// produced: {"unidata_version": "15.1.0", "vectors": [{"input": "...",
// "key": "..."}, ...]}, inputs being valid Unicode (no lone surrogates).
var textKeyOracleFile = filepath.Join("testdata", "text_key_oracle.json")

// TestTextKeyOracleVectors checks textKey against the oracle file's vectors,
// when the file is there.
func TestTextKeyOracleVectors(t *testing.T) {
	data, err := os.ReadFile(textKeyOracleFile)
	if errors.Is(err, os.ErrNotExist) {
		t.Skipf("no oracle vectors: pkg/cypher/%s is absent; it holds {\"unidata_version\": ..., \"vectors\": [{\"input\": ..., \"key\": ...}]}",
			filepath.ToSlash(textKeyOracleFile))
	}
	require.NoError(t, err)
	var oracle struct {
		UnidataVersion string `json:"unidata_version"`
		Vectors        []struct {
			Input string `json:"input"`
			Key   string `json:"key"`
		} `json:"vectors"`
	}
	require.NoError(t, json.Unmarshal(data, &oracle))
	require.NotEmpty(t, oracle.Vectors)
	t.Logf("%d vectors from Unicode %s; NornicDB's tables: unicode %s, x/text norm %s", len(oracle.Vectors), oracle.UnidataVersion, unicode.Version, norm.Version)
	mismatches := 0
	for _, vector := range oracle.Vectors {
		if got := textKey(vector.Input); got != vector.Key {
			mismatches++
			if mismatches <= 50 {
				t.Errorf("textKey(%+q) = %+q, oracle %+q", vector.Input, got, vector.Key)
			}
		}
	}
	require.Zero(t, mismatches, "of %d vectors", len(oracle.Vectors))
}
