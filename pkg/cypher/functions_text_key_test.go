package cypher

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
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

// keySpaceRun is a run of the whitespace class as string.regexReplace reads
// it (GetCachedRegex: Go's regexp, whose \s is ASCII's tab, line feed, form
// feed, carriage return and space).
const keySpaceRun = `[\s\x{0b}\x{1c}-\x{1f}\x{85}\p{Z}]+`

// keySpace is that class for every code point, and trim's whitespace
// (unicode.IsSpace) is inside it, so once its runs are one space each only
// a leading or trailing space is left to trim.
func TestTextKeySpaceClass(t *testing.T) {
	class, err := GetCachedRegex("^" + keySpaceRun + "$")
	require.NoError(t, err)
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if !utf8.ValidRune(r) {
			continue
		}
		inClass := class.MatchString(string(r))
		if keySpace(r) != inClass {
			t.Fatalf("keySpace(U+%04X) = %v, the class says %v", r, keySpace(r), inClass)
		}
		if unicode.IsSpace(r) && !inClass {
			t.Fatalf("trim trims U+%04X, which isn't in the class", r)
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

// textKeyReference is textKey as the Cypher functions of its steps compute
// it, slowly: normalize's NFKC, toLower, replace, normalize again, split(…,
// '') into code points with each decimal digit mapped (referenceDigits) and
// joined, string.regexReplace of the whitespace runs, and trim.
func textKeyReference(t *testing.T, digits map[rune]byte, text string) string {
	lowered := cypherfn.LowerCaseText(norm.NFKC.String(text))
	folded := norm.NFKC.String(strings.Replace(strings.Replace(lowered, "ß", "ss", -1), "ς", "σ", -1))
	var joined strings.Builder
	for _, character := range strings.Split(folded, "") {
		r, _ := utf8.DecodeRuneInString(character)
		if digit, isDigit := digits[r]; isDigit {
			joined.WriteByte(digit)
		} else {
			joined.WriteString(character)
		}
	}
	spaces, err := GetCachedRegex(keySpaceRun)
	require.NoError(t, err)
	return trimCharacters(spaces.ReplaceAllString(joined.String(), " "), "", true, true)
}

// textKey equals its reference for every code point, alone and between
// letters and spaces, and for random texts of the characters each step
// changes.
func TestTextKeyMatchesReference(t *testing.T) {
	digits := referenceDigits()
	check := func(text string) {
		if got, want := textKey(text), textKeyReference(t, digits, text); got != want {
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

// An ASCII text that is already a key is returned without allocating; one
// that isn't allocates once.
func TestTextKeyAllocations(t *testing.T) {
	var sink string
	for _, text := range []string{"", "acme corp 42", "a.b-c/d"} {
		require.Zero(t, testing.AllocsPerRun(100, func() { sink = textKey(text) }), "%+q", text)
	}
	for _, text := range []string{"ACME  Corp", " acme", "acme\t42"} {
		require.Equal(t, 1.0, testing.AllocsPerRun(100, func() { sink = textKey(text) }), "%+q", text)
	}
	_ = sink
}

// textKeyOracleDir holds an external oracle's vectors, keys the same steps
// written in Cypher gave on NornicDB (the default parser, develop 1b436931)
// with a digit map from Python's unicodedata: text-key-curated.json and
// text-key-codepoints.json.gz (every scalar value), vectors {"in": …,
// "out": …} (each a string or null) as one JSON array or one after another;
// versions.json, the versions and the statement they were produced with;
// SHA256SUMS, their digests. The code point file (5.6 MB) isn't committed:
// put it there, untracked, to run it. NORNICDB_TEXT_KEY_ORACLE_DIR names
// another directory.
func textKeyOracleDir() string {
	if dir := os.Getenv("NORNICDB_TEXT_KEY_ORACLE_DIR"); dir != "" {
		return dir
	}
	return filepath.Join("testdata", "text_key_oracle")
}

var textKeyOracleFiles = []string{"text-key-curated.json", "text-key-codepoints.json.gz"}

type textKeyVector struct {
	In  *string `json:"in"`
	Out *string `json:"out"`
}

// readTextKeyVectors reads a vector file, gzip-compressed when its name ends
// in .gz; false when it isn't there.
func readTextKeyVectors(t *testing.T, path string) ([]textKeyVector, bool) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false
	}
	require.NoError(t, err)
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		unzipped, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer unzipped.Close()
		reader = unzipped
	}
	buffered := bufio.NewReaderSize(reader, 1<<20)
	first := byte(0)
	for {
		c, err := buffered.ReadByte()
		require.NoError(t, err, "%s is empty", path)
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			first = c
			require.NoError(t, buffered.UnreadByte())
			break
		}
	}
	decoder := json.NewDecoder(buffered)
	var vectors []textKeyVector
	if first == '[' {
		_, err := decoder.Token()
		require.NoError(t, err)
		for decoder.More() {
			var vector textKeyVector
			require.NoError(t, decoder.Decode(&vector))
			vectors = append(vectors, vector)
		}
		return vectors, true
	}
	for {
		var vector textKeyVector
		err := decoder.Decode(&vector)
		if errors.Is(err, io.EOF) {
			return vectors, true
		}
		require.NoError(t, err)
		vectors = append(vectors, vector)
	}
}

// TestTextKeyOracleVectors checks textKey against the oracle's vectors, when
// they are there (textKeyOracleDir).
func TestTextKeyOracleVectors(t *testing.T) {
	dir := textKeyOracleDir()
	if versions, err := os.ReadFile(filepath.Join(dir, "versions.json")); err == nil {
		t.Logf("oracle versions.json: %s", strings.TrimSpace(string(versions)))
	}
	t.Logf("NornicDB's tables: unicode %s (%d decimal digits beyond ASCII), x/text norm %s, x/text cases %s",
		unicode.Version, len(referenceDigits()), norm.Version, cases.UnicodeVersion)
	checkTextKeyOracleSums(t, dir)
	found := false
	for _, name := range textKeyOracleFiles {
		vectors, ok := readTextKeyVectors(t, filepath.Join(dir, name))
		if !ok {
			t.Logf("%s: absent", name)
			continue
		}
		found = true
		t.Run(name, func(t *testing.T) { checkTextKeyVectors(t, vectors) })
	}
	if !found {
		t.Skipf("no oracle vectors: none of %s is in %s (NORNICDB_TEXT_KEY_ORACLE_DIR names another directory)",
			strings.Join(textKeyOracleFiles, ", "), filepath.ToSlash(dir))
	}
}

// checkTextKeyOracleSums checks each oracle file that is there against its
// digest in SHA256SUMS, when that is there.
func checkTextKeyOracleSums(t *testing.T, dir string) {
	sums, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSpace(string(sums)), "\n") {
		fields := strings.Fields(line)
		require.Len(t, fields, 2, "SHA256SUMS line %q", line)
		name := strings.TrimPrefix(fields[1], "*")
		data, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		digest := sha256.Sum256(data)
		require.Equal(t, fields[0], hex.EncodeToString(digest[:]), "%s's SHA-256", name)
	}
}

// checkTextKeyVectors checks that each vector's out is its in's key. A null
// in is skipped: the key of null is null (TestTextKeyFunction), where the
// oracle's Cypher gives ''. A mismatch only in digits, where the oracle's
// digit table and Go's unicode.Nd read a code point differently, is counted
// apart too.
func checkTextKeyVectors(t *testing.T, vectors []textKeyVector) {
	require.NotEmpty(t, vectors)
	mismatches, digitMismatches, nulls := 0, 0, 0
	for _, vector := range vectors {
		if vector.In == nil {
			nulls++
			continue
		}
		got := textKey(*vector.In)
		if vector.Out != nil && got == *vector.Out {
			continue
		}
		mismatches++
		if vector.Out != nil && onlyDigitsDiffer(got, *vector.Out) {
			digitMismatches++
		}
		if mismatches <= 50 {
			want := "null"
			if vector.Out != nil {
				want = fmt.Sprintf("%+q", *vector.Out)
			}
			t.Errorf("textKey(%+q) (%s) = %+q, oracle %s", *vector.In, textKeyCodePoints(*vector.In), got, want)
		}
	}
	t.Logf("%d vectors (%d with a null input, skipped): %d mismatches, %d of them only in digits", len(vectors), nulls, mismatches, digitMismatches)
	require.Zero(t, mismatches)
}

// onlyDigitsDiffer reports whether two keys differ only at code points where
// one has an ASCII digit.
func onlyDigitsDiffer(got, want string) bool {
	a, b := []rune(got), []rune(want)
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] && !('0' <= a[index] && a[index] <= '9') && !('0' <= b[index] && b[index] <= '9') {
			return false
		}
	}
	return true
}

func textKeyCodePoints(text string) string {
	points := make([]string, 0, len(text))
	for _, r := range text {
		points = append(points, fmt.Sprintf("U+%04X", r))
	}
	return strings.Join(points, " ")
}
