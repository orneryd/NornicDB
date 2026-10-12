package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// string.caseFold and string.asciiDigits, with normalize(s, NFKC) and
// string.regexReplace, compute an identity match key in Cypher (Personal
// Documents): NFKC, full case folding, whitespace runs as one space,
// decimal digits of any script as ASCII, and '-./' dropped from a key of
// digits only.
func TestStringKeyFunctions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "string_keys"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN string.caseFold('Straße') AS v":                             "strasse",
		"RETURN string.caseFold('STRASSE') AS v":                            "strasse",
		"RETURN string.caseFold('ΣΊΣΥΦΟΣ σίσυφος') AS v":                    "σίσυφοσ σίσυφοσ",
		"RETURN string.caseFold('ǅemal') AS v":                              "ǆemal",
		"RETURN string.caseFold(normalize('ǅemal', NFKC)) AS v":             "džemal",
		"RETURN string.caseFold(null) AS v":                                 nil,
		"RETURN string.asciiDigits('٢٠٢٦ १२३ １２ 𝟘𝟙 ৭') AS v":                "2026 123 12 01 7",
		"RETURN string.asciiDigits('No. 42-b') AS v":                        "No. 42-b",
		"RETURN string.asciiDigits('Ⅻ ½ ①') AS v":                           "Ⅻ ½ ①",
		"RETURN string.asciiDigits(null) AS v":                              nil,
		"CYPHER 25 RETURN string.caseFold('Ǆ') = string.caseFold('ǆ') AS v": true,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for _, query := range []string{"RETURN string.caseFold(5) AS v", "RETURN string.asciiDigits(['1']) AS v"} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	_, err := exec.Execute(ctx, "RETURN string.caseFold('a', 'b') AS v", nil)
	require.Error(t, err)

	key := "WITH string.regexReplace(string.asciiDigits(string.caseFold(normalize($raw, NFKC))), '\\\\s+', ' ') AS k " +
		"WITH trim(k) AS k RETURN CASE WHEN k =~ '[0-9./-]+' THEN string.regexReplace(k, '[-./]', '') ELSE k END AS key"
	for raw, want := range map[string]string{
		"  Müller   STRASSE\t1 ": "müller strasse 1",
		"MÜLLER Straße 1":        "müller strasse 1",
		"١٢٣-٤٥٦/٧٨":             "12345678",
		"１２．３４":                  "1234",
		"A-12.3":                 "a-12.3",
	} {
		result, err := exec.Execute(ctx, key, map[string]interface{}{"raw": raw})
		require.NoError(t, err, raw)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, raw)
	}
}
