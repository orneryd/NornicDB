package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The functions reference's identity match key is plain Cypher: NFKC, lower
// case with ß and final sigma folded, decimal digits of any script mapped by
// a parameter, whitespace runs as one space, and -./ dropped from a key of
// digits only. The two examples give the results the reference shows.
func TestIdentityMatchKeyInCypher(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "matchkey"))
	ctx := context.Background()
	const statement = `WITH replace(replace(toLower(normalize($raw, NFKC)), 'ß', 'ss'), 'ς', 'σ') AS folded
WITH trim(string.regexReplace(reduce(out = '', c IN split(folded, '') | out + coalesce($digits[c], c)), '\\s+', ' ')) AS k
RETURN CASE WHEN k =~ '[0-9./-]+' THEN string.regexReplace(k, '[-./]', '') ELSE k END AS key`
	digits := map[string]interface{}{}
	for index, digit := range []rune("٠١٢٣٤٥٦٧٨٩") {
		digits[string(digit)] = string(rune('0' + index))
	}
	for raw, want := range map[string]string{
		"  MÜLLER   Straße 1": "müller strasse 1",
		"١٢٣-٤٥٦/٧٨":          "12345678",
		"ΟΔΟΣ":                "οδοσ",
		"ＡＢＣ　１２":              "abc 12",
	} {
		result, err := exec.Execute(ctx, statement, map[string]interface{}{"raw": raw, "digits": digits})
		require.NoError(t, err, raw)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, raw)
	}
}
