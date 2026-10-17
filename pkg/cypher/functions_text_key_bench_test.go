package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// textKey per text: ASCII already a key (no allocation), ASCII to fold, and
// texts beyond ASCII already a key, with compatibility forms, Greek and
// another script's digits.
func BenchmarkTextKey(b *testing.B) {
	for _, c := range []struct{ name, text string }{
		{"ascii-key", "acme corporation 42"},
		{"ascii-fold", "  ACME   Corporation\t42 "},
		{"unicode-key", "müller strasse 12"},
		{"fullwidth", "ＡＣＭＥ\u3000Ｃｏｒｐｏｒａｔｉｏｎ\u3000４２"},
		{"greek", "ΟΔΟΣ ΣΙΣΥΦΟΥ \u03aa\u0301"},
		{"digits", "رقم ٢٠٢٦/٠٤ — सं २०२६"},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var sink string
			for i := 0; i < b.N; i++ {
				sink = textKey(c.text)
			}
			_ = sink
		})
	}
}

// nornicdb.text.key over 1,000 names in one statement, and the same keys
// folded in plain Cypher with a digit map passed as a parameter.
func BenchmarkTextKeyQuery(b *testing.B) {
	names := make([]interface{}, 1000)
	for i := range names {
		names[i] = fmt.Sprintf("  Müller-ＳＴＲＡßＥ  %d ٢٠٢٦ Name%d", i, i%7)
	}
	digits := map[string]interface{}{}
	for _, zero := range keyDigitZeros {
		for value := rune(0); value < 10; value++ {
			digits[string(zero+value)] = string('0' + value)
		}
	}
	for _, c := range []struct{ name, query string }{
		{"function", "UNWIND $names AS s RETURN nornicdb.text.key(s) AS k"},
		{"cypher-fold", "UNWIND $names AS s " +
			"WITH replace(replace(toLower(normalize(s, NFKC)), 'ß', 'ss'), 'ς', 'σ') AS folded " +
			"RETURN trim(string.regexReplace(reduce(out = '', c IN split(normalize(folded, NFKC), '') | out + coalesce($digits[c], c)), '\\s+', ' ')) AS k"},
	} {
		b.Run(c.name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "textkeybench"))
			ctx := context.Background()
			params := map[string]interface{}{"names": names, "digits": digits}
			if _, err := exec.Execute(ctx, c.query, params); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, c.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
