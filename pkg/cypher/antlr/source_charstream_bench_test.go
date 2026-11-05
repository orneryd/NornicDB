package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"
	"testing"
)

// Public lexer and validator qualification for both source stages. Live
// cancellation contexts are not expired; deterministic tests cover interruption.
func BenchmarkANTLRSourceStreamPublic(b *testing.B) {
	for _, workload := range []struct{ name, query string }{
		{"ASCII", `MATCH (n:Person) WHERE n.age > 21 RETURN n.name`},
		{"Unicode", `RETURN 'λ😀' AS value`},
		{"Projected", `RETURN '\u03bb' AS value`},
		{"LoneUnit", `RETURN '\ud800' AS value`},
		{"LongASCII", "RETURN '" + strings.Repeat("x", 4096) + "' AS value"},
		{"LongUnicode", "RETURN '" + strings.Repeat("界", 512) + "' AS value"},
	} {
		for _, stage := range []SourceStage{RawSource, AcceptedSource} {
			stageName := "Raw"
			query := workload.query
			if stage == AcceptedSource {
				stageName = "Accepted"
				view := cyphertext.NewSource(b.Context(), query, cyphertext.RawSource)
				if view.Err != nil {
					b.Fatal(view.Err)
				}
				query = view.Text
			}
			for _, cancelable := range []bool{false, true} {
				name := workload.name + "/" + stageName + "/background"
				ctx := context.Background()
				if cancelable {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					b.Cleanup(cancel)
					name = workload.name + "/" + stageName + "/cancelable"
				}
				b.Run(name+"/Lex", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := LexContext(ctx, query, stage); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run(name+"/ValidateCold", func(b *testing.B) {
					statementSyntaxCache.Lock()
					clear(statementSyntaxCache.entries)
					statementSyntaxCache.Unlock()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						cold := query + " /* cold " + strconv.Itoa(i) + " */"
						if err := ValidateContext(ctx, cold, stage); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run(name+"/ValidateWarm", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if err := ValidateContext(ctx, query, stage); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
