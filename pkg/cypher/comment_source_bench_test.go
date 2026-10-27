package cypher

import (
	"context"
	"strings"
	"testing"
)

// BenchmarkCommentSourceConsumers keeps comment projection costs visible through
// the statement and diagnostic consumers, with one fixed failed-grammar verdict
// so parser prediction is not conflated with the comment formatting work.
func BenchmarkCommentSourceConsumers(b *testing.B) {
	for _, input := range []struct{ name, query string }{
		{"short", "MATCH (n) /* short note */ RETURN n"},
		{"multiline", "MATCH (n) /* line1\r\nline2\rline3\n */ RETURN n"},
		{"large", "MATCH (n) /*" + strings.Repeat("note\n", 1024) + "*/ RETURN n"},
	} {
		b.Run("statement/"+input.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if StripComments(input.query) == "" {
					b.Fatal("statement disappeared")
				}
			}
		})
		b.Run("diagnostic/"+input.name, func(b *testing.B) {
			b.ReportAllocs()
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				if redactLiteralsContext(ctx, input.query, true, func(string) bool { return false }) == "" {
					b.Fatal("diagnostic disappeared")
				}
			}
		})
	}
}
