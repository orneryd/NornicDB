package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// Measure the shared selector owner with a retained product and with new text.
func BenchmarkCourseChangeUseReference(b *testing.B) {
	for _, query := range []struct{ name, text string }{
		{"static", "USE tenant MATCH (n) RETURN n"},
		{"optional_graph", "USE GRAPH tenant MATCH (n) RETURN n"},
		{"dynamic", "USE graph.byName('tenant') MATCH (n) RETURN n"},
	} {
		b.Run(query.name, func(b *testing.B) {
			if _, _, _, err := fabric.ParseUseClause(b.Context(), query.text, false, cyphertext.RawSource); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := fabric.ParseUseClause(b.Context(), query.text, false, cyphertext.RawSource); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("new_text", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			query := fmt.Sprintf("USE tenant%d MATCH (n) RETURN n", i)
			if _, _, _, err := fabric.ParseUseClause(b.Context(), query, false, cyphertext.RawSource); err != nil {
				b.Fatal(err)
			}
		}
	})
}
