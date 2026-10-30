package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// The identical benchmark source is overlaid on the reviewed parent. It
// measures affected cold construction, cached facts and held-product typing;
// no unrelated execution/storage workload stands in for compiler cost.
func BenchmarkCanonicalBoundCompilation(b *testing.B) {
	ensureBuiltInProceduresRegistered()
	for _, width := range []int{1, 8, 32} {
		items := make([]string, width)
		for i := range items {
			items[i] = fmt.Sprintf("%d AS x%d", i, i)
		}
		query := "WITH " + strings.Join(items, ",") + " WITH *, x0 + 1 AS y WHERE y > 0 RETURN y, [v IN [x0] | v + 1] AS xs"
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bound-cost"))
			ctx := context.Background()
			compiler := projectionCompilation{executor: exec, source: query}
			facts := compiler.root(ctx, resultmetadata.Known(nil), resultmetadata.Known(nil))
			if facts.err != nil || facts.bound == nil {
				b.Fatalf("admission: %v", facts.err)
			}
			b.Run("cold-constructor", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					compiler := projectionCompilation{executor: exec, source: query}
					if f := compiler.root(ctx, resultmetadata.Known(nil), resultmetadata.Known(nil)); f.err != nil {
						b.Fatal(f.err)
					}
				}
			})
			exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource)
			b.Run("cached-facts", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource)
				}
			})
			held := context.WithValue(ctx, boundStatementProgramKey{}, facts.bound)
			b.Run("held-type-adapter", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := exec.enrichBoundTypes(held, query, nil, nil, "", false); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
