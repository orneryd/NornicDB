package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkStretchEffectsConvergence(b *testing.B) {
	for _, mode := range []string{"cold", "compiled"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			text := "CREATE (n:Doc {copied:m.source})"
			if _, err := pipelineClauseEffectsFor(ctx, text); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := text
				if mode == "cold" {
					query = fmt.Sprintf("CREATE (n:Doc%d {copied:m.source})", i)
				}
				if _, err := pipelineClauseEffectsFor(ctx, query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStretchMapKeyAdmission(b *testing.B) {
	b.Run("BenchmarkStretchMapKeyConvergence", func(b *testing.B) {
		for _, text := range []string{"{alpha: 1,beta:2,gamma:3}", "{`odd key`: {inner:1},plain:m.source}"} {
			b.Run(text, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var tokens stretchTokens
					addStretchMapKeys(text, &tokens)
					if len(tokens.keys) == 0 {
						b.Fatal("no keys")
					}
				}
			})
		}
	})
	b.Run("BenchmarkStretchMapKeyColdConvergence", func(b *testing.B) {
		serial := 0
		for _, quoted := range []bool{false, true} {
			b.Run(fmt.Sprint(quoted), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					serial++
					text := fmt.Sprintf("{alpha: %d,beta:2,gamma:3}", serial)
					if quoted {
						text = fmt.Sprintf("{`odd key`: {inner:%d},plain:m.source}", serial)
					}
					var tokens stretchTokens
					addStretchMapKeys(text, &tokens)
					if len(tokens.keys) == 0 {
						b.Fatal("no keys")
					}
				}
			})
		}
	})
}

func BenchmarkPatternPropertyAdmission(b *testing.B) {
	b.Run("BenchmarkPatternPropertyBindingConvergence", func(b *testing.B) {
		for _, mode := range []string{"cold", "compiled", "reserved"} {
			b.Run(mode, func(b *testing.B) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "binding_convergence"))
				ctx := context.Background()
				if mode == "reserved" {
					ctx = withParams(ctx, map[string]interface{}{"__nornic_pattern_property_0": int64(9)})
				}
				row := map[string]interface{}{"v": int64(1)}
				pattern := "(n:BindingConvergence {id:v})"
				if _, _, err := e.bindPatternProperties(ctx, pattern, row); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					text := pattern
					if mode == "cold" {
						text = fmt.Sprintf("(n:BindingConvergence%d {id:v})", i)
					}
					if _, _, err := e.bindPatternProperties(ctx, text, row); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
	b.Run("BenchmarkPublicPatternPropertyConvergence", func(b *testing.B) {
		e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "binding_convergence"))
		ctx := context.Background()
		query := "UNWIND [1,2] AS wanted MERGE (n:BindingConvergence {id:wanted}) RETURN n.id"
		if _, err := e.Execute(ctx, query, nil); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, err := e.Execute(ctx, query, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(r.Rows) != 2 {
				b.Fatal(r.Rows)
			}
		}
	})
}
