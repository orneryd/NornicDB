package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Compare the same escaped-string source on each side. The pre-fix splitter
// produces incorrect boundaries, so correctness is asserted by the public
// matrix rather than inside this scanner-cost benchmark.
func BenchmarkTopLevelAndConjunctsEscapedString(b *testing.B) {
	const expression = `n.text = 'O\'Reilly AND Sons' AND n.id = 1`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if len(splitTopLevelAndConjuncts(expression)) == 0 {
			b.Fatal("missing conjuncts")
		}
	}
}

// The same pre-existing valid map is measured on both sides. Quoted-key
// correctness is established separately, rather than benchmarking a base error.
func BenchmarkMapTextConsumers(b *testing.B) {
	const text = "{first:'a,b:c{}[]()',second:[1,2,3],third:{leaf:'alpha'}}"
	for _, name := range []string{"context", "strict", "syntax", "cached_plan"} {
		b.Run(name, func(b *testing.B) {
			e := NewStorageExecutor(newTestMemoryEngine(b))
			ctx := withExpressionFailureSlot(context.Background())
			if _, err := parsePropertyExpressions(text); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				switch name {
				case "context":
					e.evaluateMapLiteralFull(ctx, text, nil, nil, nil, nil, nil, 0)
					err = getExpressionFailure(ctx)
				case "strict":
					_, err = e.parseSetMergeMapLiteralStrict(ctx, text)
				case "syntax":
					err = validateMapLiteralSyntax(text)
				case "cached_plan":
					_, err = parsePropertyExpressions(text)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Selected-path planning reads node and relationship property maps through
// the same canonical token owner as materialization. The query deliberately
// asks for a value the fixture does not have: the pre-fix side dropped that
// constraint, while the corrected side evaluates it. The benchmark measures
// the full public path with statement caches disabled and does not assert the
// result so both sides remain measurable.
func BenchmarkSelectedPathPropertyMapPlanning(b *testing.B) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "selected_map_bench"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:R6Bench {id:1})-[:R6Bench {`first,last`:1}]->(:R6Bench {id:2})", nil)
	if err != nil {
		b.Fatal(err)
	}
	query := "MATCH p = SHORTEST 1 (a:R6Bench {id:1})(()-[:R6Bench {`first,last`:2}]->())+(b:R6Bench {id:2}) RETURN count(*) AS c"
	if _, err = e.Execute(ctx, query, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err = e.Execute(ctx, query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
