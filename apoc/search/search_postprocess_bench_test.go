// Post-processing benchmarks for the apoc.search helpers that do more than one
// labeled scan: the suggestion owners, the fuzzy suggestion mapping, the score
// collection and the reduced-result search.
//
// The sub-benchmark names and the populations match the harness the
// pre-convergence bodies were measured with, so a run of this file and a run of
// that harness compare directly.
package search_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/search"
)

// BenchmarkAPOCSearchPostProcessing measures the helpers whose body continues
// after the labeled scan: Suggest (with an explicit limit, with the ten-entry
// default, and with an omitted limit, which is what the autocomplete name
// passes), DidYouMean, Score over the scan's matches, and NodeReduced.
func BenchmarkAPOCSearchPostProcessing(b *testing.B) {
	ctx := context.Background()
	for _, population := range scanBenchSizes {
		functions := search.New(&scanBenchStorage{nodes: scanBenchNodes(population.size)})
		matched := functions.Prefix(ctx, "Person", "name", "Alice")
		for _, helper := range []struct {
			name string
			call func()
		}{
			{name: "suggestLimit5", call: func() { functions.Suggest(ctx, "Person", "name", "Alice", 5) }},
			{name: "suggestDefault", call: func() { functions.Suggest(ctx, "Person", "name", "Alice", 10) }},
			{name: "autocomplete", call: func() { functions.Suggest(ctx, "Person", "name", "Alice") }},
			{name: "didYouMean", call: func() { functions.DidYouMean(ctx, "Person", "name", "Alise1") }},
			{name: "score", call: func() { functions.Score(ctx, matched, "name", "Alice") }},
			{name: "nodeReduced", call: func() {
				functions.NodeReduced(ctx, "Person", map[string]interface{}{"status": "active"}, 10)
			}},
		} {
			b.Run(population.name+"/"+helper.name, func(b *testing.B) {
				if len(matched) == 0 {
					b.Fatal("benchmark population must produce matches")
				}
				helper.call()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					helper.call()
				}
			})
		}
	}
}
