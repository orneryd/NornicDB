package cypher

import (
	"strconv"
	"testing"
	"time"
)

// nor307WidePublicQuery is the report's wide read written as a public embedded
// statement: a deterministic read whose every row is one 18-key map, so the
// executor caches it and each repeat is a result-cache hit. $salt only marks a
// distinct cache entry; it changes nothing about the row shape.
func nor307WidePublicQuery(rows int) string {
	return "UNWIND range(1, " + strconv.Itoa(rows) + ") AS i RETURN {" +
		"name: 'name-' + toString(i), seq: i, score: toFloat(i) / 2, rank: i," +
		" active: i % 2 = 0, group: 'group-' + toString(i % 17)," +
		" kind: 'kind-' + toString(i % 5), owner: 'owner-' + toString(i % 31)," +
		" region: 'region-' + toString(i % 7), status: 'status-' + toString(i % 3) + $salt," +
		" label: 'label-' + toString(i % 11), notes: null, unit: i, weight: toFloat(i) / 4," +
		" version: i % 1000, tags: ['a', 'b'], scores: [i, i + 1, i + 2]," +
		" aliases: ['first', 'second']} AS row"
}

// BenchmarkNOR307PublicWideCachedRead measures the report's wide read through
// the supported embedded Execute API on one executor and one store:
//
//   - miss varies only $salt, so every operation executes the statement and
//     publishes its result instead of serving one; it is the cost a hit avoids.
//   - hit repeats one entry after the cache reported serving it.
//
// Both arms measure the same public statement and result shape, and each keeps
// its own published images, so a hit is never mixed with execution work.
func BenchmarkNOR307PublicWideCachedRead(b *testing.B) {
	for _, rows := range []int{nor307WideCacheHitRows, 21748} {
		query := nor307WidePublicQuery(rows)
		name := "rows-" + strconv.Itoa(rows)
		b.Run(name, func(b *testing.B) {
			b.Run("miss", func(b *testing.B) {
				exec := NewStorageExecutor(newTestMemoryEngine(b), QueryCachePolicy{MaxEntries: 16, TTL: time.Hour})
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					result, err := exec.Execute(b.Context(), query, map[string]any{"salt": "miss-" + strconv.Itoa(i)})
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != rows {
						b.Fatalf("expected %d rows, got %d", rows, len(result.Rows))
					}
					nor307BenchmarkSink = result
				}
			})
			b.Run("hit", func(b *testing.B) {
				exec := NewStorageExecutor(newTestMemoryEngine(b), QueryCachePolicy{MaxEntries: 16, TTL: time.Hour})
				params := map[string]any{"salt": "hit"}
				if _, err := exec.Execute(b.Context(), query, params); err != nil {
					b.Fatal(err)
				}
				warm, err := exec.Execute(b.Context(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(warm.Rows) != rows {
					b.Fatalf("expected %d rows, got %d", rows, len(warm.Rows))
				}
				hitsBefore, _, _, _, _ := exec.cache.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := exec.Execute(b.Context(), query, params)
					if err != nil {
						b.Fatal(err)
					}
					nor307BenchmarkSink = result
				}
				hitsAfter, _, _, _, _ := exec.cache.Stats()
				if hitsAfter <= hitsBefore {
					b.Fatalf("the measured repeats were not cache hits (hits %d -> %d)", hitsBefore, hitsAfter)
				}
			})
		})
	}
}
