package storage

// Root-cause diagnostic (campaign v1.5.2, lane D, group G-testfixture-lifetime-audit; run by lane A).
// Test-side copy of BenchmarkBadgerEngine_BulkCreateNodes (badger_test.go:2022-2047)
// with exactly one change: the deferred close reads the engine variable when
// it runs (defer func() { engine.Close() }()) instead of binding the first
// engine at defer time (badger_test.go:2029). Run beside the original:
//   -run '^$' -bench '^(BenchmarkBadgerEngine_BulkCreateNodes|BenchmarkRCABulkCreateNodesDeferCurrent)$' -benchtime=3x

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

func BenchmarkRCABulkCreateNodesDeferCurrent(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { engine.Close() }()

			nodes := make([]*Node, size)
			for i := 0; i < size; i++ {
				nodes[i] = &Node{
					ID:     NodeID(prefixTestID(fmt.Sprintf("bulk-%06d", i))),
					Labels: []string{"Bulk"},
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				engine.Close()
				engine, _ = fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
				engine.BulkCreateNodes(b.Context(), nodes)
			}
		})
	}
}
