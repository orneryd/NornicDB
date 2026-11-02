package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// BenchmarkLabelRebuildReadCosts measures complete results in both states;
// a fast partial result is never accepted as a performance baseline. Index
// removal is fixture preparation outside the measured reads.
func BenchmarkLabelRebuildReadCosts(b *testing.B) {
	for _, size := range []int{1000, 60000} {
		b.Run(fmt.Sprintf("nodes%d", size), func(b *testing.B) {
			engine, err := fixture.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(b, err)
			for offset := 0; offset < size; offset += 1000 {
				nodes := make([]*Node, 0, min(1000, size-offset))
				for index := offset; index < min(offset+1000, size); index++ {
					nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("bench:n%06d", index)), Labels: []string{"Record"}, Properties: map[string]any{"value": int64(index)}})
				}
				require.NoError(b, engine.BulkCreateNodes(b.Context(), nodes))
			}
			for _, state := range []string{"ready", "building"} {
				if state == "building" {
					engine.labelIndexReadyAt.Store(0)
					require.NoError(b, engine.withUpdate(b.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error { return txn.Delete(labelIndexReadyKey) }))
					require.NoError(b, engine.db.DropPrefix(b.Context(), prefixDropOptions{engine: engine, metadata: true}, []byte{prefixLabelIndex}))
				}
				b.Run(state, func(b *testing.B) {
					for _, mode := range []string{"slice", "projected"} {
						b.Run(mode, func(b *testing.B) {
							benchmarkCompleteScopedLabelRead(b, engine, "bench:", "Record", []string{"value"}, size, mode)
						})
					}
				})
			}
		})
	}
}
