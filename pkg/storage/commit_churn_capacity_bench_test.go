package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// churnNodes is the row count of one public churn statement: the release
// qualification workload creates and then deletes 2000 nodes per cycle.
const churnNodes = 2000

// BenchmarkEntityWriteLocksCapacity isolates the transaction's entity write
// lock accumulator at the sizes a statement reaches: inline, its index
// threshold, every power-of-two capacity step the growth passes, and a bulk
// delete's holding.
func BenchmarkEntityWriteLocksCapacity(b *testing.B) {
	for _, count := range []int{1, 2, entityWriteLockIndexAfter, entityWriteLockIndexAfter + 1, 16, 17, 32, 33, 64, 65, 128, 129, 256, 257, 512, 513, 1024, 1025, 2048, 2049, churnNodes} {
		b.Run(fmt.Sprintf("locks=%d", count), func(b *testing.B) {
			requests := make([]uniqueConstraintLockRequest, count)
			for at := range requests {
				requests[at] = uniqueConstraintLockRequest{key: entityWriteLockKey(bufferedNodeEntity, fmt.Sprintf("test:lock-%d", at))}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var heldCapacity int
			for i := 0; i < b.N; i++ {
				var locks entityWriteLocks
				for _, request := range requests {
					locks.add(request)
				}
				heldCapacity = cap(locks.held)
				if len(locks.held) != count {
					b.Fatalf("held %d of %d locks", len(locks.held), count)
				}
				if count > entityWriteLockIndexAfter && len(locks.index) != count {
					b.Fatalf("indexed %d of %d locks", len(locks.index), count)
				}
			}
			b.ReportMetric(float64(heldCapacity), "held-cap")
		})
	}
}

// BenchmarkPhysicalCacheProjectionCapacity isolates the projection's fact
// accumulator on the key mix one labeled node write hands it: the node body,
// its label index entry, an embedding chunk, and the MVCC keys that install no
// cached body. churnNodes nodes' keys are staged, as one churn statement's
// commit stages them.
func BenchmarkPhysicalCacheProjectionCapacity(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	ctx := b.Context()
	_, err = engine.CreateNode(ctx, &Node{ID: "test:seed", Labels: []string{"Churn"}})
	require.NoError(b, err)
	seedNum, ok := engine.idDict.lookupNodeNumID("test:seed")
	require.True(b, ok, "test:seed must have a numeric ID")

	keys := make([][]byte, 0, 5*churnNodes)
	for row := 0; row < churnNodes; row++ {
		id := NodeID(fmt.Sprintf("test:projection-%d", row))
		keys = append(keys,
			nodeKey(id),
			labelIndexKey("Churn", seedNum),
			embeddingKey(id, 0),
			[]byte{prefixMVCCNode, 0x01},
			[]byte{prefixTemporalHead, 0x01},
		)
	}
	wantFacts := 3 * churnNodes
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var projection physicalCacheProjection
		for _, key := range keys {
			projection.add(engine, key, nil)
		}
		if projection.unknown {
			b.Fatal("every staged key belongs to a classified family")
		}
		if len(projection.facts) != wantFacts {
			b.Fatalf("projected %d of %d facts", len(projection.facts), wantFacts)
		}
	}
}
