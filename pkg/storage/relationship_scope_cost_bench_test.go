package storage

import (
	"fmt"
	"testing"
)

// Isolate the durable owner from I/O. Every body key is already classified by
// its operation, exactly as UpdateEdge stages it. Unrelated cached types remain
// valid throughout, so their population should not determine this write's cost.
func BenchmarkRelationshipScopeCoveredWrites(b *testing.B) {
	for _, cached := range []int{0, 100000} {
		for _, raw := range []bool{false, true} {
			b.Run(fmt.Sprintf("unrelated=%d/raw=%t", cached, raw), func(b *testing.B) {
				e := NewMemoryEngine()
				defer e.Close()
				engine := e.BadgerEngine
				entries := make([]*Edge, cached)
				for i := range entries {
					entries[i] = &Edge{ID: EdgeID(fmt.Sprintf("test:unrelated-%d", i)), StartNode: "test:u", EndNode: "test:v", Type: "UNRELATED"}
				}
				engine.edgeTypeCache["UNRELATED"] = edgeListCacheEntry{edges: entries, readTs: engine.nodeCacheCommitTs.Load()}
				tx := &BadgerTransaction{}
				if raw {
					tx.pendingWrites = make(map[string][]byte)
				}
				for i := 0; i < 128; i++ {
					edge := &Edge{ID: EdgeID(fmt.Sprintf("test:updated-%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "CHANGED"}
					tx.operations = append(tx.operations, Operation{Type: OpUpdateEdge, EdgeID: edge.ID, Edge: edge, OldEdge: edge})
					if raw {
						tx.pendingWrites[string(edgeKey(edge.ID))] = []byte{1}
					}
				}
				scope := transactionNodeCacheWrites(tx)
				start := engine.nodeCacheCommitTs.Load()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					engine.cacheCommitNodeWrites(start+uint64(i)+1, scope)
				}
				b.StopTimer()
			})
		}
	}
}
