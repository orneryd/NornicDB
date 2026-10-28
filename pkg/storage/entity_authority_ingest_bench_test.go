package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkEntityAuthorityFootprint models the endpoint and relationship
// identities in one 2,000-row relationship batch, without disk I/O.
func BenchmarkEntityAuthorityFootprint(b *testing.B) {
	for _, n := range []int{1, 2000} {
		b.Run(fmt.Sprintf("edges=%d", n), func(b *testing.B) {
			endpoints := make([]NodeID, 0, 2*n)
			edges := make([]EdgeID, 0, n)
			for i := 0; i < n; i++ {
				endpoints = append(endpoints, NodeID(fmt.Sprintf("test:s%d", i)), NodeID(fmt.Sprintf("test:t%d", i)))
				edges = append(edges, EdgeID(fmt.Sprintf("test:e%d", i)))
			}
			keys := entityCommitKeys(nil, edges, endpoints)
			sm := NewSchemaManager()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				release, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "bench", keys)
				if err != nil {
					b.Fatal(err)
				}
				release()
			}
		})
	}
}
