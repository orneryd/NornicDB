package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkPendingFirstClaim measures the first claim after the catalog is
// lost (a restart or invalidation) with a large live pending backlog.
func BenchmarkPendingFirstClaim(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("pending=%d", n), func(b *testing.B) {
			e, err := NewBadgerEngine(b.TempDir())
			require.NoError(b, err)
			defer e.Close()
			e.SetEmbeddingsEnabled(true)
			const chunk = 10_000
			for from := 0; from < n; from += chunk {
				nodes := make([]*Node, 0, chunk)
				for i := from; i < from+chunk && i < n; i++ {
					nodes = append(nodes, pendingCandidateNode(i))
				}
				require.NoError(b, e.BulkCreateNodes(nodes))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e.InvalidatePendingEmbeddingsIndex()
				if e.FindNodeNeedingEmbedding() == nil {
					b.Fatal("no pending node")
				}
			}
		})
	}
}
