// Author: #1683 implementation lane (Codex), 2026-11-01.
package cypher

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This carrier uses release APIs so identical source can measure the old and
// compact owners. The original 6k/12k profile fixture is deliberately unchanged.
// Measure after GC while the executor lives; the result cache remains disabled.
func BenchmarkNOR148FulltextLargeDocumentRetention(b *testing.B) {
	for _, size := range []int{32 << 10, 256 << 10, 9 << 20} {
		for _, mixedCase := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/mixed_case=%t", size, mixedCase), func(b *testing.B) {
				// In-memory Badger rejects values above its 64 KiB threshold.
				// Real large documents use the disk-backed value log; keep the
				// default storage limits and exercise that supported shape.
				base, err := fixtureowner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(b.TempDir()))
				require.NoError(b, err)
				require.False(b, base.IsInMemory())
				executor, store := fulltextMaintainedFixtureOn(b, base, 4)
				// Exclude the fixed service startup from a one-document delta
				// in both arms. The target corpus is still first read below.
				service := search.NewService(store, search.DefaultVectorDimensions, nil)
				executor.SetSearchService(service)
				b.Cleanup(func() { require.NoError(b, service.Close()) })
				word := "contract hotel services schedule "
				if mixedCase {
					word = "Contract Hotel Services Schedule "
				}
				text := strings.Repeat(word, (size+len(word)-1)/len(word))[:size]
				node, err := store.GetNode(b.Context(), "n000000")
				require.NoError(b, err)
				node.Properties["text"] = text
				require.NoError(b, store.UpdateNode(b.Context(), storage.NodeUpdateRequest{After: node}))
				node, text = nil, ""
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				query := "CALL db.index.fulltext.queryNodes('source_text','hotel') YIELD node,score RETURN count(node) AS n"
				b.ResetTimer()
				for range b.N {
					for range 12 {
						result, err := executor.Execute(b.Context(), query, nil)
						require.NoError(b, err)
						require.Equal(b, [][]any{{int64(1)}}, result.Rows)
					}
				}
				b.StopTimer()
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
				b.Logf("large-document-retention indexed=1 text-bytes=%d mixed-case=%t calls=%d heap-before=%d heap-after-gc=%d delta-bytes=%d KB-per-indexed-document=%.3f", size, mixedCase, b.N*12, before.HeapAlloc, after.HeapAlloc, delta, float64(delta)/1024)
				runtime.KeepAlive(executor)
			})
		}
	}
}
