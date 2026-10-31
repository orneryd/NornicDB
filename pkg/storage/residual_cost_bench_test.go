package storage

import (
	"fmt"
	"testing"
	"time"
)

// Author: #1548 implementation lane (Codex). The paired runner applies these
// exact published benchmark bytes to both revisions. Existing workloads retain
// their setup, data, failure checks and allocation reporting.
func BenchmarkResidualCosts(b *testing.B) {
	b.Run("relationship-body", func(b *testing.B) {
		engine := benchEngine(b)
		for _, id := range []NodeID{"test:cost-a", "test:cost-b"} {
			if _, err := engine.CreateNode(&Node{ID: id}); err != nil {
				b.Fatal(err)
			}
		}
		id := EdgeID("test:cost-edge")
		if err := engine.CreateEdge(&Edge{ID: id, StartNode: "test:cost-a", EndNode: "test:cost-b", Type: "COST", Properties: map[string]any{"weight": int64(7)}}); err != nil {
			b.Fatal(err)
		}
		if _, err := engine.GetStoredEdge(id); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			edge, err := engine.GetStoredEdge(id)
			if err != nil {
				b.Fatal(err)
			}
			if edge.Properties["weight"] != int64(7) {
				b.Fatal("relationship body changed")
			}
		}
	})
	b.Run("relationship-type", BenchmarkBadger_GetEdgesByType_CacheHit)
	b.Run("commit", BenchmarkHighPerformanceDisjointEdgeUpdates)
	b.Run("numeric-list", BenchmarkDecodeTokenizedPropertiesGraphitiVector)
	// Author: #1548 implementation lane (Codex). Schema admission owns caller
	// containers too; these external API rows include that cold-path copy cost.
	b.Run("schema-definition", func(b *testing.B) {
		properties := make([]string, 32)
		for i := range properties {
			properties[i] = fmt.Sprintf("property-%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			schema := NewSchemaManager()
			if err := schema.AddCompositeIndex("idx", "L", properties); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("transaction-metadata", func(b *testing.B) {
		engine := benchEngine(b)
		tx, err := engine.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		defer tx.Rollback()
		metadata := map[string]any{"nested": map[string]any{"values": make([]int64, 32)}}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := tx.SetMetadata(metadata); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
	// External callers pay for ingress ownership even when the internal ACK
	// route does not call these APIs. Drain outside the timer to bound retained
	// generations and measure public Append CPU/allocations on identical input.
	for _, length := range []int{32, 1024} {
		for _, kind := range []string{"node", "edge"} {
			b.Run(fmt.Sprintf("buffer-append-%s/%d", kind, length), func(b *testing.B) {
				buffer := NewWriteBehindBuffer(time.Hour, 1000000, func(*CommitBuffer) error { return nil })
				defer buffer.Close()
				values := make([]int64, length)
				for i := range values {
					values[i] = int64(i)
				}
				properties := map[string]any{"nested": map[string]any{"values": values}}
				node := &Node{ID: "test:cost-buffer-node", Labels: []string{"COST"}, Properties: properties}
				edge := &Edge{ID: "test:cost-buffer-edge", StartNode: node.ID, EndNode: "test:cost-peer", Type: "COST", Properties: properties}
				buffer.SetDelayBounds(time.Hour, time.Hour)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if kind == "node" {
						if err := buffer.Append(BufferedMutation{Kind: BufferedOpNode, Node: node}); err != nil {
							b.Fatal(err)
						}
					} else {
						if err := buffer.Append(BufferedMutation{Kind: BufferedOpEdge, Edge: edge}); err != nil {
							b.Fatal(err)
						}
					}
					if (i+1)%1024 == 0 {
						b.StopTimer()
						if err := buffer.Flush(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
				}
				b.StopTimer()
			})
		}
	}
}
