package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkProjectedSmallPropertyLabelScan measures the Product-label scan
// shape used by Northwind's low-stock query: a sparse label population and
// three small projected properties, with a selective predicate in the caller.
func BenchmarkProjectedSmallPropertyLabelScan(b *testing.B) {
	for _, layout := range []struct {
		name         string
		experimental bool
	}{{"standard", false}, {"experimental", true}} {
		for _, projected := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/projected=%t", layout.name, projected), func(b *testing.B) {
				ctx := context.Background()
				engine, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, ExperimentalLayout: layout.experimental, Projected: projected})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = engine.Close() })
				const products = 4096
				ids := make([]NodeID, 2*products+512)
				for i := range ids {
					ids[i] = NodeID(fmt.Sprintf("bench:n%05d", i))
				}
				for start := 0; start < 2*products+512; start += 512 {
					end := min(start+512, 2*products+512)
					nodes := make([]*Node, 0, end-start)
					for i := start; i < end; i++ {
						label := "Other"
						if i < products {
							label = "Product"
						}
						nodes = append(nodes, &Node{ID: ids[i], Labels: []string{label}, Properties: map[string]any{
							"productName": fmt.Sprintf("Product %05d", i), "unitsInStock": int64(i % 100), "unitPrice": float64(i%100) + 0.25,
						}})
					}
					if err := engine.BulkCreateNodes(ctx, nodes); err != nil {
						b.Fatal(err)
					}
				}
				properties := []string{"productName", "unitsInStock", "unitPrice"}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					b.StopTimer()
					for _, id := range ids {
						engine.cacheDeleteNode(id, nodeCacheAllForms)
					}
					b.StartTimer()
					seen := 0
					kept := 0
					err := engine.StreamNodesByLabelProjectedInScope(ctx, "bench:", "Product", properties, func(node *Node) error {
						seen++
						if node.Properties["unitsInStock"].(int64) < 25 {
							kept++
						}
						return nil
					})
					if err != nil {
						b.Fatal(err)
					}
					if seen != products || kept != 1025 {
						b.Fatalf("unexpected scan counts: %d, %d", seen, kept)
					}
				}
			})
		}
	}
}
