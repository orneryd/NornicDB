package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Review lane R1 of PR #1826: allocation and time of the routes whose work the
// PR changes outside the queries it measures. Compiles on head and base.

func r1BenchEngine(b *testing.B) *BadgerEngine {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	return engine
}

func r1BenchNode(i int) *Node {
	return &Node{ID: NodeID(fmt.Sprintf("bench:n-%06d", i)), Labels: []string{"Doc"}, Properties: map[string]any{
		"key": fmt.Sprintf("k-%06d", i), "seq": int64(i), "status": "current", "path": "/docs/example", "body": strings.Repeat("documentation body ", 128),
	}}
}

// One update commit per iteration, in a database that declares unique
// constraints: the commit reads the stored image of the updated node first.
func BenchmarkR1UpdateCommit(b *testing.B) {
	for _, constraints := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("constraints=%d", constraints), func(b *testing.B) {
			engine := r1BenchEngine(b)
			ctx := context.Background()
			schema := engine.GetSchemaForNamespace("bench")
			for c := 0; c < constraints; c++ {
				if err := schema.AddConstraint(Constraint{Name: fmt.Sprintf("r1_c%d", c), Type: ConstraintUnique, EntityType: ConstraintEntityNode, Label: fmt.Sprintf("L%d", c), Properties: []string{fmt.Sprintf("p%d", c)}}); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := engine.CreateNode(ctx, r1BenchNode(0)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				i++
				tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
				if err != nil {
					b.Fatal(err)
				}
				node, err := tx.GetNode(ctx, "bench:n-000000")
				if err != nil {
					b.Fatal(err)
				}
				after := CopyNode(node)
				after.Properties["seq"] = int64(i)
				if err := ApplyNodeUpdate(ctx, tx, NodeUpdateRequest{Before: node, After: after}); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// One label scan of two small properties per iteration.
func BenchmarkR1LabelScanProjected(b *testing.B) {
	for _, count := range []int{500, 5000, 12000} {
		b.Run(fmt.Sprintf("nodes=%d", count), func(b *testing.B) {
			engine := r1BenchEngine(b)
			ctx := context.Background()
			for start := 0; start < count; start += 500 {
				var nodes []*Node
				for i := start; i < min(start+500, count); i++ {
					nodes = append(nodes, r1BenchNode(i))
				}
				if err := engine.BulkCreateNodes(ctx, nodes); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < count; i++ {
				engine.cacheDeleteNode(NodeID(fmt.Sprintf("bench:n-%06d", i)), nodeCacheAllForms)
			}
			b.ReportAllocs()
			for b.Loop() {
				seen := 0
				if err := engine.StreamNodesByLabelProjected(ctx, "Doc", []string{"key", "seq"}, func(*Node) error { seen++; return nil }); err != nil {
					b.Fatal(err)
				}
				if seen != count {
					b.Fatal(seen)
				}
			}
			engine.nodeBodyCacheMu.RLock()
			b.ReportMetric(float64(engine.nodeBodyCacheBytes), "cached-B")
			engine.nodeBodyCacheMu.RUnlock()
		})
	}
}

// One point read of three small properties of a node with a 2.4 KB body, warm.
func BenchmarkR1PointProjectedWarm(b *testing.B) {
	engine := r1BenchEngine(b)
	ctx := context.Background()
	if _, err := engine.CreateNode(ctx, r1BenchNode(0)); err != nil {
		b.Fatal(err)
	}
	engine.cacheDeleteNode("bench:n-000000", nodeCacheAllForms)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := engine.GetNodeProjected(ctx, "bench:n-000000", []string{"key", "path", "status"}); err != nil {
			b.Fatal(err)
		}
	}
}

// Unrelated single-node updates while four readers scan a label projected:
// the time a write takes beside the scans.
func BenchmarkR1WriteBesideLabelScans(b *testing.B) {
	engine := r1BenchEngine(b)
	ctx := context.Background()
	const count = 5000
	for start := 0; start < count; start += 500 {
		var nodes []*Node
		for i := start; i < start+500; i++ {
			nodes = append(nodes, r1BenchNode(i))
		}
		if err := engine.BulkCreateNodes(ctx, nodes); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := engine.CreateNode(ctx, &Node{ID: "bench:other", Labels: []string{"Other"}, Properties: map[string]any{"seq": int64(0)}}); err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{}, 4)
	for range 4 {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-stop:
					return
				default:
					_ = engine.StreamNodesByLabelProjected(ctx, "Doc", []string{"key", "seq"}, func(*Node) error { return nil })
				}
			}
		}()
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		if err := engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "bench:other", Labels: []string{"Other"}, Properties: map[string]any{"seq": int64(i)}}}); err != nil {
			b.Fatal(err)
		}
	}
	close(stop)
	for range 4 {
		<-done
	}
}
