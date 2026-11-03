package cypher

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The wrapper's read benches drive the public Engine methods a statement calls,
// so the measurement covers the whole read exit: the read itself, the ID-space
// projection, and the namespace filtering a namespaced wrapper performs. Both a
// small and a large source are measured so a per-record cost and a fixed cost
// are distinguishable.

// benchmarkReadRecords builds one namespace's node and relationship records of
// the given size, as an engine read returns them: namespace-prefixed IDs.
func benchmarkReadRecords(size int) ([]*storage.Node, []*storage.Edge) {
	nodes := make([]*storage.Node, 0, size)
	edges := make([]*storage.Edge, 0, size)
	for i := 0; i < size; i++ {
		suffix := strconv.Itoa(i)
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID("tenant:n" + suffix),
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"i": int64(i)},
		})
		edges = append(edges, &storage.Edge{
			ID:         storage.EdgeID("tenant:e" + suffix),
			StartNode:  "tenant:x",
			EndNode:    storage.NodeID("tenant:n" + suffix),
			Type:       "REL",
			Properties: map[string]interface{}{"i": int64(i)},
		})
	}
	return nodes, edges
}

// BenchmarkTransactionWrapperVisibleAt measures a snapshot-visible read through
// the wrapper: the node shape and the relationship shape over a small and a
// large source, each projecting the whole answer into the caller's namespace.
func BenchmarkTransactionWrapperVisibleAt(b *testing.B) {
	version := storage.MVCCVersion{CommitSequence: 1}
	for _, size := range []int{8, 1024} {
		nodes, edges := benchmarkReadRecords(size)
		engine := &recordingVisibilityEngine{MemoryEngine: newTestMemoryEngine(b), nodes: nodes, edges: edges}
		wrapper := &transactionStorageWrapper{underlying: engine, namespace: "tenant", separator: ":"}
		ctx := context.Background()

		b.Run(fmt.Sprintf("nodes/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				got, err := wrapper.GetNodesByLabelVisibleAt(ctx, "Doc", version)
				if err != nil || len(got) != size {
					b.Fatalf("nodes=%d err=%v, want %d", len(got), err, size)
				}
			}
		})
		b.Run(fmt.Sprintf("edges/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				got, err := wrapper.GetEdgesByTypeVisibleAt(ctx, "REL", version)
				if err != nil || len(got) != size {
					b.Fatalf("edges=%d err=%v, want %d", len(got), err, size)
				}
			}
		})
	}
}

// BenchmarkTransactionWrapperAdjacency measures the transaction's live
// adjacency read through the wrapper, whose projection returns every record the
// read returned.
func BenchmarkTransactionWrapperAdjacency(b *testing.B) {
	for _, size := range []int{8, 1024} {
		base := newTestMemoryEngine(b)
		tenant := storage.NewNamespacedEngine(base, "tenant")
		if _, err := tenant.CreateNode(b.Context(), &storage.Node{ID: "x", Labels: []string{"N"}}); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < size; i++ {
			if err := tenant.CreateEdge(b.Context(), &storage.Edge{
				ID:         storage.EdgeID("e" + strconv.Itoa(i)),
				StartNode:  "x",
				EndNode:    "x",
				Type:       "REL",
				Properties: map[string]interface{}{"i": int64(i)},
			}); err != nil {
				b.Fatal(err)
			}
		}
		tx, err := base.BeginTransaction(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = tx.Rollback() })
		if err := tx.SetNamespace("tenant"); err != nil {
			b.Fatal(err)
		}
		wrapper := &transactionStorageWrapper{tx: tx, underlying: tenant, namespace: "tenant", separator: ":"}

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				got, err := wrapper.GetOutgoingEdges(context.Background(), "x")
				if err != nil || len(got) != size {
					b.Fatalf("edges=%d err=%v, want %d", len(got), err, size)
				}
			}
		})
	}
}

// BenchmarkTransactionWrapperCount measures both count decision outcomes: the
// committed counter while the transaction staged no change of that kind, and
// the scan of the collection the transaction sees once it did.
func BenchmarkTransactionWrapperCount(b *testing.B) {
	for _, size := range []int{8, 1024} {
		for _, staged := range []bool{false, true} {
			decision := "counter"
			if staged {
				decision = "scan"
			}
			base := newTestMemoryEngine(b)
			tenant := storage.NewNamespacedEngine(base, "tenant")
			if _, err := tenant.CreateNode(b.Context(), &storage.Node{ID: "x", Labels: []string{"N"}}); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < size; i++ {
				if _, err := tenant.CreateNode(b.Context(), &storage.Node{ID: storage.NodeID("n" + strconv.Itoa(i)), Labels: []string{"N"}}); err != nil {
					b.Fatal(err)
				}
				if err := tenant.CreateEdge(b.Context(), &storage.Edge{
					ID:        storage.EdgeID("e" + strconv.Itoa(i)),
					StartNode: "x",
					EndNode:   "x",
					Type:      "REL",
				}); err != nil {
					b.Fatal(err)
				}
			}
			tx, err := base.BeginTransaction(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = tx.Rollback() })
			if err := tx.SetNamespace("tenant"); err != nil {
				b.Fatal(err)
			}
			if staged {
				if _, err := tx.CreateNode(b.Context(), &storage.Node{ID: "tenant:pending", Labels: []string{"N"}}); err != nil {
					b.Fatal(err)
				}
				if err := tx.CreateEdge(b.Context(), &storage.Edge{ID: "tenant:pending", StartNode: "tenant:x", EndNode: "tenant:x", Type: "REL"}); err != nil {
					b.Fatal(err)
				}
			}
			wrapper := &transactionStorageWrapper{tx: tx, underlying: tenant, namespace: "tenant", separator: ":"}

			b.Run(fmt.Sprintf("%s/nodes/%d", decision, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := wrapper.NodeCount(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%s/edges/%d", decision, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := wrapper.EdgeCount(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
