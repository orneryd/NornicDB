package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkLockedHubRelationshipReads measures a transaction's reads of a
// hub node's relationships (NOR-213): without the hub's write lock, with it
// and no commit after the snapshot, and with it after another commit (the
// case that reads the last committed view).
func BenchmarkLockedHubRelationshipReads(b *testing.B) {
	const degree = 20000
	for _, mode := range []string{"unlocked", "locked-snapshot-current", "locked-after-commit"} {
		for _, headers := range []bool{false, true} {
			read := "edges"
			if headers {
				read = "headers"
			}
			b.Run(fmt.Sprintf("deg=%d/%s/%s", degree, mode, read), func(b *testing.B) {
				ctx := context.Background()
				tx, hub := lockedHubFixture(b, degree, mode)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if headers {
						edges, answered, err := tx.OutgoingEdgeHeaders(ctx, hub)
						if err != nil || !answered || len(edges) != degree {
							b.Fatalf("headers: %d answered %v: %v", len(edges), answered, err)
						}
						continue
					}
					edges, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: hub})
					if err != nil || len(edges) != degree {
						b.Fatalf("edges: %d: %v", len(edges), err)
					}
				}
			})
		}
	}
}

func lockedHubFixture(b *testing.B, degree int, mode string) (*BadgerTransaction, NodeID) {
	b.Helper()
	ctx := context.Background()
	engine := newTestMemoryEngine(b)
	hub := NodeID("test:hub")
	nodes := []*Node{{ID: hub, Labels: []string{"Hub"}}}
	edges := make([]*Edge, 0, degree)
	for i := 0; i < degree; i++ {
		leaf := NodeID(fmt.Sprintf("test:leaf-%d", i))
		nodes = append(nodes, &Node{ID: leaf, Labels: []string{"Leaf"}})
		edges = append(edges, &Edge{ID: EdgeID(fmt.Sprintf("test:e-%d", i)), StartNode: hub, EndNode: leaf, Type: "LINK", Properties: map[string]interface{}{"i": int64(i)}})
	}
	if err := engine.BulkCreateNodes(ctx, nodes); err != nil {
		b.Fatal(err)
	}
	if err := engine.BulkCreateEdges(ctx, edges); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tx.Rollback() })
	if err := tx.SetNamespace("test"); err != nil {
		b.Fatal(err)
	}
	if _, err := tx.GetNode(ctx, hub); err != nil {
		b.Fatal(err)
	}
	if mode == "locked-after-commit" {
		if _, err := engine.CreateNode(ctx, &Node{ID: "test:peer", Labels: []string{"Leaf"}}); err != nil {
			b.Fatal(err)
		}
	}
	if mode != "unlocked" {
		if _, err := tx.LockNodeForWrite(ctx, hub); err != nil {
			b.Fatal(err)
		}
	}
	return tx, hub
}
