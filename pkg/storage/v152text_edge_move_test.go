package storage

// v1.5.2 verification, built from the text of Plane item N15-35 (NOR-169):
// a transaction moves a relationship away from its old start node while
// another writer deletes that old node. Expected after both finish: no stored
// relationship names a missing node, and the deleted node has no adjacency.
// Run with write buffering off and on, many rounds.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestV152TextRelationshipMoveAgainstOldEndpointDelete(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			ctx := context.Background()
			// Control: the same move with no concurrent delete must succeed.
			for _, id := range []NodeID{"test:c-old", "test:c-end", "test:c-new"} {
				if _, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"P"}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.CreateEdge(ctx, &Edge{ID: "test:c-e", StartNode: "test:c-old", EndNode: "test:c-end", Type: "LINK"}); err != nil {
				t.Fatal(err)
			}
			ctl, err := b.BeginTransaction(ctx, OrdinaryBegin)
			if err != nil {
				t.Fatal(err)
			}
			ctlErr := ctl.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{ID: "test:c-e", StartNode: "test:c-new", EndNode: "test:c-end", Type: "LINK"}})
			if ctlErr == nil {
				ctlErr = ctl.Commit()
			} else {
				_ = ctl.Rollback()
			}
			moved, _ := b.GetEdge(ctx, "test:c-e")
			t.Logf("V152TEXT control move without delete: err=%v edge=%+v", ctlErr, moved)
			if ctlErr != nil {
				t.Fatalf("control: a relationship move without a concurrent delete failed: %v", ctlErr)
			}
			moveErrs, deleteErrs, dangling, staleAdjacency := 0, 0, 0, 0
			const rounds = 200
			for round := 0; round < rounds; round++ {
				old := NodeID(fmt.Sprintf("test:old-%d", round))
				end := NodeID(fmt.Sprintf("test:end-%d", round))
				fresh := NodeID(fmt.Sprintf("test:new-%d", round))
				edge := EdgeID(fmt.Sprintf("test:e-%d", round))
				for _, id := range []NodeID{old, end, fresh} {
					if _, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"P"}}); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.CreateEdge(ctx, &Edge{ID: edge, StartNode: old, EndNode: end, Type: "LINK"}); err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				start := make(chan struct{})
				var moveErr, deleteErr error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
					if err != nil {
						moveErr = err
						return
					}
					if err := tx.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{ID: edge, StartNode: fresh, EndNode: end, Type: "LINK"}}); err != nil {
						_ = tx.Rollback()
						moveErr = err
						return
					}
					moveErr = tx.Commit()
				}()
				go func() {
					defer wg.Done()
					<-start
					deleteErr = b.DeleteNode(ctx, old)
				}()
				close(start)
				wg.Wait()
				if moveErr != nil {
					moveErrs++
					if moveErrs <= 3 {
						t.Logf("V152TEXT round=%d move error: %v (delete err=%v)", round, moveErr, deleteErr)
					}
				}
				if deleteErr != nil {
					deleteErrs++
				}
				if e, err := b.GetEdge(ctx, edge); err == nil && e != nil {
					for _, id := range []NodeID{e.StartNode, e.EndNode} {
						if n, err := b.GetNode(ctx, id); err != nil || n == nil {
							dangling++
							t.Logf("V152TEXT round=%d edge %s -> %s names missing node %s (move err=%v, delete err=%v)", round, e.StartNode, e.EndNode, id, moveErr, deleteErr)
						}
					}
				}
				if deleteErr == nil {
					adj, err := b.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBoth, Node: old})
					if err == nil && len(adj) > 0 {
						staleAdjacency++
						t.Logf("V152TEXT round=%d deleted node %s still has %d adjacent edges", round, old, len(adj))
					}
				}
			}
			t.Logf("V152TEXT NOR-169 writeBehind=%v rounds=%d moveErrors=%d deleteErrors=%d dangling=%d staleAdjacency=%d", writeBehind, rounds, moveErrs, deleteErrs, dangling, staleAdjacency)
			if dangling > 0 || staleAdjacency > 0 {
				t.Errorf("relationship move and old-endpoint delete left %d dangling relationship endpoints and %d stale adjacency reads", dangling, staleAdjacency)
			}
		})
	}
}
