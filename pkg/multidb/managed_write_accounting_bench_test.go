package multidb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Compare identical public managed writes before/after accounting convergence.
// Delete/recreate measures both operations; updates retain a constant payload.
func BenchmarkManagedQuotaWritePath(b *testing.B) {
	for _, buffered := range []bool{false, true} {
		for _, quota := range []string{"unlimited", "count", "tracked", "bytes"} {
			for _, route := range []string{"direct-node", "direct-edge", "transaction-node", "transaction-edge", "direct-node-delete-recreate", "direct-edge-delete-recreate", "transaction-node-delete-recreate", "transaction-edge-delete-recreate"} {
				b.Run(fmt.Sprintf("buffered=%v/%s/%s", buffered, quota, route), func(b *testing.B) {
					check := func(err error) {
						if err != nil {
							b.Fatal(err)
						}
					}
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: buffered})
					check(err)
					manager, err := NewDatabaseManager(base, nil)
					check(err)
					b.Cleanup(func() { check(manager.Close()) })
					const db = "quota_bench"
					check(manager.CreateDatabase(db))
					engine, err := manager.GetStorage(db)
					check(err)
					for _, id := range []storage.NodeID{"a", "b", "n"} {
						_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"payload": "fixed"}})
						check(err)
					}
					stamp := time.Now().UTC()
					node := &storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"payload": "fixed"}, CreatedAt: stamp, UpdatedAt: stamp}
					edge := &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "fixed"}, CreatedAt: stamp, UpdatedAt: stamp}
					check(engine.CreateEdge(edge))
					if quota == "count" {
						check(manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 1000, MaxEdges: 1000}}))
					}
					if quota == "tracked" || quota == "bytes" {
						manager.GetStorageSize(db)
					}
					if quota == "bytes" {
						check(manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxBytes: 1 << 30}}))
					}
					tracker := engine.(*sizeTrackingEngine)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch route {
						case "direct-node":
							check(engine.UpdateNode(node))
						case "direct-edge":
							check(engine.UpdateEdge(edge))
						case "direct-node-delete-recreate":
							check(engine.DeleteNode("n"))
							_, err = engine.CreateNode(node)
							check(err)
						case "direct-edge-delete-recreate":
							check(engine.DeleteEdge("e"))
							check(engine.CreateEdge(edge))
						default:
							tx, err := base.BeginTransaction()
							check(err)
							check(tx.SetNamespace(db))
							switch route {
							case "transaction-node":
								body := *node
								body.ID = db + ":n"
								check(tx.UpdateNode(&body))
							case "transaction-edge":
								body := *edge
								body.ID, body.StartNode, body.EndNode = db+":e", db+":a", db+":b"
								check(tx.UpdateEdge(&body))
							case "transaction-node-delete-recreate":
								check(tx.DeleteNode(db + ":n"))
								body := *node
								body.ID = db + ":n"
								_, err = tx.CreateNode(&body)
								check(err)
							case "transaction-edge-delete-recreate":
								check(tx.DeleteEdge(db + ":e"))
								body := *edge
								body.ID, body.StartNode, body.EndNode = db+":e", db+":a", db+":b"
								check(tx.CreateEdge(&body))
							}
							check(tracker.CommitTransaction(context.Background(), tx))
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}
