package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// populateUnwindDeleteFixture seeds a fresh in-memory engine with `batchSize`
// source/target node pairs joined by TAINT_FLOWS_TO edges and returns the
// uids/edge IDs the benchmark iteration will drive through the executor.
func populateUnwindDeleteFixture(b *testing.B, batchSize int) (*storage.MemoryEngine, []string, []storage.EdgeID) {
	b.Helper()
	store := storage.NewMemoryEngine()
	uids := make([]string, 0, batchSize)
	edgeIDs := make([]storage.EdgeID, 0, batchSize)
	for j := 0; j < batchSize; j++ {
		uid := fmt.Sprintf("source-%d", j)
		targetUID := fmt.Sprintf("target-%d", j)
		sourceID, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID("nornic:" + uid),
			Labels:     []string{"Function"},
			Properties: map[string]any{"uid": uid},
		})
		if err != nil {
			b.Fatal(err)
		}
		targetID, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID("nornic:" + targetUID),
			Labels:     []string{"Function"},
			Properties: map[string]any{"uid": targetUID},
		})
		if err != nil {
			b.Fatal(err)
		}
		edgeID := storage.EdgeID(fmt.Sprintf("nornic:edge-%d", j))
		if err := store.CreateEdge(&storage.Edge{
			ID:        edgeID,
			StartNode: sourceID,
			EndNode:   targetID,
			Type:      "TAINT_FLOWS_TO",
		}); err != nil {
			b.Fatal(err)
		}
		uids = append(uids, uid)
		edgeIDs = append(edgeIDs, edgeID)
	}
	return store, uids, edgeIDs
}

func BenchmarkUnwindRelationshipDeleteTransactionRouting(b *testing.B) {
	const query = `UNWIND $uids AS suid
MATCH (s:Function {uid: suid})-[rel:TAINT_FLOWS_TO]->()
DELETE rel`

	for _, batchSize := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			for _, explicit := range []bool{false, true} {
				name := "autocommit"
				if explicit {
					name = "explicit"
				}
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					b.ReportAllocs()

					// DELETE is destructive, so each iteration needs fresh edges.
					// Pre-build every iteration's fixture up front so the hot path
					// only pays for BEGIN/UNWIND-DELETE/COMMIT and iteration cost
					// isn't dominated by re-instantiating the storage engine.
					type fixture struct {
						store   *storage.MemoryEngine
						exec    *StorageExecutor
						uids    []string
						edgeIDs []storage.EdgeID
					}
					fixtures := make([]fixture, b.N)
					for i := 0; i < b.N; i++ {
						store, uids, edgeIDs := populateUnwindDeleteFixture(b, batchSize)
						fixtures[i] = fixture{
							store:   store,
							exec:    NewStorageExecutor(store),
							uids:    uids,
							edgeIDs: edgeIDs,
						}
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						f := fixtures[i]
						if explicit {
							if _, err := f.exec.Execute(ctx, "BEGIN", nil); err != nil {
								b.Fatal(err)
							}
						}
						result, err := f.exec.Execute(ctx, query, map[string]any{"uids": f.uids})
						if err != nil {
							b.Fatal(err)
						}
						if explicit {
							if _, err := f.exec.Execute(ctx, "COMMIT", nil); err != nil {
								b.Fatal(err)
							}
						}

						b.StopTimer()
						if result.Stats == nil || result.Stats.RelationshipsDeleted != batchSize {
							b.Fatalf("relationships deleted = %+v, want %d", result.Stats, batchSize)
						}
						for _, edgeID := range f.edgeIDs {
							if _, err := f.store.GetEdge(edgeID); err == nil {
								b.Fatalf("edge %s survived", edgeID)
							}
						}
						if err := f.store.Close(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
				})
			}
		})
	}
}
