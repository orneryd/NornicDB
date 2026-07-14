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

					for i := 0; i < b.N; i++ {
						// DELETE is destructive, so each iteration needs a fresh fixture.
						// Keep setup out of the hot path without prebuilding b.N engines,
						// which can blow up memory as the harness increases iteration count.
						b.StopTimer()
						store, uids, edgeIDs := populateUnwindDeleteFixture(b, batchSize)
						exec := NewStorageExecutor(store)
						b.StartTimer()
						if explicit {
							if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
								b.Fatal(err)
							}
						}
						result, err := exec.Execute(ctx, query, map[string]any{"uids": uids})
						if err != nil {
							b.Fatal(err)
						}
						if explicit {
							if _, err := exec.Execute(ctx, "COMMIT", nil); err != nil {
								b.Fatal(err)
							}
						}

						b.StopTimer()
						if result.Stats == nil || result.Stats.RelationshipsDeleted != batchSize {
							b.Fatalf("relationships deleted = %+v, want %d", result.Stats, batchSize)
						}
						for _, edgeID := range edgeIDs {
							if _, err := store.GetEdge(edgeID); err == nil {
								b.Fatalf("edge %s survived", edgeID)
							}
						}
						if err := store.Close(); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
				})
			}
		})
	}
}
