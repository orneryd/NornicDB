package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Embedding the concrete engine preserves its optional read capabilities.
// Count real adjacency work without changing the selected storage contract.
type portedNor408WorkEngine struct {
	*storage.NamespacedEngine
	adjacencyReads atomic.Int64
}

func (s *portedNor408WorkEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	s.adjacencyReads.Add(1)
	return s.NamespacedEngine.ReadAdjacentEdges(ctx, request)
}

// The reported synthetic population: 346 documents, 142 attachment edges,
// and unrelated nodes bringing the total to 1,000 or 49,000. Every two
// attachments share a parent; the requested forty children have one sibling.
// This does not purport to reconstruct the unavailable heavy production data.
func portedNor408Fixture(t testing.TB, total int) *StorageExecutor {
	exec, _ := portedNor408WorkFixture(t, total)
	return exec
}

func portedNor408WorkFixture(t testing.TB, total int) (*StorageExecutor, *portedNor408WorkEngine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "portedNor408")
	ctx := context.Background()
	for i := 0; i < total; i++ {
		label := "Unrelated"
		if i < 346 {
			label = "PDDocument"
		}
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{label}, Properties: map[string]interface{}{"id": int64(i)}})
		require.NoError(t, err)
	}
	for i := 0; i < 142; i++ {
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("a%d", i)), Type: "ATTACHMENT_OF", StartNode: storage.NodeID(fmt.Sprintf("n%d", i)), EndNode: storage.NodeID(fmt.Sprintf("n%d", 200+i/2))}))
	}
	work := &portedNor408WorkEngine{NamespacedEngine: store}
	exec := NewStorageExecutor(work, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err := exec.Execute(ctx, "CREATE INDEX portedNor408_id FOR (d:PDDocument) ON (d.id)", nil)
	require.NoError(t, err)
	return exec, work
}

func portedNor408Query(optional, right, oneHop bool) string {
	clause := "MATCH "
	if optional {
		clause = "OPTIONAL MATCH "
	}
	pattern := "(d)-[:ATTACHMENT_OF]->(:PDDocument)<-[:ATTACHMENT_OF]-(sibling:PDDocument)"
	if right {
		pattern = "(sibling:PDDocument)-[:ATTACHMENT_OF]->(:PDDocument)<-[:ATTACHMENT_OF]-(d)"
	}
	if oneHop {
		pattern = "(d)-[:ATTACHMENT_OF]->(sibling:PDDocument)"
		if right {
			pattern = "(sibling:PDDocument)<-[:ATTACHMENT_OF]-(d)"
		}
	}
	return "UNWIND $ids AS id MATCH (d:PDDocument {id: id}) " + clause + pattern + " WHERE sibling.id <> d.id RETURN d.id AS id, sibling.id AS sibling ORDER BY id, sibling"
}

func portedNor408IDs(n int) []interface{} {
	ids := make([]interface{}, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	return ids
}

func portedNor408Expected(n int, oneHop bool) [][]interface{} {
	rows := make([][]interface{}, n)
	for i := range rows {
		sibling := i ^ 1
		if oneHop {
			sibling = 200 + i/2
		}
		rows[i] = []interface{}{int64(i), int64(sibling)}
	}
	return rows
}

func TestPortedNOR408BoundAttachmentOrientations(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, total := range []int{1000, 49000} {
			t.Run(fmt.Sprintf("nodes%d", total), func(t *testing.T) {
				exec := portedNor408Fixture(t, total)
				for _, count := range []int{1, 10, 40} {
					for _, optional := range []bool{false, true} {
						for _, oneHop := range []bool{false, true} {
							for _, right := range []bool{false, true} {
								t.Run(fmt.Sprintf("ids%d/optional%t/oneHop%t/right%t", count, optional, oneHop, right), func(t *testing.T) {
									result, err := exec.Execute(t.Context(), portedNor408Query(optional, right, oneHop), map[string]interface{}{"ids": portedNor408IDs(count)})
									require.NoError(t, err)
									require.Equal(t, []string{"id", "sibling"}, result.Columns)
									require.Equal(t, portedNor408Expected(count, oneHop), result.Rows)
								})
							}
						}
					}
				}
			})
		}
	})
}

func TestPortedNOR408BoundAttachmentWorkDoesNotExpandUnboundPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, work := portedNor408WorkFixture(t, 1000)
		for _, count := range []int{1, 10, 40} {
			for _, optional := range []bool{false, true} {
				for _, oneHop := range []bool{false, true} {
					t.Run(fmt.Sprintf("ids%d/optional%t/oneHop%t", count, optional, oneHop), func(t *testing.T) {
						reads := [2]int64{}
						for index, right := range []bool{false, true} {
							work.adjacencyReads.Store(0)
							result, err := exec.Execute(t.Context(), portedNor408Query(optional, right, oneHop), map[string]interface{}{"ids": portedNor408IDs(count)})
							require.NoError(t, err)
							require.Equal(t, portedNor408Expected(count, oneHop), result.Rows)
							reads[index] = work.adjacencyReads.Load()
						}
						t.Logf("left adjacency reads=%d; right=%d", reads[0], reads[1])
						require.Positive(t, reads[0], "the counter must observe actual traversal, not a bypassed wrapper")
						require.Positive(t, reads[1])
						require.LessOrEqual(t, reads[1], 2*reads[0], "equivalent bound-end traversal must not expand the unbound label population")
					})
				}
			}
		}
	})
}
