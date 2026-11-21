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
type nor408WorkEngine struct {
	*storage.NamespacedEngine
	adjacencyReads atomic.Int64
}

func (s *nor408WorkEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	s.adjacencyReads.Add(1)
	return s.NamespacedEngine.ReadAdjacentEdges(ctx, request)
}

// The reported synthetic population: 346 documents, 142 attachment edges,
// and unrelated nodes bringing the total to 1,000 or 49,000. Every two
// attachments share a parent; the requested forty children have one sibling.
// This does not purport to reconstruct the unavailable heavy production data.
func nor408Fixture(t testing.TB, total int) *StorageExecutor {
	exec, _ := nor408WorkFixture(t, total)
	return exec
}

func nor408WorkFixture(t testing.TB, total int) (*StorageExecutor, *nor408WorkEngine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor408")
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
	work := &nor408WorkEngine{NamespacedEngine: store}
	exec := NewStorageExecutor(work, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err := exec.Execute(ctx, "CREATE INDEX nor408_id FOR (d:PDDocument) ON (d.id)", nil)
	require.NoError(t, err)
	return exec, work
}

func nor408Query(optional, right, oneHop bool) string {
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

func nor408IDs(n int) []interface{} {
	ids := make([]interface{}, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	return ids
}

func nor408Expected(n int, oneHop bool) [][]interface{} {
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

func TestNOR408BoundAttachmentOrientations(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, total := range []int{1000, 49000} {
			t.Run(fmt.Sprintf("nodes%d", total), func(t *testing.T) {
				exec := nor408Fixture(t, total)
				for _, count := range []int{1, 10, 40} {
					for _, optional := range []bool{false, true} {
						for _, oneHop := range []bool{false, true} {
							for _, right := range []bool{false, true} {
								t.Run(fmt.Sprintf("ids%d/optional%t/oneHop%t/right%t", count, optional, oneHop, right), func(t *testing.T) {
									result, err := exec.Execute(t.Context(), nor408Query(optional, right, oneHop), map[string]interface{}{"ids": nor408IDs(count)})
									require.NoError(t, err)
									require.Equal(t, []string{"id", "sibling"}, result.Columns)
									require.Equal(t, nor408Expected(count, oneHop), result.Rows)
								})
							}
						}
					}
				}
			})
		}
	})
}

func TestNOR408BoundAttachmentWorkDoesNotExpandUnboundPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, work := nor408WorkFixture(t, 1000)
		for _, count := range []int{1, 10, 40} {
			for _, optional := range []bool{false, true} {
				for _, oneHop := range []bool{false, true} {
					t.Run(fmt.Sprintf("ids%d/optional%t/oneHop%t", count, optional, oneHop), func(t *testing.T) {
						reads := [2]int64{}
						for index, right := range []bool{false, true} {
							work.adjacencyReads.Store(0)
							result, err := exec.Execute(t.Context(), nor408Query(optional, right, oneHop), map[string]interface{}{"ids": nor408IDs(count)})
							require.NoError(t, err)
							require.Equal(t, nor408Expected(count, oneHop), result.Rows)
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

// Profile this benchmark; timing ratios are observations, never flaky unit
// assertions. Setup and complete expected-result checks are outside the timer.
func BenchmarkNOR408BoundAttachmentOrientations(b *testing.B) {
	for _, mode := range relationshipEqualityParsers {
		b.Run(mode.name, func(b *testing.B) {
			b.Cleanup(mode.setup())
			for _, total := range []int{1000, 49000} {
				b.Run(fmt.Sprintf("nodes%d", total), func(b *testing.B) {
					exec := nor408Fixture(b, total)
					for _, count := range []int{1, 10, 40} {
						for _, optional := range []bool{false, true} {
							for _, oneHop := range []bool{false, true} {
								for _, right := range []bool{false, true} {
									b.Run(fmt.Sprintf("ids%d/optional%t/oneHop%t/right%t", count, optional, oneHop, right), func(b *testing.B) {
										query := nor408Query(optional, right, oneHop)
										params := map[string]interface{}{"ids": nor408IDs(count)}
										result, err := exec.Execute(context.Background(), query, params)
										require.NoError(b, err)
										require.Equal(b, nor408Expected(count, oneHop), result.Rows)
										b.ReportAllocs()
										b.ResetTimer()
										for i := 0; i < b.N; i++ {
											_, err := exec.Execute(context.Background(), query, params)
											if err != nil {
												b.Fatal(err)
											}
										}
									})
								}
							}
						}
					}
				})
			}
		})
	}
}

// A right-end seed still returns path elements in the written order and
// retains the outer row when the optional chain has no matching path.
func TestNOR408BoundAttachmentPathBindings(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := nor408Fixture(t, 1000)
		result, err := exec.Execute(t.Context(), `
			UNWIND [0, 300] AS id
			MATCH (d:PDDocument {id: id})
			OPTIONAL MATCH p=(sibling:PDDocument)-[first:ATTACHMENT_OF]->(hub:PDDocument)<-[last:ATTACHMENT_OF]-(d)
			WHERE sibling.id <> d.id
			RETURN id, sibling.id, hub.id, [n IN nodes(p) | n.id] AS nodeIDs,
				CASE WHEN p IS NULL THEN null ELSE relationships(p) = [first, last] END AS preservedEdges
			ORDER BY id`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{
			{int64(0), int64(1), int64(200), []interface{}{int64(1), int64(200), int64(0)}, true},
			{int64(300), nil, nil, nil, nil},
		}, result.Rows)
	})
}

func TestNOR408BoundAttachmentChainedAdapter(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, work := nor408WorkFixture(t, 1000)
		node, err := work.GetNode(t.Context(), "n0")
		require.NoError(t, err)
		rows, handled := exec.pipelineApplyChainedMatch(t.Context(), []pipelineRow{{"d": node}},
			"MATCH (sibling:PDDocument)-[:ATTACHMENT_OF]->(:PDDocument)<-[:ATTACHMENT_OF]-(d) WHERE sibling.id <> d.id")
		require.True(t, handled)
		require.Len(t, rows, 1)
		require.Equal(t, int64(1), rows[0]["sibling"].(*storage.Node).Properties["id"])
		require.Equal(t, int64(2), work.adjacencyReads.Load())
	})
}
