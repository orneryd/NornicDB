package cypher

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type boundedPathWorkEngine struct {
	storage.Engine
	reads int
}

func (s *boundedPathWorkEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.reads++
	edges, err := s.Engine.GetOutgoingEdges(id)
	return stableBoundPathEdges(edges), err
}
func (s *boundedPathWorkEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.reads++
	edges, err := s.Engine.GetIncomingEdges(id)
	return stableBoundPathEdges(edges), err
}
func stableBoundPathEdges(edges []*storage.Edge) []*storage.Edge {
	out := append([]*storage.Edge(nil), edges...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func TestBoundEndpointPathLimitBoundsWork(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound_limit")
	for _, id := range []storage.NodeID{"a", "d"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "00-direct", StartNode: "a", EndNode: "d", Type: "R"}))
	for i := 0; i < 16; i++ {
		id := storage.NodeID(fmt.Sprintf("side%d", i))
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
		for j := 0; j < 2; j++ {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("side%02d-%d", i, j)), StartNode: "a", EndNode: id, Type: "R"}))
		}
	}
	engine := &boundedPathWorkEngine{Engine: store}
	exec := NewStorageExecutor(engine)
	query := "MATCH (a) WHERE id(a)=$start MATCH (b) WHERE id(b)=$end MATCH p=(a)-[:R*0..5]-(b) RETURN length(p) AS size LIMIT $limit"
	limited, err := exec.Execute(context.Background(), query, map[string]interface{}{"start": "a", "end": "d", "limit": int64(1)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, limited.Rows)
	limitedReads := engine.reads
	engine.reads = 0
	all, err := exec.Execute(context.Background(), query, map[string]interface{}{"start": "a", "end": "d", "limit": int64(10000)})
	require.NoError(t, err)
	require.Len(t, all.Rows, 993)
	require.Less(t, limitedReads, engine.reads, "LIMIT must stop traversal, not only truncate its final rows")
	require.LessOrEqual(t, limitedReads, 8, "direct destination should finish without scanning the cyclic side graph")
}

// A cap is valid only after every bound identity and filtering clause is checked.
func TestBoundEndpointLimitPreservesSelection(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound_controls")
			for _, id := range []storage.NodeID{"a", "b", "d", "isolated"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{{ID: "00-direct", StartNode: "a", EndNode: "d", Type: "R"}, {ID: "10-ab", StartNode: "a", EndNode: "b", Type: "R"}, {ID: "11-bd", StartNode: "b", EndNode: "d", Type: "R"}, {ID: "12-bd", StartNode: "b", EndNode: "d", Type: "R"}, {ID: "20-loop", StartNode: "a", EndNode: "a", Type: "R"}, {ID: "99-pinned", StartNode: "a", EndNode: "d", Type: "R"}} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			prefix := "MATCH (a) WHERE id(a)='a' MATCH (d) WHERE id(d)='d' MATCH p=(a)-[:R*0..3]-(d) "
			for _, test := range []struct {
				name, query string
				rows        [][]interface{}
			}{
				{"bound_relationship", "MATCH (a)-[r:R]->(d) WHERE id(r)='99-pinned' WITH a,d,r MATCH (a)-[r:R]->(d) RETURN id(r) AS edge LIMIT 1", [][]interface{}{{"99-pinned"}}},
				{"order", prefix + "RETURN length(p) AS n ORDER BY n DESC LIMIT 1", [][]interface{}{{int64(3)}}},
				{"filter", prefix + "WHERE length(p)>1 RETURN length(p) AS n ORDER BY n LIMIT 1", [][]interface{}{{int64(2)}}},
				{"zero", prefix + "RETURN length(p) AS n LIMIT 0", nil},
				{"unreachable", "MATCH (a) WHERE id(a)='a' MATCH (d) WHERE id(d)='isolated' MATCH p=(a)-[:R*0..3]-(d) RETURN length(p) AS n LIMIT 1", nil},
				{"zero_hop", "MATCH (a) WHERE id(a)='a' MATCH (d) WHERE id(d)='a' MATCH p=(a)-[:R*0..3]-(d) RETURN length(p) AS n LIMIT 1", [][]interface{}{{int64(0)}}},
				{"multiple_rows", "UNWIND ['d','isolated'] AS destination MATCH (a) WHERE id(a)='a' MATCH (d) WHERE id(d)=destination MATCH p=(a)-[:R*0..3]-(d) RETURN length(p) AS n ORDER BY n LIMIT 1", [][]interface{}{{int64(1)}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, test.query, nil)
					require.NoError(t, err)
					if len(test.rows) == 0 {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, test.rows, result.Rows)
					}
				})
			}
		})
	}
}
