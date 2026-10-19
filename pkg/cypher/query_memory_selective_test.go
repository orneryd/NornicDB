package cypher

import (
	"context"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type queryMemoryEdgeScanEngine struct {
	*storage.NamespacedEngine
	visited        int
	wholeVisited   int
	adjacencyReads int
}

func (s *queryMemoryEdgeScanEngine) StreamEdgesByType(ctx context.Context, kind string, visit func(*storage.Edge) error) error {
	return s.NamespacedEngine.StreamEdgesByType(ctx, kind, func(edge *storage.Edge) error {
		s.visited++
		if kind == "" {
			s.wholeVisited++
		}
		return visit(edge)
	})
}

func (s *queryMemoryEdgeScanEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.adjacencyReads++
	return s.NamespacedEngine.GetOutgoingEdges(id)
}

func (s *queryMemoryEdgeScanEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	s.adjacencyReads++
	return s.NamespacedEngine.GetIncomingEdges(id)
}

// Count edges, not node scans: rejecting an unrelated edge after reading it
// still turns a degree/ID lookup into graph-sized work and allocation.
func TestQueryMemorySelectiveAccessSkipsDecoyEdges(t *testing.T) {
	store := &queryMemoryEdgeScanEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	seedSinkProbeGraph(t, exec, 4000)
	ctx := context.Background()
	seed, err := exec.Execute(ctx, "MATCH (a:Function {uid:'fn'})-[r]->(b) RETURN id(a), id(b), elementId(r)", nil)
	require.NoError(t, err)
	require.Len(t, seed.Rows, 1)
	params := map[string]interface{}{"a": seed.Rows[0][0], "b": seed.Rows[0][1], "r": seed.Rows[0][2]}
	for _, tc := range []struct{ name, query string }{
		{"type_alternatives", "MATCH ()-[r:QUERIES_TABLE|ALTERNATIVE]->() RETURN count(r)"},
		{"label_seed", "MATCH (a:Function)-[r]->(b) RETURN count(r)"},
		{"bound_start", "MATCH (a) WHERE id(a)=$a MATCH (a)-[r]->(b) RETURN count(r)"},
		{"bound_end", "MATCH (b) WHERE id(b)=$b MATCH (a)-[r]->(b) RETURN count(r)"},
		{"bound_both", "MATCH (a) WHERE id(a)=$a MATCH (b) WHERE id(b)=$b MATCH (a)-[r]->(b) RETURN count(r)"},
		{"bound_predicate", "MATCH (a:Function {uid:'fn'}) MATCH (a)-[r]->(b) WHERE type(r) IN ['QUERIES_TABLE'] RETURN count(r)"},
		{"relationship_element_id", "MATCH ()-[r]->() WHERE elementId(r)=$r RETURN count(r)"},
		{"relationship_id", "MATCH ()-[r]->() WHERE id(r)=$r RETURN count(r)"},
		{"endpoint_property_seed", "MATCH (:Function {uid:'fn'})-[r]->() RETURN count(r)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Compile/cache parser plans before measuring execution allocation.
			_, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			store.visited, store.wholeVisited, store.adjacencyReads = 0, 0, 0
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result, err := exec.Execute(ctx, tc.query, params)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			t.Logf("selective query: visited %d streamed edges, allocated %d bytes with 4000 decoys", store.visited, after.TotalAlloc-before.TotalAlloc)
			require.Zero(t, store.wholeVisited, "selective plans must never read the graph-wide cursor")
			require.LessOrEqual(t, store.visited, 1, "typed scans must only visit selected relationship types")
			if tc.name == "label_seed" {
				require.Equal(t, 1, store.adjacencyReads)
			}
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
		})
	}
}
