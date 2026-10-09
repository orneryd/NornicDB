package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Rows that bind one entity through different copies see and write one
// entity, as in Neo4j 5.26.30, whose answers these are (#907).
func TestRowsShareOneEntity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_entities"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:RE {id: 1})-[:RR {w: 0}]->(:RE {id: 2})", nil)
	require.NoError(t, err)
	inTransaction := func(query string) [][]interface{} {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	for query, want := range map[string][][]interface{}{
		// A MERGE row sees the ON MATCH write of a later row.
		"UNWIND [1, 2] AS k MERGE (m:RW {k: 1}) ON CREATE SET m.c = k ON MATCH SET m.m = k RETURN k, m.c AS c, m.m AS mm ORDER BY k": {
			{int64(1), int64(1), int64(2)}, {int64(2), int64(1), int64(2)}},
		"UNWIND [1, 2, 3] AS k MERGE (m:RW {k: 1}) ON MATCH SET m.n = coalesce(m.n, 0) + 1 RETURN k, m.n AS n ORDER BY k": {
			{int64(1), int64(2)}, {int64(2), int64(2)}, {int64(3), int64(2)}},
		// A SET row reads what the rows before it wrote, and doesn't store a
		// stale copy over it.
		"UNWIND [1, 2] AS k MATCH (m:RE {id: 1}) SET m.a = CASE k WHEN 1 THEN 1 ELSE m.a END, m.b = CASE k WHEN 2 THEN 2 ELSE m.b END RETURN k, m.a AS a, m.b AS b ORDER BY k": {
			{int64(1), int64(1), int64(2)}, {int64(2), int64(1), int64(2)}},
		"UNWIND [1, 2, 3] AS k MATCH (m:RE {id: 1}) SET m.n = coalesce(m.n, 0) + 1 RETURN k, m.n AS n ORDER BY k": {
			{int64(1), int64(3)}, {int64(2), int64(3)}, {int64(3), int64(3)}},
		"UNWIND [1, 2] AS k MATCH (m:RE {id: 1}) SET m.n = coalesce(m.n, 0) + 1 WITH m MATCH (x:RE {id: 1}) RETURN x.n AS n": {
			{int64(2)}, {int64(2)}},
		// Relationships too.
		"UNWIND [1, 2] AS k MATCH (:RE {id: 1})-[r:RR]->() SET r.w = r.w + k RETURN k, r.w AS w ORDER BY k": {
			{int64(1), int64(3)}, {int64(2), int64(3)}},
		// REMOVE and FOREACH.
		"MATCH (m:RE {id: 1}) SET m.x = 1, m.y = 2 WITH m UNWIND [1, 2] AS k MATCH (n:RE {id: 1}) REMOVE n.x RETURN k, n.x AS x, n.y AS y ORDER BY k": {
			{int64(1), nil, int64(2)}, {int64(2), nil, int64(2)}},
		"UNWIND [1, 2] AS k MATCH (m:RE {id: 1}) FOREACH (i IN [k] | SET m.f = coalesce(m.f, 0) + i) RETURN k, m.f AS f ORDER BY k": {
			{int64(1), int64(3)}, {int64(2), int64(3)}},
	} {
		require.Equal(t, want, inTransaction(query), query)
	}
}

func TestRefreshRowEntities(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_refresh"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:RF {id: 1})-[:RR]->(:RF {id: 2})", nil)
	require.NoError(t, err)
	store := exec.getStorage(ctx)
	nodes, err := store.GetNodesByLabel("RF")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	first, _ := store.GetNode(nodes[0].ID)
	second, _ := store.GetNode(nodes[0].ID)
	other, _ := store.GetNode(nodes[1].ID)
	edges, err := store.GetEdgesByType("RR")
	require.NoError(t, err)
	edgeA, _ := store.GetEdge(edges[0].ID)
	edgeB, _ := store.GetEdge(edges[0].ID)

	// One row: nothing to share.
	single := []pipelineRow{{"n": first}}
	exec.refreshRowEntities(ctx, single)
	require.Same(t, first, single[0]["n"])

	// Distinct entities and shared copies: no read, nothing changes.
	distinct := []pipelineRow{{"n": first, "x": nil}, {"n": other}, {"m": first}}
	exec.refreshRowEntities(ctx, distinct)
	require.Same(t, first, distinct[0]["n"])
	require.Same(t, other, distinct[1]["n"])

	// Two copies of one node or relationship: every row binds one version.
	rows := []pipelineRow{{"n": first, "r": edgeA, "nil": (*storage.Node)(nil)}, {"n": second, "r": edgeB, "nilEdge": (*storage.Edge)(nil)}}
	exec.refreshRowEntities(ctx, rows)
	require.Same(t, rows[0]["n"], rows[1]["n"])
	require.Same(t, rows[0]["r"], rows[1]["r"])

	// An entity deleted since keeps its copies.
	require.NoError(t, store.DeleteEdge(edgeA.ID))
	require.NoError(t, store.DeleteNode(other.ID))
	otherCopy := &storage.Node{ID: other.ID}
	edgeCopy := &storage.Edge{ID: edgeA.ID}
	deleted := []pipelineRow{{"n": other, "r": edgeA}, {"n": otherCopy, "r": edgeCopy}}
	exec.refreshRowEntities(ctx, deleted)
	require.Same(t, other, deleted[0]["n"])
	require.Same(t, otherCopy, deleted[1]["n"])
	require.Same(t, edgeCopy, deleted[1]["r"])
}
