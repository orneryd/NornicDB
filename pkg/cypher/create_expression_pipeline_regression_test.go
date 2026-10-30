package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateEvaluatesToStringConcatenationProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:T {uuid: 't' + toString(0), label: toString(42)})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:T {uuid: 't0'}) RETURN n.uuid, n.label", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"t0", "42"}}, res.Rows)
}

func TestUnwindMatchCreateRelationshipCreatesEdges(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:T {uuid: 't' + toString(0)}), (:T {uuid: 't' + toString(1)})", nil)
	require.NoError(t, err)

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{"source": "t0", "target": "t1", "relID": "t0-t1"},
		},
	}

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (source:T {uuid: row.source})
MATCH (target:T {uuid: row.target})
CREATE (source)-[:REL {uuid: row.relID}]->(target)
`, params)
	require.NoError(t, err)

	countRes, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, countRes.Rows)
}
