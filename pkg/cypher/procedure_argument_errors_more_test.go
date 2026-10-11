package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The argument errors of apoc.load / apoc.export / apoc.import, gds.* and
// the apoc.algo label forms, and the gds link-prediction and CSV export
// procedures called from Cypher (#907).
func TestProcedureArgumentErrorsAndCalls(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetAllowLocalAPOCFileAccess(true)
	_, err := exec.Execute(ctx, "CREATE (a:Person {name: 'a'})-[:KNOWS]->(b:Person {name: 'b'})-[:KNOWS]->(c:Person {name: 'c'})", nil)
	require.NoError(t, err)

	for name, call := range map[string]func() error{
		"load.csv url":            func() error { _, err := exec.callApocLoadCsv(ctx, nil); return err },
		"load.csv config":         func() error { _, err := exec.callApocLoadCsv(ctx, []interface{}{"x.csv", int64(1)}); return err },
		"import.json url":         func() error { _, err := exec.callApocImportJson(ctx, nil); return err },
		"export.json.all file":    func() error { _, err := exec.callApocExportJsonAll(ctx, []interface{}{int64(1)}); return err },
		"export.json.query query": func() error { _, err := exec.callApocExportJsonQuery(ctx, nil); return err },
		"export.json.query file":  func() error { _, err := exec.callApocExportJsonQuery(ctx, []interface{}{"RETURN 1", int64(1)}); return err },
		"export.csv.all file":     func() error { _, err := exec.callApocExportCsvAll(ctx, []interface{}{int64(1)}); return err },
		"export.csv.query query":  func() error { _, err := exec.callApocExportCsvQuery(ctx, nil); return err },
		"export.csv.query file":   func() error { _, err := exec.callApocExportCsvQuery(ctx, []interface{}{"RETURN 1", int64(1)}); return err },
		"graph.project rels":      func() error { _, err := exec.callGdsGraphProject([]interface{}{"g", "*", int64(1)}); return err },
		"graph.drop name":         func() error { _, err := exec.callGdsGraphDrop(nil); return err },
		"fastRP.stream name":      func() error { _, err := exec.callGdsFastRPStream(nil); return err },
		"fastRP.stream config":    func() error { _, err := exec.callGdsFastRPStream([]interface{}{"g", int64(1)}); return err },
		"fastRP.stats name":       func() error { _, err := exec.callGdsFastRPStats(nil); return err },
		"fastRP.stats config":     func() error { _, err := exec.callGdsFastRPStats([]interface{}{"g", int64(1)}); return err },
	} {
		require.Error(t, call(), name)
	}

	// Projections: none (everything), a map entry without a label override.
	result, err := exec.callGdsGraphProject([]interface{}{"g_none", nil, nil})
	require.NoError(t, err)
	require.Equal(t, []any{"g_none", 3, 2, int64(10)}, result.Rows[0])
	result, err = exec.callGdsGraphProject([]interface{}{"g_map", map[string]interface{}{"Person": map[string]interface{}{}}, "KNOWS"})
	require.NoError(t, err)
	require.Equal(t, []any{"g_map", 3, 2, int64(10)}, result.Rows[0])

	// Link prediction and CSV export, called from Cypher.
	for _, procedure := range []string{"commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
		_, err := exec.Execute(ctx, "MATCH (a:Person {name: 'a'}) CALL gds.linkPrediction."+procedure+".stream({sourceNode: a, topK: 2}) YIELD node1 RETURN node1", nil)
		require.NoError(t, err, procedure)
	}
	_, err = exec.Execute(ctx, "CALL apoc.export.csv.all('', {})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL apoc.export.csv.query('MATCH (n:Person) RETURN n.name AS name', '', {})", nil)
	require.NoError(t, err)

	// Whole-graph algorithms over every node: an empty list or ''.
	for _, scope := range []string{"[]", "''"} {
		result, err := exec.Execute(ctx, "CALL apoc.algo.wcc("+scope+") YIELD node RETURN node", nil)
		require.NoError(t, err, scope)
		require.Len(t, result.Rows, 3, scope)
	}
}
