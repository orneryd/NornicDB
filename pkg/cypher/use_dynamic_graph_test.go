package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newCompositeFixture is composite pcomp with constituents other → pother
// and third → pthird, each holding one :EI node (v 'o' and 't'), and an
// executor on pcomp.
func newCompositeFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("pother"))
	require.NoError(t, mgr.CreateDatabase("pthird"))
	require.NoError(t, mgr.CreateCompositeDatabase("pcomp", []multidb.ConstituentRef{
		{Alias: "other", DatabaseName: "pother", Type: "local", AccessMode: "read_write"},
		{Alias: "third", DatabaseName: "pthird", Type: "local", AccessMode: "read_write"},
	}))
	adapter := &testDatabaseManagerAdapter{manager: mgr}
	for db, v := range map[string]string{"pother": "o", "pthird": "t"} {
		store, err := mgr.GetStorage(db)
		require.NoError(t, err)
		exec := NewStorageExecutor(store)
		exec.SetDatabaseManager(adapter)
		_, err = exec.Execute(context.Background(), "CREATE (:EI {v: $v})", map[string]interface{}{"v": v})
		require.NoError(t, err)
	}
	store, err := mgr.GetStorage("pcomp")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(adapter)
	return exec
}

func rowsOf(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

// TestCompositeDynamicGraphReferences: on a composite database a USE clause
// may look its graph up with graph.byName / graph.byElementId, per row in a
// CALL subquery, and graph.names() lists the graphs (Cypher Manual,
// "Composite databases"; #738).
func TestCompositeDynamicGraphReferences(t *testing.T) {
	exec := newCompositeFixture(t)

	require.Equal(t, [][]interface{}{{"o"}}, rowsOf(t, exec, "USE graph.byName('pcomp.other') MATCH (n:EI) RETURN n.v AS v", nil))
	require.Equal(t, [][]interface{}{{"t"}}, rowsOf(t, exec, "USE graph.byName($g) MATCH (n:EI) RETURN n.v AS v", map[string]interface{}{"g": "pcomp.third"}))
	require.Equal(t, [][]interface{}{{"o"}}, rowsOf(t, exec, "USE graph.byName('pcomp.' + 'other') MATCH (n:EI) RETURN n.v AS v", nil))
	require.Equal(t, [][]interface{}{{"t"}}, rowsOf(t, exec, "CALL { USE graph.byName($g) MATCH (n:EI) RETURN n.v AS v } RETURN v", map[string]interface{}{"g": "pcomp.third"}))

	require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, rowsOf(t, exec, "RETURN graph.names() AS g", nil))
	require.Equal(t, [][]interface{}{{"pcomp.other", "o"}, {"pcomp.third", "t"}},
		rowsOf(t, exec, "UNWIND graph.names() AS g CALL { USE graph.byName(g) MATCH (n:EI) RETURN n.v AS v } RETURN g, v", nil))
	require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, rowsOf(t, exec, "RETURN graph.propertiesByName('pcomp.other') AS p", nil))
	require.Equal(t, [][]interface{}{{int64(1)}}, rowsOf(t, exec, "RETURN 1 AS x", nil))

	elementID := rowsOf(t, exec, "USE pcomp.third MATCH (n:EI) RETURN elementId(n) AS e", nil)[0][0]
	require.Equal(t, [][]interface{}{{"t"}}, rowsOf(t, exec, "USE graph.byElementId($e) MATCH (n:EI) RETURN n.v AS v", map[string]interface{}{"e": elementID}))

	rowsOf(t, exec, "USE graph.byName('pcomp.other') CREATE (:EI {v: 'dyn'})", nil)
	require.ElementsMatch(t, []interface{}{"o", "dyn"}, rowsOf(t, exec, "USE pcomp.other MATCH (n:EI) RETURN collect(n.v) AS vs", nil)[0][0])

	for query, code := range map[string]string{
		"USE graph.byName('nope') MATCH (n) RETURN n":              "Neo.ClientError.Database.DatabaseNotFound",
		"USE graph.byName($n) MATCH (n) RETURN n":                  "Neo.ClientError.Statement.TypeError",
		"USE graph.byElementId('garbage') MATCH (n) RETURN n":      "Neo.ClientError.Statement.ArgumentError",
		"USE graph.foo('x') MATCH (n) RETURN n":                    "Neo.ClientError.Statement.SyntaxError",
		"RETURN graph.propertiesByName('nope') AS p":               "Neo.ClientError.Database.DatabaseNotFound",
		"RETURN graph.byName('pcomp.other') AS g":                  "Neo.ClientError.Statement.SyntaxError",
		"USE graph.byName('pcomp.other') USE pcomp.other RETURN 1": "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(context.Background(), query, map[string]interface{}{"n": int64(5)})
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	_, err := exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)
	require.Error(t, err, "graph data on a composite needs a USE")
}

// TestGraphFunctionsOutsideComposite: graph.names() and
// graph.propertiesByName() are Neo4j's "Unknown function" on a standard
// database, and graph.byName outside a USE clause is a SyntaxError (Neo4j
// 5.26).
func TestGraphFunctionsOutsideComposite(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for query, message := range map[string]string{
		"RETURN graph.names() AS g":               "Unknown function 'graph.names'",
		"UNWIND graph.names() AS g RETURN g":      "Unknown function 'graph.names'",
		"RETURN graph.propertiesByName('x') AS p": "Unknown function 'graph.propertiesByName'",
		"RETURN graph.byName('nornic') AS g":      "`graph.byName` is only allowed at the first position of a USE clause.",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.EqualError(t, err, message, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}
