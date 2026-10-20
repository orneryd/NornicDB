package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// Command and row statements share semantic admission and frame lowering, even
// though only row statements run through the pipeline. Each phase has its own
// fixture: Cypher forbids schema and data writes in one explicit transaction.
func TestFabricLogicalStatementBoundary(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, commands := range []bool{false, true} {
					t.Run(fmt.Sprintf("normalization_%v/composite_%v/explicit_%v/commands_%v", normalize, composite, explicit, commands), func(t *testing.T) {
						testFabricLogicalStatementBoundary(t, normalize, composite, explicit, commands)
					})
				}
			}
		}
	}
}

func testFabricLogicalStatementBoundary(t *testing.T, normalize, composite, explicit, commands bool) {
	config.SetCypherQueryNormalizationEnabled(normalize)
	base := newFabricCallRowsExecutor(t)
	manager := base.dbManager.(*testDatabaseManagerAdapter).manager
	store, err := manager.GetStorage("one")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	prefix, selection := "", ""
	if composite {
		exec = fabricCallRowsCompositeExecutor(t, base)
		prefix, selection = "USE comp.one ", "USE comp.one "
	}
	ctx := context.Background()
	if explicit {
		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer exec.Execute(ctx, "ROLLBACK", nil)
		if composite {
			_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
			require.True(t, ok)
		}
	}
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		result, runErr := exec.Execute(ctx, prefix+query, params)
		require.NoError(t, runErr, query)
		return result
	}
	if commands {
		run("CREATE INDEX fabric_scope_frame_idx FOR (n:FabricScopeFrame) ON (n.id)", nil)
		result := run("SHOW INDEXES YIELD name WHERE name = $index RETURN name", map[string]interface{}{"index": "fabric_scope_frame_idx"})
		require.Equal(t, []string{"name"}, result.Columns)
		require.Equal(t, [][]interface{}{{"fabric_scope_frame_idx"}}, result.Rows)
		result = run("SHOW CONSTRAINTS YIELD name WHERE name = 'fabric_scope_frame_none' RETURN name", nil)
		require.Equal(t, []string{"name"}, result.Columns)
		require.Empty(t, result.Rows)
		result = run("TERMINATE TRANSACTIONS 'one-transaction-999999999'", nil)
		require.Equal(t, []string{"transactionId", "username", "message"}, result.Columns)
		require.Equal(t, [][]interface{}{{"one-transaction-999999999", nil, "Transaction not found."}}, result.Rows)
		run("DROP INDEX fabric_scope_frame_idx", nil)
		result = run("SHOW INDEXES YIELD name WHERE name = 'fabric_scope_frame_idx' RETURN name", nil)
		require.Empty(t, result.Rows)
		return
	}
	run("CREATE (:FabricScopeFrame {id:1})-[:FRAME]->(:FabricScopeFrame {id:2})", nil)
	result := run("WITH 1 AS marker CALL (marker) { "+selection+"MATCH (n:FabricScopeFrame {id:marker}) WITH n RETURN elementId(n) AS direct,[elementId(n)] AS nested } RETURN marker,direct,nested", nil)
	require.Len(t, result.Rows, 1)
	require.Equal(t, int64(1), result.Rows[0][0])
	require.NotEmpty(t, result.Rows[0][1])
	require.Equal(t, []interface{}{result.Rows[0][1]}, result.Rows[0][2])
	result = run("UNWIND [1,2] AS x CALL { WITH x "+selection+"MATCH (n:FabricScopeFrame) WHERE n.id = x RETURN collect(n.id) AS ids } RETURN x,ids ORDER BY x", map[string]interface{}{"x": 99})
	require.Equal(t, [][]interface{}{{int64(1), []interface{}{int64(1)}}, {int64(2), []interface{}{int64(2)}}}, result.Rows)
	result = run("MATCH (r:FabricScopeFrame {id:1}) CALL { "+selection+"MATCH ()-[r:FRAME]->(n:FabricScopeFrame) RETURN n.id AS value } RETURN value", nil)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	result = run("WITH 7 AS x CALL (x) { "+selection+"WITH x AS value FINISH } RETURN x", nil)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
}

func TestFabricLogicalStatementBoundaryReference(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("statement boundary oracle requires --ref neo-ref")
	}
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	tx, err := session.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	created, err := tx.Run(ctx, "CREATE (:FabricScopeFrameReference {id:1})-[:FRAME]->(:FabricScopeFrameReference {id:2})", nil)
	require.NoError(t, err)
	_, err = created.Consume(ctx)
	require.NoError(t, err)
	fabricCallRowsAssertReference(t, tx, "WITH 1 AS marker CALL (marker) { MATCH (n:FabricScopeFrameReference {id:marker}) WITH n RETURN elementId(n) AS direct,[elementId(n)] AS nested } RETURN marker,direct = nested[0] AS same", "[[1,true]]")
	fabricCallRowsAssertReference(t, tx, "UNWIND [1,2] AS x CALL { WITH x MATCH (n:FabricScopeFrameReference) WHERE n.id = x RETURN collect(n.id) AS ids } RETURN x,ids ORDER BY x", "[[1,[1]],[2,[2]]]")
	fabricCallRowsAssertReference(t, tx, "WITH 7 AS x CALL (x) { WITH x AS value FINISH } RETURN x", "[[7]]")
}
