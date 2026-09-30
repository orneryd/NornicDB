package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallVectorQueryNodes_YieldWhere_ElementIDMatchesRealNode(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx', 'Doc', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CREATE (d:Doc {
	id: 'doc-1',
	embedding: [1.0, 0.0, 0.0]
})
`, nil)
	require.NoError(t, err)

	idRes, err := exec.Execute(ctx, "MATCH (d:Doc {id:'doc-1'}) RETURN elementId(d) AS id", nil)
	require.NoError(t, err)
	require.Len(t, idRes.Rows, 1)
	rootID, ok := idRes.Rows[0][0].(string)
	require.True(t, ok)
	require.NotEmpty(t, rootID)

	res, err := exec.Execute(ctx, `
CALL db.index.vector.queryNodes('idx', 10, [1.0, 0.0, 0.0])
YIELD node, score
WHERE elementId(node) = $rootID
RETURN node.id AS id, score
`, map[string]interface{}{"rootID": rootID})
	require.NoError(t, err)
	require.Equal(t, []string{"id", "score"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "doc-1", res.Rows[0][0])
}

func TestProcedureOutputTypes(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	for input, expected := range map[string]string{
		"ANY": "", "": "", "INTEGER?": "Integer", "LIST<ANY>": "List<Any>", "LIST<LIST<STRING>>": "List<List<String>>",
	} {
		require.Equal(t, expected, procedureStaticType(input))
	}
	require.Nil(t, procedureOutputTypes("CALL missing.procedure()"))
	require.Equal(t, map[string]string{"label": "String"}, procedureOutputTypes("CALL db.labels()"))
	require.Equal(t, "List<String>", procedureOutputTypes("CALL db.indexes()")["properties"])
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{
		Name: "test.yieldTypes", Mode: ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: "valid", Type: "BOOLEAN"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return nil, nil
	}))
	t.Cleanup(ClearUserProcedures)
	require.Equal(t, map[string]string{"valid": "Boolean"}, procedureOutputTypes("CALL test.yieldTypes()"))
}

func TestCallYieldWhereRejectsNonBooleanValue(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:CallYieldWhereLabel)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label RETURN label", nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func TestCallYieldWhereCanReferenceYieldAlias(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:YieldWhereAlias)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `CALL db.labels() YIELD label AS alias WHERE alias = 'YieldWhereAlias' RETURN alias`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"alias"}, result.Columns)
	require.Equal(t, [][]interface{}{{"YieldWhereAlias"}}, result.Rows)
}

func TestStandaloneCallYieldWhereOrderAndPage(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:YieldTailAlpha), (:YieldTailBeta)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `CALL db.labels() YIELD label AS name
WHERE name STARTS WITH 'YieldTail'
ORDER BY name DESC SKIP 1 LIMIT 1
RETURN name`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, result.Columns)
	require.Equal(t, [][]interface{}{{"YieldTailAlpha"}}, result.Rows)
}
