package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestApocDoItUsesCallingTransaction(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "do_it"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	r, err := e.Execute(ctx, "CALL apoc.cypher.doIt('CREATE (n:DoIt {v:$value}) RETURN n.v AS v', {value:7}) YIELD value RETURN value.v AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
	r, err = e.Execute(ctx, "MATCH (n:DoIt) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), r.Rows[0][0])
	_, err = e.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	r, err = e.Execute(ctx, "MATCH (n:DoIt) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), r.Rows[0][0])
	r, err = e.Execute(ctx, "CALL apoc.cypher.doIt('RETURN $x AS value',{x:9}) YIELD value RETURN value.value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9)}}, r.Rows)
}

func TestApocDoItDeclarationAndBoundaries(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "do_it_boundaries"))
	requirements := QueryPermissionRequirements(t.Context(), "CALL apoc.cypher.doIt('RETURN 1', {})")
	require.True(t, requirements.Write)
	readOnly := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" })
	_, err := e.Execute(readOnly, "CALL apoc.cypher.doIt('CREATE (:Forbidden)',{})", nil)
	require.Error(t, err)
	for _, q := range []string{"CALL apoc.cypher.doIt()", "CALL apoc.cypher.doIt('RETURN 1')", "CALL apoc.cypher.doIt('RETURN 1',{},3)", "CALL apoc.cypher.doIt(1,{})", "CALL apoc.cypher.doIt('RETURN 1',3)", "CALL apoc.cypher.doit('RETURN 1',{})", "CALL apoc.math.sigmoid(1)"} {
		_, err := e.Execute(context.Background(), q, nil)
		require.Error(t, err, q)
	}
	_, err = e.Execute(context.Background(), "CALL apoc.cypher.doIt('CREATE PROCEDURE bad() AS RETURN 1',{})", nil)
	require.Error(t, err)
}
