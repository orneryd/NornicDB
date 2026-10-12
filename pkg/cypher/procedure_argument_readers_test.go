package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The shared procedure argument readers, and the argument errors of the
// procedures that use them: a null the procedure needs is its failure, a
// value of another type a TypeError (#907).
func TestProcedureArgumentReaders(t *testing.T) {
	texts, err := requiredProcedureStringList("p", []interface{}{[]string{"a", "b"}}, 0, "labels")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, texts)
	texts, err = requiredProcedureStringList("p", []interface{}{[]interface{}{"a"}}, 0, "labels")
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, texts)
	_, err = requiredProcedureStringList("p", []interface{}{int64(1)}, 0, "labels")
	require.ErrorContains(t, err, "must be LIST<STRING>")
	_, err = requiredProcedureStringList("p", []interface{}{[]interface{}{"a", int64(1)}}, 0, "labels")
	require.ErrorContains(t, err, "must be LIST<STRING>")
	_, err = requiredProcedureStringList("p", nil, 0, "labels")
	require.ErrorContains(t, err, "is null")
}

func TestProcedureArgumentErrors(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	_, err := exec.callApocCypherRunMany(ctx, []interface{}{"RETURN 1", "x"})
	require.ErrorContains(t, err, "argument params must be MAP")
	_, err = exec.callApocPeriodicIterate(ctx, "apoc.periodic.iterate", nil)
	require.ErrorContains(t, err, "argument iterate is null")
	_, err = exec.callApocPeriodicCommit(ctx, []interface{}{"MATCH (n) RETURN n", int64(1)})
	require.ErrorContains(t, err, "argument params must be MAP")

	_, err = exec.callDbIndexFulltextCreateNodeIndex(ctx, []interface{}{"ft", []interface{}{"Doc"}, nil})
	require.ErrorContains(t, err, "argument properties is null")
	_, err = exec.callDbIndexFulltextCreateRelationshipIndex(ctx, []interface{}{nil, []interface{}{"R"}, []interface{}{"p"}})
	require.ErrorContains(t, err, "argument indexName is null")
	_, err = exec.callDbIndexFulltextCreateRelationshipIndex(ctx, []interface{}{"ft", int64(3), []interface{}{"p"}})
	require.ErrorContains(t, err, "argument relationshipTypes must be LIST<STRING>")

	result, err := exec.Execute(ctx, "CALL apoc.periodic.commit('MATCH (n:Nothing) RETURN n', {limit: 5}) YIELD updates RETURN updates", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	_, err = exec.Execute(ctx, "CALL db.index.fulltext.createNodeIndex('ft_opts', ['Doc'], ['text'])", nil)
	require.NoError(t, err)
	for _, options := range []string{"{skip: -1}", "{limit: 'many'}"} {
		_, err = exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('ft_opts', 'x', "+options+") YIELD node RETURN node", nil)
		require.Error(t, err, options)
	}
}
