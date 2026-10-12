package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Index procedures' failures for a missing index or a null argument carry
// Neo4j 5.26.30's status codes: a vector query on a missing index fails for
// relationships as for nodes; a null name or count of a vector query and a
// null timeout of db.awaitIndex fail the call; a null index name is looked
// up as 'null' (#907).
func TestIndexProcedureArgumentErrorCodes(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for query, want := range map[string]string{
		"CALL db.index.vector.queryNodes('missing_vec', 2, [1.0, 2.0]) YIELD node RETURN node":                         "Neo.ClientError.Procedure.ProcedureCallFailed",
		"CALL db.index.vector.queryRelationships('missing_vec', 2, [1.0, 2.0]) YIELD relationship RETURN relationship": "Neo.ClientError.Procedure.ProcedureCallFailed",
		"WITH null AS v CALL db.index.vector.queryNodes(v, 2, [1.0, 2.0]) YIELD node RETURN node":                      "Neo.ClientError.Procedure.ProcedureCallFailed",
		"WITH null AS v CALL db.index.vector.queryNodes('missing_vec', v, [1.0, 2.0]) YIELD node RETURN node":          "Neo.ClientError.Procedure.ProcedureCallFailed",
		"WITH null AS v CALL db.awaitIndex('missing_idx', v)":                                                          "Neo.ClientError.Procedure.ProcedureCallFailed",
		"WITH null AS v CALL db.awaitIndex(v, 1)":                                                                      "Neo.ClientError.Schema.IndexNotFound",
		"WITH null AS v CALL db.resampleIndex(v)":                                                                      "Neo.ClientError.Schema.IndexNotFound",
		"CALL db.awaitIndex('missing_idx', 1)":                                                                         "Neo.ClientError.Schema.IndexNotFound",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, want, code, query)
	}
	_, err := exec.Execute(ctx, "WITH null AS v CALL db.resampleIndex(v)", nil)
	require.ErrorContains(t, err, "No such index 'null'")
}
