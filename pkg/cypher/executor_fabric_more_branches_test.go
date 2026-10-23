package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestFabricHelpers_MoreContextAndWrapperBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	// preparedFabricFromContext branches
	prepared, err := exec.preparedFabricFromContext(context.Background())
	require.NoError(t, err)
	require.Nil(t, prepared)

	ctxBad := context.WithValue(context.Background(), fabricPreparedExecKey{}, 123)
	prepared, err = exec.preparedFabricFromContext(ctxBad)
	require.Error(t, err)
	require.Nil(t, prepared)

	ctxGood := context.WithValue(context.Background(), fabricPreparedExecKey{}, &fabricPreparedExec{})
	prepared, err = exec.preparedFabricFromContext(ctxGood)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// executeViaPreparedFabricWithTx nil prepared branch
	_, err = exec.executeViaPreparedFabricWithTx(context.Background(), "RETURN 1", nil, fabric.NewFabricTransaction("tx-1"), true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not prepared")

	// normalizeFabricRowWrapper branches including sourceId fallback lookup
	stream := &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{map[string]interface{}{"sourceId": "s1", "rows": []interface{}{map[string]interface{}{"sourceId": "s1", "name": "neo"}}}}}}
	exec.normalizeFabricRowWrapper("MATCH (n) RETURN name", stream)
	require.Equal(t, []string{"name"}, stream.Columns)
	require.Equal(t, [][]interface{}{{"neo"}}, stream.Rows)

	stream = &fabric.ResultStream{Columns: []string{"not_wrapper"}, Rows: [][]interface{}{{"x"}}}
	exec.normalizeFabricRowWrapper("RETURN x", stream)
	require.Equal(t, []string{"not_wrapper"}, stream.Columns)

	stream = &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{123}}}
	exec.normalizeFabricRowWrapper("RETURN x", stream)
	require.Equal(t, []string{"__fabric_row"}, stream.Columns)

	// lookupColumnBySourceIDInRows []map branch and missing keys
	v, ok := lookupColumnBySourceIDInRows([]map[string]interface{}{{"sourceId": "a", "col": int64(7)}}, "a", "col")
	require.True(t, ok)
	require.Equal(t, int64(7), v)
	_, ok = lookupColumnBySourceIDInRows([]interface{}{42}, "a", "col")
	require.False(t, ok)
}

func TestFabricHelpers_BindCallbacksBranches(t *testing.T) {
	c := &cypherFabricExecutor{}
	require.NoError(t, c.bindCallbacksOnce(&fabric.SubTransaction{ShardName: "s"}, nil, nil))

	c.base = NewStorageExecutor(newTestMemoryEngine(t))
	require.NoError(t, c.bindCallbacksOnce(&fabric.SubTransaction{ShardName: "s"}, nil, nil))

	c.base.txContext = &TransactionContext{active: true, tx: struct{}{}}
	require.NoError(t, c.bindCallbacksOnce(&fabric.SubTransaction{ShardName: "s"}, nil, nil))

	tx := fabric.NewFabricTransaction("tx-bind")
	_, err := tx.GetOrOpen("s1", false)
	require.NoError(t, err)
	c.base.txContext = &TransactionContext{active: true, tx: tx}
	require.NoError(t, c.bindCallbacksOnce(&fabric.SubTransaction{ShardName: "s1"}, func(*fabric.SubTransaction) error { return nil }, func(*fabric.SubTransaction) error { return nil }))
	err = c.bindCallbacksOnce(&fabric.SubTransaction{ShardName: "missing"}, nil, nil)
	require.Error(t, err)
}
