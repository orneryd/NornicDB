package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// dbms.memory.transaction.total.max is dynamic, as in Neo4j 5.26.30: SHOW
// SETTINGS and dbms.listConfig say so, and dbms.setConfigValue changes it
// while the process runs, keeping the reservations in use (#1343).
func TestGlobalTransactionPoolIsDynamic(t *testing.T) {
	resetTransactionMemory(t)
	t.Cleanup(func() { processSettingValues.Delete(globalTransactionMemorySetting) })
	ctx := context.Background()
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)

	settings, err := executor.Execute(ctx, "SHOW SETTINGS YIELD name, value, isDynamic, isExplicitlySet WHERE name = 'dbms.memory.transaction.total.max' RETURN value, isDynamic, isExplicitlySet", nil)
	require.NoError(t, err)
	require.Len(t, settings.Rows, 1)
	require.Equal(t, true, settings.Rows[0][1])
	listed, err := executor.Execute(ctx, "CALL dbms.listConfig('dbms.memory.transaction.total.max') YIELD name, dynamic RETURN dynamic", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, listed.Rows)

	_, err = executor.Execute(ctx, "CALL dbms.setConfigValue('dbms.memory.transaction.total.max', '4m')", nil)
	require.NoError(t, err)
	settings, err = executor.Execute(ctx, "SHOW SETTINGS YIELD name, value, isExplicitlySet WHERE name = 'dbms.memory.transaction.total.max' RETURN value, isExplicitlySet", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"4194304", true}}, settings.Rows)
	_, err = executor.Execute(ctx, heavyCollectQuery, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
	requirePoolsEmpty(t, "nornic")

	// A change keeps what transactions hold: others' reservations stay, and
	// the new limit applies to the next reservation.
	ok, _, _ := transactionMemory.global.reserve(3 << 20)
	require.True(t, ok)
	_, err = executor.Execute(ctx, "CALL dbms.setConfigValue('dbms.memory.transaction.total.max', '2m')", nil)
	require.NoError(t, err)
	global, _ := TransactionMemoryPoolUsage("nornic")
	require.Equal(t, int64(3<<20), global)
	transactionMemory.global.release(3 << 20)

	_, err = executor.Execute(ctx, "CALL dbms.setConfigValue('dbms.memory.transaction.total.max', '0')", nil)
	require.NoError(t, err)
	result, err := executor.Execute(ctx, heavyCollectQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1000000), result.Rows[0][0])
}

func TestSetConfigValueRefusals(t *testing.T) {
	resetTransactionMemory(t)
	ctx := context.Background()
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	for statement, message := range map[string]string{
		"CALL dbms.setConfigValue('no.such.setting', '1')":                         "Unknown setting: no.such.setting",
		"CALL dbms.setConfigValue('db.nornic.memory.storage.mode', 'low')":         "Setting 'db.nornic.memory.storage.mode' is not dynamic and can not be changed at runtime",
		"CALL dbms.setConfigValue('db.memory.transaction.max', '1m')":              "Setting 'db.memory.transaction.max' is a database setting",
		"CALL dbms.setConfigValue('dbms.memory.transaction.total.max', 'lots')":    "'lots' is not a valid value for setting 'dbms.memory.transaction.total.max'",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
		code, text := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code, statement)
		require.Contains(t, text, message, statement)
	}
	_, live := liveProcessSetting(globalTransactionMemorySetting)
	require.False(t, live, "a refused value is not set")
}
