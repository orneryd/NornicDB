package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteProcedureStatementAtomicity(t *testing.T) {
	failure := errors.New("u5 deliberate failure after write")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "u5tx.writefail", Signature: "u5tx.writefail() :: (value :: INTEGER)",
		Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "CREATE (:U5AtomicWrite) RETURN 1 AS value", nil)
		if err != nil {
			return nil, err
		}
		return nil, failure
	}))
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "u5tx.fail", Signature: "u5tx.fail() :: (rejected :: INTEGER)",
		Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "rejected", Type: "INTEGER"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return nil, failure
	}))
	for _, query := range []string{
		"CALL u5tx.writefail() YIELD value RETURN value",
		"CALL apoc.cypher.doIt($statement,{}) YIELD value CALL u5tx.fail() YIELD rejected RETURN value",
	} {
		t.Run(query, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "u5_atomic")
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(context.Background(), query, map[string]interface{}{
				"statement": "CREATE (:U5AtomicWrite) RETURN 1 AS value",
			})
			require.ErrorContains(t, err, failure.Error())
			result, err := NewStorageExecutor(store).Execute(context.Background(), "MATCH (n:U5AtomicWrite) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "failed procedure statement must roll back its writes")
		})
	}
	for _, fragment := range []string{
		"CREATE (:U5AtomicWrite) WITH 1 AS keep CALL u5tx.fail() YIELD rejected RETURN rejected",
		"CALL u5tx.writefail() YIELD value RETURN value",
		"CALL apoc.cypher.doIt('CREATE (:U5AtomicWrite) RETURN 1 AS value',{}) YIELD value CALL u5tx.fail() YIELD rejected RETURN rejected",
	} {
		t.Run("composite fragment/"+fragment, func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			require.NoError(t, manager.CreateDatabase("atomic_shard"))
			require.NoError(t, manager.CreateCompositeDatabase("atomic_comp", []multidb.ConstituentRef{{
				Alias: "shard", DatabaseName: "atomic_shard", Type: "local", AccessMode: "read_write",
			}}))
			store, err := manager.GetStorage("atomic_comp")
			require.NoError(t, err)
			exec := NewStorageExecutor(store)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			_, err = exec.Execute(context.Background(), "CALL { USE atomic_comp.shard "+fragment+" } RETURN 1", nil)
			require.ErrorContains(t, err, failure.Error())
			shard, err := manager.GetStorage("atomic_shard")
			require.NoError(t, err)
			nodes, err := shard.GetNodesByLabel("U5AtomicWrite")
			require.NoError(t, err)
			require.Empty(t, nodes, "failed local fragment must roll back its writes")
		})
	}
}

func TestWriteProcedureCallbackAtomicityAcrossCarriers(t *testing.T) {
	for _, carrier := range []string{"both", "storage", "context"} {
		t.Run(carrier, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "u5_carrier")
			exec := NewStorageExecutor(store)
			fail := true
			failure := errors.New("u5 callback rejected after write")
			name := "u5tx.carrier" + carrier
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{
				Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: ProcedureModeWrite,
				Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
			}, func(ctx context.Context, nested *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				if carrier == "storage" {
					ctx = context.Background()
				}
				if carrier == "context" {
					nested = exec
				}
				result, err := nested.Execute(ctx, "CREATE (:U5CarrierWrite) RETURN 1 AS value", nil)
				if err != nil {
					return nil, err
				}
				if fail {
					return nil, failure
				}
				return result, nil
			}))
			_, err := exec.Execute(context.Background(), "CALL "+name+"() YIELD value RETURN value", nil)
			require.ErrorContains(t, err, failure.Error())
			nodes, err := store.GetNodesByLabel("U5CarrierWrite")
			require.NoError(t, err)
			require.Empty(t, nodes)
			fail = false
			_, err = exec.Execute(context.Background(), "CALL "+name+"() YIELD value RETURN value", nil)
			require.NoError(t, err)
			nodes, err = store.GetNodesByLabel("U5CarrierWrite")
			require.NoError(t, err)
			require.Len(t, nodes, 1, "successful callback commits exactly once")
		})
	}
}

func TestWriteProcedureFabricRegistrationReload(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("reload_shard"))
	require.NoError(t, manager.CreateCompositeDatabase("reload_comp", []multidb.ConstituentRef{{
		Alias: "shard", DatabaseName: "reload_shard", Type: "local", AccessMode: "read_write",
	}}))
	store, err := manager.GetStorage("reload_comp")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	const query = "CALL { USE reload_comp.shard CALL u5tx.reload() YIELD value RETURN value } RETURN value"
	spec := ProcedureSpec{Name: "u5tx.reload", Signature: "u5tx.reload() :: (value :: INTEGER)", Mode: ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}}
	require.NoError(t, RegisterUserProcedure(spec, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	spec.Mode = ProcedureModeWrite
	reject := false
	failure := errors.New("u5 reloaded procedure rejected")
	require.NoError(t, RegisterUserProcedure(spec, func(ctx context.Context, nested *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		result, err := nested.Execute(ctx, "CREATE (:U5ReloadWrite) RETURN 1 AS value", nil)
		if err != nil { return nil, err }
		if reject { return nil, failure }
		return result, nil
	}))
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	reject = true
	_, err = exec.Execute(context.Background(), query, nil)
	require.ErrorContains(t, err, failure.Error())
	shard, err := manager.GetStorage("reload_shard")
	require.NoError(t, err)
	nodes, err := shard.GetNodesByLabel("U5ReloadWrite")
	require.NoError(t, err)
	require.Len(t, nodes, 1, "successful call commits once; reloaded WRITE failure must roll back")
}
