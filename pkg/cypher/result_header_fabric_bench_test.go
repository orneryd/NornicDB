package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type resultHeaderBenchExecutor struct{ exec *StorageExecutor }

func (e *resultHeaderBenchExecutor) ExecuteQuery(ctx context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	result, err := e.exec.Execute(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	return result.Columns, result.Rows, nil
}

func (e *resultHeaderBenchExecutor) ExecuteQueryWithRecord(ctx context.Context, database string, engine storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	return e.ExecuteQuery(ctx, database, engine, query, params)
}

// No input rows means CREATE performs no writes. This measures Fabric's empty
// field dispatch, including the formerly incorrect nested-RETURN fallback.
func BenchmarkResultHeaderFabricDispatch(b *testing.B) {
	store := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = store.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "neo4j"))
	adapter := &resultHeaderBenchExecutor{exec: exec}
	local := fabric.NewLocalFragmentExecutor(adapter, func(string) (storage.Engine, error) { return store, nil })
	catalog := fabric.NewCatalog()
	catalog.Register("comp.local", &fabric.LocationLocal{DBName: "neo4j"})
	dispatcher := fabric.NewFabricExecutor(catalog, local, nil)
	fragment := &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: "comp.local", Query: "UNWIND [] AS value CALL { RETURN 1 AS before } CREATE (:HeaderBenchNoWrite)"}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := dispatcher.Execute(ctx, nil, fragment, map[string]interface{}{"nonce": i}, ""); err != nil {
			b.Fatal(err)
		}
	}
}

// Empty correlated CALLs must retain their declared fields through the next
// APPLY and final projection. Parameters bypass result caching on every run.
func BenchmarkResultHeaderFabricEmptyChain(b *testing.B) {
	store := storage.NewMemoryEngine()
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = manager.Close() })
	for _, database := range []string{"header_first", "header_second"} {
		if err := manager.CreateDatabase(database); err != nil {
			b.Fatal(err)
		}
	}
	if err := manager.CreateCompositeDatabase("header_comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "header_first", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "header_second", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		b.Fatal(err)
	}
	root, err := manager.GetStorage(manager.DefaultDatabaseName())
	if err != nil {
		b.Fatal(err)
	}
	exec := NewStorageExecutor(root)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	query := "USE header_comp CALL { USE header_comp.one UNWIND [] AS value RETURN value } CALL { WITH value USE header_comp.two RETURN value AS kept } RETURN value, kept"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"nonce": i})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Columns) != 2 {
			b.Fatalf("lost empty APPLY fields: %v", result.Columns)
		}
	}
}
