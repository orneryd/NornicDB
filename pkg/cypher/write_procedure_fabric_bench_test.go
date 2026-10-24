package cypher

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkFabricProcedureAdmission(b *testing.B) {
	for _, route := range []string{"read", "create", "registered", "apoc", "dynamicWrite", "dynamicRead"} {
		b.Run(route, func(b *testing.B) {
			manager, err := multidb.NewDatabaseManager(runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed), nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = manager.Close() })
			if err := manager.CreateDatabase("u5_fabric_shard"); err != nil {
				b.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("u5_fabric", []multidb.ConstituentRef{{Alias: "shard", DatabaseName: "u5_fabric_shard", Type: "local", AccessMode: "read_write"}}); err != nil {
				b.Fatal(err)
			}
			store, err := manager.GetStorage("u5_fabric")
			if err != nil {
				b.Fatal(err)
			}
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			const statement = "MERGE (n:U5FabricBench {id:1}) SET n.value=$value RETURN n.value AS value"
			if err := RegisterUserProcedure(ProcedureSpec{
				Name: "u5tx.fabricbench", Signature: "u5tx.fabricbench(value :: INTEGER) :: (value :: INTEGER)", Mode: ProcedureModeWrite,
				MinArgs: 1, MaxArgs: 1, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
			}, func(ctx context.Context, nested *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
				return nested.Execute(ctx, statement, map[string]interface{}{"value": args[0]})
			}); err != nil {
				b.Fatal(err)
			}
			fragment := "RETURN 1 AS value"
			inner := statement
			switch route {
			case "create":
				fragment = statement
			case "registered":
				fragment = "CALL u5tx.fabricbench($value) YIELD value RETURN value"
			case "apoc":
				fragment = "CALL apoc.cypher.doIt($statement,{value:$value}) YIELD value RETURN value.value AS value"
			case "dynamicWrite", "dynamicRead":
				fragment = "CALL apoc.cypher.run($statement,{value:$value}) YIELD value RETURN value.value AS value"
				if route == "dynamicRead" {
					inner = "RETURN $value AS value"
				}
			}
			query := "CALL { USE u5_fabric.shard " + fragment + " } RETURN value"
			params := map[string]interface{}{"value": 0, "statement": inner}
			if _, err := exec.Execute(context.Background(), query, params); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params["value"] = i
				result, err := exec.Execute(context.Background(), query, params)
				if err != nil || len(result.Rows) != 1 {
					b.Fatalf("fragment result: %v %v", result, err)
				}
			}
		})
	}
}
