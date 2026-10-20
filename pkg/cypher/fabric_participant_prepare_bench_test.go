package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkFabricPreparedRemoteParticipants(b *testing.B) {
	for _, query := range []struct{ name, text string }{
		{"local", "USE preparation.local RETURN 1 AS n"},
		{"one-remote", "USE preparation.remote RETURN 1 AS n"},
		{"two-remotes", "USE preparation.remote RETURN 1 AS n UNION ALL USE preparation.remote2 RETURN 1 AS n"},
	} {
		b.Run(query.name, func(b *testing.B) {
			base := storage.NewMemoryEngine()
			manager, err := multidb.NewDatabaseManager(base, &multidb.Config{
				RemoteEngineFactory: func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
					return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = manager.Close() })
			if err := manager.CreateDatabase("preparation_local"); err != nil {
				b.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("preparation", []multidb.ConstituentRef{
				{Alias: "local", DatabaseName: "preparation_local", Type: "local", AccessMode: "read_write"},
				{Alias: "remote", DatabaseName: "remote", Type: "remote", URI: "http://127.0.0.1:1", AccessMode: "read_write"},
				{Alias: "remote2", DatabaseName: "remote2", Type: "remote", URI: "http://127.0.0.1:1", AccessMode: "read_write"},
			}); err != nil {
				b.Fatal(err)
			}
			view, err := manager.GetStorage("preparation")
			if err != nil {
				b.Fatal(err)
			}
			executor := NewStorageExecutor(view)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			// Prime plan caches; this benchmark makes no remote requests.
			if _, err := executor.prepareFabricExecution(ctx, query.text); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := executor.prepareFabricExecution(ctx, query.text); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
