package cypher

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkCompositeEntityIdentity(b *testing.B) {
	for _, query := range []struct{ name, tail string }{
		{"entities", "RETURN n,m"},
		{"equality", "RETURN n = m"},
		{"elementIds", "RETURN elementId(n),elementId(m)"},
		{"distinct", "UNWIND [n,m,n] AS x RETURN count(DISTINCT x)"},
	} {
		b.Run(query.name, func(b *testing.B) {
			mgr, err := multidb.NewDatabaseManager(runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed), nil)
			require.NoError(b, err)
			b.Cleanup(func() { _ = mgr.Close() })
			for _, database := range []string{"one", "two"} {
				require.NoError(b, mgr.CreateDatabase(database))
				store, err := mgr.GetStorage(database)
				require.NoError(b, err)
				_, err = store.CreateNode(&storage.Node{ID: "same", Labels: []string{"P"}})
				require.NoError(b, err)
			}
			require.NoError(b, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
				{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
				{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
			}))
			store, err := mgr.GetStorage(mgr.DefaultDatabaseName())
			require.NoError(b, err)
			exec := NewStorageExecutor(store)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
			statement := "USE comp CALL { USE comp.one MATCH (n:P) RETURN n } CALL { USE comp.two MATCH (m:P) RETURN m } " + query.tail
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				_, err := exec.Execute(context.Background(), statement, map[string]interface{}{"nonce": index})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
