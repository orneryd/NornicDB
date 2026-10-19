package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkManagedRemoteConstituentLookup(b *testing.B) {
	remote := storage.NewMemoryEngine()
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(multidb.ConstituentRef, string) (storage.Engine, error) { return remote, nil }
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(b, err)
	defer manager.Close()
	require.NoError(b, manager.CreateCompositeDatabase("probe", []multidb.ConstituentRef{
		{Alias: "R", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: "http://example.test"},
		{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: "http://example.test"},
	}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.GetStorage("probe.R"); err != nil {
			b.Fatal(err)
		}
	}
}
