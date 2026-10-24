package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkManagedRemoteConstituentLookup(b *testing.B) {
	remote := newTestMemoryEngine(b)
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

func TestManagedRemoteConstituentLookupWarmAllocations(t *testing.T) {
	remote := newTestMemoryEngine(t)
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(multidb.ConstituentRef, string) (storage.Engine, error) { return remote, nil }
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateCompositeDatabase("probe", []multidb.ConstituentRef{
		{Alias: "R", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: "http://example.test"},
		{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: "http://example.test"},
	}))

	_, err = manager.GetStorage("probe.R")
	require.NoError(t, err)
	var lookupErr error
	allocs := testing.AllocsPerRun(1000, func() {
		_, lookupErr = manager.GetStorage("probe.R")
	})
	require.NoError(t, lookupErr)
	require.LessOrEqual(t, allocs, 11.0, "warm remote constituent lookup must stay at or below the release-base allocation count")
}
