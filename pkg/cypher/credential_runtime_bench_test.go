package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkManagedCredentialRuntime(b *testing.B) {
	for _, operation := range []string{"lookup", "admission"} {
		b.Run(operation, func(b *testing.B) {
			cfg := multidb.DefaultConfig()
			cfg.RemoteCredentialEncryptionKey = "synthetic-benchmark-key-713926"
			remote := newTestMemoryEngine(b)
			cfg.RemoteEngineFactory = func(multidb.ConstituentRef, string) (storage.Engine, error) {
				return remote, nil
			}
			manager, err := multidb.NewDatabaseManager(b.Context(), fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), cfg)
			require.NoError(b, err)
			fixtureowner.Own(b, manager)
			require.NoError(b, manager.CreateCompositeDatabase(b.Context(), "probe", []multidb.ConstituentRef{
				{Alias: "winner", Type: "remote", AccessMode: "read", DatabaseName: "remote", URI: "http://synthetic%2Fusername-871429@example.invalid"},
			}))
			_, err = manager.GetStorage("probe.winner")
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if operation == "lookup" {
					if _, err := manager.GetStorage("probe.winner"); err != nil {
						b.Fatal(err)
					}
				} else {
					owner, err := manager.ResolveConnectionLimitOwner("probe.winner")
					if err != nil || owner != "probe" {
						b.Fatal("unexpected admission owner")
					}
				}
			}
		})
	}
}
