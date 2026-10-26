package cypher_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fmt"
	"github.com/orneryd/nornicdb/internal/enginecontract"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func externalDispositions(word string) []storage.BindingDisposition {
	var result []storage.BindingDisposition
	for _, r := range word {
		result = append(result, map[rune]storage.BindingDisposition{'O': storage.BindingOwned, 'C': storage.BindingCarried, 'D': storage.BindingDelegated, 'U': storage.BindingUnowned, 'R': storage.BindingRefused}[r])
	}
	return result
}

func runAuthPublicObligations(t *testing.T) *enginecontract.PublicProof {
	const name = "github.com/orneryd/nornicdb/pkg/auth.storeTransaction"
	issue := func(t *testing.T, consumer string) {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		_, err := base.CreateNode(&storage.Node{ID: "nornic:logical", Labels: []string{"FoundationLogical"}, Properties: map[string]interface{}{"v": int64(7)}})
		require.NoError(t, err)
		require.NoError(t, auth.WithStoreTransaction(context.Background(), base, func(view storage.Engine) error {
			typ := reflect.TypeOf(view).Elem()
			require.Equal(t, name, typ.PkgPath()+"."+typ.Name())
			cypher.FoundationRunAtomicConsumer(t, view, base, consumer)
			return nil
		}))
	}
	var subject storage.Engine
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	require.NoError(t, auth.WithStoreTransaction(context.Background(), base, func(view storage.Engine) error { subject = view; return nil }))
	obligations := map[string]enginecontract.PublicObligation{}
	expected := externalDispositions("OORRRRUODOOOOORR")
	value := reflect.ValueOf(subject.(storage.EngineBoundaryDeclaration).EngineCapabilityPolicy())
	for i := 1; i < value.NumField(); i++ {
		field := value.Type().Field(i).Name
		consumers := map[string]func(*testing.T){}
		for _, consumer := range enginecontract.LogicalConsumers[field] {
			consumers[consumer] = func(t *testing.T) { issue(t, consumer) }
		}
		obligations[field] = enginecontract.PublicObligation{Disposition: expected[i-1], Consumers: consumers}
	}
	return enginecontract.CheckPublicObligations(t, subject, obligations)
}

var publicEngineObligationFactories = map[string]func(*testing.T) *enginecontract.PublicProof{
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault.ReadFailureEngine": func(t *testing.T) *enginecontract.PublicProof {
		return cypher.FoundationCheckPublicPolicy(t, "github.com/orneryd/nornicdb/internal/testfixture/storagefault.ReadFailureEngine", externalDispositions("ODDDDDDDDDDDOORR"), func(t *testing.T) cypher.FoundationPublicFixture {
			return cypher.FoundationIssuePublicFixture(t, "ReadFailureEngine", func(engine storage.Engine) storage.Engine {
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, wal.Close()) })
				source := storage.NewWALEngine(engine, wal)
				reader, ok := storage.StoredReader(engine)
				require.True(t, ok)
				return storagefault.NewReadFailureEngine(source, reader, fmt.Errorf("injected read failure"))
			})
		})
	},

	"github.com/orneryd/nornicdb/pkg/replication.ReplicatedEngine": func(t *testing.T) *enginecontract.PublicProof {
		return cypher.FoundationCheckPublicPolicy(t, "github.com/orneryd/nornicdb/pkg/replication.ReplicatedEngine", externalDispositions("DDDDDDDDDDDDOORR"), func(t *testing.T) cypher.FoundationPublicFixture {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			tenant := storage.NewNamespacedEngine(base, "tenant")
			adapter, err := replication.NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, adapter.Close()) })
			cfg := replication.DefaultConfig()
			cfg.Mode = replication.ModeStandalone
			cfg.NodeID = "foundation"
			replicator, err := replication.NewReplicator(cfg, adapter)
			require.NoError(t, err)
			require.NoError(t, replicator.Start(context.Background()))
			t.Cleanup(func() { require.NoError(t, replicator.Shutdown()) })
			engine := replication.NewReplicatedEngine(base, replicator, 5*time.Second)
			view := storage.NewNamespacedEngine(engine, "tenant")
			_, err = view.CreateNode(&storage.Node{ID: "logical", Labels: []string{"FoundationLogical"}, Properties: map[string]interface{}{"v": int64(7)}})
			require.NoError(t, err)
			return cypher.FoundationPublicFixture{Engine: engine, Committed: tenant, NodeID: "logical", Consume: func(t *testing.T, consumer string) { cypher.FoundationRunUnownedConsumer(t, view, consumer) }}
		})
	},
	"github.com/orneryd/nornicdb/pkg/multidb.sizeTrackingEngine": func(t *testing.T) *enginecontract.PublicProof {
		return cypher.FoundationCheckPublicPolicy(t, "github.com/orneryd/nornicdb/pkg/multidb.sizeTrackingEngine", externalDispositions("DDDDDDDDDDDDOORR"), func(t *testing.T) cypher.FoundationPublicFixture {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			manager, err := multidb.NewDatabaseManager(base, multidb.NewConfigFromDefaultDatabase("tenant"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			engine, err := manager.GetStorage("tenant")
			require.NoError(t, err)
			return cypher.FoundationIssueLeafFixture(t, engine)
		})
	},
	"github.com/orneryd/nornicdb/pkg/auth.storeTransaction": runAuthPublicObligations,
}

func TestFoundationExternalPublicLogicalObligations(t *testing.T) {
	for name, run := range publicEngineObligationFactories {
		t.Run(name, func(t *testing.T) { enginecontract.RequirePublicProof(t, name, run(t)) })
	}
}
