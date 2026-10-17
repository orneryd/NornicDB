package cypher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// closeCountingEngine counts its closes.
type closeCountingEngine struct {
	storage.Engine
	closed *atomic.Int64
}

func (e *closeCountingEngine) Close() error {
	e.closed.Add(1)
	return e.Engine.Close()
}

// One attribution serves a whole result: a remote constituent is opened once
// for all of a result's entities, not once per entity, and closed once when
// the result is released; the answers survive the release, and a new entity
// after it opens the constituent once more.
func TestEntityDatabaseAttributionOpensEachConstituentOnce(t *testing.T) {
	var opened, closed atomic.Int64
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			opened.Add(1)
			engine := storage.NewMemoryEngine()
			for _, id := range []storage.NodeID{"r1", "r2", "r3", "r4"} {
				if _, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Remote"}}); err != nil {
					return nil, err
				}
			}
			return &closeCountingEngine{Engine: engine, closed: &closed}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("attr_local"))
	local, err := manager.GetStorage("attr_local")
	require.NoError(t, err)
	_, err = local.CreateNode(&storage.Node{ID: "l1", Labels: []string{"Local"}})
	require.NoError(t, err)
	require.NoError(t, manager.CreateCompositeDatabase("attr_cmp", []multidb.ConstituentRef{
		{Alias: "loc", DatabaseName: "attr_local", Type: "local", AccessMode: "read"},
		{Alias: "rem", DatabaseName: "attr_remote", Type: "remote", AccessMode: "read", URI: "http://unused.invalid"},
	}))

	attribution := NewEntityDatabaseAttribution(context.Background(), &testDatabaseManagerAdapter{manager: manager}, "attr_cmp", nil)
	require.NotNil(t, attribution)
	require.Zero(t, opened.Load(), "construction opens nothing")
	for _, id := range []storage.NodeID{"r1", "r2", "r3", "r1", "l1", "missing"} {
		attribution.Resolve(id, "")
	}
	require.Equal(t, "attr_remote", attribution.Resolve("r2", ""))
	require.Equal(t, "attr_local", attribution.Resolve("l1", ""))
	require.Equal(t, "attr_cmp", attribution.Resolve("missing", ""), "an entity no constituent holds is the selection's")
	require.EqualValues(t, 1, opened.Load(), "one remote open for the whole result")
	require.Zero(t, closed.Load())

	attribution.Release()
	require.EqualValues(t, 1, closed.Load(), "released once")
	require.Equal(t, "attr_remote", attribution.Resolve("r3", ""), "a known answer needs no lookup")
	require.EqualValues(t, 1, opened.Load())
	require.Equal(t, "attr_remote", attribution.Resolve("r4", ""))
	require.EqualValues(t, 2, opened.Load(), "a new entity after release opens the constituent once more")
	attribution.Release()
	attribution.Release()
	require.EqualValues(t, 2, closed.Load(), "each open closes once")

	require.Nil(t, NewEntityDatabaseAttribution(context.Background(), &testDatabaseManagerAdapter{manager: manager}, "attr_local", nil), "a standard database needs no attribution")
	var none *EntityDatabaseAttribution
	none.Release()
}

// A constituent whose factory fails is tried once per lookup, not once per
// entity: the failure is kept until the result is released, and a new entity
// after the release tries it once more.
func TestEntityDatabaseAttributionTriesFailedConstituentOnce(t *testing.T) {
	var attempts atomic.Int64
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			attempts.Add(1)
			return nil, errors.New("remote unreachable")
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("fail_local"))
	local, err := manager.GetStorage("fail_local")
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"l1", "l2", "l3"} {
		_, err = local.CreateNode(&storage.Node{ID: id, Labels: []string{"Local"}})
		require.NoError(t, err)
	}
	require.NoError(t, manager.CreateCompositeDatabase("fail_cmp", []multidb.ConstituentRef{
		{Alias: "rem", DatabaseName: "fail_remote", Type: "remote", AccessMode: "read", URI: "http://unused.invalid"},
		{Alias: "loc", DatabaseName: "fail_local", Type: "local", AccessMode: "read"},
	}))

	attribution := NewEntityDatabaseAttribution(context.Background(), &testDatabaseManagerAdapter{manager: manager}, "fail_cmp", nil)
	require.NotNil(t, attribution)
	// Entities no constituent holds visit every constituent, the failing one
	// included, whatever order the constituents are visited in.
	for _, id := range []storage.NodeID{"missing1", "missing2", "missing3"} {
		require.Equal(t, "fail_cmp", attribution.Resolve(id, ""))
	}
	for _, id := range []storage.NodeID{"l1", "l2", "l3"} {
		require.Equal(t, "fail_local", attribution.Resolve(id, ""))
	}
	require.EqualValues(t, 1, attempts.Load(), "a failed constituent is tried once per lookup")

	attribution.Release()
	require.Equal(t, "fail_cmp", attribution.Resolve("missing4", ""))
	require.Equal(t, "fail_cmp", attribution.Resolve("missing5", ""))
	require.EqualValues(t, 2, attempts.Load(), "a new lookup after release tries it once more")
	attribution.Release()
}
