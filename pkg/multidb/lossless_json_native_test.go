package multidb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/jsontext"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLosslessCatalogSchemaNativeReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *storage.BadgerEngine {
		engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
		require.NoError(t, err)
		return fixtureowner.Own(t, engine)
	}
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	engine := open()
	manager, err := NewDatabaseManager(context.Background(), engine, nil)
	require.NoError(t, err)
	defer func(owner *DatabaseManager) { require.NoError(t, owner.Close()) }(manager)
	for _, name := range []string{"ordinary_café", `literal\ud800`, "catalog_" + high, "catalog_" + low} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
	}
	for _, name := range []string{"unit_left", "unit_right"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
	}
	aliasTargets := map[string]string{"Route_" + high: "unit_left", "Route_" + low: "unit_right"}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "unit_routes", []ConstituentRef{
		{Alias: "Route_" + high, DatabaseName: "unit_left", Type: "local", AccessMode: "read_write"},
		{Alias: "Route_" + low, DatabaseName: "unit_right", Type: "local", AccessMode: "read_write"},
	}))
	checkAliases := func() {
		refs, err := manager.GetCompositeConstituents("unit_routes")
		require.NoError(t, err)
		require.Len(t, refs, 2, "distinct UTF16 units must not collide during case normalization")
		for _, ref := range refs {
			require.Equal(t, aliasTargets[ref.Alias], ref.DatabaseName)
		}
		for alias, target := range aliasTargets {
			actual, err := manager.ResolveConnectionLimitOwner("unit_routes." + alias)
			require.NoError(t, err)
			require.Equal(t, target, actual, "alias bytes %x", alias)
		}
	}
	checkAliases()
	ns := storage.NewNamespacedEngine(engine, "nornic")
	node := &storage.Node{ID: storage.NodeID("identity_" + high), Labels: []string{"Label" + high}, Properties: map[string]any{high: high, low: low, `literal\ud800`: `literal\ud800`}}
	id, err := ns.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, engine.GetSchemaForNamespace("nornic").AddPropertyIndex("index_"+high, "Label"+high, []string{high}))
	require.NoError(t, manager.Close())
	snapshot := func() map[string][]byte {
		native, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
		require.NoError(t, err)
		values := map[string][]byte{}
		err = native.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				values[string(item.KeyCopy(nil))] = value
			}
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, native.Close())
		return values
	}
	before := snapshot()
	engine = open()
	manager, err = NewDatabaseManager(context.Background(), engine, nil)
	require.NoError(t, err)
	defer func(owner *DatabaseManager) { require.NoError(t, owner.Close()) }(manager)
	for _, name := range []string{"ordinary_café", `literal\ud800`, "catalog_" + high, "catalog_" + low} {
		require.True(t, manager.Exists(name), "identity bytes %x", name)
	}
	checkAliases()
	got, err := storage.NewNamespacedEngine(engine, "nornic").GetNode(id)
	require.NoError(t, err)
	require.Equal(t, node.Labels, got.Labels)
	require.Equal(t, node.Properties, got.Properties)
	_, found := engine.GetSchemaForNamespace("nornic").GetPropertyIndex("Label"+high, high)
	require.True(t, found)
	// Existing ordinary names retain standard JSON meaning alongside new units.
	metadata, err := storage.NewNamespacedEngine(engine, "system").GetNode(metadataNodeID)
	require.NoError(t, err)
	var oldReader map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(metadata.Properties["data"].(string)), &oldReader))
	require.Contains(t, oldReader, "ordinary_café")
	require.Contains(t, oldReader, `literal\ud800`)
	require.NoError(t, manager.Close())
	after := snapshot()
	for key, value := range before {
		require.True(t, bytes.Equal(value, after[key]), "startup must not rewrite logical key %x (before %x, after %x)", key, sha256.Sum256(value), sha256.Sum256(after[key]))
	}
	require.Len(t, after, len(before), "read/reopen adds no hidden identity records")
}

func TestCompleteCatalogStartupGuardPreservesInitialization(t *testing.T) {
	for _, missing := range []string{"server-id", "server-created-at", "database-id", "default-database", "system-database"} {
		t.Run(missing, func(t *testing.T) {
			inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			first, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = first.Close() })
			system := storage.NewNamespacedEngine(inner, "system")
			metadata, err := system.GetNode(metadataNodeID)
			require.NoError(t, err)
			var records map[string]*DatabaseInfo
			require.NoError(t, jsontext.Unmarshal([]byte(metadata.Properties["data"].(string)), &records))
			switch missing {
			case "server-id":
				delete(metadata.Properties, "server_id")
			case "server-created-at":
				delete(metadata.Properties, "server_created_at")
			case "database-id":
				records["nornic"].ID = ""
			case "default-database":
				delete(records, "nornic")
			case "system-database":
				delete(records, "system")
			}
			encoded, err := jsontext.Marshal(records)
			require.NoError(t, err)
			metadata.Properties["data"] = string(encoded)
			require.NoError(t, system.UpdateNode(metadata))
			restored, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = restored.Close() })
			require.True(t, restored.Exists("nornic"))
			require.True(t, restored.Exists("system"))
			persisted, err := system.GetNode(metadataNodeID)
			require.NoError(t, err)
			require.NotEmpty(t, persisted.Properties["server_id"])
			require.NotEmpty(t, persisted.Properties["server_created_at"])
			var loaded map[string]*DatabaseInfo
			require.NoError(t, jsontext.Unmarshal([]byte(persisted.Properties["data"].(string)), &loaded))
			for _, info := range loaded {
				require.NotEmpty(t, info.ID)
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, restored.ensureSystemDatabases(canceled, true), context.Canceled)
		})
	}
}
