package cypher

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDescriptorPublicShowAndBothCatalogBuildersUseSafeViews(t *testing.T) {
	const key = "synthetic-public-descriptor-key"
	const password = "synthetic-public-descriptor-password"
	const user = "enc:v1:literal-public-user"
	plan, err := multidb.NewCatalogUpgradeV4("system", key)
	require.NoError(t, err)
	legacy, err := json.Marshal(map[string]any{"descriptor": map[string]any{"name": "descriptor", "type": "composite", "status": "online", "constituents": []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://uri-user:synthetic-uri-password@127.0.0.1:1/ordinary", AuthMode: "user_password", User: user, Password: password}}}})
	require.NoError(t, err)
	secured, _, err := plan.Transform(t.Context(), string(legacy))
	require.NoError(t, err)
	inner := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(inner, "system")
	_, err = system.CreateNode(&storage.Node{ID: "databases:metadata", Labels: []string{"_System", "_Metadata"}, Properties: map[string]any{"type": "databases", "data": secured}})
	require.NoError(t, err)
	cfg := multidb.DefaultConfig()
	cfg.RemoteCredentialEncryptionKey = key
	var resolved []multidb.ConstituentRef
	var remotes []*storage.RemoteEngine
	cfg.RemoteEngineFactory = func(ref multidb.ConstituentRef, _ string) (storage.Engine, error) {
		resolved = append(resolved, ref)
		remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password})
		if remote != nil {
			remotes = append(remotes, remote)
		}
		return remote, err
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, remote := range remotes {
			_ = remote.Close()
		}
		if err := manager.Close(); err != nil {
			t.Fatalf("close manager: %v", err)
		}
	})
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	executor := NewStorageExecutor(engine)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	result, err := executor.Execute(t.Context(), "SHOW CONSTITUENTS FOR COMPOSITE DATABASE descriptor", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, user, result.Rows[0][7])
	require.Equal(t, "user_password", result.Rows[0][6])
	uri, ok := result.Rows[0][4].(string)
	require.True(t, ok)
	require.Contains(t, uri, "127.0.0.1:1/ordinary")
	require.NotContains(t, uri, "synthetic-")
	require.Empty(t, resolved, "SHOW must not construct an execution engine")
	stored, err := manager.GetCompositeConstituents("descriptor")
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.NotEmpty(t, stored[0].ConnectionV1)
	require.Empty(t, stored[0].URI)
	first, err := executor.buildFabricCatalog()
	require.NoError(t, err)
	second := fabric.NewCatalog()
	require.NoError(t, second.PopulateFromManager(manager))
	for _, catalog := range []*fabric.Catalog{first, second} {
		location, err := catalog.Resolve("descriptor.r")
		require.NoError(t, err)
		remote, ok := location.(*fabric.LocationRemote)
		require.True(t, ok)
		require.Equal(t, stored[0].ConnectionV1, remote.CredentialRevision)
		require.Equal(t, "user_password", remote.AuthMode)
		require.NotContains(t, remote.URI, "synthetic-")
		require.Contains(t, remote.URI, "/ordinary")
		require.Empty(t, remote.Password, "opaque revision must not be stuffed into PASSWORD")
		_, err = remote.ResolveEngine("Bearer forwarded-token")
		require.NoError(t, err)
	}
	require.NotEmpty(t, resolved)
	for _, ref := range resolved {
		require.Equal(t, user, ref.User)
		require.Equal(t, password, ref.Password)
		require.Empty(t, ref.ConnectionV1)
		require.Equal(t, "http://127.0.0.1:1/ordinary", ref.URI)
	}
}
