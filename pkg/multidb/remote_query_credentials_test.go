package multidb

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteQueryCredentialPersistence(t *testing.T) {
	const secret = "synthetic-query-credential-739261"
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "legacy"}[legacy], func(t *testing.T) {
			dir := t.TempDir()
			inner, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			cfg := DefaultConfig()
			cfg.RemoteCredentialEncryptionKey = "synthetic-query-key-173926"
			cfg.RemoteEngineFactory = func(ref ConstituentRef, token string) (storage.Engine, error) {
				endpoint, err := url.Parse(ref.URI)
				require.NoError(t, err)
				for _, key := range []string{"password", "api_key", "X-Amz-Signature"} {
					require.Equal(t, []string{secret, secret}, endpoint.Query()[key])
				}
				require.Equal(t, "keep", endpoint.Query().Get("region"))
				return storage.NewMemoryEngine(), nil
			}
			manager, err := NewDatabaseManager(inner, cfg)
			require.NoError(t, err)
			ref := ConstituentRef{Alias: "r", Type: "remote", DatabaseName: "remote", AccessMode: "read", URI: "http://example.test?region=keep&password=" + secret + "&password=" + secret + "&api_key=" + secret + "&api_key=" + secret + "&X-Amz-Signature=" + secret + "&X-Amz-Signature=" + secret}
			if legacy {
				manager.databases["probe"] = &DatabaseInfo{Name: "probe", Type: "composite", Status: "online", Constituents: []ConstituentRef{ref}}
				node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, err)
				data, err := json.Marshal(manager.databases)
				require.NoError(t, err)
				node.Properties["data"] = string(data)
				require.NoError(t, storage.NewNamespacedEngine(inner, "system").UpdateNode(node))
			} else {
				require.NoError(t, manager.CreateCompositeDatabase("probe", []ConstituentRef{ref}))
			}
			require.NoError(t, manager.Close())
			for reopen := 0; reopen < 2; reopen++ {
				inner, err = storage.NewBadgerEngine(dir)
				require.NoError(t, err)
				manager, err = NewDatabaseManager(inner, cfg)
				require.NoError(t, err)
				node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, err)
				if strings.Contains(node.Properties["data"].(string), secret) {
					t.Error("persisted query contains synthetic credential")
				}
				_, err = manager.GetStorage("probe.r")
				require.NoError(t, err)
				require.NoError(t, manager.Close())
			}
		})
	}
}
