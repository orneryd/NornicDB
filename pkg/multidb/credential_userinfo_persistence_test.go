package multidb

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialUsernameOnlyCatalogPersistence(t *testing.T) {
	const username = "synthetic/token-user-871429"
	for _, form := range []string{"absent", "empty", "legacy-normalized"} {
		t.Run(form, func(t *testing.T) {
			dir := t.TempDir()
			// Registered constructors retain the fixture for the whole subtest,
			// including the explicit close/reopen assertions below.
			engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			cfg := DefaultConfig()
			cfg.RemoteCredentialEncryptionKey = "synthetic-userinfo-owner-key-317"
			cfg.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
				require.Equal(t, username, ref.User)
				require.Empty(t, ref.Password)
				// The manager closes only its inner store at remote close, so the
				// factory return's lifetime belongs to this test.
				return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
			}
			mgr, err := NewDatabaseManager(t.Context(), engine, cfg)
			require.NoError(t, err)
			endpoint := &url.URL{Scheme: "http", Host: "example.invalid", User: url.User(username)}
			if form == "empty" {
				endpoint.User = url.UserPassword(username, "")
			}
			ref := ConstituentRef{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read", URI: endpoint.String()}
			if form == "legacy-normalized" {
				ref.URI = "http://example.invalid"
				ref.User = username
				mgr.databases["probe"] = &DatabaseInfo{Name: "probe", Type: "composite", Status: "online", Constituents: []ConstituentRef{ref}}
				system := storage.NewNamespacedEngine(engine, "system")
				node, err := system.GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, err)
				data, err := json.Marshal(mgr.databases)
				require.NoError(t, err)
				node.Properties["data"] = string(data)
				require.NoError(t, system.UpdateNode(storage.NodeUpdateRequest{After: node}))
			} else {
				require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "probe", []ConstituentRef{ref}))
			}
			require.NoError(t, mgr.Close())
			if form == "legacy-normalized" {
				markClosedASCIICatalogFixtureV3(t, dir)
			}
			for reopen := 0; reopen < 2; reopen++ {
				if form == "legacy-normalized" && reopen == 0 {
					engine, err = openCatalogUpgradeFixture(t, dir, cfg)
				} else {
					engine, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				}
				require.NoError(t, err)
				mgr, err = NewDatabaseManager(t.Context(), engine, cfg)
				require.NoError(t, err)
				node, err := storage.NewNamespacedEngine(engine, "system").GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, err)
				if strings.Contains(node.Properties["data"].(string), username) || strings.Contains(node.Properties["data"].(string), url.PathEscape(username)) {
					t.Error("URL username credential copied into plaintext catalog user field; capture withheld")
				}
				_, err = mgr.GetStorage("probe.r")
				require.NoError(t, err)
				require.NoError(t, mgr.Close())
			}
		})
	}
}
