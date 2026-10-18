package multidb

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Independent review of the persisted metadata boundary, including both writers.
func TestSolReview1513PersistedCredentials(t *testing.T) {
	for _, writer := range []string{"create", "add"} {
		for _, form := range []string{"explicit", "uri", "both"} {
			t.Run(writer+"/"+form, func(t *testing.T) {
				const password = "synthetic-sol1513-persist-836291"
				inner, err := storage.NewBadgerEngine(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				cfg := DefaultConfig()
				cfg.RemoteCredentialEncryptionKey = "synthetic-sol1513-key-A"
				manager, err := NewDatabaseManager(inner, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer manager.Close()
				ref := ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"}
				if form != "uri" {
					ref.AuthMode, ref.User, ref.Password = "user_password", "admin", password
				}
				if form != "explicit" {
					ref.URI = "http://admin:" + password + "@127.0.0.1:1"
				}
				if writer == "create" {
					err = manager.CreateCompositeDatabase("review1513", []ConstituentRef{ref})
				} else {
					err = manager.CreateCompositeDatabase("review1513", nil)
					if err == nil {
						err = manager.AddConstituent("review1513", ref)
					}
				}
				if err != nil {
					t.Fatal("credential setup failed")
				}
				node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
				if err != nil {
					t.Fatal(err)
				}
				data, _ := node.Properties["data"].(string)
				plain := strings.Contains(data, password)
				t.Logf("persisted plaintext=%t cipher marker=%t", plain, strings.Contains(data, remoteCredentialPrefix))
				if plain {
					t.Error("persisted system metadata contains the synthetic password; value withheld")
				}
				if !strings.Contains(data, remoteCredentialPrefix) {
					t.Error("persisted credential is not encrypted")
				}
			})
		}
	}
}

func TestRemoteCredentialPersistenceMigration(t *testing.T) {
	const password = "synthetic-migration-password-729104"
	const key = "synthetic-migration-key-632719"
	for _, form := range []string{"explicit", "uri", "both"} {
		t.Run(form, func(t *testing.T) {
			dir := t.TempDir()
			inner, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			config := DefaultConfig()
			config.RemoteCredentialEncryptionKey = key
			manager, err := NewDatabaseManager(inner, config)
			require.NoError(t, err)
			ref := ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"}
			if form != "uri" {
				ref.AuthMode, ref.User, ref.Password = "user_password", "admin", password
			}
			if form != "explicit" {
				ref.URI = "http://admin:" + password + "@127.0.0.1:1"
			}
			// Seed an actual legacy metadata node, bypassing the corrected writer.
			manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{ref}}
			system := storage.NewNamespacedEngine(inner, "system")
			node, err := system.GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			legacy, err := json.Marshal(manager.databases)
			require.NoError(t, err)
			node.Properties["data"] = string(legacy)
			require.NoError(t, system.UpdateNode(node))
			require.NoError(t, manager.Close())

			var originalCiphertext string
			for reopen := 0; reopen < 2; reopen++ {
				inner, err = storage.NewBadgerEngine(dir)
				require.NoError(t, err)
				config.RemoteEngineFactory = func(ref ConstituentRef, token string) (storage.Engine, error) {
					require.Equal(t, "admin", ref.User)
					require.Equal(t, password, ref.Password)
					require.NotContains(t, ref.URI, "@")
					return storage.NewMemoryEngine(), nil
				}
				manager, err = NewDatabaseManager(inner, config)
				require.NoError(t, err)
				stored, err := manager.GetCompositeConstituents("legacy")
				require.NoError(t, err)
				require.True(t, isEncryptedRemoteCredential(stored[0].Password))
				require.NotContains(t, stored[0].URI, "@")
				if reopen == 0 {
					originalCiphertext = stored[0].Password
				} else {
					require.Equal(t, originalCiphertext, stored[0].Password)
				}
				node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, err)
				require.NotContains(t, node.Properties["data"], password)
				_, err = manager.GetStorage("legacy.r")
				require.NoError(t, err)
				require.NoError(t, manager.Close())
			}
			inner, err = storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			config.RemoteCredentialEncryptionKey = "synthetic-wrong-key-738192"
			manager, err = NewDatabaseManager(inner, config)
			require.NoError(t, err)
			_, err = manager.GetStorage("legacy.r")
			require.Error(t, err)
			require.NotContains(t, err.Error(), password)
			require.NoError(t, manager.Close())
		})
	}
}

func TestRemoteCredentialPersistenceEveryWriter(t *testing.T) {
	config := DefaultConfig()
	config.RemoteCredentialEncryptionKey = "synthetic-writer-key-617391"
	inner := storage.NewMemoryEngine()
	manager, err := NewDatabaseManager(inner, config)
	require.NoError(t, err)
	defer manager.Close()
	const password = "synthetic-writer-password-619382"
	// A non-credential metadata update must normalize every existing record too.
	manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{{Alias: "r", Type: "remote", URI: "http://admin:" + password + "@127.0.0.1:1"}}}
	require.NoError(t, manager.SetDatabaseStatus("legacy", "offline"))
	node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	require.NotContains(t, node.Properties["data"], password)
	require.NotContains(t, manager.databases["legacy"].Constituents[0].URI, "@")
}

func TestRemoteCredentialPersistenceInput(t *testing.T) {
	for _, test := range []struct{ name, uri, mode, user, password, want string }{
		{"explicit-cipher-prefix", "http://127.0.0.1:1", "user_password", "admin", "enc:v1:synthetic-plaintext", "enc:v1:synthetic-plaintext"},
		{"uri-empty-password", "http://admin@127.0.0.1:1", "", "", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-input-key-793128"
			config.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
				require.Equal(t, test.want, ref.Password)
				return storage.NewMemoryEngine(), nil
			}
			manager, err := NewDatabaseManager(storage.NewMemoryEngine(), config)
			require.NoError(t, err)
			defer manager.Close()
			require.NoError(t, manager.CreateCompositeDatabase("input", []ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: test.uri, AuthMode: test.mode, User: test.user, Password: test.password}}))
			_, err = manager.GetStorage("input.r")
			require.NoError(t, err)
		})
	}
}

func TestRemoteCredentialPersistenceUpgrade(t *testing.T) {
	const password = "synthetic-legacy-password-783192"
	for _, failure := range []string{"missing-key", "read-only"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			inner, err := storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = inner.Close() })
			config := DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-legacy-key-319827"
			manager, err := NewDatabaseManager(inner, config)
			require.NoError(t, err)
			manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://admin:" + password + "@127.0.0.1:1"}}}
			node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			data, err := json.Marshal(manager.databases)
			require.NoError(t, err)
			node.Properties["data"] = string(data)
			require.NoError(t, storage.NewNamespacedEngine(inner, "system").UpdateNode(node))
			require.NoError(t, manager.Close())
			inner, err = storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			base := &leaderAwareEngine{Engine: inner, leader: failure != "read-only"}
			config.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
				require.Equal(t, "admin", ref.User)
				require.Equal(t, password, ref.Password)
				require.NotContains(t, ref.URI, "@")
				return storage.NewMemoryEngine(), nil
			}
			if failure == "missing-key" {
				config.RemoteCredentialEncryptionKey = ""
			}
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			manager, err = NewDatabaseManager(base, config)
			require.NoError(t, err)
			_, err = manager.GetStorage("legacy.r")
			require.NoError(t, err)
			require.Equal(t, 1, strings.Count(logs.String(), "legacy remote credentials remain unencrypted"))
			require.Contains(t, logs.String(), "NORNICDB_REMOTE_CREDENTIALS_KEY")
			require.NotContains(t, logs.String(), password)
			if failure == "read-only" {
				require.EqualValues(t, 0, base.updateCalls.Load())
			}
			node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			require.Contains(t, node.Properties["data"], password)
			// Later writable open with a key upgrades the same stored catalog.
			require.NoError(t, manager.Close())
			inner, err = storage.NewBadgerEngine(dir)
			require.NoError(t, err)
			base = &leaderAwareEngine{Engine: inner, leader: true}
			config.RemoteCredentialEncryptionKey = "synthetic-legacy-key-319827"
			logs.Reset()
			manager, err = NewDatabaseManager(base, config)
			require.NoError(t, err)
			_, err = manager.GetStorage("legacy.r")
			require.NoError(t, err)
			node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			require.NotContains(t, node.Properties["data"], password)
			require.NotContains(t, logs.String(), "legacy remote credentials remain unencrypted")
			require.NoError(t, manager.Close())
		})
	}
}

func TestRemoteCredentialPersistenceNewInputRequiresKey(t *testing.T) {
	for _, writer := range []string{"create", "add"} {
		var explicitError string
		for _, form := range []string{"explicit", "uri", "both"} {
			t.Run(writer+"/"+form, func(t *testing.T) {
				inner := storage.NewMemoryEngine()
				manager, err := NewDatabaseManager(inner, DefaultConfig())
				require.NoError(t, err)
				defer manager.Close()
				ref := ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"}
				if form != "uri" {
					ref.AuthMode, ref.User, ref.Password = "user_password", "admin", "synthetic-new-password-813792"
				}
				if form != "explicit" {
					ref.URI = "http://admin:synthetic-new-password-813792@127.0.0.1:1"
				}
				if writer == "create" {
					err = manager.CreateCompositeDatabase("new", []ConstituentRef{ref})
				} else {
					require.NoError(t, manager.CreateCompositeDatabase("new", nil))
					err = manager.AddConstituent("new", ref)
				}
				require.Error(t, err)
				if form == "explicit" {
					explicitError = err.Error()
				} else {
					require.Equal(t, explicitError, err.Error())
				}
				node, readErr := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
				require.NoError(t, readErr)
				require.NotContains(t, node.Properties["data"], "synthetic-new-password-813792")
			})
		}
	}
}
