package multidb

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Independent review of the persisted metadata boundary, including both writers.
func TestSolReview1513PersistedCredentials(t *testing.T) {
	for _, writer := range []string{"create", "add"} {
		for _, form := range []string{"explicit", "uri", "both"} {
			t.Run(writer+"/"+form, func(t *testing.T) {
				const password = "synthetic-sol1513-persist-836291"
				inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				cfg := DefaultConfig()
				cfg.RemoteCredentialEncryptionKey = "synthetic-sol1513-key-A"
				manager, err := NewDatabaseManager(t.Context(), inner, cfg)
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
					err = manager.CreateCompositeDatabase(t.Context(), "review1513", []ConstituentRef{ref})
				} else {
					err = manager.CreateCompositeDatabase(t.Context(), "review1513", nil)
					if err == nil {
						err = manager.AddConstituent(t.Context(), "review1513", ref)
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
	for _, route := range []string{"direct", "native-backup"} {
		t.Run(route, func(t *testing.T) {
			for _, form := range []string{"explicit", "uri", "both", "encrypted-password", "literal-uri-prefix"} {
				t.Run(form, func(t *testing.T) {
					dir := t.TempDir()
					// Every constructor result is registered under this subtest; the
					// explicit close/reopen assertions below stay the authoritative
					// lifetime evidence for the store they name.
					inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
					require.NoError(t, err)
					config := DefaultConfig()
					config.RemoteCredentialEncryptionKey = key
					manager, err := NewDatabaseManager(t.Context(), inner, config)
					require.NoError(t, err)
					ref := ConstituentRef{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1"}
					if form == "explicit" || form == "both" || form == "encrypted-password" {
						ref.AuthMode, ref.User, ref.Password = "user_password", "admin", password
					}
					if form == "uri" || form == "both" || form == "literal-uri-prefix" {
						ref.URI = "http://admin:" + password + "@127.0.0.1:1"
					}
					if form == "encrypted-password" {
						cipher, err := newRemoteCredentialCipher(key)
						require.NoError(t, err)
						ref.Password, err = cipher.encrypt(password)
						require.NoError(t, err)
					}
					if form == "literal-uri-prefix" {
						ref.URI += "?token=enc:v1:literal"
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
					markClosedASCIICatalogFixtureV3(t, dir)

					if route == "native-backup" {
						// Native Badger backup of the closed V3 artifact: the fixture is
						// never reopened through storage.NewBadgerEngine, which would run
						// the catalog upgrade gate and consume it before the proof.
						sourceDigest := closedCatalogFixtureDigest(t, dir)
						sourceDB, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil).WithReadOnly(true).WithMemTableSize(8 << 20).WithBlockCacheSize(8 << 20)))
						require.NoError(t, err)
						var corpus bytes.Buffer
						_, err = sourceDB.Backup(&corpus, 0)
						require.NoError(t, err)
						require.NoError(t, sourceDB.Close())
						require.Equal(t, sourceDigest, closedCatalogFixtureDigest(t, dir))
						backupPath := filepath.Join(t.TempDir(), "catalog-v3.backup")
						require.NoError(t, os.WriteFile(backupPath, corpus.Bytes(), 0o600))
						backupBytes, err := os.ReadFile(backupPath)
						require.NoError(t, err)
						backupFileDigest := sha256.Sum256(backupBytes)
						// No DatabaseManager is attached while the target restores.
						restoredDir := t.TempDir()
						restored, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(restoredDir))
						require.NoError(t, err)
						require.NoError(t, restored.Restore(backupPath))
						require.NoError(t, restored.Close())
						dir = restoredDir
						t.Cleanup(func() {
							reread, err := os.ReadFile(backupPath)
							require.NoError(t, err)
							require.Equal(t, backupFileDigest, sha256.Sum256(reread), "restore rewrote the original backup file")
						})
					}

					var originalCiphertext string
					for reopen := 0; reopen < 2; reopen++ {
						if reopen == 0 {
							inner, err = openCatalogUpgradeFixture(t, dir, config)
						} else {
							inner, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
						}
						require.NoError(t, err)
						config.RemoteEngineFactory = func(ref ConstituentRef, token string) (storage.Engine, error) {
							require.Equal(t, "admin", ref.User)
							require.Equal(t, password, ref.Password)
							require.NotContains(t, ref.URI, "@")
							// The manager's Close releases only the inner store, so the
							// factory return's lifetime belongs to this test.
							return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
						}
						manager, err = NewDatabaseManager(t.Context(), inner, config)
						require.NoError(t, err)
						stored, err := manager.GetCompositeConstituents("legacy")
						require.NoError(t, err)
						require.True(t, isEncryptedRemoteCredential(stored[0].ConnectionV1))
						require.NotContains(t, stored[0].URI, "@")
						if reopen == 0 {
							originalCiphertext = stored[0].ConnectionV1
						} else {
							require.Equal(t, originalCiphertext, stored[0].ConnectionV1)
						}
						node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
						require.NoError(t, err)
						require.NotContains(t, node.Properties["data"], password)
						_, err = manager.GetStorage("legacy.r")
						require.NoError(t, err)
						require.NoError(t, manager.Close())
					}
					inner, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
					require.NoError(t, err)
					config.RemoteCredentialEncryptionKey = "synthetic-wrong-key-738192"
					manager, err = NewDatabaseManager(t.Context(), inner, config)
					require.NoError(t, err)
					_, err = manager.GetStorage("legacy.r")
					require.Error(t, err)
					require.NotContains(t, err.Error(), password)
					require.NoError(t, manager.Close())
				})
			}
		})
	}
}

func TestRemoteCredentialPersistenceEveryWriter(t *testing.T) {
	config := DefaultConfig()
	config.RemoteCredentialEncryptionKey = "synthetic-writer-key-617391"
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, config)
	require.NoError(t, err)
	defer manager.Close()
	const password = "synthetic-writer-password-619382"
	// An unrelated metadata write must never perform an implicit upgrade.
	manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{{Alias: "r", Type: "remote", URI: "http://admin:" + password + "@127.0.0.1:1"}}}
	require.NoError(t, manager.SetDatabaseStatus(t.Context(), "legacy", "offline"))
	node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
	require.NoError(t, err)
	require.Contains(t, node.Properties["data"], password)
	require.Equal(t, "http://admin:"+password+"@127.0.0.1:1", manager.databases["legacy"].Constituents[0].URI)
	require.Empty(t, manager.databases["legacy"].Constituents[0].ConnectionV1)
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
				return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
			}
			manager, _, err := newCredentialPersistenceFixture(t, config)
			require.NoError(t, err)
			defer manager.Close()
			require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "input", []ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: test.uri, AuthMode: test.mode, User: test.user, Password: test.password}}))
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
			inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			config := DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-legacy-key-319827"
			manager, err := NewDatabaseManager(t.Context(), inner, config)
			require.NoError(t, err)
			manager.databases["legacy"] = &DatabaseInfo{Name: "legacy", Type: "composite", Status: "online", Constituents: []ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://admin:" + password + "@127.0.0.1:1"}}}
			node, err := storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			data, err := json.Marshal(manager.databases)
			require.NoError(t, err)
			node.Properties["data"] = string(data)
			require.NoError(t, storage.NewNamespacedEngine(inner, "system").UpdateNode(node))
			require.NoError(t, manager.Close())
			inner, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			base := &leaderAwareEngine{opaqueEntityEngine: &opaqueEntityEngine{Engine: inner}, leader: failure != "read-only"}
			config.RemoteEngineFactory = func(ref ConstituentRef, _ string) (storage.Engine, error) {
				require.Equal(t, "admin", ref.User)
				require.Equal(t, password, ref.Password)
				require.NotContains(t, ref.URI, "@")
				// The manager's Close releases only the inner store, so the
				// factory return's lifetime belongs to this test.
				return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
			}
			if failure == "missing-key" {
				config.RemoteCredentialEncryptionKey = ""
			}
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			manager, err = NewDatabaseManager(t.Context(), base, config)
			require.NoError(t, err)
			_, err = manager.GetStorage("legacy.r")
			require.NoError(t, err)
			require.NotContains(t, logs.String(), "reopen on writable storage to migrate")
			require.NotContains(t, logs.String(), password)
			if failure == "read-only" {
				require.EqualValues(t, 0, base.updateCalls.Load())
			}
			node, err = storage.NewNamespacedEngine(inner, "system").GetNode(storage.NodeID(metadataNodeID))
			require.NoError(t, err)
			require.Contains(t, node.Properties["data"], password)
			// Only an explicit native upgrade transforms this constructed V3 catalog.
			require.NoError(t, manager.Close())
			markClosedASCIICatalogFixtureV3(t, dir)
			if failure == "missing-key" {
				before := closedCatalogFixtureDigest(t, dir)
				missingCfg := *config
				missingCfg.RemoteCredentialEncryptionKey = ""
				rejected, openErr := openCatalogUpgradeFixture(t, dir, &missingCfg)
				require.Error(t, openErr)
				require.Nil(t, rejected)
				require.Equal(t, before, closedCatalogFixtureDigest(t, dir), "missing-key upgrade changed the V3 fixture")
			}
			config.RemoteCredentialEncryptionKey = "synthetic-legacy-key-319827"
			inner, err = openCatalogUpgradeFixture(t, dir, config)
			require.NoError(t, err)
			base = &leaderAwareEngine{opaqueEntityEngine: &opaqueEntityEngine{Engine: inner}, leader: true}
			config.RemoteCredentialEncryptionKey = "synthetic-legacy-key-319827"
			logs.Reset()
			manager, err = NewDatabaseManager(t.Context(), base, config)
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
				manager, inner, err := newCredentialPersistenceFixture(t, DefaultConfig())
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
					err = manager.CreateCompositeDatabase(t.Context(), "new", []ConstituentRef{ref})
				} else {
					require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "new", nil))
					err = manager.AddConstituent(t.Context(), "new", ref)
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

func newCredentialPersistenceFixture(t *testing.T, cfg *Config) (*DatabaseManager, *storage.MemoryEngine, error) {
	t.Helper()
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, cfg)
	return manager, inner, err
}
