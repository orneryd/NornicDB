package server

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestPreparedImportAndDatabaseStorageLayouts(t *testing.T) {
	for _, mode := range []string{"unencrypted", "password", "local"} {
		t.Run(mode, func(t *testing.T) { checkPreparedImportAndDatabaseStorageLayouts(t, mode) })
	}
}

func checkPreparedImportAndDatabaseStorageLayouts(t *testing.T, encryptionMode string) {
	configureEncryption := func(config *nornicdb.Config) {
		if encryptionMode == "unencrypted" {
			return
		}
		config.Database.EncryptionEnabled = true
		config.Database.EncryptionProvider = encryptionMode
		config.Database.EncryptionPassword = "synthetic encryption test password"
		config.Database.EncryptionMasterKey = "synthetic-key-for-badger-test-32"
	}
	root := t.TempDir()
	inputs := t.TempDir()
	nodes := filepath.Join(inputs, "nodes.csv")
	edges := filepath.Join(inputs, "edges.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID,name:string\na,Ada\nb,Bea\n"), 0600))
	require.NoError(t, os.WriteFile(edges, []byte(":START_ID,:END_ID\na,b\na,a\n"), 0600))
	settings := nornicdb.DefaultConfig()
	configureEncryption(settings)
	key, err := nornicdb.InitializeStorageEncryption(root, settings)
	require.NoError(t, err)
	legacy, err := storage.OpenBadgerForOfflineImport(root, "legacy", key)
	require.NoError(t, err)
	_, err = adminimport.ImportFull(t.Context(), legacy, adminimport.Options{
		DatabaseName: "legacy", NodeSources: []string{"Person=" + nodes},
	})
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	open := func(dataDir string) (*Server, func()) {
		config := nornicdb.DefaultConfig()
		configureEncryption(config)
		config.Database.MVCCLifecycleEnabled = true
		config.Database.MVCCRetentionMaxVersions = 2
		config.Auth.Enabled = false
		config.Database.AsyncWritesEnabled = false
		config.Memory.DecayEnabled = false
		config.Memory.AutoLinksEnabled = false
		config.Memory.EmbeddingEnabled = false
		db, err := nornicdb.Open(dataDir, config)
		require.NoError(t, err)
		serverConfig := DefaultConfig()
		serverConfig.ProcessConfig = config
		serverConfig.EmbeddingEnabled = false
		serverConfig.MCPEnabled = false
		server, err := New(db, nil, serverConfig)
		if err != nil {
			_ = db.Close()
			require.NoError(t, err)
		}
		closed := false
		closeServer := func() {
			if closed {
				return
			}
			closed = true
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			require.NoError(t, server.Stop(ctx))
			require.NoError(t, db.Close())
		}
		t.Cleanup(closeServer)
		return server, closeServer
	}
	server, closeServer := open(root)
	query := func(database, statement string) TransactionResponse {
		result := postStatement(t, server, database, statement, "")
		require.Empty(t, result.Errors)
		return result
	}
	storeID := func(name string) string {
		info, err := server.dbManager.GetDatabase(name)
		require.NoError(t, err)
		return info.IsolatedStoreID
	}
	count := func(database, statement string, expected int) {
		result := query(database, statement)
		require.Len(t, result.Results, 1)
		require.Len(t, result.Results[0].Data, 1)
		require.EqualValues(t, expected, result.Results[0].Data[0].Row[0])
	}

	query("system", "CREATE DATABASE legacy")
	query("system", "CREATE DATABASE shared OPTIONS {storage: 'shared'}")
	query("system", "CREATE DATABASE dedicated")
	query("system", "CREATE DATABASE explicit OPTIONS {storage: 'isolated'}")
	query("system", "CREATE DATABASE experimental OPTIONS {layout: 'experimental'}")
	require.Empty(t, storeID("legacy"))
	require.Empty(t, storeID("shared"))
	require.NotEmpty(t, storeID("dedicated"))
	require.NotEmpty(t, storeID("explicit"))
	experimentalID := storeID("experimental")
	require.NotEmpty(t, experimentalID)
	store, err := server.dbManager.GetStorage("dedicated")
	require.NoError(t, err)
	native, ok := storage.UnwrapEngine(store).(*storage.BadgerEngine)
	require.True(t, ok)
	require.Equal(t, true, native.LifecycleStatus()["enabled"])
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	query("dedicated", "CREATE (:Person {name:'Separate'})")
	query("shared", "CREATE (:Person {name:'Root'})")
	experimentalBody := strings.Repeat("experimental restore content ", 128)
	query("experimental", "CREATE (:Person {name:'Experiment', text:'"+experimentalBody+"'})")
	checkExperimental := func(want int) {
		options, err := server.dbManager.DatabaseOptions("experimental")
		require.NoError(t, err)
		require.Equal(t, multidb.LayoutExperimental, options[multidb.OptionPhysicalLayout])
		require.Equal(t, experimentalID, storeID("experimental"))
		count("experimental", "MATCH (n:Person) RETURN count(n)", want)
		result := query("experimental", "MATCH (n:Person {name:'Experiment'}) RETURN n.text")
		require.Equal(t, experimentalBody, result.Results[0].Data[0].Row[0])
	}
	checkExperimental(1)
	require.NotEmpty(t, postStatement(t, server, "system", "ALTER DATABASE dedicated SET OPTION storage 'shared'", "").Errors)

	key, err = nornicdb.ReadStorageEncryptionKey(root, settings)
	require.NoError(t, err)
	if settings.Database.EncryptionEnabled {
		_, err := nornicdb.ReadStorageEncryptionKey(root, nornicdb.DefaultConfig())
		require.Error(t, err)
	}
	id, _, err := adminimport.StageFullWithKey(t.Context(), root, adminimport.Options{
		DatabaseName: "loaded", NodeSources: []string{"Person=" + nodes},
		RelSources: []string{"KNOWS=" + edges},
	}, key)
	require.NoError(t, err)
	require.False(t, server.dbManager.Exists("loaded"))
	canceled, cancelPublish := context.WithCancel(t.Context())
	cancelPublish()
	require.ErrorIs(t, server.dbManager.PublishPreparedDatabase(canceled, "loaded", id), context.Canceled)
	require.False(t, server.dbManager.Exists("loaded"))
	count("shared", "MATCH (n:Person) RETURN count(n)", 1)
	publish := func(database string, expected int) {
		response := makeRequest(t, server, http.MethodPost, "/admin/databases/imports/publish", map[string]string{
			"database": database, "import_id": id,
		}, "")
		require.Equal(t, expected, response.Code, response.Body.String())
	}
	publish("wrong", http.StatusBadRequest)
	publish("shared", http.StatusConflict)
	publish("loaded", http.StatusOK)
	publish("loaded", http.StatusOK)
	require.Equal(t, id, storeID("loaded"))
	count("loaded", "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r)", 2)
	count("loaded", "MATCH (:Person {name:'Ada'})-[:KNOWS]->(n:Person {name:'Bea'}) RETURN count(n)", 1)
	query("loaded", "CREATE (:Person {name:'Carol'})")
	query("system", "CREATE ALIAS archive_alias FOR DATABASE loaded")
	resolved, err := server.dbManager.ResolveDatabase("archive_alias")
	require.NoError(t, err)
	require.Equal(t, "loaded", resolved)
	backupPath := filepath.Join(inputs, "mixed.backup")
	backup := makeRequest(t, server, http.MethodPost, "/admin/backup", map[string]string{"path": backupPath}, "")
	require.Equal(t, http.StatusOK, backup.Code, backup.Body.String())
	require.FileExists(t, backupPath)
	collections, err := qdrantgrpc.NewDatabaseCollectionStore(server.dbManager, nil)
	require.NoError(t, err)
	snapshots := qdrantgrpc.NewSnapshotsService(qdrantgrpc.DefaultConfig(), collections, server.db.GetBaseStorageForManager(), inputs, nil)
	full, err := snapshots.CreateFull(t.Context(), &qpb.CreateFullSnapshotRequest{})
	require.NoError(t, err)
	snapshotPath := filepath.Join(inputs, "full", full.GetSnapshotDescription().GetName())
	snapshot, err := os.Open(snapshotPath)
	require.NoError(t, err)
	archive, readErr := storage.ReadDatabaseBackupArchive(snapshot)
	require.NoError(t, snapshot.Close())
	require.NoError(t, readErr)
	require.Equal(t, settings.Database.EncryptionEnabled, archive.RequiresEncryption)
	members := archive.Stores
	require.Len(t, members, 5)
	require.NotNil(t, members[""])
	require.NotNil(t, members[storeID("explicit")])
	require.NotNil(t, members[storeID("dedicated")])
	require.NotNil(t, members[experimentalID])
	require.NotNil(t, members[id])

	partialBackup := filepath.Join(inputs, "root-only.backup")
	require.NoError(t, server.db.Backup(t.Context(), partialBackup))
	partialDestination := filepath.Join(t.TempDir(), "incomplete")
	require.ErrorIs(t, multidb.RestoreDatabaseBackup(t.Context(), partialBackup, partialDestination), storage.ErrInvalidData)
	require.NoDirExists(t, partialDestination)

	closeServer()
	experimentalDir, err := storage.PreparedDatabaseDirectory(root, experimentalID)
	require.NoError(t, err)
	manifestFile, err := os.Open(filepath.Join(experimentalDir, "MANIFEST"))
	require.NoError(t, err)
	manifest, _, err := badger.ReplayManifestFile(manifestFile, 0, badger.DefaultOptions(experimentalDir))
	require.NoError(t, manifestFile.Close())
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Tables)
	for _, table := range manifest.Tables {
		require.Equal(t, badgeroptions.NornicColumnar, table.Compression)
	}
	server, _ = open(root)
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	count("shared", "MATCH (n:Person) RETURN count(n)", 1)
	count("dedicated", "MATCH (n:Person) RETURN count(n)", 1)
	checkExperimental(1)
	count("loaded", "MATCH (n:Person) RETURN count(n)", 3)
	count("loaded", "MATCH ()-[r:KNOWS]->() RETURN count(r)", 2)
	query("system", "DROP DATABASE loaded")
	directory, err := storage.PreparedDatabaseDirectory(root, id)
	require.NoError(t, err)
	require.NoDirExists(t, directory)
	query("system", "CREATE DATABASE loaded")
	require.NotEqual(t, id, storeID("loaded"))
	count("loaded", "MATCH (n) RETURN count(n)", 0)

	restoredRoot := filepath.Join(t.TempDir(), "restored")
	canceledRestore, cancelRestore := context.WithCancel(t.Context())
	defer cancelRestore()
	require.ErrorIs(t, multidb.RestoreDatabaseBackupWithKey(canceledRestore, backupPath, restoredRoot, func(stage string) ([]byte, error) {
		key, err := nornicdb.InitializeStorageEncryption(stage, settings)
		cancelRestore()
		return key, err
	}), context.Canceled)
	require.NoDirExists(t, restoredRoot)
	leftovers, err := os.ReadDir(filepath.Dir(restoredRoot))
	require.NoError(t, err)
	require.Empty(t, leftovers, "temporary restore stores were removed")
	if settings.Database.EncryptionEnabled {
		require.ErrorContains(t, multidb.RestoreDatabaseBackup(t.Context(), backupPath, restoredRoot), "requires encryption")
		require.NoDirExists(t, restoredRoot)
	}
	require.NoError(t, multidb.RestoreDatabaseBackupWithKey(t.Context(), backupPath, restoredRoot, func(stage string) ([]byte, error) {
		return nornicdb.InitializeStorageEncryption(stage, settings)
	}))
	if settings.Database.EncryptionEnabled {
		restoredKey, err := nornicdb.ReadStorageEncryptionKey(restoredRoot, settings)
		require.NoError(t, err)
		require.NotEqual(t, key, restoredKey)
		dedicatedDir, err := storage.PreparedDatabaseDirectory(restoredRoot, id)
		require.NoError(t, err)
		require.Error(t, storage.VerifyBadgerEncryptionKey(dedicatedDir, nil))
		require.NoError(t, storage.VerifyBadgerEncryptionKey(dedicatedDir, restoredKey))
		experimentalDir, err := storage.PreparedDatabaseDirectory(restoredRoot, experimentalID)
		require.NoError(t, err)
		require.Error(t, storage.VerifyBadgerEncryptionKey(experimentalDir, nil))
		require.NoError(t, storage.VerifyBadgerEncryptionKey(experimentalDir, restoredKey))
		wrongKey := append([]byte(nil), restoredKey...)
		wrongKey[0] ^= 0xff
		require.Error(t, storage.VerifyBadgerEncryptionKey(experimentalDir, wrongKey))
	}
	server, closeServer = open(restoredRoot)
	require.Empty(t, storeID("legacy"))
	require.Empty(t, storeID("shared"))
	require.Equal(t, id, storeID("loaded"))
	require.NotEmpty(t, storeID("dedicated"))
	count("explicit", "MATCH (n) RETURN count(n)", 0)
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	count("shared", "MATCH (n:Person {name:'Root'}) RETURN count(n)", 1)
	count("dedicated", "MATCH (n:Person {name:'Separate'}) RETURN count(n)", 1)
	checkExperimental(1)
	count("loaded", "MATCH (n:Person) RETURN count(n)", 3)
	resolved, err = server.dbManager.ResolveDatabase("archive_alias")
	require.NoError(t, err)
	require.Equal(t, "loaded", resolved)
	count(resolved, "MATCH (:Person {name:'Ada'})-[:KNOWS]->(:Person {name:'Bea'}) RETURN count(*)", 1)
	query("experimental", "CREATE (:Person {name:'AfterRestore', text:'"+experimentalBody+"'})")
	closeServer()
	server, _ = open(restoredRoot)
	checkExperimental(2)

	server, _ = open(t.TempDir())
	query("nornic", "CREATE (:RestoreSentinel)")
	restore := makeRequest(t, server, http.MethodPost, "/admin/restore", map[string]string{"path": backupPath}, "")
	require.Equal(t, http.StatusConflict, restore.Code, restore.Body.String())
	count("nornic", "MATCH (n:RestoreSentinel) RETURN count(n)", 1)
}
