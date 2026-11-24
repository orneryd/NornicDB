package server

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestPreparedImportAndDatabaseStorageLayouts(t *testing.T) {
	root := t.TempDir()
	inputs := t.TempDir()
	nodes := filepath.Join(inputs, "nodes.csv")
	edges := filepath.Join(inputs, "edges.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID,name:string\na,Ada\nb,Bea\n"), 0600))
	require.NoError(t, os.WriteFile(edges, []byte(":START_ID,:END_ID\na,b\na,a\n"), 0600))
	legacy, err := storage.OpenBadgerForOfflineImport(root, "legacy")
	require.NoError(t, err)
	_, err = adminimport.ImportFull(t.Context(), legacy, adminimport.Options{
		DatabaseName: "legacy", NodeSources: []string{"Person=" + nodes},
	})
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	open := func(dataDir string) (*Server, func()) {
		config := nornicdb.DefaultConfig()
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
	require.Empty(t, storeID("legacy"))
	require.Empty(t, storeID("shared"))
	require.NotEmpty(t, storeID("dedicated"))
	require.NotEmpty(t, storeID("explicit"))
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	query("dedicated", "CREATE (:Person {name:'Separate'})")
	query("shared", "CREATE (:Person {name:'Root'})")
	require.NotEmpty(t, postStatement(t, server, "system", "ALTER DATABASE dedicated SET OPTION storage 'shared'", "").Errors)

	id, _, err := adminimport.StageFull(t.Context(), root, adminimport.Options{
		DatabaseName: "loaded", NodeSources: []string{"Person=" + nodes},
		RelSources: []string{"KNOWS=" + edges},
	})
	require.NoError(t, err)
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
	members, readErr := storage.ReadDatabaseBackupArchive(snapshot)
	require.NoError(t, snapshot.Close())
	require.NoError(t, readErr)
	require.Len(t, members, 4)
	require.NotNil(t, members[""])
	require.NotNil(t, members[storeID("explicit")])
	require.NotNil(t, members[storeID("dedicated")])
	require.NotNil(t, members[id])

	partialBackup := filepath.Join(inputs, "root-only.backup")
	require.NoError(t, server.db.Backup(t.Context(), partialBackup))
	partialDestination := filepath.Join(t.TempDir(), "incomplete")
	require.ErrorIs(t, multidb.RestoreDatabaseBackup(t.Context(), partialBackup, partialDestination), storage.ErrInvalidData)
	require.NoDirExists(t, partialDestination)

	closeServer()
	server, _ = open(root)
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	count("shared", "MATCH (n:Person) RETURN count(n)", 1)
	count("dedicated", "MATCH (n:Person) RETURN count(n)", 1)
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
	require.NoError(t, multidb.RestoreDatabaseBackup(t.Context(), backupPath, restoredRoot))
	server, _ = open(restoredRoot)
	require.Empty(t, storeID("legacy"))
	require.Empty(t, storeID("shared"))
	require.Equal(t, id, storeID("loaded"))
	require.NotEmpty(t, storeID("dedicated"))
	count("explicit", "MATCH (n) RETURN count(n)", 0)
	count("legacy", "MATCH (n:Person) RETURN count(n)", 2)
	count("shared", "MATCH (n:Person {name:'Root'}) RETURN count(n)", 1)
	count("dedicated", "MATCH (n:Person {name:'Separate'}) RETURN count(n)", 1)
	count("loaded", "MATCH (n:Person) RETURN count(n)", 3)
	resolved, err = server.dbManager.ResolveDatabase("archive_alias")
	require.NoError(t, err)
	require.Equal(t, "loaded", resolved)
	count(resolved, "MATCH (:Person {name:'Ada'})-[:KNOWS]->(:Person {name:'Bea'}) RETURN count(*)", 1)

	server, _ = open(t.TempDir())
	query("nornic", "CREATE (:RestoreSentinel)")
	restore := makeRequest(t, server, http.MethodPost, "/admin/restore", map[string]string{"path": backupPath}, "")
	require.Equal(t, http.StatusConflict, restore.Code, restore.Body.String())
	count("nornic", "MATCH (n:RestoreSentinel) RETURN count(n)", 1)
}
