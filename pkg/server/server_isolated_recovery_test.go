package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedPreparedDatabasePublication(t *testing.T) {
	var databaseConfig *nornicdb.Config
	server, users := setupTestServerWithDatabaseConfig(t, func(cfg *nornicdb.Config) { databaseConfig = cfg }, nil)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	router := server.buildRouter()
	nodes := filepath.Join(t.TempDir(), "nodes.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID\na\n"), 0600))
	id, _, err := adminimport.StageFull(t.Context(), databaseConfig.Database.DataDir, adminimport.Options{
		DatabaseName: "prepared", NodeSources: []string{"Item=" + nodes},
	})
	require.NoError(t, err)
	for _, actor := range []struct {
		name string
		code int
	}{{"reader", http.StatusForbidden}, {"admin", http.StatusOK}} {
		body := []byte(`{"database":"prepared","import_id":"` + id + `"}`)
		req := httptest.NewRequest(http.MethodPost, "/admin/databases/imports/publish", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+getAuthToken(t, users, actor.name))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		require.Equal(t, actor.code, out.Code, out.Body.String())
		require.Equal(t, actor.code == http.StatusOK, server.dbManager.Exists("prepared"))
	}
}

func TestIsolatedStoreCorruptionRecovery(t *testing.T) {
	t.Run("corrupt manifest", func(t *testing.T) { checkIsolatedStoreRecovery(t, false, false, false) })
	t.Run("missing manifest", func(t *testing.T) { checkIsolatedStoreRecovery(t, true, false, false) })
	t.Run("encrypted store", func(t *testing.T) { checkIsolatedStoreRecovery(t, false, true, false) })
	t.Run("experimental encrypted store", func(t *testing.T) { checkIsolatedStoreRecovery(t, false, true, true) })
}

func TestCreateDatabaseExperimentalLayout(t *testing.T) {
	server, users := setupTestServerWithDatabaseConfig(t, nil, nil)
	token := "Bearer " + getAuthToken(t, users, "admin")
	for _, statement := range []string{
		"CREATE DATABASE trial OPTIONS {layout: 'experimental'}",
		"CREATE DATABASE ordinary OPTIONS {layout: 'standard', storage: 'shared'}",
		"CREATE DATABASE implicit",
	} {
		require.Empty(t, postStatement(t, server, "system", statement, token).Errors)
	}
	options, err := server.dbManager.DatabaseOptions("trial")
	require.NoError(t, err)
	require.Equal(t, multidb.LayoutExperimental, options[multidb.OptionPhysicalLayout])
	require.Equal(t, multidb.StorageIsolated, options[multidb.OptionStorageLayout])
	options, err = server.dbManager.DatabaseOptions("ordinary")
	require.NoError(t, err)
	require.Equal(t, multidb.LayoutStandard, options[multidb.OptionPhysicalLayout])
	options, err = server.dbManager.DatabaseOptions("implicit")
	require.NoError(t, err)
	require.Equal(t, multidb.StorageIsolated, options[multidb.OptionStorageLayout])
	for _, statement := range []string{
		"CREATE DATABASE badlayout OPTIONS {layout: 'unknown'}",
		"CREATE DATABASE badlayout OPTIONS {layout: 'experimental', storage: 'shared'}",
		"ALTER DATABASE trial SET OPTION layout 'standard'",
		"ALTER DATABASE trial REMOVE OPTION layout",
	} {
		require.NotEmpty(t, postStatement(t, server, "system", statement, token).Errors)
	}
	require.False(t, server.dbManager.Exists("badlayout"))
}

func checkIsolatedStoreRecovery(t *testing.T, missingManifest, encrypted, experimental bool) {
	t.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", "true")
	root := t.TempDir()
	settings := func() *nornicdb.Config {
		cfg := nornicdb.DefaultConfig()
		cfg.Auth.Enabled = false
		cfg.Database.AsyncWritesEnabled = false
		cfg.Database.WALAutoCompactionEnabled = false
		cfg.Memory.DecayEnabled, cfg.Memory.AutoLinksEnabled, cfg.Memory.EmbeddingEnabled = false, false, false
		cfg.Database.EncryptionEnabled = encrypted
		cfg.Database.EncryptionPassword = "synthetic recovery test password"
		return cfg
	}
	open := func() (*Server, func()) {
		cfg := settings()
		db, err := nornicdb.Open(root, cfg)
		require.NoError(t, err)
		serverCfg := DefaultConfig()
		serverCfg.ProcessConfig, serverCfg.EmbeddingEnabled, serverCfg.MCPEnabled = cfg, false, false
		server, err := New(db, nil, serverCfg)
		if err != nil {
			_ = db.Close()
			require.NoError(t, err)
		}
		closed := false
		closeDB := func() {
			if !closed {
				closed = true
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, server.Stop(ctx))
				require.NoError(t, db.Close())
			}
		}
		t.Cleanup(closeDB)
		return server, closeDB
	}
	server, closeDB := open()
	key, err := nornicdb.ReadStorageEncryptionKey(root, settings())
	require.NoError(t, err)
	createDatabase := "CREATE DATABASE recoverable"
	recoveryNode := "CREATE (:Recovery {value: 1})"
	recoveryPayload := ""
	if experimental {
		createDatabase += " OPTIONS {layout: 'experimental'}"
		recoveryPayload = strings.Repeat("recovery payload ", 64)
		recoveryNode = "CREATE (:Recovery {value: 1, payload: '" + recoveryPayload + "'})"
	}
	for db, statement := range map[string]string{
		"system": createDatabase,
		"nornic": "CREATE (:Healthy {value: 1})",
	} {
		require.Empty(t, postStatement(t, server, db, statement, "").Errors)
	}
	require.Empty(t, postStatement(t, server, "recoverable", recoveryNode, "").Errors)
	if experimental {
		options, err := server.dbManager.DatabaseOptions("recoverable")
		require.NoError(t, err)
		require.Equal(t, multidb.LayoutExperimental, options[multidb.OptionPhysicalLayout])
	}
	info, err := server.dbManager.GetDatabase("recoverable")
	require.NoError(t, err)
	dir, err := storage.PreparedDatabaseDirectory(root, info.IsolatedStoreID)
	require.NoError(t, err)

	nodes := filepath.Join(t.TempDir(), "nodes.csv")
	require.NoError(t, os.WriteFile(nodes, []byte(":ID\nseed\n"), 0600))
	id, _, err := adminimport.StageFullWithKey(t.Context(), root, adminimport.Options{
		DatabaseName: "seeded", NodeSources: []string{"Seed=" + nodes},
	}, key)
	require.NoError(t, err)
	require.NoError(t, server.dbManager.PublishPreparedDatabase(t.Context(), "seeded", id))
	require.Empty(t, postStatement(t, server, "seeded", "CREATE (:Seed {value: 2})", "").Errors)
	seededDir, err := storage.PreparedDatabaseDirectory(root, id)
	require.NoError(t, err)
	closeDB()

	// Capture the first node, then append a durable WAL record after that snapshot.
	walDir := filepath.Join(dir, "wal")
	wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, EncryptionKey: key})
	require.NoError(t, err)
	native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
		storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, EncryptionKey: key, ExperimentalLayout: experimental}),
	)
	require.NoError(t, err)
	snapshots := filepath.Join(dir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshots, 0700))
	require.NoError(t, storage.SaveStreamingSnapshot(t.Context(), native, filepath.Join(snapshots, "snapshot-00000000000000000001.json"), storage.SnapshotOptions{Sequence: wal.Sequence(), EncryptionKey: key}))
	require.NoError(t, native.Close())
	require.NoError(t, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{
		Node: &storage.Node{ID: "after-snapshot", Labels: []string{"Recovery"}, Properties: map[string]any{"payload": recoveryPayload}},
	}, "recoverable"))
	require.NoError(t, wal.Close())
	damage := []byte("corrupted native manifest")
	manifest := filepath.Join(dir, "MANIFEST")
	if missingManifest {
		require.NoError(t, os.Remove(manifest))
	} else {
		require.NoError(t, os.WriteFile(manifest, damage, 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(seededDir, "MANIFEST"), damage, 0600))

	server, closeDB = open()
	checkCount := func(database, statement string, want int) {
		result := postStatement(t, server, database, statement, "")
		require.Empty(t, result.Errors)
		require.EqualValues(t, want, result.Results[0].Data[0].Row[0])
	}
	checkCount("nornic", "MATCH (n:Healthy) RETURN count(n)", 1)
	checkCount("recoverable", "MATCH (n:Recovery) RETURN count(n)", 2)
	if experimental {
		checkCount("recoverable", "MATCH (n:Recovery {payload: '"+recoveryPayload+"'}) RETURN count(n)", 2)
		options, err := server.dbManager.DatabaseOptions("recoverable")
		require.NoError(t, err)
		require.Equal(t, multidb.LayoutExperimental, options[multidb.OptionPhysicalLayout])
	}
	failed, err := server.dbManager.GetDatabase("seeded")
	require.NoError(t, err)
	require.Equal(t, "offline", failed.Status)
	_, err = server.dbManager.GetStorage("seeded")
	require.ErrorIs(t, err, multidb.ErrDatabaseOffline)
	require.ErrorIs(t, server.dbManager.SetDatabaseStatus(t.Context(), "seeded", "online"), multidb.ErrDatabaseOffline)
	unchanged, err := os.ReadFile(filepath.Join(seededDir, "MANIFEST"))
	require.NoError(t, err)
	require.Equal(t, damage, unchanged)
	preserved, err := filepath.Glob(dir + ".corrupted-*")
	require.NoError(t, err)
	require.Len(t, preserved, 1)
	_, _, err = storage.ReadPreparedDatabase(root, info.IsolatedStoreID, "recoverable")
	require.NoError(t, err)
	if encrypted {
		require.Error(t, storage.VerifyBadgerEncryptionKey(dir, nil))
		require.NoError(t, storage.VerifyBadgerEncryptionKey(dir, key))
	}
	closeDB()
	server, _ = open()
	checkCount("recoverable", "MATCH (n:Recovery) RETURN count(n)", 2)
	if experimental {
		checkCount("recoverable", "MATCH (n:Recovery {payload: '"+recoveryPayload+"'}) RETURN count(n)", 2)
	}
}
