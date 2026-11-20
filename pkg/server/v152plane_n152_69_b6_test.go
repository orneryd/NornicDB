package server

// NOR-287 remaining consumer audit: externally reported native count sites.
//
// Scope of this file (no production change):
//   - GET /admin/stats        -> handleAdminStats (server_admin.go:42-43)
//   - GET /db/{database}      -> handleDatabaseInfo native branch (server_db.go:412-419)
//   - GET /status (fallback)  -> readableDatabaseCounts (server_public.go:287-291)
//
// Every case counts real nodes through a real server route. The only injected
// element is the repository storage read-fault fixture
// (internal/testfixture/storagefault) layered under the production
// multidb.NewDatabaseManager construction seam; the failing operation is a real
// native count read on a real engine, not a fabricated storage value.
//
// These tests record what an operator or client can observe today. Where the
// approved contract documents a failure marker (the composite statsPartial
// provenance, covered in nor287_composite_count_partial_audit_test.go) the test
// asserts it. The native assertions below pin the current bad observable
// answer (a silent zero/undercount); the separate *_error_acceptance_test.go
// files assert the original ticket requirement that failed reads remain
// distinguishable from valid empty results without demanding a new field.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// errNOR287CountReadFailure is the injected native count-read error.
var errNOR287CountReadFailure = errors.New("NOR-287 injected native count read failure")

// nor287FaultedDatabaseManager replaces server.dbManager with a production
// multidb.DatabaseManager whose base engine is the server's real base storage
// decorated by the repository storage-fault fixture. Writes stay real; counts
// fail only while the returned setter selects a count operation.
//
// This is DI at the manager seam Server.New uses
// (multidb.NewDatabaseManager(db.GetBaseStorageForManager(), ...)), so the
// databases below are built and counted through the same owner a configured
// server uses.
func nor287FaultedDatabaseManager(t *testing.T, server *Server) (*multidb.DatabaseManager, func(storagefault.ReadOperation)) {
	t.Helper()

	base := server.db.GetBaseStorageForManager()
	require.NotNil(t, base)
	readEngine, ok := base.(storagefault.ReadEngine)
	require.True(t, ok, "server base storage must expose streaming and prefix count capabilities")
	stored, ok := storage.StoredReader(base)
	require.True(t, ok, "server base storage must expose the stored entity reader")

	faults := storagefault.NewReadFailureEngine(readEngine, stored, errNOR287CountReadFailure)
	manager, err := multidb.NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)

	previous := server.dbManager
	server.dbManager = manager
	t.Cleanup(func() {
		server.dbManager = previous
		stopTestServer(t, server)
		// The manager owns the fault decorator, which owns the shared base:
		// closing it here is the same terminal close the swapped manager would
		// perform on shutdown. The server and database cleanup ignore the
		// resulting already-closed error.
		_ = manager.Close()
	})
	return manager, faults.SetFailure
}

// nor287CreateDatabaseWithNodes creates name in the manager and stores nodeCount
// real nodes in it.
func nor287CreateDatabaseWithNodes(t *testing.T, manager *multidb.DatabaseManager, name string, nodeCount int) {
	t.Helper()
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
	engine, err := manager.GetStorage(name)
	require.NoError(t, err)
	for i := 0; i < nodeCount; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("%s-n%d", name, i)),
			Labels:     []string{"NOR287"},
			Properties: map[string]any{"index": int64(i)},
		}
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
}

// nor287Counts creates the two-database fixture used by the native consumers:
// 3 nodes total, all stored and counted through the manager.
func nor287Counts(t *testing.T, manager *multidb.DatabaseManager) {
	t.Helper()
	nor287CreateDatabaseWithNodes(t, manager, "nor287_counts_a", 2)
	nor287CreateDatabaseWithNodes(t, manager, "nor287_counts_b", 1)
}

// nor287DecodeJSON decodes a route response into a JSON object.
func nor287DecodeJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload), string(body))
	return payload
}

// TestNOR287AdminStatsNativeCountFailureIsSilent audits GET /admin/stats.
//
// server_admin.go:42-43 discards both native count errors and publishes the
// zero value in node_count/edge_count (and in per_database). The approved
// endpoints catalog (docs/api-reference/openapi.yaml /admin/stats) documents
// node_count/edge_count integers and no failure field, so this test records the
// current externally visible answer for both healthy controls and a failed
// native read.
func TestNOR287AdminStatsNativeCountFailureIsSilent(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	databaseBlock := func(t *testing.T) map[string]any {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/admin/stats", nil, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		payload := nor287DecodeJSON(t, response.Body.Bytes())
		block, ok := payload["database"].(map[string]any)
		require.True(t, ok, "admin stats must report the database block")
		return block
	}

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		block := databaseBlock(t)
		require.Equal(t, float64(0), block["node_count"])
		require.Equal(t, float64(0), block["edge_count"])
		perDatabase, ok := block["per_database"].(map[string]any)
		require.True(t, ok)
		// The default database exists and is empty; no failure marker at all.
		require.Contains(t, perDatabase, "nornic")
		require.NotContains(t, block, "partial")
		require.NotContains(t, block, "error")
	})

	nor287Counts(t, manager)

	t.Run("healthy non-empty databases report every node", func(t *testing.T) {
		block := databaseBlock(t)
		require.Equal(t, float64(3), block["node_count"])
		perDatabase, ok := block["per_database"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, map[string]any{"node_count": float64(2), "edge_count": float64(0)}, perDatabase["nor287_counts_a"])
		require.Equal(t, map[string]any{"node_count": float64(1), "edge_count": float64(0)}, perDatabase["nor287_counts_b"])
	})

	t.Run("failed node count is published as a successful zero", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		block := databaseBlock(t)
		// Three nodes are stored; the route answers 200 with no signal that the
		// count read failed, and every database reports the zero value.
		require.Equal(t, float64(0), block["node_count"])
		perDatabase, ok := block["per_database"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, map[string]any{"node_count": float64(0), "edge_count": float64(0)}, perDatabase["nor287_counts_a"])
		require.NotContains(t, block, "partial")
		require.NotContains(t, block, "error")
		require.NotContains(t, block, "statsPartial")
	})

	t.Run("failed edge count is published as a successful zero", func(t *testing.T) {
		setFailure(storagefault.EdgeCountRead)
		block := databaseBlock(t)
		require.Equal(t, float64(0), block["edge_count"])
		require.Equal(t, float64(3), block["node_count"], "only the failed count degrades")
	})

	setFailure("")
	t.Run("counts recover when the read owner recovers", func(t *testing.T) {
		block := databaseBlock(t)
		require.Equal(t, float64(3), block["node_count"])
	})
}

// TestNOR287DatabaseInfoNativeCountFailureIsSilent audits the native branch of
// GET /db/{database} (server_db.go:412-419): both count errors are replaced with
// 0 while the response keeps status "online" and HTTP 200.
//
// The composite branch of the same route documents provenance and a
// statsPartial marker (docs/architecture/multi-db-architecture.md:183-188); the
// native response documents neither, so a failed native count is observable
// only as a smaller number.
func TestNOR287DatabaseInfoNativeCountFailureIsSilent(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	info := func(t *testing.T, name string) map[string]any {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/db/"+name, nil, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		return nor287DecodeJSON(t, response.Body.Bytes())
	}

	nor287CreateDatabaseWithNodes(t, manager, "nor287_info_empty", 0)
	nor287CreateDatabaseWithNodes(t, manager, "nor287_info_nodes", 4)

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		payload := info(t, "nor287_info_empty")
		require.Equal(t, "standard", payload["type"])
		require.Equal(t, "online", payload["status"])
		require.Equal(t, float64(0), payload["nodeCount"])
		require.Equal(t, float64(0), payload["edgeCount"])
		require.NotContains(t, payload, "error")
		require.NotContains(t, payload, "statsPartial")
	})

	t.Run("healthy non-empty database reports every node", func(t *testing.T) {
		payload := info(t, "nor287_info_nodes")
		require.Equal(t, float64(4), payload["nodeCount"])
	})

	t.Run("failed node count is published as a successful zero", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		payload := info(t, "nor287_info_nodes")
		require.Equal(t, float64(0), payload["nodeCount"])
		// The response still declares the database online and carries no
		// failure signal for the count that could not be read.
		require.Equal(t, "online", payload["status"])
		require.NotContains(t, payload, "error")
		require.NotContains(t, payload, "reachable")
		require.NotContains(t, payload, "statsPartial")
	})

	setFailure("")
	t.Run("count recovers when the read owner recovers", func(t *testing.T) {
		require.Equal(t, float64(4), info(t, "nor287_info_nodes")["nodeCount"])
	})
}

// TestNOR287StatusFallbackCountFailureIsSilent audits the primary-source
// fallback in readableDatabaseCounts (server_public.go:287-291) through its
// public consumer GET /status.
//
// The fallback runs only when the base storage provides no cached per-namespace
// prefix counters, which is the supported in-memory store mode
// (nornicdb.Open("", ...)). The databases are therefore created in a manager
// over a real Badger engine in memory, decorated by the count-read fault
// fixture; the process database stays memory-backed exactly so the prefix
// fast path stays unavailable.
//
// GET /metrics reaches the same owner for a non-administrator reader
// (server_public.go:174); the separate TestNOR287MetricsScopedCountErrorAcceptance
// actually executes the authorized non-administrator renderer, rather than
// inferring its answer from this shared helper.
func TestNOR287StatusFallbackCountFailureIsSilent(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open("", cfg)
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	dbServer, err := New(db, nil, serverConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopTestServer(t, dbServer)
		_ = db.Close()
	})

	// The in-memory process database has no ListNamespaces/prefix counters, so
	// readableDatabaseCounts takes the fallback path (server_public.go:287-291).
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	faults := storagefault.NewReadFailureEngine(base, base, errNOR287CountReadFailure)
	manager, err := multidb.NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	dbServer.dbManager = manager

	status := func(t *testing.T) map[string]any {
		t.Helper()
		response := makeRequest(t, dbServer, http.MethodGet, "/status", nil, "")
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		payload := nor287DecodeJSON(t, response.Body.Bytes())
		block, ok := payload["database"].(map[string]any)
		require.True(t, ok, "status must report the database block")
		require.Equal(t, "running", payload["status"])
		return block
	}

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		block := status(t)
		require.Equal(t, float64(0), block["nodes"])
		require.Equal(t, float64(0), block["edges"])
	})

	nor287Counts(t, manager)

	t.Run("healthy non-empty databases report every node", func(t *testing.T) {
		block := status(t)
		require.Equal(t, float64(3), block["nodes"])
	})

	t.Run("failed node count is published as a successful zero", func(t *testing.T) {
		faults.SetFailure(storagefault.NodeCountRead)
		block := status(t)
		require.Equal(t, float64(0), block["nodes"])
		// /status documents node_count/edge_count only
		// (docs/api-reference/openapi.yaml /status); there is no partial marker
		// to assert, so the observable answer is an unqualified zero.
		require.NotContains(t, block, "partial")
		require.NotContains(t, block, "error")
	})

	faults.SetFailure("")
	t.Run("count recovers when the read owner recovers", func(t *testing.T) {
		require.Equal(t, float64(3), status(t)["nodes"])
	})
}
