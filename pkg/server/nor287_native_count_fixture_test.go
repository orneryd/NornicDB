package server

// NOR-287 native count fault fixture.
//
// Shared by the permanent regression tests for native count error
// propagation:
//
//	GET /admin/stats             -> handleAdminStats            (server_admin.go)
//	GET /db/{database}           -> native branch of handleDatabaseInfo (server_db.go)
//	GET /status                  -> readableDatabaseCounts (server_public.go)
//	GET /metrics (non-admin)     -> readableDatabaseCounts renderer scope (server_public.go)
//	heimdallDBRouter.Stats       -> error-returning router contract (server_heimdall.go)
//
// Every case counts real nodes and edges through a real server route. The only
// injected element is the repository storage read-fault fixture
// (internal/testfixture/storagefault) layered under the production
// multidb.NewDatabaseManager construction seam through
// nor287FaultedDatabaseManager; the failing operation is a real native count
// read on a real engine, not a fabricated storage value.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
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

// nor287DecodeJSON decodes a route response into a JSON object.
func nor287DecodeJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload), string(body))
	return payload
}

// nor287RequireNoInternalDetail asserts that an error answer reports the failed
// count without the internal storage cause: the HTTP surface must keep
// internal I/O details out of the response, and healthy answers are unaffected.
func nor287RequireNoInternalDetail(t *testing.T, code int, body []byte) {
	t.Helper()
	if code >= http.StatusOK && code < http.StatusMultipleChoices {
		return
	}
	require.NotContainsf(t, string(body), errNOR287CountReadFailure.Error(),
		"HTTP %d must not expose the internal count-read cause: %s", code, body)
}
