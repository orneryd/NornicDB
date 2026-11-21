package server

// Lane L6 (closeout): G-composite-get-constituent, remaining count consumers of
// P-N152-69: assistant stats (server_heimdall.go:335-336), metrics
// (heimdall/metrics.go:381-382 via /metrics) and the /status fallback
// (server_public.go:249-256, :287-292). Same fault fixture as the NOR-287
// acceptance tests; each case logs the count call's own error next to the answer.

import (
	"net/http"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestL6CountConsumersHeimdallMetrics(t *testing.T) {
	server, authenticator := setupTestServer(t)
	readerToken := "Bearer " + getAuthToken(t, authenticator, "reader")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)
	server.SetObsRegistry(prometheus.NewRegistry())
	nor287SeedGraph(t, manager, "nor287_router_graph", 2, 1)
	router := newHeimdallDBRouter(server.db, manager, nil)
	defer router.Close()
	ctx := heimdallAuthDisabled()
	for _, op := range []storagefault.ReadOperation{"", storagefault.NodeCountRead, storagefault.EdgeCountRead} {
		setFailure(op)
		engine, gErr := manager.GetStorage("nor287_router_graph")
		require.NoError(t, gErr)
		n, nErr := engine.NodeCount(t.Context())
		e, eErr := engine.EdgeCount(t.Context())
		stats, sErr := router.Stats(ctx, "nor287_router_graph")
		t.Logf("L6_CONSUMER heimdall failure=%q count-input NodeCount=%d err=%v EdgeCount=%d err=%v | router.Stats=%+v err=%v", op, n, nErr, e, eErr, stats, sErr)
		if op != "" && sErr == nil {
			t.Errorf("CAUSE server_heimdall.go:335-336: count error discarded, Stats answers %+v with no error", stats)
		}
		response := makeRequest(t, server, http.MethodGet, "/metrics", nil, readerToken)
		nodes, nodesOK := nor287LegacyMetric(response.Body.String(), "nornicdb_nodes_total")
		edges, edgesOK := nor287LegacyMetric(response.Body.String(), "nornicdb_edges_total")
		t.Logf("L6_CONSUMER metrics failure=%q GET /metrics code=%d nornicdb_nodes_total=%v(%v) nornicdb_edges_total=%v(%v)", op, response.Code, nodes, nodesOK, edges, edgesOK)
		if op != "" && response.Code == http.StatusOK && ((op == storagefault.NodeCountRead && nodesOK) || (op == storagefault.EdgeCountRead && edgesOK)) {
			t.Errorf("CAUSE metrics: the failed count is still published as a definite value")
		}
	}
	setFailure("")
}

func TestL6CountConsumersStatusFallback(t *testing.T) {
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
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	faults := storagefault.NewReadFailureEngine(base, base, errNOR287CountReadFailure)
	manager, err := multidb.NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	dbServer.dbManager = manager
	nor287AcceptanceGraph(t, manager)
	for _, op := range []storagefault.ReadOperation{"", storagefault.NodeCountRead, storagefault.EdgeCountRead} {
		faults.SetFailure(op)
		engine, gErr := manager.GetStorage("nor287_acc_a")
		require.NoError(t, gErr)
		n, nErr := engine.NodeCount(t.Context())
		e, eErr := engine.EdgeCount(t.Context())
		response := makeRequest(t, dbServer, http.MethodGet, "/status", nil, "")
		var database any
		if response.Code == http.StatusOK {
			database = nor287DecodeJSON(t, response.Body.Bytes())["database"]
		}
		t.Logf("L6_CONSUMER status failure=%q count-input(nor287_acc_a) NodeCount=%d err=%v EdgeCount=%d err=%v | GET /status code=%d database=%v", op, n, nErr, e, eErr, response.Code, database)
		if op != "" && response.Code == http.StatusOK && (nErr != nil || eErr != nil) {
			t.Errorf("CAUSE server_public.go:249-256/:287-292: count error skips the add; /status answers 200 with %v", database)
		}
	}
	faults.SetFailure("")
}
