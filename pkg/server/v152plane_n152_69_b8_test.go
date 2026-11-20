package server

// NOR-287 error-aware native count acceptance tests (status fallback and
// non-administrator metrics renderer).
//
// Continuation of nor287_native_count_error_acceptance_test.go; the same
// approved acceptance rule applies (see nor287AcceptCountAnswer): a 2xx answer
// MUST NOT report a failed count as a definite zero on a population that is
// known to be non-empty, while an HTTP error, omitted count, unknown count,
// existing structured failure signal or accurate alternative count is accepted
// without prescribing any new status, field or schema.
//
//   GET /status  -> readableDatabaseCounts fallback  (server_public.go:283-293)
//   GET /metrics -> readableDatabaseCounts, non-admin scope (server_public.go:174-183)
//
// Both paths discard both count errors, so the aggregate becomes an
// unqualified zero. These tests therefore FAIL on the current implementation.
// The /metrics case is exercised as a real authorized non-administrator
// scrape: RBAC is enforced (a nil authenticator would be an administrator and
// would bypass the scoped path) and an observability registry is injected, so
// the legacy renderer really runs.

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// nor287LegacyMetric returns one rendered legacy metric value and whether the
// renderer emitted it at all.
func nor287LegacyMetric(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
			return value, err == nil
		}
	}
	return 0, false
}

// nor287MetricPayload exposes a rendered metric as the reportable count the
// acceptance rule evaluates; an unrendered metric is an omitted count.
func nor287MetricPayload(t *testing.T, body, name string) map[string]any {
	t.Helper()
	payload := make(map[string]any, 1)
	if value, ok := nor287LegacyMetric(body, name); ok {
		payload[name] = value
	}
	return payload
}

// TestNOR287StatusFallbackCountErrorAcceptance requires the /status fallback in
// readableDatabaseCounts (server_public.go:283-293) to stop reporting a failed
// count as a successful zero.
//
// The fallback runs only when the base storage provides no cached per-namespace
// prefix counters, which is the supported in-memory store mode
// (nornicdb.Open("", ...)). The databases therefore live in a manager over a
// real in-memory Badger engine decorated by the count-read fault fixture, while
// the process database stays memory-backed so the prefix fast path stays
// unavailable and this fallback is the code under test.
func TestNOR287StatusFallbackCountErrorAcceptance(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open("", cfg)
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	// Nil authenticator: this route's admission is not under test here, and the
	// fallback is reached with the anonymous administrator identity.
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

	block := func(t *testing.T) (int, map[string]any) {
		t.Helper()
		response := makeRequest(t, dbServer, http.MethodGet, "/status", nil, "")
		if response.Code != http.StatusOK {
			return response.Code, nil
		}
		payload := nor287DecodeJSON(t, response.Body.Bytes())
		database, ok := payload["database"].(map[string]any)
		require.True(t, ok, response.Body.String())
		return response.Code, database
	}

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		code, database := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(0), database["nodes"])
		require.Equal(t, float64(0), database["edges"])
	})

	nor287AcceptanceGraph(t, manager)

	t.Run("healthy non-empty databases report every node and edge", func(t *testing.T) {
		code, database := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(nor287AcceptanceNodes), database["nodes"])
		require.Equal(t, float64(nor287AcceptanceEdges), database["edges"])
	})

	t.Run("failed node count is not answered as a successful zero", func(t *testing.T) {
		faults.SetFailure(storagefault.NodeCountRead)
		code, database := block(t)
		nor287AcceptCountAnswer(t, "GET /status database.nodes", code, database, []string{"nodes"}, nor287AcceptanceNodes)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining status fields", code)
			return
		}
		require.Equal(t, float64(nor287AcceptanceEdges), database["edges"], "an unrelated healthy count must stay accurate")
	})

	t.Run("failed edge count is not answered as a successful zero", func(t *testing.T) {
		faults.SetFailure(storagefault.EdgeCountRead)
		code, database := block(t)
		nor287AcceptCountAnswer(t, "GET /status database.edges", code, database, []string{"edges"}, nor287AcceptanceEdges)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining status fields", code)
			return
		}
		require.Equal(t, float64(nor287AcceptanceNodes), database["nodes"], "an unrelated healthy count must stay accurate")
	})

	faults.SetFailure("")

	t.Run("healthy answer returns when the read owner recovers", func(t *testing.T) {
		code, database := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(nor287AcceptanceNodes), database["nodes"])
		require.Equal(t, float64(nor287AcceptanceEdges), database["edges"])
	})
}

// TestNOR287MetricsScopedCountErrorAcceptance requires the non-administrator
// GET /metrics renderer to stop reporting a failed count as a successful zero.
//
// handleMetrics computes the admitted population through
// readableDatabaseCounts and passes it to observability.RenderMetrics as the
// legacy read scope, where nornicdb_nodes_total/nornicdb_edges_total take
// scope.Nodes/scope.Edges (legacy_translation.go:163-170). The reader is a real
// viewer role under enforced RBAC and the scrape is a real route request with
// an injected registry, so the scoped renderer actually runs.
func TestNOR287MetricsScopedCountErrorAcceptance(t *testing.T) {
	server, authenticator := setupTestServer(t)
	readerToken := "Bearer " + getAuthToken(t, authenticator, "reader")
	adminToken := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)
	server.SetObsRegistry(prometheus.NewRegistry())

	systemEngine, err := manager.GetStorage("system")
	require.NoError(t, err)
	systemNodes, err := systemEngine.NodeCount(t.Context())
	require.NoError(t, err)
	systemEdges, err := systemEngine.EdgeCount(t.Context())
	require.NoError(t, err)
	nodesActual := systemNodes + nor287AcceptanceNodes
	edgesActual := systemEdges + nor287AcceptanceEdges
	require.Positive(t, nodesActual)
	require.Positive(t, edgesActual)

	scrape := func(t *testing.T, token string) (int, http.Header, string) {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/metrics", nil, token)
		return response.Code, response.Header(), response.Body.String()
	}

	t.Run("healthy empty user databases retain the system baseline", func(t *testing.T) {
		code, header, body := scrape(t, readerToken)
		require.Equal(t, http.StatusOK, code, body)
		require.Equal(t, "database", header.Get("X-NornicDB-Metrics-Scope"))
		nodes, ok := nor287LegacyMetric(body, "nornicdb_nodes_total")
		require.True(t, ok)
		require.Equal(t, float64(systemNodes), nodes)
		edges, ok := nor287LegacyMetric(body, "nornicdb_edges_total")
		require.True(t, ok)
		require.Equal(t, float64(systemEdges), edges)
	})
	nor287AcceptanceGraph(t, manager)

	t.Run("administrator scrape is served by the registry, not the read scope", func(t *testing.T) {
		code, header, _ := scrape(t, adminToken)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, "server", header.Get("X-NornicDB-Metrics-Scope"),
			"the administrator view bypasses readableDatabaseCounts; the reader scrape below is the scoped path")
	})

	t.Run("healthy non-empty databases report every node and edge", func(t *testing.T) {
		code, header, body := scrape(t, readerToken)
		require.Equal(t, http.StatusOK, code, body)
		require.Equal(t, "database", header.Get("X-NornicDB-Metrics-Scope"), "a non-administrator scrape must use the admitted read scope")
		nodes, ok := nor287LegacyMetric(body, "nornicdb_nodes_total")
		require.True(t, ok, "the renderer must emit the legacy node total")
		require.Equal(t, float64(nodesActual), nodes)
		edges, ok := nor287LegacyMetric(body, "nornicdb_edges_total")
		require.True(t, ok, "the renderer must emit the legacy edge total")
		require.Equal(t, float64(edgesActual), edges)
	})

	t.Run("failed node count is not rendered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		code, header, body := scrape(t, readerToken)
		nor287AcceptCountAnswer(t, "GET /metrics nornicdb_nodes_total", code, nor287MetricPayload(t, body, "nornicdb_nodes_total"), []string{"nornicdb_nodes_total"}, nodesActual)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining metrics catalogue", code)
			return
		}
		require.Equal(t, "database", header.Get("X-NornicDB-Metrics-Scope"))
		edges, ok := nor287LegacyMetric(body, "nornicdb_edges_total")
		require.True(t, ok)
		require.Equal(t, float64(edgesActual), edges, "an unrelated healthy count must stay accurate")
	})

	t.Run("failed edge count is not rendered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.EdgeCountRead)
		code, header, body := scrape(t, readerToken)
		nor287AcceptCountAnswer(t, "GET /metrics nornicdb_edges_total", code, nor287MetricPayload(t, body, "nornicdb_edges_total"), []string{"nornicdb_edges_total"}, edgesActual)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining metrics catalogue", code)
			return
		}
		require.Equal(t, "database", header.Get("X-NornicDB-Metrics-Scope"))
		nodes, ok := nor287LegacyMetric(body, "nornicdb_nodes_total")
		require.True(t, ok)
		require.Equal(t, float64(nodesActual), nodes, "an unrelated healthy count must stay accurate")
	})

	setFailure("")

	t.Run("healthy answer returns when the read owner recovers", func(t *testing.T) {
		code, _, body := scrape(t, readerToken)
		require.Equal(t, http.StatusOK, code, body)
		nodes, ok := nor287LegacyMetric(body, "nornicdb_nodes_total")
		require.True(t, ok)
		require.Equal(t, float64(nodesActual), nodes)
		edges, ok := nor287LegacyMetric(body, "nornicdb_edges_total")
		require.True(t, ok)
		require.Equal(t, float64(edgesActual), edges)
	})
}
