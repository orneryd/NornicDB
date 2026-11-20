package server

// NOR-287 error-aware native count acceptance tests (admin stats, native
// database info).
//
// This file is the acceptance counterpart of the diagnostic audit in
// nor287_native_count_consumer_audit_test.go. The audit pins today's observable
// answer, including the silent zero, and is therefore expected to pass on the
// current implementation. These tests state the required answer and therefore
// FAIL on the current implementation:
//
//   GET /admin/stats   -> handleAdminStats      (server_admin.go:42-46)
//   GET /db/{database} -> native branch         (server_db.go:412-419)
//
// Both sites replace a failed NodeCount/EdgeCount read with 0 and keep
// answering 2xx with no failure signal, so a client or operator cannot
// distinguish "this database is empty" from "the count could not be read".
//
// Approved acceptance rule, applied per reported count on a population that is
// known to be non-empty (see nor287AcceptCountAnswer):
//
//   - a successful (2xx) answer MUST NOT report the failed count as a definite
//     zero, and
//   - ANY of the following resolves the case and is accepted: an HTTP error
//     answer, an omitted count, an unknown/non-numeric count, an existing
//     structured failure signal, or an accurate count obtained by another read.
//
// The rule deliberately does not prescribe a new status value, response field,
// field name or schema. Healthy empty, healthy non-empty and post-recovery
// controls pin the accurate answer so the fault case cannot be satisfied by
// degrading ordinary answers.
//
// Every case counts real nodes and edges through a real server route. The only
// injected element is the repository storage read-fault fixture
// (internal/testfixture/storagefault) layered under the production
// multidb.NewDatabaseManager construction seam through
// nor287FaultedDatabaseManager; the failing operation is a real native count
// read on a real engine, not a fabricated storage value.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Acceptance fixture population: nor287SeedGraph stores these in the
// nor287Acceptance* databases.
const (
	nor287AcceptanceNodes int64 = 3
	nor287AcceptanceEdges int64 = 2
)

// nor287SeedGraph creates name and stores nodeCount real nodes plus edgeCount
// real edges between them, so a count failure has a nonzero population to hide.
func nor287SeedGraph(t *testing.T, manager *multidb.DatabaseManager, name string, nodeCount, edgeCount int) {
	t.Helper()
	nor287CreateDatabaseWithNodes(t, manager, name, nodeCount)
	engine, err := manager.GetStorage(name)
	require.NoError(t, err)
	for i := 0; i < edgeCount; i++ {
		err := engine.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("%s-e%d", name, i)),
			StartNode: storage.NodeID(fmt.Sprintf("%s-n0", name)),
			EndNode:   storage.NodeID(fmt.Sprintf("%s-n%d", name, nodeCount-1)),
			Type:      "NOR287",
		})
		require.NoError(t, err)
	}
}

// nor287AcceptanceGraph stores the shared nonzero population: 3 nodes and 2
// edges over two physical databases.
func nor287AcceptanceGraph(t *testing.T, manager *multidb.DatabaseManager) {
	t.Helper()
	nor287SeedGraph(t, manager, "nor287_acc_a", 2, 1)
	nor287SeedGraph(t, manager, "nor287_acc_b", 1, 1)
}

// nor287LookupCount returns the value reported at path, distinguishing an
// omitted field from a reported one.
func nor287LookupCount(payload map[string]any, path ...string) (any, bool) {
	var current any = payload
	for _, step := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[step]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// nor287NumericCount reports whether value is a definite numeric count.
func nor287NumericCount(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	}
	return 0, false
}

// nor287StructuredFailureReason reports an existing structured failure signal
// anywhere in an answer. Only documented/structurally explicit signals are
// recognised; the set is intentionally small and is used solely to ACCEPT an
// answer, never to require a field.
func nor287StructuredFailureReason(value any, prefix string) (string, bool) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			item := typed[key]
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			switch key {
			case "error":
				if message, ok := item.(string); item != nil && (!ok || message != "") {
					return path + " is set", true
				}
			case "statsPartial", "partial", "degraded", "unknown":
				if flag, ok := item.(bool); ok && flag {
					return path + " is true", true
				}
			case "reachable":
				if flag, ok := item.(bool); ok && !flag {
					return path + " is false", true
				}
			}
			if reason, ok := nor287StructuredFailureReason(item, path); ok {
				return reason, true
			}
		}
	case []any:
		for index, item := range typed {
			if reason, ok := nor287StructuredFailureReason(item, fmt.Sprintf("%s[%d]", prefix, index)); ok {
				return reason, true
			}
		}
	}
	return "", false
}

// nor287AcceptCountAnswer applies the approved acceptance rule to one reported
// count. actual is the population verified through the healthy control.
//
// It rejects a definite incorrect count in a successful answer without a
// failure signal. HTTP error, omitted count, unknown count, accurate count,
// or a structured failure signal is accepted and recorded.
func nor287AcceptCountAnswer(t *testing.T, subject string, code int, payload map[string]any, path []string, actual int64) {
	t.Helper()

	if code < http.StatusOK || code >= http.StatusMultipleChoices {
		t.Logf("%s: accepted HTTP %d failure answer", subject, code)
		return
	}
	if reason, ok := nor287StructuredFailureReason(payload, ""); ok {
		t.Logf("%s: accepted structured failure signal (%s)", subject, reason)
		return
	}

	raw, present := nor287LookupCount(payload, path...)
	if !present {
		t.Logf("%s: accepted omitted count %v", subject, path)
		return
	}
	value, numeric := nor287NumericCount(raw)
	if !numeric {
		t.Logf("%s: accepted unknown count %v=%#v", subject, path, raw)
		return
	}
	if value == float64(actual) {
		t.Logf("%s: accepted accurate count %v=%d", subject, path, actual)
		return
	}

	require.Equalf(t, float64(actual), value,
		"%s: HTTP %d reports definite count %v=%v without a failure signal after the count read failed; verified population is %d",
		subject, code, path, value, actual)
}

// TestNOR287AdminStatsCountErrorAcceptance requires GET /admin/stats to stop
// reporting a failed native count as a successful zero.
//
// server_admin.go:42-46 discards both count errors and publishes the zero value
// in node_count/edge_count and in per_database. With three nodes and two edges
// stored, a failed read is answered as an unqualified zero, so this test fails
// until the route reports the failure, omits the count, or answers with an
// error.
func TestNOR287AdminStatsCountErrorAcceptance(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	block := func(t *testing.T) (int, map[string]any) {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/admin/stats", nil, token)
		if response.Code != http.StatusOK {
			return response.Code, nil
		}
		payload := nor287DecodeJSON(t, response.Body.Bytes())
		database, ok := payload["database"].(map[string]any)
		require.True(t, ok, response.Body.String())
		return response.Code, database
	}

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		code, stats := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(0), stats["node_count"])
		require.Equal(t, float64(0), stats["edge_count"])
	})

	nor287AcceptanceGraph(t, manager)

	t.Run("healthy non-empty databases report every node and edge", func(t *testing.T) {
		code, stats := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(nor287AcceptanceNodes), stats["node_count"])
		require.Equal(t, float64(nor287AcceptanceEdges), stats["edge_count"])
		perDatabase, ok := stats["per_database"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, map[string]any{"node_count": float64(2), "edge_count": float64(1)}, perDatabase["nor287_acc_a"])
		require.Equal(t, map[string]any{"node_count": float64(1), "edge_count": float64(1)}, perDatabase["nor287_acc_b"])
	})

	t.Run("failed node count is not answered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		code, stats := block(t)
		nor287AcceptCountAnswer(t, "GET /admin/stats database.node_count", code, stats, []string{"node_count"}, nor287AcceptanceNodes)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining admin stats fields", code)
			return
		}
		// Only the injected operation degrades: the edge population is intact
		// and must stay accurate.
		require.Equal(t, float64(nor287AcceptanceEdges), stats["edge_count"], "an unrelated healthy count must stay accurate")
		perDatabase, ok := stats["per_database"].(map[string]any)
		require.True(t, ok)
		entry, ok := perDatabase["nor287_acc_a"].(map[string]any)
		require.True(t, ok, "the readable database must still be reported")
		nor287AcceptCountAnswer(t, "GET /admin/stats per_database[nor287_acc_a].node_count", code, entry, []string{"node_count"}, 2)
	})

	t.Run("failed edge count is not answered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.EdgeCountRead)
		code, stats := block(t)
		nor287AcceptCountAnswer(t, "GET /admin/stats database.edge_count", code, stats, []string{"edge_count"}, nor287AcceptanceEdges)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining admin stats fields", code)
			return
		}
		require.Equal(t, float64(nor287AcceptanceNodes), stats["node_count"], "an unrelated healthy count must stay accurate")
		perDatabase, ok := stats["per_database"].(map[string]any)
		require.True(t, ok)
		entry, ok := perDatabase["nor287_acc_b"].(map[string]any)
		require.True(t, ok, "the readable database must still be reported")
		nor287AcceptCountAnswer(t, "GET /admin/stats per_database[nor287_acc_b].edge_count", code, entry, []string{"edge_count"}, 1)
	})

	setFailure("")

	t.Run("healthy answer returns when the read owner recovers", func(t *testing.T) {
		code, stats := block(t)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(nor287AcceptanceNodes), stats["node_count"])
		require.Equal(t, float64(nor287AcceptanceEdges), stats["edge_count"])
	})
}

// TestNOR287DatabaseInfoCountErrorAcceptance requires the native branch of
// GET /db/{database} to stop reporting a failed count as a successful zero.
//
// server_db.go:412-419 replaces both count errors with 0 while the answer keeps
// HTTP 200 and status "online". With four nodes and two edges stored, a failed
// read is answered as an unqualified zero, so this test fails until the route
// reports the failure, omits the count, or answers with an error.
func TestNOR287DatabaseInfoCountErrorAcceptance(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	info := func(t *testing.T, name string) (int, map[string]any) {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/db/"+name, nil, token)
		if response.Code != http.StatusOK {
			return response.Code, nil
		}
		return response.Code, nor287DecodeJSON(t, response.Body.Bytes())
	}

	nor287SeedGraph(t, manager, "nor287_acc_empty", 0, 0)
	nor287SeedGraph(t, manager, "nor287_acc_info", 4, 2)

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		code, payload := info(t, "nor287_acc_empty")
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, "standard", payload["type"])
		require.Equal(t, "online", payload["status"])
		require.Equal(t, float64(0), payload["nodeCount"])
		require.Equal(t, float64(0), payload["edgeCount"])
	})

	t.Run("healthy non-empty database reports every node and edge", func(t *testing.T) {
		code, payload := info(t, "nor287_acc_info")
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(4), payload["nodeCount"])
		require.Equal(t, float64(2), payload["edgeCount"])
	})

	t.Run("failed node count is not answered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		code, payload := info(t, "nor287_acc_info")
		nor287AcceptCountAnswer(t, "GET /db/nor287_acc_info nodeCount", code, payload, []string{"nodeCount"}, 4)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining database info fields", code)
			return
		}
		require.Equal(t, float64(2), payload["edgeCount"], "an unrelated healthy count must stay accurate")
	})

	t.Run("failed edge count is not answered as a successful zero", func(t *testing.T) {
		setFailure(storagefault.EdgeCountRead)
		code, payload := info(t, "nor287_acc_info")
		nor287AcceptCountAnswer(t, "GET /db/nor287_acc_info edgeCount", code, payload, []string{"edgeCount"}, 2)
		if code != http.StatusOK {
			t.Logf("accepted HTTP %d for the remaining database info fields", code)
			return
		}
		require.Equal(t, float64(4), payload["nodeCount"], "an unrelated healthy count must stay accurate")
	})

	setFailure("")

	t.Run("healthy answer returns when the read owner recovers", func(t *testing.T) {
		code, payload := info(t, "nor287_acc_info")
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, float64(4), payload["nodeCount"])
		require.Equal(t, float64(2), payload["edgeCount"])
	})
}
