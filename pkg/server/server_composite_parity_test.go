package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPCompositeSchemaFlows_E2E(t *testing.T) {
	server, authenticator := setupTestServer(t)
	adminToken := getAuthToken(t, authenticator, "admin")

	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cmp_schema_a"}))
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cmp_schema_b"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "cmp_schema", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "cmp_schema_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "cmp_schema_b", Type: "local", AccessMode: "read_write"},
	}))

	// Composite root schema introspection must be rejected (Neo4j Fabric semantics).
	rootShow := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "SHOW INDEXES"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, rootShow.Code)
	rootPayload := decodeTxPayload(t, rootShow)
	rootErrors := txErrors(rootPayload)
	require.NotEmpty(t, rootErrors)
	// The engine raises NotAllowed; HTTP reports the engine's code (#575).
	require.Equal(t, "Neo.ClientError.Statement.NotAllowed", rootErrors[0].Code)
	require.Contains(t, rootErrors[0].Message, "requires a constituent target")

	// Constituent-scoped schema create should succeed over HTTP transaction endpoint.
	createResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a CREATE INDEX cmp_http_idx FOR (n:Doc) ON (n.id)"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, createResp.Code, createResp.Body.String())
	require.Empty(t, txErrors(decodeTxPayload(t, createResp)))

	// SHOW INDEXES on constituent returns the created index.
	showResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a SHOW INDEXES"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, showResp.Code, showResp.Body.String())
	showPayload := decodeTxPayload(t, showResp)
	require.Empty(t, txErrors(showPayload))
	require.True(t, txResultContainsName(showPayload, "cmp_http_idx"), "expected cmp_http_idx in SHOW INDEXES result")

	// DROP INDEX on constituent removes it.
	dropResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a DROP INDEX cmp_http_idx"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, dropResp.Code, dropResp.Body.String())
	require.Empty(t, txErrors(decodeTxPayload(t, dropResp)))

	showAfterDrop := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a SHOW INDEXES"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, showAfterDrop.Code, showAfterDrop.Body.String())
	showAfterDropPayload := decodeTxPayload(t, showAfterDrop)
	require.Empty(t, txErrors(showAfterDropPayload))
	require.False(t, txResultContainsName(showAfterDropPayload, "cmp_http_idx"), "expected cmp_http_idx to be dropped")

	// Composite root constraint introspection must be rejected.
	rootShowConstraints := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "SHOW CONSTRAINTS"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, rootShowConstraints.Code)
	rootConstraintErrors := txErrors(decodeTxPayload(t, rootShowConstraints))
	require.NotEmpty(t, rootConstraintErrors)
	require.Equal(t, "Neo.ClientError.Statement.NotAllowed", rootConstraintErrors[0].Code)
	require.Contains(t, rootConstraintErrors[0].Message, "requires a constituent target")

	createConstraintResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a CREATE CONSTRAINT cmp_http_con FOR (n:Doc) REQUIRE n.id IS UNIQUE"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, createConstraintResp.Code, createConstraintResp.Body.String())
	require.Empty(t, txErrors(decodeTxPayload(t, createConstraintResp)))

	showConstraintResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a SHOW CONSTRAINTS"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, showConstraintResp.Code, showConstraintResp.Body.String())
	showConstraintPayload := decodeTxPayload(t, showConstraintResp)
	require.Empty(t, txErrors(showConstraintPayload))
	require.True(t, txResultContainsName(showConstraintPayload, "cmp_http_con"), "expected cmp_http_con in SHOW CONSTRAINTS result")

	dropConstraintResp := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a DROP CONSTRAINT cmp_http_con"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, dropConstraintResp.Code, dropConstraintResp.Body.String())
	require.Empty(t, txErrors(decodeTxPayload(t, dropConstraintResp)))

	showConstraintAfterDrop := makeRequest(t, server, http.MethodPost, "/db/cmp_schema/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "USE cmp_schema.a SHOW CONSTRAINTS"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, showConstraintAfterDrop.Code, showConstraintAfterDrop.Body.String())
	showConstraintAfterDropPayload := decodeTxPayload(t, showConstraintAfterDrop)
	require.Empty(t, txErrors(showConstraintAfterDropPayload))
	require.False(t, txResultContainsName(showConstraintAfterDropPayload, "cmp_http_con"), "expected cmp_http_con to be dropped")
}

func TestCompositeRootSearchEndpointsRejected(t *testing.T) {
	server, authenticator := setupTestServer(t)
	adminToken := getAuthToken(t, authenticator, "admin")

	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cmp_search_a"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "cmp_search", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "cmp_search_a", Type: "local", AccessMode: "read_write"},
	}))

	searchResp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": "cmp_search",
		"query":    "hello",
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusBadRequest, searchResp.Code)
	require.Contains(t, strings.ToLower(searchResp.Body.String()), "composite database")

	rebuildResp := makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{
		"database": "cmp_search",
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusBadRequest, rebuildResp.Code)
	require.Contains(t, strings.ToLower(rebuildResp.Body.String()), "composite database")

	similarResp := makeRequest(t, server, http.MethodPost, "/nornicdb/similar", map[string]any{
		"database": "cmp_search",
		"node_id":  "missing",
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusBadRequest, similarResp.Code)
	require.Contains(t, strings.ToLower(similarResp.Body.String()), "composite database")
}

func TestDatabaseInfoCompositeStatsProvenanceDeterministic(t *testing.T) {
	server, authenticator := setupTestServer(t)
	adminToken := getAuthToken(t, authenticator, "admin")

	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cmp_stats_a"}))
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cmp_stats_b"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "cmp_stats", []multidb.ConstituentRef{
		{Alias: "b", DatabaseName: "cmp_stats_b", Type: "local", AccessMode: "read_write"},
		{Alias: "a", DatabaseName: "cmp_stats_a", Type: "local", AccessMode: "read_write"},
	}))

	createA := makeRequest(t, server, http.MethodPost, "/db/cmp_stats_a/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Stat {id: 'a1'})"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, createA.Code)
	require.Empty(t, txErrors(decodeTxPayload(t, createA)))

	createB := makeRequest(t, server, http.MethodPost, "/db/cmp_stats_b/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Stat {id: 'b1'})"}},
	}, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, createB.Code)
	require.Empty(t, txErrors(decodeTxPayload(t, createB)))

	infoResp := makeRequest(t, server, http.MethodGet, "/db/cmp_stats", nil, "Bearer "+adminToken)
	require.Equal(t, http.StatusOK, infoResp.Code, infoResp.Body.String())

	var payload map[string]any
	require.NoError(t, json.Unmarshal(infoResp.Body.Bytes(), &payload))
	require.Equal(t, "constituent_sum", anyString(payload["statsAggregation"]))
	require.Equal(t, false, payload["statsPartial"])
	require.Equal(t, float64(2), payload["nodeCount"])
	require.Contains(t, payload, "searchStrategy")

	rawStats, ok := payload["statsProvenance"].([]any)
	require.True(t, ok)
	require.Len(t, rawStats, 2)

	aliases := make([]string, 0, len(rawStats))
	for _, item := range rawStats {
		m, ok := item.(map[string]any)
		require.True(t, ok)
		aliases = append(aliases, anyString(m["alias"]))
		require.Contains(t, m, "nodeCount")
		require.Contains(t, m, "edgeCount")
		require.Contains(t, m, "nodeStorageBytes")
		require.Contains(t, m, "managedEmbeddingBytes")
		require.Contains(t, m, "searchReady")
		require.Contains(t, m, "searchBuilding")
		require.Contains(t, m, "searchInitialized")
		require.Contains(t, m, "searchStrategy")
	}
	sorted := append([]string(nil), aliases...)
	sort.Strings(sorted)
	require.Equal(t, sorted, aliases, "statsProvenance must be deterministic (alias ascending)")
}

func TestCompositeConstituentStatsPartialBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	missingReq := httptest.NewRequest(http.MethodGet, "/db/missing_cmp", nil)
	stats, partial := server.compositeConstituentStats(missingReq, "missing_cmp")
	require.True(t, partial)
	require.Nil(t, stats)

	dbManager, err := multidb.NewDatabaseManager(t.Context(), server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			require.Equal(t, "Bearer remote-token", authToken)
			return nil, fmt.Errorf("remote stats unavailable")
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateCompositeDatabase(t.Context(), "cmp_partial_stats", []multidb.ConstituentRef{
		{Alias: "r", DatabaseName: "remote_db", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
	}))
	server.dbManager = dbManager

	req := httptest.NewRequest(http.MethodGet, "/db/cmp_partial_stats", nil)
	req.Header.Set("Authorization", "Bearer remote-token")
	stats, partial = server.compositeConstituentStats(req, "cmp_partial_stats")
	require.True(t, partial)
	require.Len(t, stats, 1)
	require.Equal(t, false, stats[0]["reachable"])
	require.Contains(t, stats[0]["error"], "remote stats unavailable")
	require.Equal(t, int64(0), stats[0]["nodeCount"])
}

type txError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func decodeTxPayload(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	return payload
}

func txErrors(payload map[string]any) []txError {
	raw, _ := payload["errors"].([]any)
	out := make([]txError, 0, len(raw))
	for _, v := range raw {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, txError{
			Code:    anyString(m["code"]),
			Message: anyString(m["message"]),
		})
	}
	return out
}

func txResultContainsName(payload map[string]any, name string) bool {
	results, _ := payload["results"].([]any)
	for _, resAny := range results {
		res, ok := resAny.(map[string]any)
		if !ok {
			continue
		}
		colsAny, _ := res["columns"].([]any)
		nameIdx := -1
		for i, c := range colsAny {
			if anyString(c) == "name" {
				nameIdx = i
				break
			}
		}
		if nameIdx < 0 {
			continue
		}
		dataAny, _ := res["data"].([]any)
		for _, rowAny := range dataAny {
			rowObj, ok := rowAny.(map[string]any)
			if !ok {
				continue
			}
			rowVals, _ := rowObj["row"].([]any)
			if nameIdx < len(rowVals) && anyString(rowVals[nameIdx]) == name {
				return true
			}
		}
	}
	return false
}

func anyString(v any) string {
	s, _ := v.(string)
	return s
}
