package server

// explain_plan_http_test.go — plan delivery over the HTTP transaction API
// (#744 §2): EXPLAIN results carry "plan", PROFILE results carry "profile"
// (plus "plan"), and ordinary queries carry neither, matching Neo4j's JSON.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPExplainProfilePlanDelivery(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	post := func(stmt string) map[string]any {
		rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": stmt}},
		}, token)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw), rec.Body.String())
		require.Empty(t, raw["errors"], rec.Body.String())
		results := raw["results"].([]any)
		require.Len(t, results, 1)
		return results[0].(map[string]any)
	}

	explain := post("EXPLAIN MATCH (n:Person) RETURN n.name")
	plan, ok := explain["plan"].(map[string]any)
	require.True(t, ok, "EXPLAIN result carries the plan")
	require.Equal(t, "ProduceResults", plan["operatorType"])
	_, hasProfile := explain["profile"]
	require.False(t, hasProfile, "EXPLAIN has no profile key")

	profiled := post("PROFILE MATCH (n:Person) RETURN n.name")
	profile, ok := profiled["profile"].(map[string]any)
	require.True(t, ok, "PROFILE result carries the profile")
	require.Equal(t, "ProduceResults", profile["operatorType"])
	_, hasPlan := profiled["plan"]
	require.True(t, hasPlan, "PROFILE also carries the plan key")

	ordinary := post("MATCH (n:Person) RETURN n.name")
	_, hasPlan = ordinary["plan"]
	require.False(t, hasPlan, "ordinary queries carry no plan")
	_, hasProfile = ordinary["profile"]
	require.False(t, hasProfile, "ordinary queries carry no profile")
}
