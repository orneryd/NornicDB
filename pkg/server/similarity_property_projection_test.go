package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func setupSimilarityPropertyPolicy(t *testing.T, be *storage.BadgerEngine, namespace, label string) {
	t.Helper()
	schema := be.GetSchemaForNamespace(namespace)
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "query", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "querybinding", ProfileRef: "query", TargetLabels: []string{label}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}))
	be.SetDecayEnabled(true)
}

func TestHTTPSimilarProjectsSuppressedProperties(t *testing.T) {
	server, _ := setupTestServer(t)
	engine, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	be := storage.UnwrapEngine(engine).(*storage.BadgerEngine)
	setupSimilarityPropertyPolicy(t, be, server.dbManager.DefaultDatabaseName(), "Doc")
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"root", "other"} {
		_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, ChunkEmbeddings: [][]float32{{1, 0}}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	req := httptest.NewRequest("POST", "/nornicdb/similar", strings.NewReader(`{"node_id":"root","limit":1}`)).WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}}))
	w := httptest.NewRecorder()
	server.handleSimilar(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	var results []*nornicdb.SearchResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &results))
	require.Len(t, results, 1)
	require.NotContains(t, results[0].Node.Properties, "secret")
	require.Equal(t, "yes", results[0].Node.Properties["public"])
	raw, err := engine.GetNode("other")
	require.NoError(t, err)
	require.Contains(t, raw.Properties, "secret")
}
