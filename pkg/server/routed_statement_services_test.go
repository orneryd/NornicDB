package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

func TestHTTPRoutedStatementFinishesTargetServices(t *testing.T) {
	for _, route := range []string{"USE", "Fabric"} {
		t.Run(route, func(t *testing.T) {
			s, users := setupTestServer(t)
			defer stopTestServer(t, s)
			ctx := context.Background()
			require.NoError(t, s.dbManager.CreateDatabase("service_target"))
			require.NoError(t, s.dbManager.CreateCompositeDatabase("service_composite", []multidb.ConstituentRef{
				{Alias: "docs", DatabaseName: "service_target", Type: "local", AccessMode: "read_write"},
			}))
			engine, err := s.dbManager.GetStorage("service_target")
			require.NoError(t, err)
			// Build the existing target's empty index first. A later build
			// must not hide a write that omitted live indexing.
			index, err := s.db.GetOrCreateSearchService("service_target", engine)
			require.NoError(t, err)
			require.NoError(t, index.BuildIndexes(ctx))
			var mutations atomic.Int32
			s.db.GetCypherExecutor().SetNodeMutatedCallback(func(string) { mutations.Add(1) })
			s.invalidateAllExecutors()
			query := "USE service_target CREATE (:Doc {content: 'routedquokkaproof'}) RETURN 1 AS written"
			database := "nornic"
			if route == "Fabric" {
				database = "service_composite"
				query = "CALL { USE service_composite.docs CREATE (:Doc {content: 'routedquokkaproof'}) RETURN 1 AS written } RETURN written"
			}
			response := makeRequest(t, s, http.MethodPost, "/db/"+database+"/tx/commit",
				map[string]any{"statements": []map[string]any{{"statement": query}}}, "Bearer "+getAuthToken(t, users, "admin"))
			var body TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Empty(t, body.Errors, response.Body.String())
			require.Positive(t, mutations.Load(), "the actual target reaches the existing mutation sink")
			found, err := index.Search(ctx, "routedquokkaproof", nil, &search.SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, found.Results, 1, "the existing target index contains the routed write")
			if route == "USE" {
				s.executorsMu.RLock()
				cached := s.executors["service_target"]
				s.executorsMu.RUnlock()
				require.NotNil(t, cached, "the admitted reusable target is published")
			}
		})
	}
}

// Heimdall's router builds its executors with its own finisher: a USE or
// Fabric statement routed from one of them reaches the actual target's
// search index and mutation sink after the target admits it, as over HTTP,
// and a reusable USE target is published in the router's cache.
func TestHeimdallRoutedStatementFinishesTargetServices(t *testing.T) {
	for _, route := range []string{"USE", "Fabric"} {
		t.Run(route, func(t *testing.T) {
			s, _ := setupTestServer(t)
			defer stopTestServer(t, s)
			ctx := context.Background()
			require.NoError(t, s.dbManager.CreateDatabase("heimdall_target"))
			require.NoError(t, s.dbManager.CreateCompositeDatabase("heimdall_composite", []multidb.ConstituentRef{
				{Alias: "docs", DatabaseName: "heimdall_target", Type: "local", AccessMode: "read_write"},
			}))
			engine, err := s.dbManager.GetStorage("heimdall_target")
			require.NoError(t, err)
			index, err := s.db.GetOrCreateSearchService("heimdall_target", engine)
			require.NoError(t, err)
			require.NoError(t, index.BuildIndexes(ctx))
			var mutations atomic.Int32
			s.db.GetCypherExecutor().SetNodeMutatedCallback(func(string) { mutations.Add(1) })
			router := newHeimdallDBRouter(s.db, s.dbManager, nil)
			query := "USE heimdall_target CREATE (:Doc {content: 'heimdallquokkaproof'}) RETURN 1 AS written"
			database := "nornic"
			if route == "Fabric" {
				database = "heimdall_composite"
				query = "CALL { USE heimdall_composite.docs CREATE (:Doc {content: 'heimdallquokkaproof'}) RETURN 1 AS written } RETURN written"
			}
			rows, err := router.Query(auth.WithRequestAuthenticationDisabled(ctx), database, query, nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Positive(t, mutations.Load(), "the actual target reaches the existing mutation sink")
			found, err := index.Search(ctx, "heimdallquokkaproof", nil, &search.SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, found.Results, 1, "the existing target index contains the routed write")
			if route == "USE" {
				require.NotNil(t, router.cachedExecutor("heimdall_target"), "the admitted reusable target is published")
			}
		})
	}
}
