package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

func TestMapBraceSyntaxTriviaMatrixHTTP(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if uri == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI for the pinned Neo4j differential")
	}
	ctx := context.Background()
	service, authenticator := setupTestServer(t)
	local := httptest.NewServer(service.buildRouter())
	defer local.Close()
	client := &http.Client{Timeout: 30 * time.Second}
	n := httpDifferentialExecutor{client: client, endpoint: local.URL + "/db/nornic/tx/commit", authorization: "Bearer " + getAuthToken(t, authenticator, "admin")}
	r := httpDifferentialExecutor{client: client, endpoint: uri + "/db/neo4j/tx/commit"}
	querytextmatrix.MapBraceCases(func(item querytextmatrix.MapBraceCase) {
		t.Run(item.Name, func(t *testing.T) {
			expected := r.Execute(ctx, item.Query)
			actual := n.Execute(ctx, item.Query)
			if actual.Code != expected.Code || (!item.Explain && !differential.Same(item.Query, expected, actual)) {
				t.Fatalf("query=%s reference=%+v local=%+v", item.Query, expected, actual)
			}
		})
	})
	for _, executor := range []httpDifferentialExecutor{n, r} {
		if result := executor.Execute(ctx, "MATCH (n:MapBraceMatrix) DETACH DELETE n"); result.Failed() {
			t.Fatalf("fixture cleanup: %+v", result)
		}
	}
}
