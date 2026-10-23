package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/differential"
	"github.com/orneryd/nornicdb/testing/cypher/subqueryeval"
	"github.com/stretchr/testify/require"
)

func TestExistsSubqueryEvaluationOwnerHTTP(t *testing.T) {
	url := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if url == "" {
		t.Skip("requires live Neo4j reference")
	}
	server, auth := setupTestServer(t)
	local := httptest.NewServer(server.buildRouter())
	defer local.Close()
	client := &http.Client{Timeout: differential.DefaultOptions.StatementTimeout}
	ref := httpDifferentialExecutor{client: client, endpoint: url + "/db/neo4j/tx/commit"}
	nornic := httpDifferentialExecutor{client: client, endpoint: local.URL + "/db/nornic/tx/commit", authorization: "Bearer " + getAuthToken(t, auth, "admin")}
	ctx := context.Background()
	for _, backend := range []httpDifferentialExecutor{ref, nornic} {
		require.NoError(t, backend.Reset(ctx))
		for _, setup := range subqueryeval.Setup {
			outcome := backend.Execute(ctx, setup)
			require.Empty(t, outcome.Code, "%+v", outcome)
		}
	}
	defer ref.Reset(ctx)
	for _, tc := range subqueryeval.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			query := strings.ReplaceAll(tc.Query, "$nonce", "1")
			expected := ref.Execute(ctx, query)
			actual := nornic.Execute(ctx, query)
			require.Empty(t, expected.Code, "%+v", expected)
			require.True(t, differential.Same(query, expected, actual), "Neo4j=%+v NornicDB=%+v", expected, actual)
		})
	}
}
