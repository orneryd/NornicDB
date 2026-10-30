package resolvers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/orneryd/nornicdb/internal/testutil/remotegraph"
	"github.com/orneryd/nornicdb/pkg/graphql/generated"
	"github.com/stretchr/testify/require"
)

func TestGraphQLCatalogDescriptorPublicViewsAndFabric(t *testing.T) {
	manager, peer := remotegraph.New(t)
	resolver := NewResolver(nil, manager)
	t.Cleanup(resolver.EventBroker.Close)
	server := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: resolver}))
	run := func(statement string) [][]interface{} {
		t.Helper()
		body, err := json.Marshal(map[string]interface{}{
			"query":     "query($statement:String!){cypher(input:{statement:$statement,database:\"nornic\"}){columns rows rowCount}}",
			"variables": map[string]interface{}{"statement": statement},
		})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)
		var result struct {
			Errors []interface{} `json:"errors"`
			Data   struct {
				Cypher struct {
					Rows [][]interface{} `json:"rows"`
				} `json:"cypher"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Empty(t, result.Errors, response.Body.String())
		rows := result.Data.Cypher.Rows
		// GraphQL's JSON scalar contract wraps scalar cells in {"value": ...}.
		// Assert that exact protocol envelope before sharing semantic checks.
		for _, row := range rows {
			for i, cell := range row {
				object, ok := cell.(map[string]interface{})
				require.True(t, ok, "GraphQL scalar cell must use JSON object envelope")
				require.Len(t, object, 1)
				value, present := object["value"]
				require.True(t, present)
				row[i] = value
			}
		}
		return rows
	}
	remotegraph.CheckPublicRead(t, peer, run)
}
