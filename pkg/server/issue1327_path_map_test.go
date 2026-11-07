package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIssue1327HTTPAndEmbeddedMaps(t *testing.T) {
	prior := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(prior) })
	srv, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	_, err := srv.db.ExecuteCypher(t.Context(), "CREATE (:Issue1327A)-[:ISSUE1327]->(:Issue1327B)", nil)
	require.NoError(t, err)
	for _, parser := range []string{"nornic", "antlr"} {
		config.SetParserType(parser)
		for _, marker := range []any{true, false, int64(17), nil} {
			t.Run(fmt.Sprintf("%s/%v", parser, marker), func(t *testing.T) {
				query := "RETURN {_pathResult:$marker, keep:'value'} AS m"
				params := map[string]any{"marker": marker}
				embedded, err := srv.db.ExecuteCypher(t.Context(), query, params)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"_pathResult": marker, "keep": "value"}, embedded.Rows[0][0])
				rec := makeRequest(t, srv, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}}, token)
				require.Equal(t, http.StatusOK, rec.Code)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Empty(t, response.Errors)
				expected := marker
				if marker == int64(17) {
					expected = float64(17)
				}
				require.Equal(t, map[string]any{"_pathResult": expected, "keep": "value"}, response.Results[0].Data[0].Row[0])
			})
		}
		t.Run(parser+"/graph map", func(t *testing.T) {
			query := "MATCH (a:Issue1327A)-[r:ISSUE1327]->(b:Issue1327B) RETURN {_pathResult:true, nodes:[a,b], rels:[r]} AS m"
			embedded, err := srv.db.ExecuteCypher(t.Context(), query, nil)
			require.NoError(t, err)
			m, ok := embedded.Rows[0][0].(map[string]any)
			require.True(t, ok)
			require.Equal(t, true, m["_pathResult"])
			require.Len(t, m["nodes"], 2)
			require.Len(t, m["rels"], 1)
			rec := makeRequest(t, srv, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": query}}}, token)
			require.Equal(t, http.StatusOK, rec.Code)
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Empty(t, response.Errors)
			got, ok := response.Results[0].Data[0].Row[0].(map[string]any)
			require.True(t, ok)
			require.Equal(t, true, got["_pathResult"])
			require.Len(t, got["nodes"], 2)
			require.Len(t, got["rels"], 1)
		})
	}
}
