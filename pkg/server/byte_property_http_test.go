package server

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestBytePropertyHTTPRoundTrip(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	// HTTP JSON has no byte parameter type; a value written through an embedded
	// typed binding must still be returned as signed bytes over HTTP.
	_, err := server.db.ExecuteCypher(t.Context(), "MERGE (n:ByteHTTP) SET n += $m", map[string]any{"m": map[string]any{"v": []byte{0, 127, 128, 255}}})
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		endpoint := "/db/nornic/tx/commit"
		if explicit {
			opened := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
			var tx TransactionResponse
			require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &tx))
			endpoint = tx.Commit
		}
		response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []any{map[string]any{"statement": "MATCH (n:ByteHTTP) SET n += $m RETURN n.v, n", "parameters": map[string]any{"m": map[string]any{"flag": true}}, "resultDataContents": []string{"row", "graph"}}}}, token)
		var result map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Empty(t, result["errors"])
		data := result["results"].([]any)[0].(map[string]any)["data"].([]any)[0].(map[string]any)
		row := data["row"].([]any)
		want := []any{float64(0), float64(127), float64(-128), float64(-1)}
		require.Equal(t, want, row[0])
		require.Equal(t, want, row[1].(map[string]any)["v"])
		require.Equal(t, want, data["graph"].(map[string]any)["nodes"].([]any)[0].(map[string]any)["properties"].(map[string]any)["v"])
	}
}
