package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericWireGraphHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	store, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "wire-a", Labels: []string{"NumericWire"}, Properties: map[string]interface{}{"big": int64(9007199254740993), "v": float64(2), "nested": []interface{}{float64(3)}}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "wire-b", Labels: []string{"NumericWire"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "wire-r", StartNode: "wire-a", EndNode: "wire-b", Type: "WIRE", Properties: map[string]interface{}{"v": float64(4)}}))
	host := httptest.NewServer(server.buildRouter())
	t.Cleanup(host.Close)
	request, err := http.NewRequest(http.MethodPost, host.URL+"/nornicdb/graph/"+database+"/neighborhood", strings.NewReader(`{"node_ids":["wire-a"],"depth":1}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := host.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var payload struct {
		Nodes []struct{ Properties map[string]interface{} }
		Edges []struct{ Properties map[string]interface{} }
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber() // Independent lexical oracle for the public HTTP response.
	require.NoError(t, decoder.Decode(&payload))
	found := false
	for _, node := range payload.Nodes {
		if big, ok := node.Properties["big"]; ok {
			found = true
			require.Equal(t, json.Number("9007199254740993"), big)
			require.Equal(t, json.Number("2.0"), node.Properties["v"])
			require.Equal(t, []interface{}{json.Number("3.0")}, node.Properties["nested"])
		}
	}
	require.True(t, found)
	require.Len(t, payload.Edges, 1)
	require.Equal(t, json.Number("4.0"), payload.Edges[0].Properties["v"])
}

func TestNumericWireOrdinaryHTTPResponse(t *testing.T) {
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpresponse.WritePropertyJSON(w, http.StatusOK, struct {
			Value  float64                `json:"value"`
			Nested map[string]interface{} `json:"nested"`
		}{2, map[string]interface{}{"v": float64(3)}})
	}))
	defer host.Close()
	response, err := http.Get(host.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	var body map[string]interface{}
	require.NoError(t, valueconvert.CypherInputJSON.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, float64(2), body["value"])
	require.Equal(t, map[string]interface{}{"v": float64(3)}, body["nested"])
}
