package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/fabricnametest"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCompositeKeywordNamesHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("one"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"}}))
	peer := httptest.NewServer(server.buildRouter())
	t.Cleanup(peer.Close)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	client := &http.Client{Timeout: 10 * time.Second}
	request := func(t *testing.T, method, endpoint, query string, params map[string]interface{}) (TransactionResponse, string) {
		t.Helper()
		statements := []map[string]interface{}{}
		if query != "" {
			statements = append(statements, map[string]interface{}{"statement": query, "parameters": params})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		require.Empty(t, result.Errors, "%s", query)
		return result, response.Header.Get("Location")
	}
	request(t, http.MethodPost, peer.URL+"/db/comp/tx/commit", "USE comp.one CREATE (:KeywordNameSeed {v:7})", nil)
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			for _, tc := range fabricnametest.Cases() {
				t.Run(tc.Name, func(t *testing.T) {
					endpoint := peer.URL + "/db/comp/tx/commit"
					if explicit {
						_, location := request(t, http.MethodPost, peer.URL+"/db/comp/tx", "", nil)
						require.NotEmpty(t, location)
						endpoint = location
						defer request(t, http.MethodDelete, location, "", nil)
					}
					result, _ := request(t, http.MethodPost, endpoint, tc.Query, tc.Params)
					require.Len(t, result.Results, 1)
					require.Equal(t, []string{"k"}, result.Results[0].Columns)
					require.Len(t, result.Results[0].Data, 1)
					require.Equal(t, []interface{}{float64(7)}, result.Results[0].Data[0].Row)
				})
			}
		})
	}
}
