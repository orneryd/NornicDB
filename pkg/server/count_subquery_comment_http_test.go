package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// HTTP's existing successful route is a control for the Bolt regression:
// request serialization, routing, authentication, and both transaction modes
// must preserve the same COUNT-subquery value under both parser settings.
func TestHTTPCountSubqueryCommentGap(t *testing.T) {
	previousParser := config.GetParserType()
	previousNormalization := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() {
		config.SetParserType(previousParser)
		config.SetCypherQueryNormalizationEnabled(previousNormalization)
	})
	srv, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	client := peer.Client()
	client.Timeout = 10 * time.Second
	database := srv.dbManager.DefaultDatabaseName()
	request := func(t *testing.T, path, query string, empty bool) (TransactionResponse, int) {
		t.Helper()
		statements := []map[string]interface{}{}
		if !empty {
			statements = append(statements, map[string]interface{}{"statement": query})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, peer.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var payload TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
		require.Empty(t, payload.Errors)
		return payload, response.StatusCode
	}
	for _, parser := range []string{"nornic", "antlr"} {
		for _, normalize := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, comment := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/normalize=%t/explicit=%t/comment=%t", parser, normalize, explicit, comment), func(t *testing.T) {
						config.SetParserType(parser)
						config.SetCypherQueryNormalizationEnabled(normalize)
						path := "/db/" + database + "/tx/commit"
						if explicit {
							opened, status := request(t, "/db/"+database+"/tx", "", true)
							require.Equal(t, http.StatusCreated, status)
							commitURL, err := url.Parse(opened.Commit)
							require.NoError(t, err)
							require.NotEmpty(t, commitURL.Path)
							path = strings.TrimSuffix(commitURL.Path, "/commit")
						}
						gap := " "
						if comment {
							gap = " /*gap*/ "
						}
						payload, status := request(t, path, "RETURN (COUNT"+gap+"{ LIMIT 1 RETURN 1 AS x }) AS v", false)
						require.Equal(t, http.StatusOK, status)
						require.Len(t, payload.Results, 1)
						require.Equal(t, []string{"v"}, payload.Results[0].Columns)
						require.Len(t, payload.Results[0].Data, 1)
						require.Equal(t, []interface{}{float64(1)}, payload.Results[0].Data[0].Row)
						if explicit {
							_, status = request(t, path+"/commit", "", true)
							require.Equal(t, http.StatusOK, status)
						}
					})
				}
			}
		}
	}
}
