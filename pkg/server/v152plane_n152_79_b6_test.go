package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

// Exercise authenticated HTTP and assert a structured runtime failure, rather
// than deriving transport behavior solely from the status classifier.
func TestNOR358HTTPClosedStoreReadStatus(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			server, authenticator := setupTestServer(t)
			auth := "Bearer " + getAuthToken(t, authenticator, "admin")
			payload := func(query string) map[string]interface{} {
				return map[string]interface{}{"statements": []map[string]interface{}{{"statement": query}}}
			}
			healthy := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", payload("CREATE (:Doc {id:'a'})"), auth)
			require.Equal(t, http.StatusOK, healthy.Code)
			var good struct {
				Errors []interface{} `json:"errors"`
			}
			require.NoError(t, json.Unmarshal(healthy.Body.Bytes(), &good))
			require.Empty(t, good.Errors)
			require.NoError(t, server.db.GetBaseStorageForManager().Close())
			for _, query := range []string{"MATCH (n:Doc) RETURN count(n) AS c", "MATCH (n:Doc {id:'a'}) RETURN n.id AS id"} {
				t.Run(query, func(t *testing.T) {
					response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", payload(query), auth)
					t.Logf("HTTP %d body=%s", response.Code, response.Body.String())
					require.NotEmpty(t, response.Body.Bytes(), "runtime failure must include its structured error")
					var body struct {
						Errors []struct {
							Code    string `json:"code"`
							Message string `json:"message"`
						} `json:"errors"`
					}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
					require.Len(t, body.Errors, 1)
					require.Contains(t, body.Errors[0].Message, "storage closed")
					require.Equal(t, nornicerrors.TransientDatabaseUnavailable, body.Errors[0].Code)
				})
			}
		})
	}
}
