package server

import (
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/url"
	"testing"
)

func TestPR1612HTTPCommentAdmission(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, normalize := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalize=%v", normalize), func(t *testing.T) {
			config.SetCypherQueryNormalizationEnabled(normalize)
			server, auth := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, auth, "admin")
			request := func(path string, statements []map[string]any) TransactionResponse {
				t.Helper()
				r := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
				var out TransactionResponse
				require.NoError(t, json.Unmarshal(r.Body.Bytes(), &out))
				return out
			}
			for _, explicit := range []bool{false, true} {
				for _, tc := range []struct{ q, code string }{
					{"CREATE (:R1612Invalid) /* incomplete", "Neo.ClientError.Statement.SyntaxError"},
					{"RETURN 7 /* incomplete", "Neo.ClientError.Statement.SyntaxError"},
					{"/* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
					{"PROFILE /* closed */", "Neo.ClientError.Statement.SyntaxError"},
					{"/* closed */", "Neo.ClientError.Statement.SyntaxError"},
				} {
					t.Run(fmt.Sprintf("explicit=%v/%s", explicit, tc.q), func(t *testing.T) {
						path := "/db/nornic/tx/commit"
						if explicit {
							opened := request("/db/nornic/tx", []map[string]any{})
							require.Empty(t, opened.Errors)
							commit, err := url.Parse(opened.Commit)
							require.NoError(t, err)
							require.NotEmpty(t, commit.Path)
							path = commit.Path
						}
						out := request(path, []map[string]any{{"statement": tc.q}})
						require.Len(t, out.Errors, 1)
						require.Equal(t, tc.code, out.Errors[0].Code)
						require.Empty(t, out.Results)
					})
				}
			}
			out := request("/db/nornic/tx/commit", []map[string]any{{"statement": "MATCH (n:R1612Invalid) RETURN count(n) AS count"}})
			require.Empty(t, out.Errors)
			require.Len(t, out.Results, 1)
			require.Len(t, out.Results[0].Data, 1)
			require.Equal(t, float64(0), out.Results[0].Data[0].Row[0])
		})
	}
}
