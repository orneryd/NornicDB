package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNOR167EscapedUnwindHeadersHTTP checks the three source-escaped alias
// forms through the Neo4j-compatible HTTP transaction endpoint on both parsers.
func TestNOR167EscapedUnwindHeadersHTTP(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			escapedIdentifier := `a\u0062`
			for _, tc := range []struct {
				query string
				rows  [][]any
			}{
				{"UNWIND ['value'] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", [][]any{{"value"}}},
				{"UNWIND [] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", nil},
				{"RETURN 1 AS `" + escapedIdentifier + "`", [][]any{{float64(1)}}},
			} {
				assert.True(t, strings.Contains(tc.query, `\u0062`), "query must contain the literal Unicode escape: %q", tc.query)
				httpResponse := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
					"statements": []map[string]any{{"statement": tc.query}},
				}, token)
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, httpResponse.Code, tc.query)
				var decoded struct {
					Results []struct {
						Columns []string `json:"columns"`
						Data    []struct {
							Row []any `json:"row"`
						} `json:"data"`
					} `json:"results"`
					Errors []any `json:"errors"`
				}
				if !assert.NoError(t, json.Unmarshal(httpResponse.Body.Bytes(), &decoded), tc.query) {
					continue
				}
				if !assert.Empty(t, decoded.Errors, tc.query) || !assert.Len(t, decoded.Results, 1, tc.query) {
					continue
				}
				assert.Equal(t, []string{"ab"}, decoded.Results[0].Columns, tc.query)
				if tc.rows == nil {
					assert.Empty(t, decoded.Results[0].Data, tc.query)
				} else {
					rows := make([][]any, len(decoded.Results[0].Data))
					for i, row := range decoded.Results[0].Data {
						rows[i] = row.Row
					}
					assert.Equal(t, tc.rows, rows, tc.query)
				}
				t.Logf("statement=%q columns=%v rows=%v", tc.query, decoded.Results[0].Columns, decoded.Results[0].Data)
			}
		})
	}
}
