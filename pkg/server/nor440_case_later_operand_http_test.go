package server

// NOR-440 reproduction over the real HTTP transport: arithmetic whose CASE …
// END is not the first operand and is not in parentheses, with an operator
// inside the CASE that belongs to the CASE.
//
// Every row is posted to /db/nornic/tx/commit through the server router on both
// parsers, and asserts the product contract (the Neo4j 5.26 answer). Rows are
// reproduction only: they fail while the defect is present and none pins the
// current answer. The statement classes mirror the embedded suite so the same
// measured trigger family is exercised on both public paths.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/caseinputs"
	"github.com/stretchr/testify/require"
)

// TestNOR440CaseLaterOperandHTTP measures the same trigger family and controls
// over the HTTP transaction endpoint on both parsers.
func TestNOR440CaseLaterOperandHTTP(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		parser := parser
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())

			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")

			for _, tc := range caseinputs.Cases() {
				tc := tc
				t.Run(tc.Class+"/"+tc.ID, func(t *testing.T) {
					// Isolate graph data while reusing the authenticated server fixture.
					cleared := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "MATCH (n) DETACH DELETE n"}}}, token)
					require.Contains(t, []int{http.StatusOK, http.StatusCreated}, cleared.Code, cleared.Body.String())
					var clearResult TransactionResponse
					require.NoError(t, json.Unmarshal(cleared.Body.Bytes(), &clearResult))
					require.Empty(t, clearResult.Errors, cleared.Body.String())
					statements := make([]map[string]any, 0, len(tc.Setup)+1)
					for _, statement := range tc.Setup {
						statements = append(statements, map[string]any{"statement": statement})
					}
					measured := map[string]any{"statement": tc.Query}
					if len(tc.Params) > 0 {
						measured["parameters"] = tc.Params
					}
					statements = append(statements, measured)

					response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit",
						map[string]any{"statements": statements}, token)
					require.Containsf(t, []int{http.StatusOK, http.StatusCreated}, response.Code,
						"%s: HTTP status %d", tc.ID, response.Code)

					var decoded TransactionResponse
					decoder := json.NewDecoder(bytes.NewReader(response.Body.Bytes()))
					decoder.UseNumber()
					require.NoErrorf(t, decoder.Decode(&decoded), "%s: response %s", tc.ID, response.Body.String())
					require.Emptyf(t, decoded.Errors, "%s: %s", tc.ID, tc.Query)
					require.Lenf(t, decoded.Results, len(statements), "%s: %s", tc.ID, tc.Query)

					rows := make([][]any, 0, len(decoded.Results[len(decoded.Results)-1].Data))
					for _, row := range decoded.Results[len(decoded.Results)-1].Data {
						normalized := make([]any, len(row.Row))
						for i, value := range row.Row {
							normalized[i] = value
						}
						rows = append(rows, normalized)
					}
					if tc.WantRows != nil {
						nor440HTTPAssertJSONRows(t, tc.WantRows, rows, tc.ID, parser, tc.Query)
						return
					}
					require.Lenf(t, rows, 1, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
					require.Lenf(t, rows[0], 1, "%s [parser=%s]: %s", tc.ID, parser, tc.Query)
					nor440HTTPAssertJSONRows(t, [][]any{{tc.Want}}, rows, tc.ID, parser, tc.Query)
				})
			}
		})
	}
}

// HTTP JSON has no INTEGER/FLOAT tag for a number such as8. Compare exact
// small-domain JSON values rather than inventing a Go numeric type at the wire.
func nor440HTTPAssertJSONRows(t *testing.T, want, got [][]any, id, parser, query string) {
	t.Helper()
	expected, err := json.Marshal(want)
	require.NoError(t, err)
	actual, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual), "%s [parser=%s]: %s", id, parser, query)
}
