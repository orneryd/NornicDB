package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestHTTPProjectionGeneratedMatrix(t *testing.T) {
	testHTTPGroupingMatrix396(t, statementfactstest.ProjectionCases(), statementfactstest.NewProjectionOracle(t))
}

func testHTTPGroupingMatrix396(t *testing.T, samples []statementfactstest.Case, oracle *statementfactstest.ProjectionOracle) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	run := func(t *testing.T, path, query string) (TransactionResponse, string) {
		statements := []map[string]any{}
		if query != "" {
			statements = append(statements, map[string]any{"statement": query})
		}
		response := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
		require.Less(t, response.Code, 400)
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		return result, response.Header().Get("Location")
	}
	if oracle != nil {
		seed, _ := run(t, "/db/nornic/tx/commit", "CREATE (:ProjectionSemanticSentinel)")
		require.Empty(t, seed.Errors)
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range samples {
				for _, explicit := range []bool{false, true} {
					for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/explicit=%v/mode=%s", sample.Name, explicit, prefix), func(t *testing.T) {
							referenceRows := oracle.Check(t, sample, prefix, explicit)
							target := "/db/nornic/tx/commit"
							if explicit {
								opened, location := run(t, "/db/nornic/tx", "")
								require.Empty(t, opened.Errors)
								parsed, err := url.Parse(location)
								require.NoError(t, err)
								target = parsed.Path
								defer makeRequest(t, server, http.MethodDelete, target, nil, token)
							}
							result, _ := run(t, target, prefix+sample.Query)
							if sample.Code != "" {
								require.Len(t, result.Errors, 1)
								require.Equal(t, sample.Code, result.Errors[0].Code)
							} else {
								require.Empty(t, result.Errors)
								require.Len(t, result.Results, 1)
								require.Equal(t, sample.Columns, result.Results[0].Columns)
								rows := make([][]any, len(result.Results[0].Data))
								for index, row := range result.Results[0].Data {
									rows[index] = row.Row
								}
								oracle.CheckRows(t, prefix, referenceRows, rows)
							}
						})
					}
				}
			}
		})
	}
}

func TestHTTPProjectionPermutationMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.ProjectionPermutationCases())
	require.Len(t, samples, 1344)
	testHTTPGroupingMatrix396(t, samples, oracle)
}

func TestHTTPNestedScopeMatrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScopeCases())
	testHTTPGroupingMatrix396(t, samples, oracle)
}

func TestHTTPNestedScope25Matrix(t *testing.T) {
	samples, oracle := statementfactstest.NewProjectionPermutationOracle(t, statementfactstest.NestedScope25Cases())
	testHTTPGroupingMatrix396(t, samples, oracle)
}
