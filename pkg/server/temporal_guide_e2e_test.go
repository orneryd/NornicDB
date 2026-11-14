package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/temporalguide"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Real loopback HTTP, authentication, transaction endpoint, query execution and
// native storage: the same published statements used by executor unit tests.
func TestTemporalGuideHTTPEndToEnd(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, scenario := range []struct {
				name string
				run  func(*testing.T, temporalguide.Runner)
			}{{"ownership", temporalguide.Ownership}, {"generic ledger", temporalguide.GenericLedger}, {"constraint blocks", temporalguide.Contracts}, {"retained history", temporalguide.History}, {"epoch bounds", temporalguide.EpochBounds}, {"downloadable script", temporalguide.OwnershipScript}, {"cost declarations", temporalguide.CostExamples}} {
				t.Run(scenario.name, func(t *testing.T) {
					f := newTemporalHistoryHTTPFixture(t)
					network := httptest.NewServer(f.server.buildRouter())
					t.Cleanup(network.Close)
					client := network.Client()
					scenario.run(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
						body, err := json.Marshal(map[string]interface{}{"statements": []interface{}{map[string]interface{}{"statement": query, "parameters": params}}})
						if err != nil {
							return nil, err
						}
						request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, network.URL+f.endpoint, bytes.NewReader(body))
						if err != nil {
							return nil, err
						}
						request.Header.Set("Authorization", f.token)
						request.Header.Set("Content-Type", "application/json")
						response, err := client.Do(request)
						if err != nil {
							return nil, err
						}
						defer response.Body.Close()
						var decoded TransactionResponse
						if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
							return nil, err
						}
						if len(decoded.Errors) > 0 {
							return nil, fmt.Errorf("%s: %s", decoded.Errors[0].Code, decoded.Errors[0].Message)
						}
						require.Equal(t, http.StatusOK, response.StatusCode)
						var rows [][]interface{}
						for _, result := range decoded.Results {
							for _, data := range result.Data {
								rows = append(rows, data.Row)
							}
						}
						return rows, nil
					})
				})
			}
		})
	}
}
