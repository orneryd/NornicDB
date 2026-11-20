package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// An HTTP timeout response alone does not prove evaluator work stopped. Hold
// one bounded expression at its third item, observe its actual deadline, and
// join the backend request before checking that no fourth item was evaluated.
func TestNOR357HTTPDeadlineStopsExpressionWorkAfterTimeoutResponse(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previousParser := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previousParser) })
			t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "500ms")
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			name := "zz.nor357http_" + parser
			var calls atomic.Int64
			var armed, missingDeadline, sawCancellation atomic.Bool
			release := make(chan struct{})
			var releaseOnce sync.Once
			probe := func(ctx context.Context, x int64) int64 {
				if _, ok := ctx.Deadline(); !ok {
					missingDeadline.Store(true)
				}
				if calls.Add(1) == 3 && armed.Load() {
					select {
					case <-ctx.Done():
						sawCancellation.Store(true)
					case <-release:
					}
				}
				return x
			}
			handler, err := registry.Compile(probe, registry.AdmissionObservation(probe))
			require.NoError(t, err)
			previousLookup := cypher.PluginFunctionLookup
			cypher.PluginFunctionLookup = func(function string) (*cypher.PluginFunctionHandler, bool) {
				if function == name {
					return handler, true
				}
				if previousLookup != nil {
					return previousLookup(function)
				}
				return nil, false
			}
			t.Cleanup(func() { cypher.PluginFunctionLookup = previousLookup })
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				require.Eventually(t, func() bool { return server.activeTxReqs.Load() == 0 }, 3*time.Second, time.Millisecond,
					"the backend request must join before fixture/plugin teardown")
			})
			query := fmt.Sprintf("WITH $run AS run RETURN reduce(s = 0, x IN $items | s + %s(x)) AS v, run", name)
			request := func(run int64) (int, TransactionResponse) {
				t.Helper()
				response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
					"statements": []map[string]any{{"statement": query, "parameters": map[string]any{
						"items": []int64{1, 2, 3, 4, 5, 6, 7, 8}, "run": run,
					}}},
				}, token)
				var decoded TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded), response.Body.String())
				return response.Code, decoded
			}
			status, healthy := request(1)
			require.Equal(t, http.StatusOK, status)
			require.Empty(t, healthy.Errors)
			require.Equal(t, []any{float64(36), float64(1)}, healthy.Results[0].Data[0].Row)
			require.EqualValues(t, 8, calls.Load())
			require.False(t, missingDeadline.Load())

			calls.Store(0)
			armed.Store(true)
			status, canceled := request(2)
			require.Equal(t, http.StatusServiceUnavailable, status)
			require.NotEmpty(t, canceled.Errors)
			require.Eventually(t, func() bool { return server.activeTxReqs.Load() == 0 }, 2*time.Second, time.Millisecond,
				"503 must not leave the statement running")
			require.True(t, sawCancellation.Load())
			require.False(t, missingDeadline.Load())
			require.EqualValues(t, 3, calls.Load(), "the expression must not evaluate a fourth item after the deadline")

			armed.Store(false)
			calls.Store(0)
			status, retried := request(3)
			require.Equal(t, http.StatusOK, status)
			require.Empty(t, retried.Errors)
			require.Equal(t, []any{float64(36), float64(3)}, retried.Results[0].Data[0].Row)
			require.EqualValues(t, 8, calls.Load(), "distinct run parameters keep the healthy retry observable")
		})
	}
}
