package graphql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestGraphQLHTTPReservationComposition(t *testing.T) {
	for _, outcome := range []string{"GET-large", "POST-large", "POST-chunked-large", "headers-large", "GET-small", "POST-small", "GET-invalid", "POST-invalid", "graphql-transport-ws", "graphql-ws"} {
		t.Run(outcome, func(t *testing.T) {
			db := testDB(t)
			limit := int64(128 << 10)
			upgraded := outcome == "graphql-transport-ws" || outcome == "graphql-ws"
			if upgraded {
				limit = 512 << 10
			}
			policy := memory.NewPolicy(limit, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			h := newHandler(db, testutil.DatabaseManager(t, db), policy)
			returned := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				if !upgraded {
					h.ServeHTTP(w, r)
					return
				}
				// Model the already-admitted HTTP layer. The optional interface
				// also replays the pre-fix head, which had no scope propagation.
				wire := memory.NewWireRequest(policy)
				_ = wire.Run(func() error {
					if err := wire.Grow(memory.HTTPRequestWireBytes(r)); err != nil {
						return err
					}
					if binding, ok := any(wire).(interface {
						Context(context.Context) context.Context
					}); ok {
						r = r.WithContext(binding.Context(r.Context()))
					}
					h.ServeHTTP(w, r)
					return nil
				})
			}))
			defer server.Close()
			if upgraded {
				dialer := websocket.Dialer{Subprotocols: []string{outcome}}
				conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?auth=wire", nil)
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
				require.NoError(t, conn.WriteJSON(map[string]any{"type": "connection_init"}))
				var reply map[string]any
				require.NoError(t, conn.ReadJSON(&reply))
				require.Equal(t, "connection_ack", reply["type"])
				require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond, "idle upgrade must not retain the HTTP metadata reservation")
				_ = conn.Close()
			} else {
				method := http.MethodGet
				payload := `{"query":"{__typename}","variables":{"bulk":"x"}}`
				query := url.Values{"query": {"{__typename}"}, "variables": {`{"bulk":"x"}`}}
				if strings.Contains(outcome, "large") {
					payload = `{"query":"{__typename}","variables":{"bulk":"` + strings.Repeat("x", 100<<10) + `"}}`
					query.Set("variables", `{"bulk":"`+strings.Repeat("x", 100<<10)+`"}`)
				}
				if outcome == "GET-invalid" {
					query.Set("variables", "{invalid")
				}
				if outcome == "POST-invalid" {
					payload = "{"
				}
				var body io.Reader
				path := server.URL + "?" + query.Encode()
				if strings.HasPrefix(outcome, "POST") || outcome == "headers-large" {
					method, path, body = http.MethodPost, server.URL, strings.NewReader(payload)
				}
				request, err := http.NewRequest(method, path, body)
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				if outcome == "POST-chunked-large" {
					request.ContentLength = -1
				}
				if outcome == "headers-large" {
					for i := 0; i < 30; i++ {
						request.Header.Set("X-Wire-"+strings.Repeat("a", i+1), strings.Repeat("x", 3500))
					}
				}
				response, err := server.Client().Do(request)
				require.NoError(t, err)
				data, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				require.NoError(t, err)
				if strings.Contains(outcome, "large") {
					require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(data))
					require.Contains(t, string(data), websocketMemoryErrorCode)
				} else if strings.Contains(outcome, "invalid") {
					require.Equal(t, http.StatusBadRequest, response.StatusCode, string(data))
				} else {
					require.Equal(t, http.StatusOK, response.StatusCode, string(data))
				}
			}
			select {
			case <-returned:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not return")
			}
			require.Zero(t, policy.Reserved())
		})
	}
}
