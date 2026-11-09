package graphql

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestPredecodeReservationLifetime(t *testing.T) {
	for _, protocol := range []string{"graphql-transport-ws", "graphql-ws"} {
		for _, outcome := range []string{"invalid-init-array", "invalid-init-string", "bad-envelope", "wrong-first-message", "drop-before-init", "drop-partial-init", "cancel-partial-init", "pressure", "capacity", "operation-error", "normal-close"} {
			t.Run(protocol+"/"+outcome, func(t *testing.T) {
				db := testDB(t)
				used := int64(1)
				if outcome == "pressure" {
					used = 512 << 10
				}
				policy := memory.NewPolicy(512<<10, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used}, nil }, time.Now)
				require.NoError(t, policy.Refresh())
				h := newHandler(db, testutil.DatabaseManager(t, db), policy)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				returned := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(returned)
					h.ServeHTTP(w, r.WithContext(ctx))
				}))
				defer server.Close()
				dialer := websocket.Dialer{Subprotocols: []string{protocol}}
				conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
				switch outcome {
				case "drop-before-init":
					_ = conn.Close()
				case "drop-partial-init", "cancel-partial-init":
					writer, err := conn.NextWriter(websocket.TextMessage)
					require.NoError(t, err)
					_, err = writer.Write([]byte(`{"type":"connection_init","payload":{"bulk":"` + strings.Repeat("x", 32<<10)))
					require.NoError(t, err)
					require.Eventually(t, func() bool { return policy.Reserved() > 0 }, time.Second, time.Millisecond)
					if outcome == "cancel-partial-init" {
						cancel()
					} else {
						_ = conn.Close()
					}
				case "normal-close", "operation-error":
					require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"connection_init"}`)))
					var reply map[string]any
					require.NoError(t, conn.ReadJSON(&reply))
					require.Equal(t, "connection_ack", reply["type"])
					if outcome == "operation-error" {
						kind := "subscribe"
						if protocol == "graphql-ws" {
							kind = "start"
						}
						require.NoError(t, conn.WriteJSON(map[string]any{"id": "bad", "type": kind, "payload": []any{}}))
						_, _, _ = conn.ReadMessage()
					}
					_ = conn.Close()
				default:
					payload := []byte(`{"type":"connection_init"}`)
					switch outcome {
					case "invalid-init-array":
						payload = []byte(`{"type":"connection_init","payload":[]}`)
					case "invalid-init-string":
						payload = []byte(`{"type":"connection_init","payload":"bad"}`)
					case "bad-envelope":
						payload = []byte("{")
					case "wrong-first-message":
						payload = []byte(`{"type":"subscribe"}`)
					case "capacity":
						payload = []byte(`{"type":"connection_init","payload":{"bulk":"` + strings.Repeat("x", 300<<10) + `"}}`)
					}
					_ = conn.WriteMessage(websocket.TextMessage, payload)
					if !strings.HasPrefix(outcome, "invalid-init-") {
						_, _, _ = conn.ReadMessage()
					}
				}
				select {
				case <-returned:
				case <-time.After(time.Second):
					_ = conn.Close()
					t.Error("transport must return on cancellation/error/drop")
					select {
					case <-returned:
					case <-time.After(time.Second):
						t.Fatal("transport did not stop after disconnect")
					}
				}
				_ = conn.Close()
				require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond,
					"returned transport must release every reservation; retained %d", policy.Reserved())
			})
		}
	}
}
