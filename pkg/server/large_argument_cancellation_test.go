package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// cancellationBody places a real CancelFunc at a known transport boundary.
// EOF follows successful body decoding and precedes statement dispatch; it is
// not evidence that a particular parser/procedure checkpoint has been reached.
type cancellationBody struct {
	reader             *bytes.Reader
	first, end         func()
	onceFirst, onceEnd sync.Once
}

func (b *cancellationBody) Read(p []byte) (int, error) {
	b.onceFirst.Do(b.first)
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.onceEnd.Do(b.end)
	}
	return n, err
}
func (*cancellationBody) Close() error { return nil }

// The transaction handler retention counter tracks the actual handler, not
// the timeout response. Waiting for it to retire therefore includes synchronous
// decode/Execute/cleanup, even if the caller already received a timeout.
func TestHTTPLargeArgumentCancellationRetiresHandler(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			router := server.buildRouter()
			argument := strings.Repeat("x", 1700000)
			for _, call := range []struct{ name, query string }{
				{"relationship-fulltext", "CALL db.index.fulltext.queryRelationships('cancel_missing', '" + argument + "')"},
				{"vector-drop", "CALL db.index.vector.drop('" + argument + "')"},
			} {
				for _, boundary := range []string{"body-read", "after-body"} {
					t.Run(call.name+"/"+boundary, func(t *testing.T) {
						payload, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: call.query}}})
						require.NoError(t, err)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						canceled := make(chan time.Time, 1)
						doneCancel := make(chan struct{})
						var once sync.Once
						stop := func() { once.Do(func() { defer close(doneCancel); at := time.Now(); canceled <- at; cancel() }) }
						scheduled := make(chan *time.Timer, 1)
						reader := &cancellationBody{reader: bytes.NewReader(payload), first: func() {}, end: func() {}}
						if boundary == "body-read" {
							reader.first = stop
						} else {
							reader.end = func() { scheduled <- time.AfterFunc(time.Millisecond, stop) }
						}
						req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", reader).WithContext(ctx)
						req.Header.Set("Authorization", token)
						req.Header.Set("Content-Type", "application/json")
						response := httptest.NewRecorder()
						entered := time.Now()
						router.ServeHTTP(response, req)
						responseAt := time.Now()
						if boundary == "after-body" {
							select {
							case timer := <-scheduled:
								defer func() {
									if !timer.Stop() {
										<-doneCancel
									}
								}()
							case <-time.After(5 * time.Second):
								t.Fatal("request never reached the body-decoded cancellation boundary")
							}
						}
						// A fast healthy completion before the scheduled cancellation is a
						// non-reproduction, not proof of cancellation responsiveness.
						select {
						case at := <-canceled:
							<-doneCancel
							watchdog := time.NewTimer(5 * time.Second)
							defer watchdog.Stop()
							tick := time.NewTicker(time.Millisecond)
							defer tick.Stop()
							for server.activeTxReqs.Load() != 0 {
								select {
								case <-tick.C:
								case <-watchdog.C:
									t.Fatal("backend transaction handler did not retire after cancellation")
								}
							}
							retired := time.Now()
							if response.Code == http.StatusOK && response.Body.Len() > 0 {
								var result TransactionResponse
								require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
								require.NotEmpty(t, result.Errors, "canceled request must not publish successful results")
							}
							t.Logf("payload_bytes=%d cancel_to_backend_retirement=%s request_to_response=%s response_to_retirement=%s", len(payload), retired.Sub(at), responseAt.Sub(entered), retired.Sub(responseAt))
						default:
							t.Logf("completed before scheduled cancellation: payload_bytes=%d total=%s; cancellation not exercised", len(payload), responseAt.Sub(entered))
						}
					})
				}
			}
		})
	}
}
