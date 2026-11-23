package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A REVEAL execution owns the physical store's exclusive reveal scope. Gate
// that existing owner while an ordinary graph HTTP request is cancelled. The
// request must finish without waiting for unrelated REVEAL work to complete.
// Release and join are unconditional, so an expected failure leaks no work.
func TestNOR71GraphHTTPProjectionCancellationBehindReveal(t *testing.T) {
	for _, endpoint := range []string{"neighborhood", "path"} {
		for _, preCancelled := range []bool{true, false} {
			mode := "cancel-after-start"
			if preCancelled {
				mode = "already-cancelled"
			}
			t.Run(endpoint+"/"+mode, func(t *testing.T) {
				s, _ := setupTestServer(t)
				engine := getDefaultStorage(t, s)
				be, ok := storage.UnwrapEngine(engine).(*storage.BadgerEngine)
				require.True(t, ok, "must exercise the actual Badger reveal mutex")
				// Projection acquires a reveal scope only when decay is enabled.
				// No profile suppresses these nodes; this selects the real scope path.
				be.SetDecayEnabled(true)
				for _, id := range []storage.NodeID{"nor71-a", "nor71-b"} {
					_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"NOR71"}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "nor71-ab", StartNode: "nor71-a", EndNode: "nor71-b", Type: "LINK"}))
				body := `{"node_ids":["nor71-a"],"depth":1}`
				handler := s.handleGraphNeighborhood
				if endpoint == "path" {
					body = `{"source_node_id":"nor71-a","target_node_id":"nor71-b"}`
					handler = s.handleGraphPath
				}
				request := func(ctx context.Context) *http.Request {
					ctx = context.WithValue(ctx, contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}})
					r := httptest.NewRequest(http.MethodPost, "/graph/"+endpoint, strings.NewReader(body)).WithContext(ctx)
					r.SetPathValue("database", s.dbManager.DefaultDatabaseName())
					return r
				}
				// Establish the authenticated public handler and data are healthy.
				control := httptest.NewRecorder()
				handler(control, request(t.Context()))
				require.Equal(t, http.StatusOK, control.Code, control.Body.String())

				release := be.BeginQueryRevealScope(true)
				var once sync.Once
				unlock := func() { once.Do(release) }
				defer unlock()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if preCancelled {
					cancel()
				}
				response := httptest.NewRecorder()
				started, done := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(done)
					close(started)
					handler(response, request(ctx))
				}()
				<-started
				if !preCancelled {
					// Observe this actual HTTP handler waiting in the physical
					// reveal mutex before cancelling. A start signal alone would
					// race with admission and merely retest pre-cancellation.
					waiting := false
					deadline := time.Now().Add(5 * time.Second)
					for time.Now().Before(deadline) {
						for _, trace := range shutdownGoroutines() {
							if strings.Contains(trace, "storage.(*BadgerEngine).beginQueryRevealScope(") && strings.Contains(trace, "server.(*Server).handleGraph") {
								waiting = true
								break
							}
						}
						if waiting {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					if !waiting {
						unlock()
						<-done
						t.Fatal("fixture did not observe the HTTP handler waiting in the reveal mutex; no cancellation conclusion")
					}
				}
				cancel()
				returnedWhileHeld := false
				select {
				case <-done:
					returnedWhileHeld = true
				case <-time.After(time.Second):
				}
				unlock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("handler did not settle after its fixture lock was released")
				}
				if !returnedWhileHeld {
					t.Error("cancelled HTTP graph request remained blocked behind an unrelated exclusive REVEAL scope until that scope was released")
				}
				require.NotEqual(t, http.StatusOK, response.Code, "cancelled work must not publish successful graph rows")
				// A refused request must not poison a subsequent ordinary request.
				healthy := httptest.NewRecorder()
				handler(healthy, request(t.Context()))
				require.Equal(t, http.StatusOK, healthy.Code, healthy.Body.String())
			})
		}
	}
}
