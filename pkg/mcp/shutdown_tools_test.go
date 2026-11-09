package mcp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStopDrainsInFlightToolRequests(t *testing.T) {
	for _, method := range []string{"list", "call"} {
		t.Run(method, func(t *testing.T) {
			s := NewServer(nil, nil)
			require.NoError(t, s.RegisterTool(Tool{Name: "probe"}, func(context.Context, map[string]interface{}) (interface{}, error) { return "ok", nil }))
			entered := make(chan struct{})
			stopping := make(chan struct{})
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-stopping
				if method == "list" {
					s.doListTools(r.Context())
				} else {
					_, err := s.CallTool(r.Context(), "probe", nil)
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			s.httpServer = ts.Config
			s.httpServer.RegisterOnShutdown(func() { close(stopping) })
			ts.Start()
			defer ts.Close()
			requestDone := make(chan error, 1)
			go func() {
				resp, err := ts.Client().Get(ts.URL)
				if err == nil {
					_, err = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				requestDone <- err
			}()
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, s.Stop(ctx))
			require.NoError(t, <-requestDone)
			require.NoError(t, s.Stop(ctx))
		})
	}
}
