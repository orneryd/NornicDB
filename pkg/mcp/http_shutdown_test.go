package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStopCancellationClosesActiveHTTPConnection(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	defer endpoint.Close()
	defer close(release)
	requestDone := make(chan error, 1)
	go func() {
		response, err := endpoint.Client().Get(endpoint.URL)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not enter the handler")
	}
	server := &Server{httpServer: endpoint.Config}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, server.Stop(ctx), context.Canceled)
	select {
	case err := <-requestDone:
		require.Error(t, err, "cancelled shutdown must close the active connection")
	case <-time.After(time.Second):
		t.Fatal("Stop returned while the HTTP connection remained open")
	}
	require.NoError(t, server.Stop(context.Background()))
}
