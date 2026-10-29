package observability

// Author: Codex (#1636 lane). Public component/HTTP schedules for the command
// service owner class; the handler and listener are transparent test seams.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/stretchr/testify/require"
)

type commandClassListener struct {
	net.Listener
	failure error
}

func (l *commandClassListener) Close() error {
	return errors.Join(l.Listener.Close(), net.ErrClosed, l.failure)
}

func commandClassService(t *testing.T, kind string, closeFailure error) (lifecycle.Component, *http.Server, string) {
	t.Helper()
	var component lifecycle.Component
	var server *http.Server
	var listener net.Listener
	if kind == "telemetry" {
		cfg := ObservabilityConfig{Metrics: MetricsConfig{Listen: "127.0.0.1:0"}}
		provider, err := New(context.Background(), cfg, ServiceInfo{Name: "r4-command-class"}, nil, nil)
		require.NoError(t, err)
		service, err := NewTelemetryListener(provider, NewHealth())
		require.NoError(t, err)
		component, server, listener = service, service.srv, service.ln
		if closeFailure != nil {
			service.ln = &commandClassListener{Listener: listener, failure: closeFailure}
		}
	} else {
		service, err := NewPprofListener(PprofConfig{Enabled: true, Listen: "127.0.0.1:0"})
		require.NoError(t, err)
		component, server, listener = service, service.srv, service.ln
		if closeFailure != nil {
			service.ln = &commandClassListener{Listener: listener, failure: closeFailure}
		}
	}
	return component, server, "http://" + listener.Addr().String()
}

func TestOwnerClassCommandServiceActualCompletion(t *testing.T) {
	for _, kind := range []string{"telemetry", "pprof"} {
		t.Run(kind, func(t *testing.T) {
			service, server, address := commandClassService(t, kind, nil)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				w.WriteHeader(http.StatusNoContent)
			})
			started := make(chan error, 1)
			go func() { started <- service.Start(context.Background()) }()
			client := &http.Client{Timeout: 5 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			request := make(chan error, 1)
			go func() {
				response, err := client.Get(address)
				if response != nil {
					err = errors.Join(err, response.Body.Close())
				}
				request <- err
			}()
			t.Cleanup(func() {
				finish()
				if err := service.Shutdown(context.Background()); err != nil {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			})
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("public HTTP request did not enter the owned handler")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, service.Shutdown(ctx), context.DeadlineExceeded)
			returnedEarly := false
			select {
			case err := <-started:
				returnedEarly = true
				t.Errorf("component lifetime ended before its actual handler returned: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			finish()
			requestErr := <-request
			if returnedEarly {
				require.NoError(t, requestErr) // unchanged old-product failure path
			} else if requestErr != nil {
				// A deadline can close the native connection while its handler
				// remains retained. Actual handler completion is asserted below.
				require.ErrorIs(t, requestErr, io.EOF)
			}
			if !returnedEarly {
				select {
				case err := <-started:
					if err != nil {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("component did not complete after its actual handler returned")
				}
			}
		})
	}
}

func TestOwnerClassCommandServiceRetainsNativeOutcome(t *testing.T) {
	for _, kind := range []string{"telemetry", "pprof"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("r4 command service listener close failure")
			service, server, address := commandClassService(t, kind, failure)
			server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan error, 1)
			go func() { started <- service.Start(ctx) }()
			client := &http.Client{Timeout: 2 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			response, err := client.Get(address)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			cancel()
			select {
			case err := <-started:
				require.ErrorIs(t, err, failure, "Start discarded its owned shutdown outcome")
			case <-time.After(2 * time.Second):
				t.Fatal("component did not retire after stop")
			}
		})
	}
}

type commandClassSyncWriter struct{ failure error }

func (w commandClassSyncWriter) Write(p []byte) (int, error) { return io.Discard.Write(p) }
func (w commandClassSyncWriter) Sync() error                 { return w.failure }

func TestOwnerClassCommandServiceProviderOutcome(t *testing.T) {
	failure := errors.New("r4 command provider final writer sync failure")
	provider, err := New(context.Background(), ObservabilityConfig{}, ServiceInfo{Name: "r4-command-provider"}, nil, commandClassSyncWriter{failure: failure})
	require.NoError(t, err)
	require.ErrorIs(t, provider.Shutdown(context.Background()), failure, "provider discarded its final Sync outcome")
}

func TestOwnerClassCommandServiceBorrowedStream(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()); require.NoError(t, writer.Close()) })
	provider, err := New(context.Background(), ObservabilityConfig{}, ServiceInfo{Name: "r4-borrowed-stream"}, nil, writer)
	require.NoError(t, err)
	require.NoError(t, provider.Shutdown(context.Background()), "a borrowed pipe has no durable Sync operation")
}
