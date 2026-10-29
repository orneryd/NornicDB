package util

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// ObserveListenerClose preserves the outcome of a directly owned listener
// before handing its closure to a protocol server. HTTP may replace a listener
// error with its drain deadline, and gRPC's stop API has no error result.
// This adapter observes the existing physical close; it neither schedules nor
// repeats disposal. Accepted connections retain their concrete transport type.
func ObserveListenerClose(listener net.Listener, record func(error)) net.Listener {
	return &observedListener{Listener: listener, record: record}
}

type observedListener struct {
	net.Listener
	record func(error)
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	if err != nil {
		l.record(err)
	}
	return err
}

// ShutdownHTTPServer drains HTTP requests until ctx ends, then closes remaining
// connections. Both standalone MCP and the database HTTP server use this owner.
func ShutdownHTTPServer(ctx context.Context, server *http.Server) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	go func() { done <- server.Shutdown(ctx) }()
	select {
	case err := <-done:
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return errors.Join(err, server.Close())
		}
		return err
	case <-ctx.Done():
		closeErr := server.Close()
		// The caller of this helper owns its actual completion. Server.Stop and
		// MCP.Stop bound their caller separately; neither can abandon this worker.
		return errors.Join(ctx.Err(), closeErr, <-done)
	}
}
