package util

import (
	"context"
	"errors"
	"net/http"
)

// ShutdownHTTPServer drains HTTP requests until ctx ends, then closes remaining
// connections. Both standalone MCP and the database HTTP server use this owner.
func ShutdownHTTPServer(ctx context.Context, server *http.Server) error {
	done := make(chan error, 1)
	go func() { done <- server.Shutdown(ctx) }()
	select {
	case err := <-done:
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			_ = server.Close()
		}
		return err
	case <-ctx.Done():
		_ = server.Close()
		return ctx.Err()
	}
}
