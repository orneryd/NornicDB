package main

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/server"
)

// httpAdapter wraps *server.Server so it satisfies lifecycle.Component.
//
// pkg/server.Server.Start() is NON-BLOCKING (PATTERNS.md confirmed against
// pkg/server/server.go:1637-1700) — it returns after net.Listen binds and
// spawns a goroutine for httpServer.Serve. The lifecycle.Component
// contract requires Start to block until ctx cancellation, so the adapter
// calls srv.Start() and then blocks on <-ctx.Done() (resolves RESEARCH
// Open Question 2).
//
// On Shutdown, srv.Stop(ctx) drains in-flight requests using the
// supervisor-provided fresh ctx (OBS-09).
type httpAdapter struct {
	srv *server.Server
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*httpAdapter)(nil)

// Name returns "http" for supervisor logging.
func (a *httpAdapter) Name() string { return "http" }

// Start binds the HTTP listener and blocks until ctx is cancelled.
// The actual serve loop runs in a goroutine spawned inside srv.Start.
func (a *httpAdapter) Start(ctx context.Context) error {
	// main.go may pre-start HTTP before entering lifecycle.Run so metrics,
	// health, and endpoint summaries are ready before supervision begins.
	// If already listening, do not bind again.
	if a.srv.Addr() == "" {
		if err := a.srv.Start(); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

// Shutdown drains in-flight HTTP requests within the supervisor-provided
// ctx (OBS-09 fresh-context). Per OBS-08, HTTP drains FIRST among the
// supervised components — kubelet stops routing to this pod while bolt
// and embed-workers finish their in-flight work.
func (a *httpAdapter) Shutdown(ctx context.Context) error {
	return a.srv.Stop(ctx)
}
