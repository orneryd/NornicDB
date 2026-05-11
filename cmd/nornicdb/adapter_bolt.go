package main

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// boltAdapter wraps *bolt.Server so it satisfies lifecycle.Component.
//
// pkg/bolt.Server.ListenAndServe() is BLOCKING (PATTERNS.md confirmed
// against pkg/bolt/server.go:777-833): it returns nil on clean shutdown
// (when serve()'s accept loop observes s.closed.Load() == true). On a
// Close() race, the listener.Accept loop returns *net.OpError wrapping
// net.ErrClosed — that's the expected shutdown signal, NOT an error.
// We filter it via errors.Is(err, net.ErrClosed) (resolves RESEARCH
// Open Question 3); without this filter every clean shutdown reports a
// false error to errgroup and prematurely cancels sibling components
// (Pitfall 5 from RESEARCH).
//
// On Shutdown we call srv.Close(); listener.Close() may return its own
// net.ErrClosed if the listener has already been closed — same filter
// applies.
type boltAdapter struct {
	srv *bolt.Server
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*boltAdapter)(nil)

// Name returns "bolt" for supervisor logging.
func (a *boltAdapter) Name() string { return "bolt" }

// Start blocks on ListenAndServe until Close (called from Shutdown)
// causes the accept loop to return. net.ErrClosed (the listener-closed
// signal from a clean shutdown) is filtered to nil so errgroup does not
// interpret a graceful drain as a runtime failure.
func (a *boltAdapter) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- a.srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		// Ensure Ctrl+C can always unblock a blocking ListenAndServe.
		_ = a.srv.Close()
		err := <-errCh
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("bolt: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return fmt.Errorf("bolt: %w", err)
		}
		return nil
	}
}

// Shutdown closes the Bolt listener. A double-close (e.g. ListenAndServe
// already returned because the listener was closed elsewhere) returns
// net.ErrClosed — same filter as Start.
func (a *boltAdapter) Shutdown(ctx context.Context) error {
	if err := a.srv.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
