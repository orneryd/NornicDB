package main

import (
	"context"
	"net"

	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// boltAdapter wraps *bolt.Server so it satisfies lifecycle.Component.
//
// Run enrolls the native service before launch and lets caller cancellation
// signal the same owner that Shutdown joins. Only an exclusively expected stop
// outcome is filtered; an error joined beside it remains visible.
type boltAdapter struct {
	srv       *bolt.Server
	localizer *localization.Manager
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*boltAdapter)(nil)

// Name returns "bolt" for supervisor logging.
func (a *boltAdapter) Name() string { return "bolt" }

// Start runs the service until it stops or the caller stops waiting. Actual
// completion and its retained outcome remain owned by the native server.
func (a *boltAdapter) Start(ctx context.Context) error {
	err := a.srv.Run(ctx)
	if ctx.Err() != nil && util.ErrorIsOnly(err, ctx.Err()) {
		return nil
	}
	if err != nil && !util.ErrorIsOnly(err, net.ErrClosed) {
		return newCLIError(a.localizer, localization.NornicDBCLIBoltAdapterFailed(err), err)
	}
	return nil
}

// Shutdown signals retirement and waits within the caller's deadline.
func (a *boltAdapter) Shutdown(ctx context.Context) error {
	if err := a.srv.Shutdown(ctx); err != nil && !util.ErrorIsOnly(err, net.ErrClosed) {
		return err
	}
	return nil
}
