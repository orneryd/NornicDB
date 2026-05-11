package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// ShutdownTimeout bounds the total drain budget across every component.
// Per ADR-0001 §2.8.1 / A10a this MUST be 30s — the OpenTelemetry batch
// span processor needs ~5s to flush, and the remaining headroom covers
// in-flight HTTP/Bolt requests.
const ShutdownTimeout = 30 * time.Second

// Run supervises components until SIGINT/SIGTERM, parent ctx cancellation,
// or the first non-nil error from any Component.Start.
//
// Forward order = startup order.
// Reverse order = drain order (the OBS-08 contract — encoded by the
// caller's slice order).
//
// Run returns errors.Join(runErr, shutdownErr) so callers observe both
// the trigger and any cleanup failures. A clean signal-driven exit
// returns nil.
//
// Critical correctness properties enforced here:
//
//  1. signal.NotifyContext (Go 1.16+) replaces the channel-leak-prone
//     signal.Notify(chan, ...) + <-chan pattern. The deferred stop()
//     releases the signal handler.
//  2. errgroup.WithContext cancels gctx on the FIRST non-nil Start error.
//     Components observe gctx and exit cleanly.
//  3. The shutdown ctx is derived from context.Background(), NOT gctx.
//     gctx is already cancelled by the time we reach the drain loop;
//     deriving from it would return immediately and break the OBS-09
//     fresh-context guarantee.
//  4. Components are drained in REVERSE slice order (OBS-08). DrainReverse
//     continues past per-component Shutdown errors so a single faulty
//     component cannot strand others.
func Run(parent context.Context, components ...Component) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	for _, c := range components {
		c := c // capture loop variable for closure
		g.Go(func() error {
			if err := c.Start(gctx); err != nil {
				return fmt.Errorf("%s: %w", c.Name(), err)
			}
			return nil
		})
	}

	runErr := g.Wait()

	// OBS-09: fresh context derived from Background, NOT the cancelled gctx.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()

	shutdownErr := DrainReverse(shutdownCtx, components)

	return errors.Join(runErr, shutdownErr)
}
