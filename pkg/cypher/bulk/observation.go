// Package bulk is the statement-work cancellation owner. Work done for a
// query or request observes the statement's cancellation through this
// package only, so there is one place that decides how a canceled or expired
// statement is seen.
package bulk

import "context"

// ObserveContext reports the statement's cancellation: the context's error,
// or nil while it is live.
func ObserveContext(ctx context.Context) error {
	return ctx.Err()
}

// Done is the statement's cancellation signal, for work that waits.
func Done(ctx context.Context) <-chan struct{} {
	return ctx.Done()
}

// Cause is the reason the statement was canceled or expired, or nil while it
// is live.
func Cause(ctx context.Context) error {
	return context.Cause(ctx)
}
