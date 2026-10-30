package lifecycle

import (
	"context"
	"errors"
	"fmt"
)

// DrainReverse calls Shutdown on each component in REVERSE slice order
// (last → first), accumulating per-component errors with errors.Join.
//
// One Shutdown returning a non-nil error does NOT abort the loop —
// every remaining component is still given a chance to flush. This is
// the OBS-08 mechanism: drain order is contractual, and a faulty
// component cannot strand the others.
//
// The supplied ctx should be a FRESH context.WithTimeout(
// context.Background(), ShutdownTimeout) per OBS-09; DrainReverse
// itself does not enforce that — Run is the canonical caller and
// constructs the fresh ctx before invoking DrainReverse.
func DrainReverse(ctx context.Context, components []Component) error {
	var shutdownErr error
	for i := len(components) - 1; i >= 0; i-- {
		c := components[i]
		if err := c.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("%s shutdown: %w", c.Name(), err))
		}
	}
	return shutdownErr
}
