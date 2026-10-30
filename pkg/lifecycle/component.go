package lifecycle

import "context"

// Component is anything the supervisor can start concurrently and shut
// down on demand.
//
// Start blocks until the component exits naturally or ctx is cancelled.
// Returning a non-nil error from Start cancels the entire errgroup and
// triggers the reverse-order drain.
//
// Shutdown is invoked once Start has returned for ALL components, in
// reverse registration order. The ctx passed to Shutdown has a fresh
// ~30s deadline whose lifetime is INDEPENDENT of the cancelled supervisor
// context (ADR-0001 §2.8.1 A10a / OBS-09). Implementations must honor
// the deadline but should NOT assume it is the same ctx that cancelled
// Start.
//
// Name is used to tag wrapped errors and enable structured logging.
// It must be stable for the lifetime of the component.
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
