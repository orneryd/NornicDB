// Package queryprofile carries request-owned observation across callback
// registries without a dependency on the Cypher executor or its plan types.
package queryprofile

import "context"

// Observer reports a callback whose complete database access is not visible
// to the caller's reader observations. An observed inner read cannot certify
// the rest of an opaque callback.
type Observer interface {
	// WithLifetime freezes attribution onto lifetime without retaining scope's
	// context or exposing its request values. The returned context must keep
	// lifetime's cancellation and expose the scoped observer through ObserverKey.
	WithLifetime(lifetime context.Context, scope context.Context) context.Context
	UnobservedRead(context.Context, string, string)
	// Access returns this execution's counter for the read it identifies by
	// kind and detail: the same key answers with the same counter, so every
	// reader of one read adds to one record.
	Access(context.Context, string, string) *ReadCounter
}

// ObserverKey is used only by internal context views that route request-owned
// metadata independently of retained statement bindings.
type ObserverKey struct{}

// WithObserver binds the observer to this execution lifetime.
func WithObserver(ctx context.Context, observer Observer) context.Context {
	return context.WithValue(ctx, ObserverKey{}, observer)
}

// FromContext returns this execution's observer, if the request is profiled.
func FromContext(ctx context.Context) Observer {
	observer, _ := ctx.Value(ObserverKey{}).(Observer)
	return observer
}

// OnLifetime transfers observation attribution to independently owned work.
// Request cancellation and request values are not inherited. Ordinary work
// without an observer returns lifetime directly and allocates nothing.
func OnLifetime(lifetime context.Context, scope context.Context) context.Context {
	if observer := FromContext(scope); observer != nil {
		return observer.WithLifetime(lifetime, scope)
	}
	return lifetime
}

// ReadObservation is the read-observation policy a registration declares for
// the database access its implementation performs. It is the one policy type
// every callback registration shares: the Cypher function registry and the
// APOC callable registry both name it, and an admitted callable stores the same
// value.
//
// The zero value is OpaqueReads, so a registration that doesn't state a policy
// is never trusted with an unobserved read.
type ReadObservation uint8

const (
	// OpaqueReads is a callback whose complete database access is not visible
	// to the caller's reader observations: a custom implementation, a plugin
	// closure or a built-in that reads storage through a path that doesn't
	// report logical rows and access units. Every actual invocation is
	// reported through Callback, so a profile never certifies the callback's
	// complete access count from the reads observed around it.
	OpaqueReads ReadObservation = iota
	// CanonicalReads is an implementation whose reads are complete without the
	// opaque-callback hook: it either reads no database state (a pure value
	// function) or reports its own reads through the canonical observation
	// owners. Its actual invocations are not reported as opaque callbacks.
	CanonicalReads
)

// Valid reports whether observation names a declared policy. Every registration
// owner validates through this one owner, so a policy value can't mean one
// thing to a source registration and another to an admitted callable.
func (observation ReadObservation) Valid() bool {
	switch observation {
	case OpaqueReads, CanonicalReads:
		return true
	}
	return false
}

// Callback is called when an actual callback is invoked, not during lookup or
// planning. Ordinary calls allocate no profiling state or formatted messages.
func Callback(ctx context.Context, category, name string) {
	if observer := FromContext(ctx); observer != nil {
		observer.UnobservedRead(ctx, category, name)
	}
}

// Access returns the counter the asking execution records a physical read of
// kind and detail on, or nil when the statement is not profiled. A caller adds
// actual logical rows and access units to the counter it gets; it must not
// supply estimates or count subordinate reads twice.
//
// The counter is the observer's own record for that read, not a copy: a reader
// that resolves it once adds wherever it reads, without another observer
// lookup. Resolving it for an ordinary statement allocates nothing.
func Access(ctx context.Context, kind, detail string) *ReadCounter {
	observer := FromContext(ctx)
	if observer == nil {
		return nil
	}
	return observer.Access(ctx, kind, detail)
}
