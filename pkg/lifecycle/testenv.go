package lifecycle

import (
	"context"
	"sync/atomic"
	"time"
)

// orderSeq is a process-wide monotonically increasing sequence used to
// stamp Start/Shutdown invocations so ordering assertions are deterministic
// even when multiple FakeComponents are exercised within the same nanosecond
// (the case on machines whose monotonic clock advances coarser than the
// drain loop). A wall-clock UnixNano alone is not strictly increasing on
// fast loops; this counter is.
var orderSeq atomic.Int64

func nextSeq() int64 { return orderSeq.Add(1) }

// FakeComponent is a deterministic test fixture implementing Component.
// It records start/shutdown call counts and monotonic timestamps so tests
// can assert ordering invariants (e.g. reverse-drain order).
//
// FakeComponent lives in production source rather than _test.go so that
// downstream packages — pkg/observability adapters in Plan 01-03,
// integration tests in Plan 01-04 — can reuse it without duplicating
// the fixture. This mirrors the colocated `MemoryEngine` pattern in
// pkg/storage/memory.go.
//
// All counters/timestamps are atomic; the struct is safe for concurrent
// use across the supervisor's errgroup goroutines.
type FakeComponent struct {
	name string

	// OnStart, when non-nil, replaces the default Start behavior
	// (which blocks on <-ctx.Done() and returns nil).
	OnStart func(ctx context.Context) error

	// OnShutdown, when non-nil, replaces the default Shutdown behavior
	// (which returns nil immediately).
	OnShutdown func(ctx context.Context) error

	startCount    atomic.Int32
	shutdownCount atomic.Int32
	startedAt     atomic.Int64 // UnixNano of the FIRST Start invocation (wall-clock; informational)
	shutdownAt    atomic.Int64 // UnixNano of the FIRST Shutdown invocation (wall-clock; informational)
	startSeq      atomic.Int64 // monotonic sequence stamped on FIRST Start (used for ordering)
	shutdownSeq   atomic.Int64 // monotonic sequence stamped on FIRST Shutdown (used for ordering)
}

// Compile-time interface assertion.
var _ Component = (*FakeComponent)(nil)

// NewFakeComponent returns a fresh FakeComponent with the given name.
// Override OnStart / OnShutdown on the returned pointer to inject test
// behavior.
func NewFakeComponent(name string) *FakeComponent {
	return &FakeComponent{name: name}
}

// Name implements Component.
func (f *FakeComponent) Name() string { return f.name }

// Start records the invocation timestamp + count, then either calls the
// caller-supplied OnStart override or blocks on ctx.
//
// Order matters: startedAt + startSeq are stamped BEFORE startCount is
// incremented, so any reader polling Eventually(StartCount()==1) and then
// reading StartedAtNanos()/startSeq is guaranteed to observe the stamped
// values. Inverting the order causes a 40%-repro race under -count=10.
func (f *FakeComponent) Start(ctx context.Context) error {
	if f.startedAt.CompareAndSwap(0, time.Now().UnixNano()) {
		f.startSeq.Store(nextSeq())
	}
	f.startCount.Add(1)
	if f.OnStart != nil {
		return f.OnStart(ctx)
	}
	<-ctx.Done()
	return nil
}

// Shutdown records the invocation timestamp + count, then either calls
// the caller-supplied OnShutdown override or returns nil.
//
// Order matters: shutdownAt + shutdownSeq are stamped BEFORE shutdownCount
// is incremented (same reasoning as Start above).
func (f *FakeComponent) Shutdown(ctx context.Context) error {
	if f.shutdownAt.CompareAndSwap(0, time.Now().UnixNano()) {
		f.shutdownSeq.Store(nextSeq())
	}
	f.shutdownCount.Add(1)
	if f.OnShutdown != nil {
		return f.OnShutdown(ctx)
	}
	return nil
}

// StartCount returns how many times Start has been entered.
func (f *FakeComponent) StartCount() int32 { return f.startCount.Load() }

// ShutdownCount returns how many times Shutdown has been entered.
func (f *FakeComponent) ShutdownCount() int32 { return f.shutdownCount.Load() }

// StartedAtNanos returns the UnixNano timestamp of the first Start
// invocation, or 0 if Start has not yet been called.
func (f *FakeComponent) StartedAtNanos() int64 { return f.startedAt.Load() }

// ShutdownAtNanos returns the UnixNano timestamp of the first Shutdown
// invocation, or 0 if Shutdown has not yet been called.
func (f *FakeComponent) ShutdownAtNanos() int64 { return f.shutdownAt.Load() }

// StartedBefore reports whether this component's Start was entered
// strictly before other's. Uses a monotonic process-wide sequence counter
// to remain deterministic when consecutive Starts land within the same
// wall-clock nanosecond. Returns false if either component has not
// recorded a Start.
func (f *FakeComponent) StartedBefore(other *FakeComponent) bool {
	a, b := f.startSeq.Load(), other.startSeq.Load()
	if a == 0 || b == 0 {
		return false
	}
	return a < b
}

// ShutdownBefore reports whether this component's Shutdown was entered
// strictly before other's. Uses a monotonic process-wide sequence counter
// to remain deterministic when consecutive Shutdowns land within the same
// wall-clock nanosecond (e.g. the supervisor's tight reverse-drain loop).
// Returns false if either component has not recorded a Shutdown.
func (f *FakeComponent) ShutdownBefore(other *FakeComponent) bool {
	a, b := f.shutdownSeq.Load(), other.shutdownSeq.Load()
	if a == 0 || b == 0 {
		return false
	}
	return a < b
}
