// Package testfixture owns test resources without importing their packages.
package testfixture

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// Owner is the cleanup boundary provided by testing.T, testing.B or Boundary.
// Errorf records a teardown failure at that boundary, so no fixture can close
// without its outcome being reported.
type Owner interface {
	Helper()
	Cleanup(func())
	Errorf(string, ...any)
}

// Closer is a concrete fixture whose lifetime belongs to its test.
type Closer interface {
	Close() error
}

// CloseFunc lets the owner close a release that is not a plain Close, such as
// a driver's Close(ctx) or a server's Stop(ctx), with the same order,
// once-only close and reporting as any other fixture.
type CloseFunc func() error

// Close runs the release.
func (release CloseFunc) Close() error { return release() }

// Redacted closes resource but reports a failure by label only, for fixtures
// whose close errors could disclose credentials.
func Redacted(label string, resource Closer) Closer {
	return redactedCloser{label: label, resource: resource}
}

type redactedCloser struct {
	label    string
	resource Closer
}

func (redacted redactedCloser) Close() error {
	if err := redacted.resource.Close(); err != nil {
		return errors.New(redacted.label + " close failed; details withheld")
	}
	return nil
}

// Own registers resource at construction. Dependencies listed in before are
// closed first, for example an engine before the WAL it borrows. The concrete
// values are captured even when a caller later reassigns its fixture variable.
// All dependencies and the resource are closed even if an earlier close fails,
// and every close failure is reported to owner.
func Own[T Closer](owner Owner, resource T, before ...Closer) T {
	owner.Helper()
	outcomeOf(owner)
	owner.Cleanup(ownedClose(owner, resource, before))
	return resource
}

// OwnWithClose registers resource like Own and also returns its close, for a
// test that must release the fixture before it ends, for example to reopen its
// files. The close runs once and reports through owner like the cleanup does;
// the registered cleanup does nothing after an earlier close.
func OwnWithClose[T Closer](owner Owner, resource T, before ...Closer) (T, func()) {
	owner.Helper()
	outcomeOf(owner)
	closeNow := ownedClose(owner, resource, before)
	owner.Cleanup(closeNow)
	return resource, closeNow
}

func ownedClose(owner Owner, resource Closer, before []Closer) func() {
	var done atomic.Bool
	return func() {
		// Every frame is a helper, so a failure is reported at the test that
		// called Own whether or not the compiler inlines them.
		owner.Helper()
		if done.Swap(true) {
			return
		}
		close := func(resource Closer) {
			if err := closeOwned(owner, resource); err != nil {
				owner.Helper()
				owner.Errorf("closing test fixture: %v", err)
			}
		}
		for _, dependency := range before {
			close(dependency)
		}
		close(resource)
	}
}

// Boundary owns fixtures until an explicit lifecycle boundary, such as the
// process leak check in TestMain. Its zero value is ready for use.
type Boundary struct {
	cleanups []func()
	failures []error
}

// Helper implements Owner for an explicit boundary rather than testing.T.
func (*Boundary) Helper() {}

// Cleanup registers cleanup at this boundary.
func (boundary *Boundary) Cleanup(cleanup func()) {
	boundary.cleanups = append(boundary.cleanups, cleanup)
}

// Errorf records a teardown failure; Close returns it to the boundary.
func (boundary *Boundary) Errorf(format string, arguments ...any) {
	boundary.failures = append(boundary.failures, fmt.Errorf(format, arguments...))
}

// Close runs the registered fixture cleanups after the boundary's checks and
// returns every failure they recorded.
func (boundary *Boundary) Close() error {
	for len(boundary.cleanups) != 0 {
		last := len(boundary.cleanups) - 1
		cleanup := boundary.cleanups[last]
		boundary.cleanups = boundary.cleanups[:last]
		cleanup()
	}
	failures := boundary.failures
	boundary.failures = nil
	return errors.Join(failures...)
}
