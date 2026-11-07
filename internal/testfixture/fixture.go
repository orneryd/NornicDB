// Package testfixture owns test resources without importing their packages.
package testfixture

import (
	"errors"
	"fmt"
)

// Owner is the cleanup boundary provided by testing.T, testing.B or Boundary.
type Owner interface {
	Helper()
	Cleanup(func())
	Errorf(string, ...any)
}

// Closer is a concrete fixture whose lifetime belongs to its test.
type Closer interface {
	Close() error
}

// CheckedOwner is retained for existing fixture helpers. All owners report close failures.
type CheckedOwner = Owner

// Own registers resource at construction. Dependencies listed in before are
// closed first, for example an engine before the WAL it borrows. The concrete
// values are captured even when a caller later reassigns its fixture variable.
func Own[T Closer](owner Owner, resource T, before ...Closer) T {
	return own(owner, resource, before, nil)
}

// OwnWithCloseAssertion registers resource for cleanup and accepts close errors
// only when assertion identifies them as expected. Unaccepted errors are
// reported through the owner like Own.
func OwnWithCloseAssertion[T Closer](owner Owner, resource T, assertion func(error) bool, before ...Closer) T {
	return own(owner, resource, before, assertion)
}

// OwnChecked preserves an existing close assertion through the shared owner.
// All dependencies and the resource are closed even if an earlier close fails.
func OwnChecked[T Closer](owner CheckedOwner, resource T, before ...Closer) T {
	return Own(owner, resource, before...)
}

func own[T Closer](owner Owner, resource T, before []Closer, assertion func(error) bool) T {
	owner.Helper()
	owner.Cleanup(func() {
		close := func(resource Closer) {
			if err := resource.Close(); err != nil {
				if assertion != nil && assertion(err) {
					return
				}
				owner.Helper()
				owner.Errorf("closing test fixture: %v", err)
			}
		}
		for _, dependency := range before {
			close(dependency)
		}
		close(resource)
	})
	return resource
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

// Errorf records a fixture failure for the process boundary.
func (boundary *Boundary) Errorf(format string, args ...any) {
	boundary.failures = append(boundary.failures, fmt.Errorf(format, args...))
}

// Close runs the registered fixture cleanups after the boundary's checks and returns their failures.
func (boundary *Boundary) Close() error {
	for len(boundary.cleanups) != 0 {
		last := len(boundary.cleanups) - 1
		cleanup := boundary.cleanups[last]
		boundary.cleanups = boundary.cleanups[:last]
		cleanup()
	}
	err := errors.Join(boundary.failures...)
	boundary.failures = nil
	return err
}
