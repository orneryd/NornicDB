// Package testfixture owns test resources without importing their packages.
package testfixture

// Owner is the cleanup boundary provided by testing.T, testing.B or Boundary.
type Owner interface {
	Helper()
	Cleanup(func())
}

// Closer is a concrete fixture whose lifetime belongs to its test.
type Closer interface {
	Close() error
}

// CheckedOwner reports teardown failures as well as owning fixture lifetimes.
// testing.T and testing.B implement this interface.
type CheckedOwner interface {
	Owner
	Errorf(string, ...any)
}

// Own registers resource at construction. Dependencies listed in before are
// closed first, for example an engine before the WAL it borrows. The concrete
// values are captured even when a caller later reassigns its fixture variable.
func Own[T Closer](owner Owner, resource T, before ...Closer) T {
	return own(owner, resource, before, nil)
}

// OwnChecked preserves an existing close assertion through the shared owner.
// All dependencies and the resource are closed even if an earlier close fails.
func OwnChecked[T Closer](owner CheckedOwner, resource T, before ...Closer) T {
	return own(owner, resource, before, func(err error) {
		owner.Helper()
		owner.Errorf("closing test fixture: %v", err)
	})
}

func own[T Closer](owner Owner, resource T, before []Closer, report func(error)) T {
	owner.Helper()
	owner.Cleanup(func() {
		close := func(resource Closer) {
			if err := resource.Close(); err != nil && report != nil {
				report(err)
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
}

// Helper implements Owner for an explicit boundary rather than testing.T.
func (*Boundary) Helper() {}

// Cleanup registers cleanup at this boundary.
func (boundary *Boundary) Cleanup(cleanup func()) {
	boundary.cleanups = append(boundary.cleanups, cleanup)
}

// Close runs the registered fixture cleanups after the boundary's checks.
func (boundary *Boundary) Close() {
	for len(boundary.cleanups) != 0 {
		last := len(boundary.cleanups) - 1
		cleanup := boundary.cleanups[last]
		boundary.cleanups = boundary.cleanups[:last]
		cleanup()
	}
}
