package testfixture

import "testing"

// CachedStorage asserts that a lookup whose result is cached returns the same
// instance on consecutive calls. It calls lookup twice, returns the first result
// and fails when either lookup reports an error, when a lookup returns the zero
// value, or when the two lookups differ. Reported failures name neither the
// returned value nor the wrapped error, so manager diagnostics which embed
// connection details cannot reach test output through this assertion.
func CachedStorage[T comparable](t testing.TB, lookup func() (T, error)) T {
	t.Helper()
	first, err := lookup()
	if err != nil {
		t.Fatalf("cached storage lookup failed: first call returned an error")
	}
	var zero T
	if first == zero {
		t.Fatalf("cached storage lookup returned the zero value instead of a storage instance")
	}
	second, err := lookup()
	if err != nil {
		t.Fatalf("cached storage lookup failed: second call returned an error")
	}
	if first != second {
		t.Fatalf("cached storage lookup returned a different instance on the second call")
	}
	return first
}
