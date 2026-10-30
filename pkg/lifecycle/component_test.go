package lifecycle

import "testing"

// Compile-time assertion that *FakeComponent satisfies the Component
// interface. If either symbol is missing or the methods drift, the package
// fails to build — that's the Wave-0 RED contract.
var _ Component = (*FakeComponent)(nil)

// TestComponent_InterfaceAssertion documents the compile-time contract.
// The real check is the package-level `var _ Component = (*FakeComponent)(nil)`
// line above; this Go test simply asserts the assertion is present (i.e.
// the file compiles).
func TestComponent_InterfaceAssertion(t *testing.T) {
	t.Helper()
	var c Component = (*FakeComponent)(nil)
	if c != nil {
		// FakeComponent zero-pointer satisfies Component; nothing to assert at
		// runtime — the real gate is compile-time.
		t.Log("Component interface satisfied by *FakeComponent")
	}
}
