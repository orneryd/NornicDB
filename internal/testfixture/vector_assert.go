package testfixture

import "testing"

// RequireFloat32Vector checks the complete deterministic vector without
// allocating an expected slice. The caller owns the fixture's value pattern.
func RequireFloat32Vector(t testing.TB, name string, actual []float32, dimensions int, valueAt func(int) float32) {
	t.Helper()
	if len(actual) != dimensions {
		t.Fatalf("%s has %d dimensions, want %d", name, len(actual), dimensions)
	}
	for index, got := range actual {
		if want := valueAt(index); got != want {
			t.Fatalf("%s[%d] = %v, want %v", name, index, got, want)
		}
	}
}
