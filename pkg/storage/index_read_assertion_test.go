package storage

import "testing"

// checkedIndexRead preserves value assertions while checking the index read error.
func checkedIndexRead[T any](t testing.TB) func(T, error) T {
	t.Helper()
	return func(value T, err error) T {
		t.Helper()
		if err != nil {
			t.Errorf("index read failed: %v", err)
		}
		return value
	}
}

// checkedIndexRange also preserves the index availability or narrowing result.
func checkedIndexRange[T any](t testing.TB) func(T, bool, error) (T, bool) {
	t.Helper()
	return func(value T, available bool, err error) (T, bool) {
		t.Helper()
		if err != nil {
			t.Errorf("index range read failed: %v", err)
		}
		return value, available
	}
}
