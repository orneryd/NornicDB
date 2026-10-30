package util

import "testing"

func TestSafeIntProduct(t *testing.T) {
	t.Run("fits", func(t *testing.T) {
		got, ok := SafeIntProduct(12, 8)
		if !ok || got != 96 {
			t.Fatalf("SafeIntProduct(12, 8) = (%d, %v), want (96, true)", got, ok)
		}
	})

	t.Run("overflow", func(t *testing.T) {
		if _, ok := SafeIntProduct(maxIntValue, 2); ok {
			t.Fatal("expected overflow to be reported")
		}
	})
}

func TestSafeIntAdd(t *testing.T) {
	t.Run("fits", func(t *testing.T) {
		got, ok := SafeIntAdd(12, 8)
		if !ok || got != 20 {
			t.Fatalf("SafeIntAdd(12, 8) = (%d, %v), want (20, true)", got, ok)
		}
	})

	t.Run("overflow", func(t *testing.T) {
		if _, ok := SafeIntAdd(maxIntValue, 1); ok {
			t.Fatal("expected overflow to be reported")
		}
	})
}

func TestSafeIntSum(t *testing.T) {
	if got, ok := SafeIntSum(1, 2, 3, 4); !ok || got != 10 {
		t.Fatalf("SafeIntSum(1,2,3,4) = (%d, %v), want (10, true)", got, ok)
	}
	if _, ok := SafeIntSum(1, maxIntValue); ok {
		t.Fatal("expected overflow to be reported")
	}
}

func TestSafePreallocCap(t *testing.T) {
	if got := SafePreallocCap(1000000); got != MaxPreallocHintCap {
		t.Fatalf("SafePreallocCap(1000000) = %d, want %d", got, MaxPreallocHintCap)
	}
	if got := SafePreallocCap(50, 10, 20); got != 10 {
		t.Fatalf("SafePreallocCap(50, 10, 20) = %d, want 10", got)
	}
}

func TestSafePreallocProductAndSum(t *testing.T) {
	if got := SafePreallocProduct(maxIntValue, 2); got != MaxPreallocHintCap {
		t.Fatalf("SafePreallocProduct overflow = %d, want %d", got, MaxPreallocHintCap)
	}
	if got := SafePreallocSum(maxIntValue, 1); got != MaxPreallocHintCap {
		t.Fatalf("SafePreallocSum overflow = %d, want %d", got, MaxPreallocHintCap)
	}
}
