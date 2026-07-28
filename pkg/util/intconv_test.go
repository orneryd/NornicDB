package util

import (
	"math"
	"testing"
)

func TestSafeFloat64ToInt64RejectsOutOfRange(t *testing.T) {
	if _, ok := SafeFloat64ToInt64(1e30); ok {
		t.Fatal("expected out-of-range float64 to fail int64 conversion")
	}
}

func TestSafeFloat64ToIntTruncatesTowardZero(t *testing.T) {
	got, ok := SafeFloat64ToInt(-3.9)
	if !ok || got != -3 {
		t.Fatalf("SafeFloat64ToInt(-3.9) = (%d, %v), want (-3, true)", got, ok)
	}
}

func TestSafeIntToUint32RejectsNegative(t *testing.T) {
	if _, ok := SafeIntToUint32(-1); ok {
		t.Fatal("expected negative int to fail uint32 conversion")
	}
}

func TestSafeIntToUint16RejectsOverflow(t *testing.T) {
	if _, ok := SafeIntToUint16(70000); ok {
		t.Fatal("expected overflowing int to fail uint16 conversion")
	}
}

func TestSafeIntToInt32RejectsOverflow(t *testing.T) {
	if _, ok := SafeIntToInt32(math.MaxInt32 + 1); ok {
		t.Fatal("expected overflowing int to fail int32 conversion")
	}
}
