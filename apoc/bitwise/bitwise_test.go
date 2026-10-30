package bitwise

import "testing"

func TestBitwiseCoreOps(t *testing.T) {
	if got := Op(12, "&", 10); got != 8 {
		t.Fatalf("and op: %d", got)
	}
	if got := Op(12, "OR", 10); got != 14 {
		t.Fatalf("or op: %d", got)
	}
	if got := Op(12, "^", 10); got != 6 {
		t.Fatalf("xor op: %d", got)
	}
	if got := Op(5, "<<", 2); got != 20 {
		t.Fatalf("left shift op: %d", got)
	}
	if got := Op(20, ">>", 2); got != 5 {
		t.Fatalf("right shift op: %d", got)
	}
	if got := Op(1, "bad", 2); got != 0 {
		t.Fatalf("unknown op should be 0")
	}
}

func TestBitwiseAggregatesAndBitHelpers(t *testing.T) {
	if got := And(15, 7, 3); got != 3 {
		t.Fatalf("and variadic: %d", got)
	}
	if got := And(); got != 0 {
		t.Fatalf("and empty: %d", got)
	}
	if got := Or(8, 2, 1); got != 11 {
		t.Fatalf("or variadic: %d", got)
	}
	if got := Xor(12, 10); got != 6 {
		t.Fatalf("xor variadic: %d", got)
	}
	if got := Not(12); got != ^int64(12) {
		t.Fatalf("not: %d", got)
	}

	if got := LeftShift(5, 2); got != 20 {
		t.Fatalf("left shift: %d", got)
	}
	if got := RightShift(20, 2); got != 5 {
		t.Fatalf("right shift: %d", got)
	}

	v := int64(8)
	if got := SetBit(v, 0); got != 9 {
		t.Fatalf("set bit: %d", got)
	}
	if got := ClearBit(9, 0); got != 8 {
		t.Fatalf("clear bit: %d", got)
	}
	if got := ToggleBit(8, 0); got != 9 {
		t.Fatalf("toggle bit: %d", got)
	}
	if !TestBit(9, 0) || TestBit(8, 0) {
		t.Fatalf("test bit failed")
	}
}

func TestBitwiseCountReverseRotate(t *testing.T) {
	if got := CountBits(15); got != 4 {
		t.Fatalf("count bits: %d", got)
	}
	if got := CountBits(0); got != 0 {
		t.Fatalf("count bits zero: %d", got)
	}

	if got := ReverseBits(1); got != (-1 << 63) {
		t.Fatalf("reverse bits: %d", got)
	}
	if got := RotateLeft(1, 2); got != 4 {
		t.Fatalf("rotate left: %d", got)
	}
	if got := RotateRight(4, 2); got != 1 {
		t.Fatalf("rotate right: %d", got)
	}
}
