package simd

import (
	"reflect"
	"testing"
)

func TestNormalizeUnitAxesPreservesIdentity(t *testing.T) {
	for _, input := range [][]float32{{1, 0}, {0, -1}, {0, 0, 1}, {1, 0, 0, 0, 0, 0, 0, 0}} {
		if n := Norm(input); n != 1 {
			t.Fatalf("Norm(%v) = %g, want 1", input, n)
		}
		actual := append([]float32(nil), input...)
		for i := 0; i < 4; i++ {
			NormalizeInPlace(actual)
		}
		if !reflect.DeepEqual(input, actual) {
			t.Fatalf("normalizing %v changed it to %v", input, actual)
		}
	}
}
