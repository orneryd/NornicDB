package simd

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestDotProductInt8ExactBlockBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 3, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1024, 4095, 4096, 4097, 131073} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			a, b := make([]byte, n), make([]byte, n)
			rng := rand.New(rand.NewSource(int64(n)))
			for trial := 0; trial < 4; trial++ {
				var want int64
				for i := range a {
					if trial == 0 {
						a[i], b[i] = 128, 128
					} else {
						a[i], b[i] = byte(rng.Intn(256)), byte(rng.Intn(256))
					}
					want += int64(int8(a[i])) * int64(int8(b[i]))
				}
				if got := dotProductInt8Reference(a, b); got != want {
					t.Fatalf("reference got %d want %d", got, want)
				}
				if got := DotProductInt8(a, b); got != want {
					t.Fatalf("trial %d: got %d want %d", trial, got, want)
				}
			}
		})
	}
	if got := DotProductInt8([]byte{1}, nil); got != 0 {
		t.Fatalf("mismatched dimensions: %d", got)
	}
	if got := DotProductInt8([]byte{1, 255}, []byte{2, 3}); got != -1 {
		t.Fatalf("signed interpretation: %d", got)
	}
}
