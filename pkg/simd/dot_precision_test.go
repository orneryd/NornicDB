package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// Exercise the vector block and tail boundaries used by the native dot kernel,
// with normalized signed inputs and a float64 reference to catch lost lanes.
func TestDotProductNormalizedBlockBoundaries(t *testing.T) {
	for _, size := range []int{1, 3, 4, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1024, 1031, 1536, 3072, 4097} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(size)))
			for trial := 0; trial < 20; trial++ {
				a, b := make([]float32, size), make([]float32, size)
				var an, bn float64
				for i := range a {
					a[i] = rng.Float32()*2 - 1
					b[i] = rng.Float32()*2 - 1
					an += float64(a[i]) * float64(a[i])
					bn += float64(b[i]) * float64(b[i])
				}
				for i := range a {
					a[i] /= float32(math.Sqrt(an))
					b[i] /= float32(math.Sqrt(bn))
				}
				var want float64
				for i := range a {
					want += float64(a[i]) * float64(b[i])
				}
				got := float64(DotProduct(a, b))
				if math.Abs(got-want) > 1e-6 {
					t.Fatalf("trial %d: got %.9g want %.9g", trial, got, want)
				}
			}
		})
	}
}
