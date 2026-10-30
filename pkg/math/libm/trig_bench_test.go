package libm

import (
	"math"
	"math/rand"
	"testing"
)

// BenchmarkTrig compares each musl port with the standard library function
// it replaces, over the same seeded inputs: small and medium arguments, as
// Cypher queries use them.
func BenchmarkTrig(b *testing.B) {
	rng := rand.New(rand.NewSource(907))
	inputs := make([]float64, 1024)
	for i := range inputs {
		inputs[i] = rng.Float64()*20 - 10
	}
	units := make([]float64, len(inputs))
	for i := range units {
		units[i] = rng.Float64()*2 - 1
	}
	positives := make([]float64, len(inputs))
	for i := range positives {
		positives[i] = math.Abs(inputs[i]) + 1e-3
	}
	for _, c := range []struct {
		name      string
		in        []float64
		port, std func(float64) float64
	}{
		{"sin", inputs, Sin, math.Sin},
		{"cos", inputs, Cos, math.Cos},
		{"tan", inputs, Tan, math.Tan},
		{"asin", units, Asin, math.Asin},
		{"acos", units, Acos, math.Acos},
		{"atan", inputs, Atan, math.Atan},
		{"atan2", inputs, func(x float64) float64 { return Atan2(x, 1.5) }, func(x float64) float64 { return math.Atan2(x, 1.5) }},
		{"log10", positives, Log10, math.Log10},
	} {
		for _, impl := range []struct {
			name string
			f    func(float64) float64
		}{{"musl", c.port}, {"go", c.std}} {
			b.Run(c.name+"/"+impl.name, func(b *testing.B) {
				sum := 0.0
				for i := 0; i < b.N; i++ {
					sum += impl.f(c.in[i&(len(c.in)-1)])
				}
				if sum == 42 {
					b.Log(sum)
				}
			})
		}
	}
}
