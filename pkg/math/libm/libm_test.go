package libm

import (
	"math"
	"testing"
)

// TestPowMatchesNeo4j pins the #981 evidence: Neo4j (Java Math.pow) returns
// the correctly rounded double, while Go's math.Pow is 1 ulp low for these
// inputs. The musl port must match Neo4j exactly.
func TestPowMatchesNeo4j(t *testing.T) {
	cases := []struct {
		base, exp float64
		want      uint64
	}{
		{3, 2.5, 0x402f2d4a45635640},                // 15.588457268119896
		{9007199254740993, 2.5, 0x4836a09e667f3bcd}, // 7.699711013376145e+39
		{9007199254740992.0, 2.5, 0x4836a09e667f3bcd},
	}
	for _, c := range cases {
		if got := math.Float64bits(Pow(c.base, c.exp)); got != c.want {
			t.Errorf("Pow(%v, %v) = %#x, want %#x (math.Pow gives %#x)",
				c.base, c.exp, got, c.want, math.Float64bits(math.Pow(c.base, c.exp)))
		}
	}
}

// TestBasicValues checks exact and special-case results.
func TestBasicValues(t *testing.T) {
	if Exp(0) != 1 {
		t.Errorf("Exp(0) = %v, want 1", Exp(0))
	}
	if Log2(8) != 3 {
		t.Errorf("Log2(8) = %v, want 3", Log2(8))
	}
	if Log2(0.5) != -1 {
		t.Errorf("Log2(0.5) = %v, want -1", Log2(0.5))
	}
	if Exp2(10) != 1024 {
		t.Errorf("Exp2(10) = %v, want 1024", Exp2(10))
	}
	if !math.IsInf(Exp(1000), 1) {
		t.Errorf("Exp(1000) = %v, want +Inf", Exp(1000))
	}
	if Exp(-1000) != 0 {
		t.Errorf("Exp(-1000) = %v, want 0", Exp(-1000))
	}
	if Log(1) != 0 {
		t.Errorf("Log(1) = %v, want 0", Log(1))
	}
	if !math.IsInf(Log(0), -1) {
		t.Errorf("Log(0) = %v, want -Inf", Log(0))
	}
	if !math.IsNaN(Log(-1)) {
		t.Errorf("Log(-1) = %v, want NaN", Log(-1))
	}
}

// TestCorrectlyRoundedAnchors pins correctly rounded results computed with
// high-precision (mpmath) reference arithmetic against the exact double
// inputs. These are the values Neo4j's Java Math functions return.
func TestCorrectlyRoundedAnchors(t *testing.T) {
	cases := []struct {
		name string
		got  float64
		want uint64
	}{
		{"Exp(0.1)", Exp(0.1), 0x3ff1aec7b35a00d4},
		{"Exp(1.5)", Exp(1.5), 0x4011ed3fe64fc541},
		{"Exp(-0.5)", Exp(-0.5), 0x3fe368b2fc6f960a},
		{"Log(1.5)", Log(1.5), 0x3fd9f323ecbf984c},
		{"Log(2.0)", Log(2.0), 0x3fe62e42fefa39ef},
		{"Log(0.5)", Log(0.5), 0xbfe62e42fefa39ef},
		{"Log2(1.001)", Log2(1.001), 0x3f57a013faca698f},
		{"Log2(1.5)", Log2(1.5), 0x3fe2b803473f7ad1},
		{"Log2(3.0)", Log2(3.0), 0x3ff95c01a39fbd68},
		{"Exp2(0.1)", Exp2(0.1), 0x3ff125fbee250664},
		{"Exp2(-3.5)", Exp2(-3.5), 0x3fb6a09e667f3bcd},
	}
	for _, c := range cases {
		if got := math.Float64bits(c.got); got != c.want {
			t.Errorf("%s = %#x, want %#x", c.name, got, c.want)
		}
	}
}

// TestHyperbolicSanity checks identities and asymptotes of the musl
// hyperbolic wrappers.
func TestHyperbolicSanity(t *testing.T) {
	if Sinh(0) != 0 {
		t.Errorf("Sinh(0) = %v, want 0", Sinh(0))
	}
	if Cosh(0) != 1 {
		t.Errorf("Cosh(0) = %v, want 1", Cosh(0))
	}
	if Tanh(0) != 0 {
		t.Errorf("Tanh(0) = %v, want 0", Tanh(0))
	}
	if Asinh(0) != 0 {
		t.Errorf("Asinh(0) = %v, want 0", Asinh(0))
	}
	if Acosh(1) != 0 {
		t.Errorf("Acosh(1) = %v, want 0", Acosh(1))
	}
	if Atanh(0) != 0 {
		t.Errorf("Atanh(0) = %v, want 0", Atanh(0))
	}
	if Tanh(30) != 1 || Tanh(-30) != -1 {
		t.Errorf("Tanh(±30) = (%v, %v), want (±1)", Tanh(30), Tanh(-30))
	}
	if Sinh(-2) != -Sinh(2) {
		t.Errorf("Sinh is not odd: Sinh(-2) = %v, -Sinh(2) = %v", Sinh(-2), -Sinh(2))
	}
	if Cosh(-2) != Cosh(2) {
		t.Errorf("Cosh is not even: Cosh(-2) = %v, Cosh(2) = %v", Cosh(-2), Cosh(2))
	}
	if !math.IsInf(Sinh(1000), 1) || !math.IsInf(Cosh(1000), 1) {
		t.Errorf("Sinh/Cosh(1000) should overflow to +Inf")
	}
}

// TestTranscendentalNoCatastrophicError is a broad sweep that only guards
// against gross porting errors: the ported functions must stay within a very
// wide band of Go's fdlibm results. musl and fdlibm are both ~1 ulp accurate
// but use different algorithms, so they legitimately disagree by a few ulps
// (and Go's Log2 is notably worse near 1).
func TestTranscendentalNoCatastrophicError(t *testing.T) {
	for i := 0; i < 5000; i++ {
		x := 0.001 + float64(i)*0.004
		y := -10.0 + float64(i)*0.004
		if d := ulpDiff(Exp(x), math.Exp(x)); d > 8 {
			t.Fatalf("Exp(%v): %d ulps from math.Exp", x, d)
		}
		if d := ulpDiff(Log(x), math.Log(x)); d > 8 {
			t.Fatalf("Log(%v): %d ulps from math.Log", x, d)
		}
		if d := ulpDiff(Pow(x, y), math.Pow(x, y)); d > 16 {
			t.Fatalf("Pow(%v,%v): %d ulps from math.Pow", x, y, d)
		}
	}
}

func ulpDiff(a, b float64) uint64 {
	ab := math.Float64bits(a)
	bb := math.Float64bits(b)
	if ab > bb {
		return ab - bb
	}
	return bb - ab
}
