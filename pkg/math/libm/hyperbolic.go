// Sinh is a Go port of musl's double-precision sinh function,
// Copyright (c) 2018, Arm Limited. SPDX-License-Identifier: MIT.
// See NOTICES.md for the licence text.
//
// The hyperbolic functions below are thin wrappers over musl's correctly
// rounded Exp and Log, following the musl source line by line.
package libm

import "math"

// expo2 computes exp(x)/2 for x >= log(DBL_MAX), slightly better than
// 0.5*exp(x/2)*exp(x/2). sign is +-1.0.
func expo2(x float64, sign float64) float64 {
	// k is such that k*ln2 has minimal relative error and
	// x - kln2 > log(DBL_MIN).
	const k = 2043
	const kln2 = 0x1.62066151add8bp+10

	var scale float64
	// note that k is odd and scale*scale overflows
	scale = asFloat64(uint64(uint32(0x3ff+k/2)<<20) << 32)
	// exp(x - k ln2) * 2**(k-1)
	return Exp(x-kln2) * (sign * scale) * scale
}

// Sinh returns the hyperbolic sine of x.
func Sinh(x float64) float64 {
	ix := asUint64(x)
	var w uint64
	var t, h, absx float64

	h = 0.5
	if ix>>63 != 0 {
		h = -h
	}
	// |x|
	ix &= 0x7fffffffffffffff
	absx = asFloat64(ix)
	w = ix >> 32

	// |x| < log(DBL_MAX)
	if w < 0x40862e42 {
		t = math.Expm1(absx)
		if w < 0x3ff00000 {
			if w < 0x3ff00000-(26<<20) {
				// note: this branch avoids spurious underflow
				return x
			}
			return h * (2*t - t*t/(t+1))
		}
		// note: |x|>log(0x1p26)+eps could be just h*exp(x)
		return h * (t + t/(t+1))
	}

	// |x| > log(DBL_MAX) or nan
	t = expo2(absx, 2*h)
	return t
}

// Cosh returns the hyperbolic cosine of x.
func Cosh(x float64) float64 {
	ix := asUint64(x)
	var w uint64
	var t float64

	// |x|
	ix &= 0x7fffffffffffffff
	x = asFloat64(ix)
	w = ix >> 32

	// |x| < log(2)
	if w < 0x3fe62e42 {
		if w < 0x3ff00000-(26<<20) {
			// raise inexact if x!=0
			return 1
		}
		t = math.Expm1(x)
		return 1 + t*t/(2*(1+t))
	}

	// |x| < log(DBL_MAX)
	if w < 0x40862e42 {
		t = Exp(x)
		// note: if x>log(0x1p26) then the 1/t is not needed
		return 0.5 * (t + 1/t)
	}

	// |x| > log(DBL_MAX) or nan
	t = expo2(x, 1.0)
	return t
}

// Tanh returns the hyperbolic tangent of x.
func Tanh(x float64) float64 {
	ix := asUint64(x)
	var w uint64
	sign := ix >> 63
	var t float64

	// x = |x|
	ix &= 0x7fffffffffffffff
	x = asFloat64(ix)
	w = ix >> 32

	if w > 0x3fe193ea {
		// |x| > log(3)/2 ~= 0.5493 or nan
		if w > 0x40340000 {
			// |x| > 20 or nan
			// note: this branch avoids raising overflow
			t = 1 - 0/x
		} else {
			t = math.Expm1(2 * x)
			t = 1 - 2/(t+2)
		}
	} else if w > 0x3fd058ae {
		// |x| > log(5/3)/2 ~= 0.2554
		t = math.Expm1(2 * x)
		t = t / (t + 2)
	} else if w >= 0x00100000 {
		// |x| >= 0x1p-1022, up to 2ulp error in [0.1,0.2554]
		t = math.Expm1(-2 * x)
		t = -t / (t + 2)
	} else {
		// |x| is subnormal
		t = x
	}
	if sign != 0 {
		return -t
	}
	return t
}

// Asinh returns the inverse hyperbolic sine of x.
func Asinh(x float64) float64 {
	ix := asUint64(x)
	e := ix >> 52 & 0x7ff
	s := ix >> 63

	// |x|
	ix &= 0x7fffffffffffffff
	x = asFloat64(ix)

	if e >= 0x3ff+26 {
		// |x| >= 0x1p26 or inf or nan
		x = Log(x) + 0.693147180559945309417232121458176568
	} else if e >= 0x3ff+1 {
		// |x| >= 2
		x = Log(2*x + 1/(math.Sqrt(x*x+1)+x))
	} else if e >= 0x3ff-26 {
		// |x| >= 0x1p-26, up to 1.6ulp error in [0.125,0.5]
		x = math.Log1p(x + x*x/(math.Sqrt(x*x+1)+1))
	}
	// |x| < 0x1p-26 falls through: asinh(x) rounds to x.
	if s != 0 {
		return -x
	}
	return x
}

// Acosh returns the inverse hyperbolic cosine of x.
func Acosh(x float64) float64 {
	e := asUint64(x) >> 52 & 0x7ff

	// x < 1 domain error is handled in the called functions
	if e < 0x3ff+1 {
		// |x| < 2, up to 2ulp error in [1,1.125]
		return math.Log1p(x - 1 + math.Sqrt((x-1)*(x-1)+2*(x-1)))
	}
	if e < 0x3ff+26 {
		// |x| < 0x1p26
		return Log(2*x - 1/(x+math.Sqrt(x*x-1)))
	}
	// |x| >= 0x1p26 or nan
	return Log(x) + 0.693147180559945309417232121458176568
}

// Atanh returns the inverse hyperbolic tangent of x.
func Atanh(x float64) float64 {
	ix := asUint64(x)
	e := ix >> 52 & 0x7ff
	s := ix >> 63
	var y float64

	// |x|
	ix &= 0x7fffffffffffffff
	y = asFloat64(ix)

	if e < 0x3ff-1 {
		if e >= 0x3ff-32 {
			// |x| < 0.5, up to 1.7ulp error
			y = 0.5 * math.Log1p(2*y+2*y*y/(1-y))
		}
	} else {
		// avoid overflow
		y = 0.5 * math.Log1p(2*(y/(1-y)))
	}
	if s != 0 {
		return -y
	}
	return y
}
