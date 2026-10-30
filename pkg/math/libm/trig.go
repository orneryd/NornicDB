// Sin, Cos and Tan are Go ports of musl 1.2.5's sin.c, cos.c and tan.c
// (FreeBSD msun s_sin.c, s_cos.c, s_tan.c):
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunPro, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// and of their kernels __sin.c, __cos.c (k_sin.c, k_cos.c):
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunSoft, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// and __tan.c (k_tan.c):
//
//	Copyright 2004 Sun Microsystems, Inc.  All Rights Reserved.
//
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// See NOTICES.md. Every product that feeds an addition is converted with
// float64(...), which the Go spec guarantees rounds it, so no platform fuses
// it into a multiply-add: the results are musl's on x86-64, which Neo4j's
// HotSpot intrinsics agree with for about 96% of inputs (#907).

package libm

import "math"

var (
	sinS1 = -1.66666666666666324348e-01
	sinS2 = 8.33333333332248946124e-03
	sinS3 = -1.98412698298579493134e-04
	sinS4 = 2.75573137070700676789e-06
	sinS5 = -2.50507602534068634195e-08
	sinS6 = 1.58969099521155010221e-10

	cosC1 = 4.16666666666666019037e-02
	cosC2 = -1.38888888888741095749e-03
	cosC3 = 2.48015872894767294178e-05
	cosC4 = -2.75573143513906633035e-07
	cosC5 = 2.08757232129817482790e-09
	cosC6 = -1.13596475577881948265e-11

	tanT = [13]float64{
		3.33333333333334091986e-01,
		1.33333333333201242699e-01,
		5.39682539762260521377e-02,
		2.18694882948595424599e-02,
		8.86323982359930005737e-03,
		3.59207910759131235356e-03,
		1.45620945432529025516e-03,
		5.88041240820264096874e-04,
		2.46463134818469906812e-04,
		7.81794442939557092300e-05,
		7.14072491382608190305e-05,
		-1.85586374855275456654e-05,
		2.59073051863633712884e-05,
	}
	tanPio4   = 7.85398163397448278999e-01
	tanPio4lo = 3.06161699786838301793e-17
)

// highWord is the top 32 bits of x (fdlibm's GET_HIGH_WORD).
func highWord(x float64) uint32 { return uint32(math.Float64bits(x) >> 32) }

// clearLowWord is x with its low 32 bits zero (fdlibm's SET_LOW_WORD(x, 0)).
func clearLowWord(x float64) float64 {
	return math.Float64frombits(math.Float64bits(x) &^ 0xffffffff)
}

// kernelSin is sin(x + y) for |x| <= pi/4; y is the tail of x, used when
// hasTail.
func kernelSin(x, y float64, hasTail bool) float64 {
	z := x * x
	w := z * z
	r := sinS2 + float64(z*(sinS3+float64(z*sinS4))) + float64(float64(z*w)*(sinS5+float64(z*sinS6)))
	v := z * x
	if !hasTail {
		return x + float64(v*(sinS1+float64(z*r)))
	}
	return x - ((float64(z*(float64(0.5*y)-float64(v*r))) - y) - float64(v*sinS1))
}

// kernelCos is cos(x + y) for |x| <= pi/4.
func kernelCos(x, y float64) float64 {
	z := x * x
	w := z * z
	r := float64(z*(cosC1+float64(z*(cosC2+float64(z*cosC3))))) + float64(float64(w*w)*(cosC4+float64(z*(cosC5+float64(z*cosC6)))))
	hz := float64(0.5 * z)
	w = 1.0 - hz
	return w + (((1.0 - w) - hz) + (float64(z*r) - float64(x*y)))
}

// kernelTan is tan(x + y) for |x| <= pi/4, or -1/tan(x + y) when odd.
func kernelTan(x, y float64, odd bool) float64 {
	hx := highWord(x)
	big := hx&0x7fffffff >= 0x3FE59428 // |x| >= 0.6744
	sign := false
	if big {
		sign = hx>>31 != 0
		if sign {
			x, y = -x, -y
		}
		x = (tanPio4 - x) + (tanPio4lo - y)
		y = 0.0
	}
	z := x * x
	w := z * z
	T := &tanT
	r := T[1] + float64(w*(T[3]+float64(w*(T[5]+float64(w*(T[7]+float64(w*(T[9]+float64(w*T[11])))))))))
	v := float64(z * (T[2] + float64(w*(T[4]+float64(w*(T[6]+float64(w*(T[8]+float64(w*(T[10]+float64(w*T[12])))))))))))
	s := z * x
	r = y + float64(z*(float64(s*(r+v))+y)) + float64(s*T[0])
	w = x + r
	if big {
		oddSign := 1.0
		if odd {
			oddSign = -1.0
		}
		v = oddSign - 2.0*(x+(r-float64(w*w)/(w+oddSign)))
		if sign {
			return -v
		}
		return v
	}
	if !odd {
		return w
	}
	// -1.0/(x+r) has up to 2ulp error, so compute it accurately.
	w0 := clearLowWord(w)
	v = r - (w0 - x) // w0+v = r+x
	a := -1.0 / w
	a0 := clearLowWord(a)
	return a0 + float64(a*(1.0+float64(a0*w0)+float64(a0*v)))
}

// Sin returns the sine of x (radians).
func Sin(x float64) float64 {
	ix := highWord(x) & 0x7fffffff
	if ix <= 0x3fe921fb { // |x| ~< pi/4
		if ix < 0x3e500000 { // |x| < 2**-26
			return x
		}
		return kernelSin(x, 0.0, false)
	}
	if ix >= 0x7ff00000 { // Inf or NaN
		return x - x
	}
	n, y0, y1 := remPio2(x)
	switch n & 3 {
	case 0:
		return kernelSin(y0, y1, true)
	case 1:
		return kernelCos(y0, y1)
	case 2:
		return -kernelSin(y0, y1, true)
	default:
		return -kernelCos(y0, y1)
	}
}

// Cos returns the cosine of x (radians).
func Cos(x float64) float64 {
	ix := highWord(x) & 0x7fffffff
	if ix <= 0x3fe921fb { // |x| ~< pi/4
		if ix < 0x3e46a09e { // |x| < 2**-27 * sqrt(2)
			return 1.0
		}
		return kernelCos(x, 0)
	}
	if ix >= 0x7ff00000 { // Inf or NaN
		return x - x
	}
	n, y0, y1 := remPio2(x)
	switch n & 3 {
	case 0:
		return kernelCos(y0, y1)
	case 1:
		return -kernelSin(y0, y1, true)
	case 2:
		return -kernelCos(y0, y1)
	default:
		return kernelSin(y0, y1, true)
	}
}

// Tan returns the tangent of x (radians).
func Tan(x float64) float64 {
	ix := highWord(x) & 0x7fffffff
	if ix <= 0x3fe921fb { // |x| ~< pi/4
		if ix < 0x3e400000 { // |x| < 2**-27
			return x
		}
		return kernelTan(x, 0.0, false)
	}
	if ix >= 0x7ff00000 { // Inf or NaN
		return x - x
	}
	n, y0, y1 := remPio2(x)
	return kernelTan(y0, y1, n&1 != 0)
}
