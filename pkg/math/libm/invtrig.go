// Asin, Acos, Atan2 and Log10 are Go ports of musl 1.2.5's asin.c, acos.c,
// atan2.c and log10.c (FreeBSD msun e_asin.c, e_acos.c, e_atan2.c,
// e_log10.c):
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunSoft, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// and Atan of musl's atan.c (FreeBSD msun s_atan.c):
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunPro, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// See NOTICES.md. Products that feed an addition are converted with
// float64(...) so no platform fuses them (see trig.go). Neo4j's HotSpot
// intrinsics agree with these asin, acos and atan bit for bit (#907).

package libm

import "math"

var (
	invPio2Hi = 1.57079632679489655800e+00
	invPio2Lo = 6.12323399573676603587e-17
	invPS0    = 1.66666666666666657415e-01
	invPS1    = -3.25565818622400915405e-01
	invPS2    = 2.01212532134862925881e-01
	invPS3    = -4.00555345006794114027e-02
	invPS4    = 7.91534994289814532176e-04
	invPS5    = 3.47933107596021167570e-05
	invQS1    = -2.40339491173441421878e+00
	invQS2    = 2.02094576023350569471e+00
	invQS3    = -6.88283971605453293030e-01
	invQS4    = 7.70381505559019352791e-02

	atanHi = [4]float64{
		4.63647609000806093515e-01,
		7.85398163397448278999e-01,
		9.82793723247329054082e-01,
		1.57079632679489655800e+00,
	}
	atanLo = [4]float64{
		2.26987774529616870924e-17,
		3.06161699786838301793e-17,
		1.39033110312309984516e-17,
		6.12323399573676603587e-17,
	}
	atanT = [11]float64{
		3.33333333333329318027e-01,
		-1.99999999998764832476e-01,
		1.42857142725034663711e-01,
		-1.11111104054623557880e-01,
		9.09088713343650656196e-02,
		-7.69187620504482999495e-02,
		6.66107313738753120669e-02,
		-5.83357013379057348645e-02,
		4.97687799461593236017e-02,
		-3.65315727442169155270e-02,
		1.62858201153657823623e-02,
	}
	atan2Pi   = 3.1415926535897931160e+00
	atan2PiLo = 1.2246467991473531772e-16

	log10Ivln10hi  = 4.34294481878168880939e-01
	log10Ivln10lo  = 2.50829467116452752298e-11
	log10Log10_2hi = 3.01029995663611771306e-01
	log10Log10_2lo = 3.69423907715893078616e-13
	log10Lg1       = 6.666666666666735130e-01
	log10Lg2       = 3.999999999940941908e-01
	log10Lg3       = 2.857142874366239149e-01
	log10Lg4       = 2.222219843214978396e-01
	log10Lg5       = 1.818357216161805012e-01
	log10Lg6       = 1.531383769920937332e-01
	log10Lg7       = 1.479819860511658591e-01
)

// asinR is the rational approximation asin and acos share.
func asinR(z float64) float64 {
	p := float64(z * (invPS0 + float64(z*(invPS1+float64(z*(invPS2+float64(z*(invPS3+float64(z*(invPS4+float64(z*invPS5)))))))))))
	q := 1.0 + float64(z*(invQS1+float64(z*(invQS2+float64(z*(invQS3+float64(z*invQS4)))))))
	return p / q
}

// lowWord is the bottom 32 bits of x (fdlibm's GET_LOW_WORD).
func lowWord(x float64) uint32 { return uint32(math.Float64bits(x)) }

// Asin returns the arcsine of x, in radians.
func Asin(x float64) float64 {
	hx := highWord(x)
	ix := hx & 0x7fffffff
	if ix >= 0x3ff00000 { // |x| >= 1 or NaN
		if (ix-0x3ff00000)|lowWord(x) == 0 { // asin(+-1) = +-pi/2
			return x*invPio2Hi + 0x1p-120
		}
		return 0 / (x - x)
	}
	if ix < 0x3fe00000 { // |x| < 0.5
		if ix < 0x3e500000 && ix >= 0x00100000 {
			return x
		}
		return x + float64(x*asinR(x*x))
	}
	// 1 > |x| >= 0.5
	z := (1 - math.Abs(x)) * 0.5
	s := math.Sqrt(z)
	r := asinR(z)
	if ix >= 0x3fef3333 { // |x| > 0.975
		x = invPio2Hi - (2*(s+float64(s*r)) - invPio2Lo)
	} else {
		f := clearLowWord(s) // f+c = sqrt(z)
		c := (z - float64(f*f)) / (s + f)
		x = 0.5*invPio2Hi - ((float64(2*s*r) - (invPio2Lo - 2*c)) - (0.5*invPio2Hi - 2*f))
	}
	if hx>>31 != 0 {
		return -x
	}
	return x
}

// Acos returns the arccosine of x, in radians.
func Acos(x float64) float64 {
	hx := highWord(x)
	ix := hx & 0x7fffffff
	if ix >= 0x3ff00000 { // |x| >= 1 or NaN
		if (ix-0x3ff00000)|lowWord(x) == 0 { // acos(1) = 0, acos(-1) = pi
			if hx>>31 != 0 {
				return 2*invPio2Hi + 0x1p-120
			}
			return 0
		}
		return 0 / (x - x)
	}
	if ix < 0x3fe00000 { // |x| < 0.5
		if ix <= 0x3c600000 { // |x| < 2**-57
			return invPio2Hi + 0x1p-120
		}
		return invPio2Hi - (x - (invPio2Lo - float64(x*asinR(x*x))))
	}
	if hx>>31 != 0 { // x < -0.5
		z := (1.0 + x) * 0.5
		s := math.Sqrt(z)
		w := float64(asinR(z)*s) - invPio2Lo
		return 2 * (invPio2Hi - (s + w))
	}
	// x > 0.5
	z := (1.0 - x) * 0.5
	s := math.Sqrt(z)
	df := clearLowWord(s)
	c := (z - float64(df*df)) / (s + df)
	w := float64(asinR(z)*s) + c
	return 2 * (df + w)
}

// Atan returns the arctangent of x, in radians.
func Atan(x float64) float64 {
	ix := highWord(x)
	sign := ix>>31 != 0
	ix &= 0x7fffffff
	if ix >= 0x44100000 { // |x| >= 2^66
		if math.IsNaN(x) {
			return x
		}
		z := atanHi[3] + 0x1p-120
		if sign {
			return -z
		}
		return z
	}
	id := -1
	if ix < 0x3fdc0000 { // |x| < 0.4375
		if ix < 0x3e400000 { // |x| < 2^-27
			return x
		}
	} else {
		x = math.Abs(x)
		if ix < 0x3ff30000 { // |x| < 1.1875
			if ix < 0x3fe60000 { // 7/16 <= |x| < 11/16
				id = 0
				x = (2.0*x - 1.0) / (2.0 + x)
			} else { // 11/16 <= |x| < 19/16
				id = 1
				x = (x - 1.0) / (x + 1.0)
			}
		} else {
			if ix < 0x40038000 { // |x| < 2.4375
				id = 2
				x = (x - 1.5) / (1.0 + float64(1.5*x))
			} else { // 2.4375 <= |x| < 2^66
				id = 3
				x = -1.0 / x
			}
		}
	}
	z := x * x
	w := z * z
	T := &atanT
	s1 := float64(z * (T[0] + float64(w*(T[2]+float64(w*(T[4]+float64(w*(T[6]+float64(w*(T[8]+float64(w*T[10])))))))))))
	s2 := float64(w * (T[1] + float64(w*(T[3]+float64(w*(T[5]+float64(w*(T[7]+float64(w*T[9])))))))))
	if id < 0 {
		return x - float64(x*(s1+s2))
	}
	z = atanHi[id] - ((float64(x*(s1+s2)) - atanLo[id]) - x)
	if sign {
		return -z
	}
	return z
}

// Atan2 returns the arctangent of y/x, using the signs of both to pick the
// quadrant.
func Atan2(y, x float64) float64 {
	if math.IsNaN(x) || math.IsNaN(y) {
		return x + y
	}
	ix, lx := highWord(x), lowWord(x)
	iy, ly := highWord(y), lowWord(y)
	if (ix-0x3ff00000)|lx == 0 { // x = 1.0
		return Atan(y)
	}
	m := (iy>>31)&1 | (ix>>30)&2 // 2*sign(x)+sign(y)
	ix &= 0x7fffffff
	iy &= 0x7fffffff
	if iy|ly == 0 { // y = 0
		switch m {
		case 0, 1:
			return y // atan(+-0, +anything) = +-0
		case 2:
			return atan2Pi // atan(+0, -anything) = pi
		default:
			return -atan2Pi // atan(-0, -anything) = -pi
		}
	}
	if ix|lx == 0 { // x = 0
		if m&1 != 0 {
			return -atan2Pi / 2
		}
		return atan2Pi / 2
	}
	if ix == 0x7ff00000 { // x is Inf
		if iy == 0x7ff00000 {
			switch m {
			case 0:
				return atan2Pi / 4
			case 1:
				return -atan2Pi / 4
			case 2:
				return 3 * atan2Pi / 4
			default:
				return -3 * atan2Pi / 4
			}
		}
		switch m {
		case 0:
			return 0.0
		case 1:
			return math.Copysign(0, -1)
		case 2:
			return atan2Pi
		default:
			return -atan2Pi
		}
	}
	if ix+(64<<20) < iy || iy == 0x7ff00000 { // |y/x| > 0x1p64
		if m&1 != 0 {
			return -atan2Pi / 2
		}
		return atan2Pi / 2
	}
	var z float64
	if m&2 != 0 && iy+(64<<20) < ix { // |y/x| < 0x1p-64, x < 0
		z = 0
	} else {
		z = Atan(math.Abs(y / x))
	}
	switch m {
	case 0:
		return z
	case 1:
		return -z
	case 2:
		return atan2Pi - (z - atan2PiLo)
	default:
		return (z - atan2PiLo) - atan2Pi
	}
}

// Log10 returns the decimal logarithm of x.
func Log10(x float64) float64 {
	u := math.Float64bits(x)
	hx := uint32(u >> 32)
	k := 0
	if hx < 0x00100000 || hx>>31 != 0 {
		if u<<1 == 0 {
			return -1 / (x * x) // log(+-0) = -inf
		}
		if hx>>31 != 0 {
			return (x - x) / 0.0 // log(-#) = NaN
		}
		// subnormal: scale x up
		k -= 54
		x *= 0x1p54
		u = math.Float64bits(x)
		hx = uint32(u >> 32)
	} else if hx >= 0x7ff00000 {
		return x
	} else if hx == 0x3ff00000 && u<<32 == 0 {
		return 0
	}
	// reduce x into [sqrt(2)/2, sqrt(2)]
	hx += 0x3ff00000 - 0x3fe6a09e
	k += int(hx>>20) - 0x3ff
	hx = (hx & 0x000fffff) + 0x3fe6a09e
	u = uint64(hx)<<32 | (u & 0xffffffff)
	x = math.Float64frombits(u)
	f := x - 1.0
	hfsq := float64(float64(0.5*f) * f)
	s := f / (2.0 + f)
	z := s * s
	w := z * z
	t1 := float64(w * (log10Lg2 + float64(w*(log10Lg4+float64(w*log10Lg6)))))
	t2 := float64(z * (log10Lg1 + float64(w*(log10Lg3+float64(w*(log10Lg5+float64(w*log10Lg7)))))))
	R := t2 + t1
	hi := f - hfsq
	hi = math.Float64frombits(math.Float64bits(hi) &^ 0xffffffff)
	lo := ((f - hi) - hfsq) + float64(s*(hfsq+R))
	valHi := float64(hi * log10Ivln10hi)
	dk := float64(k)
	y := float64(dk * log10Log10_2hi)
	valLo := float64(dk*log10Log10_2lo) + float64((lo+hi)*log10Ivln10lo) + float64(lo*log10Ivln10hi)
	w = y + valHi
	valLo += (y - w) + valHi
	valHi = w
	return valLo + valHi
}
