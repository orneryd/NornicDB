// remPio2 and remPio2Large are Go ports of musl 1.2.5's __rem_pio2.c and
// __rem_pio2_large.c (FreeBSD msun e_rem_pio2.c, k_rem_pio2.c):
//
//	Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
//	Developed at SunSoft, a Sun Microsystems, Inc. business.
//	Permission to use, copy, modify, and distribute this
//	software is freely granted, provided that this notice
//	is preserved.
//
// See NOTICES.md. Products that feed an addition are converted with
// float64(...) so no platform fuses them (see trig.go).

package libm

import "math"

var (
	remToint   = 1.5 / 2.220446049250313e-16 // 1.5/DBL_EPSILON
	remPio4    = 7.85398163397448278999e-01  // 0x1.921fb54442d18p-1
	remInvpio2 = 6.36619772367581382433e-01
	remPio2_1  = 1.57079632673412561417e+00
	remPio2_1t = 6.07710050650619224932e-11
	remPio2_2  = 6.07710050630396597660e-11
	remPio2_2t = 2.02226624879595063154e-21
	remPio2_3  = 2.02226624871116645580e-21
	remPio2_3t = 8.47842766036889956997e-32
)

// remPio2 reduces a finite x, |x| > pi/4, to y0 + y1 in [-pi/4, pi/4] with
// x = n*pi/2 + y0 + y1; it returns n. Sin, Cos and Tan return NaN for an
// infinite or NaN argument before reducing it.
func remPio2(x float64) (n int32, y0, y1 float64) {
	u := math.Float64bits(x)
	sign := u>>63 != 0
	ix := uint32(u>>32) & 0x7fffffff
	near := func(k float64, kn int32) (int32, float64, float64) {
		if !sign {
			z := x - float64(k*remPio2_1)
			y0 = z - float64(k*remPio2_1t)
			return kn, y0, (z - y0) - float64(k*remPio2_1t)
		}
		z := x + float64(k*remPio2_1)
		y0 = z + float64(k*remPio2_1t)
		return -kn, y0, (z - y0) + float64(k*remPio2_1t)
	}
	medium := false
	switch {
	case ix <= 0x400f6a7a: // |x| ~<= 5pi/4
		if ix&0xfffff == 0x921fb { // |x| ~= pi/2 or 2pi/2: cancellation
			medium = true
		} else if ix <= 0x4002d97c { // |x| ~<= 3pi/4
			return near(1, 1)
		} else {
			return near(2, 2)
		}
	case ix <= 0x401c463b: // |x| ~<= 9pi/4
		if ix <= 0x4015fdbc { // |x| ~<= 7pi/4
			if ix == 0x4012d97c { // |x| ~= 3pi/2
				medium = true
			} else {
				return near(3, 3)
			}
		} else {
			if ix == 0x401921fb { // |x| ~= 4pi/2
				medium = true
			} else {
				return near(4, 4)
			}
		}
	}
	if medium || ix < 0x413921fb { // |x| ~< 2^20*(pi/2), medium size
		fn := float64(x*remInvpio2) + remToint - remToint
		n = int32(fn)
		r := x - float64(fn*remPio2_1)
		w := float64(fn * remPio2_1t) // 1st round, good to 85 bits
		if r-w < -remPio4 {
			n--
			fn--
			r = x - float64(fn*remPio2_1)
			w = float64(fn * remPio2_1t)
		} else if r-w > remPio4 {
			n++
			fn++
			r = x - float64(fn*remPio2_1)
			w = float64(fn * remPio2_1t)
		}
		y0 = r - w
		ey := int(math.Float64bits(y0) >> 52 & 0x7ff)
		ex := int(ix >> 20)
		if ex-ey > 16 { // 2nd round, good to 118 bits
			t := r
			w = float64(fn * remPio2_2)
			r = t - w
			w = float64(fn*remPio2_2t) - ((t - r) - w)
			y0 = r - w
			ey = int(math.Float64bits(y0) >> 52 & 0x7ff)
			if ex-ey > 49 { // 3rd round, good to 151 bits
				t = r
				w = float64(fn * remPio2_3)
				r = t - w
				w = float64(fn*remPio2_3t) - ((t - r) - w)
				y0 = r - w
			}
		}
		y1 = (r - y0) - w
		return n, y0, y1
	}
	// z = scalbn(|x|, -ilogb(x)+23)
	z := math.Float64frombits(u&(^uint64(0)>>12) | uint64(0x3ff+23)<<52)
	var tx [3]float64
	i := 0
	for ; i < 2; i++ {
		tx[i] = float64(int32(z))
		z = (z - tx[i]) * 0x1p24
	}
	tx[i] = z
	for tx[i] == 0.0 { // skip zero terms; the first is non-zero
		i--
	}
	n, y0, y1 = remPio2Large(tx[:i+1], int(ix>>20)-(0x3ff+23))
	if sign {
		return -n, -y0, -y1
	}
	return n, y0, y1
}

// remPio2LargeIpio2 is 2/pi in 24-bit chunks, as many as a double needs.
var remPio2LargeIpio2 = [66]int32{
	0xA2F983, 0x6E4E44, 0x1529FC, 0x2757D1, 0xF534DD, 0xC0DB62,
	0x95993C, 0x439041, 0xFE5163, 0xABDEBB, 0xC561B7, 0x246E3A,
	0x424DD2, 0xE00649, 0x2EEA09, 0xD1921C, 0xFE1DEB, 0x1CB129,
	0xA73EE8, 0x8235F5, 0x2EBB44, 0x84E99C, 0x7026B4, 0x5F7E41,
	0x3991D6, 0x398353, 0x39F49C, 0x845F8B, 0xBDF928, 0x3B1FF8,
	0x97FFDE, 0x05980F, 0xEF2F11, 0x8B5A0A, 0x6D1F6D, 0x367ECF,
	0x27CB09, 0xB74F46, 0x3F669E, 0x5FEA2D, 0x7527BA, 0xC7EBE5,
	0xF17B3D, 0x0739F7, 0x8A5292, 0xEA6BFB, 0x5FB11F, 0x8D5D08,
	0x560330, 0x46FC7B, 0x6BABF0, 0xCFBC20, 0x9AF436, 0x1DA9E3,
	0x91615E, 0xE61B08, 0x659985, 0x5F14A0, 0x68408D, 0xFFD880,
	0x4D7327, 0x310606, 0x1556CA, 0x73A8C9, 0x60E27B, 0xC08C6B,
}

// remPio2LargePIo2 is pi/2 in 24-bit pieces.
var remPio2LargePIo2 = [8]float64{
	1.57079625129699707031e+00,
	7.54978941586159635335e-08,
	5.39030252995776476554e-15,
	3.28200341580791294123e-22,
	1.27065575308067607349e-29,
	1.22933308981111328932e-36,
	2.73370053816464559624e-44,
	2.16741683877804819444e-51,
}

// remPio2Large is __rem_pio2_large for double precision (prec 1, jk 4): x
// holds |input| in 24-bit pieces scaled by 2^-e0; it returns n mod 8 and
// the remainder y0 + y1.
func remPio2Large(x []float64, e0 int) (int32, float64, float64) {
	const jk = 4
	jp := jk
	var iq [20]int32
	var f, fq, q [20]float64
	jx := len(x) - 1
	// remPio2 calls this for |x| >= 2^20*(pi/2) only, so e0 >= -3 and jv,
	// which musl clamps at 0 for its float callers, is never negative.
	jv := (e0 - 3) / 24
	q0 := e0 - 24*(jv+1)
	j := jv - jx
	m := jx + jk
	for i := 0; i <= m; i, j = i+1, j+1 {
		if j < 0 {
			f[i] = 0.0
		} else {
			f[i] = float64(remPio2LargeIpio2[j])
		}
	}
	for i := 0; i <= jk; i++ {
		fw := 0.0
		for j := 0; j <= jx; j++ {
			fw += float64(x[j] * f[jx+i-j])
		}
		q[i] = fw
	}
	jz := jk
	var z float64
	var n int32
	var ih int32
	for {
		// distill q[] into iq[] reversingly
		i := 0
		j := jz
		z = q[jz]
		for ; j > 0; i, j = i+1, j-1 {
			fw := float64(int32(0x1p-24 * z))
			iq[i] = int32(z - float64(0x1p24*fw))
			z = q[j-1] + fw
		}
		// compute n
		z = math.Ldexp(z, q0)
		z -= 8.0 * math.Floor(z*0.125)
		n = int32(z)
		z -= float64(n)
		ih = 0
		if q0 > 0 { // need iq[jz-1] to determine n
			i := iq[jz-1] >> (24 - q0)
			n += i
			iq[jz-1] -= i << (24 - q0)
			ih = iq[jz-1] >> (23 - q0)
		} else if q0 == 0 {
			ih = iq[jz-1] >> 23
		} else if z >= 0.5 {
			ih = 2
		}
		if ih > 0 { // q > 0.5
			n++
			carry := int32(0)
			for i := 0; i < jz; i++ { // compute 1-q
				j := iq[i]
				if carry == 0 {
					if j != 0 {
						carry = 1
						iq[i] = 0x1000000 - j
					}
				} else {
					iq[i] = 0xffffff - j
				}
			}
			if q0 > 0 { // rare case: chance is 1 in 12
				switch q0 {
				case 1:
					iq[jz-1] &= 0x7fffff
				case 2:
					iq[jz-1] &= 0x3fffff
				}
			}
			if ih == 2 {
				z = 1.0 - z
				if carry != 0 {
					z -= math.Ldexp(1.0, q0)
				}
			}
		}
		// check if recomputation is needed
		if z == 0.0 {
			j := int32(0)
			for i := jz - 1; i >= jk; i-- {
				j |= iq[i]
			}
			if j == 0 { // need recomputation
				k := 1
				for iq[jk-k] == 0 { // k = no. of terms needed
					k++
				}
				for i := jz + 1; i <= jz+k; i++ { // add q[jz+1] to q[jz+k]
					f[jx+i] = float64(remPio2LargeIpio2[jv+i])
					fw := 0.0
					for j := 0; j <= jx; j++ {
						fw += float64(x[j] * f[jx+i-j])
					}
					q[i] = fw
				}
				jz += k
				continue
			}
		}
		break
	}
	// chop off zero terms
	if z == 0.0 {
		jz--
		q0 -= 24
		for iq[jz] == 0 {
			jz--
			q0 -= 24
		}
	} else { // break z into 24-bit if necessary
		z = math.Ldexp(z, -q0)
		if z >= 0x1p24 {
			fw := float64(int32(0x1p-24 * z))
			iq[jz] = int32(z - float64(0x1p24*fw))
			jz++
			q0 += 24
			iq[jz] = int32(fw)
		} else {
			iq[jz] = int32(z)
		}
	}
	// convert integer "bit" chunk to floating-point value
	fw := math.Ldexp(1.0, q0)
	for i := jz; i >= 0; i-- {
		q[i] = fw * float64(iq[i])
		fw *= 0x1p-24
	}
	// compute PIo2[0,...,jp]*q[jz,...,0]
	for i := jz; i >= 0; i-- {
		fw := 0.0
		for k := 0; k <= jp && k <= jz-i; k++ {
			fw += float64(remPio2LargePIo2[k] * q[i+k])
		}
		fq[jz-i] = fw
	}
	// compress fq[] into y[] (prec 1)
	fw = 0.0
	for i := jz; i >= 0; i-- {
		fw += fq[i]
	}
	y0 := fw
	if ih != 0 {
		y0 = -fw
	}
	fw = fq[0] - fw
	for i := 1; i <= jz; i++ {
		fw += fq[i]
	}
	y1 := fw
	if ih != 0 {
		y1 = -fw
	}
	return n & 7, y0, y1
}
