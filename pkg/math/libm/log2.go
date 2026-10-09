// Log2 is a Go port of musl's double-precision log2 function,
// Copyright (c) 2018, Arm Limited. SPDX-License-Identifier: MIT.
// See NOTICES.md for the licence text.

package libm

import "math"

const log2TableBits = 6

type log2Entry struct {
	invc, logc float64
}

type log2Data struct {
	invln2hi float64
	invln2lo float64
	poly     [6]float64  // First coefficient is 1.
	poly1    [10]float64 // First coefficient is 1.
	tab      [64]log2Entry
}

var log2DataTable = log2Data{
	invln2hi: 0x1.7154765200000p+0,
	invln2lo: 0x1.705fc2eefa200p-33,
	poly1: [10]float64{
		-0x1.71547652b82fep-1,
		0x1.ec709dc3a03f7p-2,
		-0x1.71547652b7c3fp-2,
		0x1.2776c50f05be4p-2,
		-0x1.ec709dd768fe5p-3,
		0x1.a61761ec4e736p-3,
		-0x1.7153fbc64a79bp-3,
		0x1.484d154f01b4ap-3,
		-0x1.289e4a72c383cp-3,
		0x1.0b32f285aee66p-3,
	},
	poly: [6]float64{
		-0x1.71547652b8339p-1,
		0x1.ec709dc3a04bep-2,
		-0x1.7154764702ffbp-2,
		0x1.2776c50034c48p-2,
		-0x1.ec7b328ea92bcp-3,
		0x1.a6225e117f92ep-3,
	},
	tab: [64]log2Entry{
		{0x1.724286bb1acf8p+0, -0x1.1095feecdb000p-1},
		{0x1.6e1f766d2cca1p+0, -0x1.08494bd76d000p-1},
		{0x1.6a13d0e30d48ap+0, -0x1.00143aee8f800p-1},
		{0x1.661ec32d06c85p+0, -0x1.efec5360b4000p-2},
		{0x1.623fa951198f8p+0, -0x1.dfdd91ab7e000p-2},
		{0x1.5e75ba4cf026cp+0, -0x1.cffae0cc79000p-2},
		{0x1.5ac055a214fb8p+0, -0x1.c043811fda000p-2},
		{0x1.571ed0f166e1ep+0, -0x1.b0b67323ae000p-2},
		{0x1.53909590bf835p+0, -0x1.a152f5a2db000p-2},
		{0x1.5014fed61adddp+0, -0x1.9217f5af86000p-2},
		{0x1.4cab88e487bd0p+0, -0x1.8304db0719000p-2},
		{0x1.49539b4334feep+0, -0x1.74189f9a9e000p-2},
		{0x1.460cbdfafd569p+0, -0x1.6552bb5199000p-2},
		{0x1.42d664ee4b953p+0, -0x1.56b23a29b1000p-2},
		{0x1.3fb01111dd8a6p+0, -0x1.483650f5fa000p-2},
		{0x1.3c995b70c5836p+0, -0x1.39de937f6a000p-2},
		{0x1.3991c4ab6fd4ap+0, -0x1.2baa1538d6000p-2},
		{0x1.3698e0ce099b5p+0, -0x1.1d98340ca4000p-2},
		{0x1.33ae48213e7b2p+0, -0x1.0fa853a40e000p-2},
		{0x1.30d191985bdb1p+0, -0x1.01d9c32e73000p-2},
		{0x1.2e025cab271d7p+0, -0x1.e857da2fa6000p-3},
		{0x1.2b404cf13cd82p+0, -0x1.cd3c8633d8000p-3},
		{0x1.288b02c7ccb50p+0, -0x1.b26034c14a000p-3},
		{0x1.25e2263944de5p+0, -0x1.97c1c2f4fe000p-3},
		{0x1.234563d8615b1p+0, -0x1.7d6023f800000p-3},
		{0x1.20b46e33eaf38p+0, -0x1.633a71a05e000p-3},
		{0x1.1e2eefdcda3ddp+0, -0x1.494f5e9570000p-3},
		{0x1.1bb4a580b3930p+0, -0x1.2f9e424e0a000p-3},
		{0x1.19453847f2200p+0, -0x1.162595afdc000p-3},
		{0x1.16e06c0d5d73cp+0, -0x1.f9c9a75bd8000p-4},
		{0x1.1485f47b7e4c2p+0, -0x1.c7b575bf9c000p-4},
		{0x1.12358ad0085d1p+0, -0x1.960c60ff48000p-4},
		{0x1.0fef00f532227p+0, -0x1.64ce247b60000p-4},
		{0x1.0db2077d03a8fp+0, -0x1.33f78b2014000p-4},
		{0x1.0b7e6d65980d9p+0, -0x1.0387d1a42c000p-4},
		{0x1.0953efe7b408dp+0, -0x1.a6f9208b50000p-5},
		{0x1.07325cac53b83p+0, -0x1.47a954f770000p-5},
		{0x1.05197e40d1b5cp+0, -0x1.d23a8c50c0000p-6},
		{0x1.03091c1208ea2p+0, -0x1.16a2629780000p-6},
		{0x1.0101025b37e21p+0, -0x1.720f8d8e80000p-8},
		{0x1.fc07ef9caa76bp-1, 0x1.6fe53b1500000p-7},
		{0x1.f4465d3f6f184p-1, 0x1.11ccce10f8000p-5},
		{0x1.ecc079f84107fp-1, 0x1.c4dfc8c8b8000p-5},
		{0x1.e573a99975ae8p-1, 0x1.3aa321e574000p-4},
		{0x1.de5d6f0bd3de6p-1, 0x1.918a0d08b8000p-4},
		{0x1.d77b681ff38b3p-1, 0x1.e72e9da044000p-4},
		{0x1.d0cb5724de943p-1, 0x1.1dcd2507f6000p-3},
		{0x1.ca4b2dc0e7563p-1, 0x1.476ab03dea000p-3},
		{0x1.c3f8ee8d6cb51p-1, 0x1.7074377e22000p-3},
		{0x1.bdd2b4f020c4cp-1, 0x1.98ede8ba94000p-3},
		{0x1.b7d6c006015cap-1, 0x1.c0db86ad2e000p-3},
		{0x1.b20366e2e338fp-1, 0x1.e840aafcee000p-3},
		{0x1.ac57026295039p-1, 0x1.0790ab4678000p-2},
		{0x1.a6d01bc2731ddp-1, 0x1.1ac056801c000p-2},
		{0x1.a16d3bc3ff18bp-1, 0x1.2db11d4fee000p-2},
		{0x1.9c2d14967feadp-1, 0x1.406464ec58000p-2},
		{0x1.970e4f47c9902p-1, 0x1.52dbe093af000p-2},
		{0x1.920fb3982bcf2p-1, 0x1.651902050d000p-2},
		{0x1.8d30187f759f1p-1, 0x1.771d2cdeaf000p-2},
		{0x1.886e5ebb9f66dp-1, 0x1.88e9c857d9000p-2},
		{0x1.83c97b658b994p-1, 0x1.9a80155e16000p-2},
		{0x1.7f405ffc61022p-1, 0x1.abe186ed3d000p-2},
		{0x1.7ad22181415cap-1, 0x1.bd0f2aea0e000p-2},
		{0x1.767dcf99eff8cp-1, 0x1.ce0a43dbf4000p-2},
	},
}

// Log2 returns the binary logarithm of x.
func Log2(x float64) float64 {
	var z, r, r2, r4, y, invc, logc, kd, hi, lo, t1, t2, t3, p float64
	var ix, iz, tmp uint64
	var top uint64
	var k, i int

	ix = asUint64(x)
	top = top16(x)
	loBound := asUint64(1.0 - 0x1.5b51p-5)
	hiBound := asUint64(1.0 + 0x1.6ab2p-5)
	if ix-loBound < hiBound-loBound {
		// Handle close to 1.0 inputs separately.
		// Fix sign of zero with downward rounding when x==1.
		if ix == asUint64(1.0) {
			return 0
		}
		r = x - 1.0
		hi = r * log2DataTable.invln2hi
		lo = r*log2DataTable.invln2lo + math.FMA(r, log2DataTable.invln2hi, -hi)
		r2 = r * r // rounding error: 0x1p-62.
		r4 = r2 * r2
		// Worst-case error is less than 0.54 ULP (0.55 ULP without fma).
		p = r2 * (log2DataTable.poly1[0] + r*log2DataTable.poly1[1])
		y = hi + p
		lo += hi - y + p
		lo += r4 * (log2DataTable.poly1[2] + r*log2DataTable.poly1[3] +
			r2*(log2DataTable.poly1[4]+r*log2DataTable.poly1[5]) +
			r4*(log2DataTable.poly1[6]+r*log2DataTable.poly1[7]+
				r2*(log2DataTable.poly1[8]+r*log2DataTable.poly1[9])))
		y += lo
		return evalAsDouble(y)
	}
	if top-0x0010 >= 0x7ff0-0x0010 {
		// x < 0x1p-1022 or inf or nan.
		if ix*2 == 0 {
			return -inf
		}
		if ix == asUint64(inf) { // log(inf) == inf.
			return x
		}
		if top&0x8000 != 0 || top&0x7ff0 == 0x7ff0 {
			return nan
		}
		// x is subnormal, normalize it.
		ix = asUint64(x * 0x1p52)
		ix -= 52 << 52
	}

	// x = 2^k z; where z is in range [OFF,2*OFF) and exact.
	// The range is split into N subintervals.
	// The ith subinterval contains z and c is near its center.
	tmp = ix - 0x3fe6000000000000
	i = int((tmp >> (52 - log2TableBits)) % 64)
	k = int(int64(tmp) >> 52) // arithmetic shift
	iz = ix - (tmp & (0xfff << 52))
	invc = log2DataTable.tab[i].invc
	logc = log2DataTable.tab[i].logc
	z = asFloat64(iz)
	kd = float64(k)

	// log2(x) = log2(z/c) + log2(c) + k.
	// r ~= z/c - 1, |r| < 1/(2*N).
	// rounding error: 0x1p-55/N.
	r = math.FMA(z, invc, -1.0)
	t1 = r * log2DataTable.invln2hi
	t2 = r*log2DataTable.invln2lo + math.FMA(r, log2DataTable.invln2hi, -t1)

	// hi + lo = r/ln2 + log2(c) + k.
	t3 = kd + logc
	hi = t3 + t1
	lo = t3 - hi + t1 + t2

	// log2(r+1) = r/ln2 + r^2*poly(r).
	// Evaluation is optimized assuming superscalar pipelined execution.
	r2 = r * r // rounding error: 0x1p-54/N^2.
	r4 = r2 * r2
	p = log2DataTable.poly[0] + r*log2DataTable.poly[1] +
		r2*(log2DataTable.poly[2]+r*log2DataTable.poly[3]) +
		r4*(log2DataTable.poly[4]+r*log2DataTable.poly[5])
	y = lo + r2*p + hi
	return evalAsDouble(y)
}
