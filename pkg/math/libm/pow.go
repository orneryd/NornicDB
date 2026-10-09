// Pow is a Go port of musl's double-precision pow function,
// Copyright (c) 2018, Arm Limited. SPDX-License-Identifier: MIT.
// See NOTICES.md for the licence text.

package libm

import "math"

const powLogTableBits = 7
const powSignBias uint64 = 0x800 << 7 // SIGN_BIAS

type powLogEntry struct {
	invc, pad, logc, logctail float64
}

type powLogData struct {
	ln2hi float64
	ln2lo float64
	poly  [7]float64 // First coefficient is 1.
	tab   [128]powLogEntry
}

var powLogDataTable = powLogData{
	ln2hi: 0x1.62e42fefa3800p-1,
	ln2lo: 0x1.ef35793c76730p-45,
	poly: [7]float64{
		-0x1p-1,
		0x1.555555555556p-2 * -2,
		-0x1.0000000000006p-2 * -2,
		0x1.999999959554ep-3 * 4,
		-0x1.555555529a47ap-3 * 4,
		0x1.2495b9b4845e9p-3 * -8,
		-0x1.0002b8b263fc3p-3 * -8,
	},
	tab: [128]powLogEntry{
		{0x1.6a00000000000p+0, 0, -0x1.62c82f2b9c800p-2, 0x1.ab42428375680p-48},
		{0x1.6800000000000p+0, 0, -0x1.5d1bdbf580800p-2, -0x1.ca508d8e0f720p-46},
		{0x1.6600000000000p+0, 0, -0x1.5767717455800p-2, -0x1.362a4d5b6506dp-45},
		{0x1.6400000000000p+0, 0, -0x1.51aad872df800p-2, -0x1.684e49eb067d5p-49},
		{0x1.6200000000000p+0, 0, -0x1.4be5f95777800p-2, -0x1.41b6993293ee0p-47},
		{0x1.6000000000000p+0, 0, -0x1.4618bc21c6000p-2, 0x1.3d82f484c84ccp-46},
		{0x1.5e00000000000p+0, 0, -0x1.404308686a800p-2, 0x1.c42f3ed820b3ap-50},
		{0x1.5c00000000000p+0, 0, -0x1.3a64c55694800p-2, 0x1.0b1c686519460p-45},
		{0x1.5a00000000000p+0, 0, -0x1.347dd9a988000p-2, 0x1.5594dd4c58092p-45},
		{0x1.5800000000000p+0, 0, -0x1.2e8e2bae12000p-2, 0x1.67b1e99b72bd8p-45},
		{0x1.5600000000000p+0, 0, -0x1.2895a13de8800p-2, 0x1.5ca14b6cfb03fp-46},
		{0x1.5600000000000p+0, 0, -0x1.2895a13de8800p-2, 0x1.5ca14b6cfb03fp-46},
		{0x1.5400000000000p+0, 0, -0x1.22941fbcf7800p-2, -0x1.65a242853da76p-46},
		{0x1.5200000000000p+0, 0, -0x1.1c898c1699800p-2, -0x1.fafbc68e75404p-46},
		{0x1.5000000000000p+0, 0, -0x1.1675cababa800p-2, 0x1.f1fc63382a8f0p-46},
		{0x1.4e00000000000p+0, 0, -0x1.1058bf9ae4800p-2, -0x1.6a8c4fd055a66p-45},
		{0x1.4c00000000000p+0, 0, -0x1.0a324e2739000p-2, -0x1.c6bee7ef4030ep-47},
		{0x1.4a00000000000p+0, 0, -0x1.0402594b4d000p-2, -0x1.036b89ef42d7fp-48},
		{0x1.4a00000000000p+0, 0, -0x1.0402594b4d000p-2, -0x1.036b89ef42d7fp-48},
		{0x1.4800000000000p+0, 0, -0x1.fb9186d5e4000p-3, 0x1.d572aab993c87p-47},
		{0x1.4600000000000p+0, 0, -0x1.ef0adcbdc6000p-3, 0x1.b26b79c86af24p-45},
		{0x1.4400000000000p+0, 0, -0x1.e27076e2af000p-3, -0x1.72f4f543fff10p-46},
		{0x1.4200000000000p+0, 0, -0x1.d5c216b4fc000p-3, 0x1.1ba91bbca681bp-45},
		{0x1.4000000000000p+0, 0, -0x1.c8ff7c79aa000p-3, 0x1.7794f689f8434p-45},
		{0x1.4000000000000p+0, 0, -0x1.c8ff7c79aa000p-3, 0x1.7794f689f8434p-45},
		{0x1.3e00000000000p+0, 0, -0x1.bc286742d9000p-3, 0x1.94eb0318bb78fp-46},
		{0x1.3c00000000000p+0, 0, -0x1.af3c94e80c000p-3, 0x1.a4e633fcd9066p-52},
		{0x1.3a00000000000p+0, 0, -0x1.a23bc1fe2b000p-3, -0x1.58c64dc46c1eap-45},
		{0x1.3a00000000000p+0, 0, -0x1.a23bc1fe2b000p-3, -0x1.58c64dc46c1eap-45},
		{0x1.3800000000000p+0, 0, -0x1.9525a9cf45000p-3, -0x1.ad1d904c1d4e3p-45},
		{0x1.3600000000000p+0, 0, -0x1.87fa06520d000p-3, 0x1.bbdbf7fdbfa09p-45},
		{0x1.3400000000000p+0, 0, -0x1.7ab890210e000p-3, 0x1.bdb9072534a58p-45},
		{0x1.3400000000000p+0, 0, -0x1.7ab890210e000p-3, 0x1.bdb9072534a58p-45},
		{0x1.3200000000000p+0, 0, -0x1.6d60fe719d000p-3, -0x1.0e46aa3b2e266p-46},
		{0x1.3000000000000p+0, 0, -0x1.5ff3070a79000p-3, -0x1.e9e439f105039p-46},
		{0x1.3000000000000p+0, 0, -0x1.5ff3070a79000p-3, -0x1.e9e439f105039p-46},
		{0x1.2e00000000000p+0, 0, -0x1.526e5e3a1b000p-3, -0x1.0de8b90075b8fp-45},
		{0x1.2c00000000000p+0, 0, -0x1.44d2b6ccb8000p-3, 0x1.70cc16135783cp-46},
		{0x1.2c00000000000p+0, 0, -0x1.44d2b6ccb8000p-3, 0x1.70cc16135783cp-46},
		{0x1.2a00000000000p+0, 0, -0x1.371fc201e9000p-3, 0x1.178864d27543ap-48},
		{0x1.2800000000000p+0, 0, -0x1.29552f81ff000p-3, -0x1.48d301771c408p-45},
		{0x1.2600000000000p+0, 0, -0x1.1b72ad52f6000p-3, -0x1.e80a41811a396p-45},
		{0x1.2600000000000p+0, 0, -0x1.1b72ad52f6000p-3, -0x1.e80a41811a396p-45},
		{0x1.2400000000000p+0, 0, -0x1.0d77e7cd09000p-3, 0x1.a699688e85bf4p-47},
		{0x1.2400000000000p+0, 0, -0x1.0d77e7cd09000p-3, 0x1.a699688e85bf4p-47},
		{0x1.2200000000000p+0, 0, -0x1.fec9131dbe000p-4, -0x1.575545ca333f2p-45},
		{0x1.2000000000000p+0, 0, -0x1.e27076e2b0000p-4, 0x1.a342c2af0003cp-45},
		{0x1.2000000000000p+0, 0, -0x1.e27076e2b0000p-4, 0x1.a342c2af0003cp-45},
		{0x1.1e00000000000p+0, 0, -0x1.c5e548f5bc000p-4, -0x1.d0c57585fbe06p-46},
		{0x1.1c00000000000p+0, 0, -0x1.a926d3a4ae000p-4, 0x1.53935e85baac8p-45},
		{0x1.1c00000000000p+0, 0, -0x1.a926d3a4ae000p-4, 0x1.53935e85baac8p-45},
		{0x1.1a00000000000p+0, 0, -0x1.8c345d631a000p-4, 0x1.37c294d2f5668p-46},
		{0x1.1a00000000000p+0, 0, -0x1.8c345d631a000p-4, 0x1.37c294d2f5668p-46},
		{0x1.1800000000000p+0, 0, -0x1.6f0d28ae56000p-4, -0x1.69737c93373dap-45},
		{0x1.1600000000000p+0, 0, -0x1.51b073f062000p-4, 0x1.f025b61c65e57p-46},
		{0x1.1600000000000p+0, 0, -0x1.51b073f062000p-4, 0x1.f025b61c65e57p-46},
		{0x1.1400000000000p+0, 0, -0x1.341d7961be000p-4, 0x1.c5edaccf913dfp-45},
		{0x1.1400000000000p+0, 0, -0x1.341d7961be000p-4, 0x1.c5edaccf913dfp-45},
		{0x1.1200000000000p+0, 0, -0x1.16536eea38000p-4, 0x1.47c5e768fa309p-46},
		{0x1.1000000000000p+0, 0, -0x1.f0a30c0118000p-5, 0x1.d599e83368e91p-45},
		{0x1.1000000000000p+0, 0, -0x1.f0a30c0118000p-5, 0x1.d599e83368e91p-45},
		{0x1.0e00000000000p+0, 0, -0x1.b42dd71198000p-5, 0x1.c827ae5d6704cp-46},
		{0x1.0e00000000000p+0, 0, -0x1.b42dd71198000p-5, 0x1.c827ae5d6704cp-46},
		{0x1.0c00000000000p+0, 0, -0x1.77458f632c000p-5, -0x1.cfc4634f2a1eep-45},
		{0x1.0c00000000000p+0, 0, -0x1.77458f632c000p-5, -0x1.cfc4634f2a1eep-45},
		{0x1.0a00000000000p+0, 0, -0x1.39e87b9fec000p-5, 0x1.502b7f526feaap-48},
		{0x1.0a00000000000p+0, 0, -0x1.39e87b9fec000p-5, 0x1.502b7f526feaap-48},
		{0x1.0800000000000p+0, 0, -0x1.f829b0e780000p-6, -0x1.980267c7e09e4p-45},
		{0x1.0800000000000p+0, 0, -0x1.f829b0e780000p-6, -0x1.980267c7e09e4p-45},
		{0x1.0600000000000p+0, 0, -0x1.7b91b07d58000p-6, -0x1.88d5493faa639p-45},
		{0x1.0400000000000p+0, 0, -0x1.fc0a8b0fc0000p-7, -0x1.f1e7cf6d3a69cp-50},
		{0x1.0400000000000p+0, 0, -0x1.fc0a8b0fc0000p-7, -0x1.f1e7cf6d3a69cp-50},
		{0x1.0200000000000p+0, 0, -0x1.fe02a6b100000p-8, -0x1.9e23f0dda40e4p-46},
		{0x1.0200000000000p+0, 0, -0x1.fe02a6b100000p-8, -0x1.9e23f0dda40e4p-46},
		{0x1.0000000000000p+0, 0, 0x0.0000000000000p+0, 0x0.0000000000000p+0},
		{0x1.0000000000000p+0, 0, 0x0.0000000000000p+0, 0x0.0000000000000p+0},
		{0x1.fc00000000000p-1, 0, 0x1.0101575890000p-7, -0x1.0c76b999d2be8p-46},
		{0x1.f800000000000p-1, 0, 0x1.0205658938000p-6, -0x1.3dc5b06e2f7d2p-45},
		{0x1.f400000000000p-1, 0, 0x1.8492528c90000p-6, -0x1.aa0ba325a0c34p-45},
		{0x1.f000000000000p-1, 0, 0x1.0415d89e74000p-5, 0x1.111c05cf1d753p-47},
		{0x1.ec00000000000p-1, 0, 0x1.466aed42e0000p-5, -0x1.c167375bdfd28p-45},
		{0x1.e800000000000p-1, 0, 0x1.894aa149fc000p-5, -0x1.97995d05a267dp-46},
		{0x1.e400000000000p-1, 0, 0x1.ccb73cdddc000p-5, -0x1.a68f247d82807p-46},
		{0x1.e200000000000p-1, 0, 0x1.eea31c006c000p-5, -0x1.e113e4fc93b7bp-47},
		{0x1.de00000000000p-1, 0, 0x1.1973bd1466000p-4, -0x1.5325d560d9e9bp-45},
		{0x1.da00000000000p-1, 0, 0x1.3bdf5a7d1e000p-4, 0x1.cc85ea5db4ed7p-45},
		{0x1.d600000000000p-1, 0, 0x1.5e95a4d97a000p-4, -0x1.c69063c5d1d1ep-45},
		{0x1.d400000000000p-1, 0, 0x1.700d30aeac000p-4, 0x1.c1e8da99ded32p-49},
		{0x1.d000000000000p-1, 0, 0x1.9335e5d594000p-4, 0x1.3115c3abd47dap-45},
		{0x1.cc00000000000p-1, 0, 0x1.b6ac88dad6000p-4, -0x1.390802bf768e5p-46},
		{0x1.ca00000000000p-1, 0, 0x1.c885801bc4000p-4, 0x1.646d1c65aacd3p-45},
		{0x1.c600000000000p-1, 0, 0x1.ec739830a2000p-4, -0x1.dc068afe645e0p-45},
		{0x1.c400000000000p-1, 0, 0x1.fe89139dbe000p-4, -0x1.534d64fa10afdp-45},
		{0x1.c000000000000p-1, 0, 0x1.1178e8227e000p-3, 0x1.1ef78ce2d07f2p-45},
		{0x1.be00000000000p-1, 0, 0x1.1aa2b7e23f000p-3, 0x1.ca78e44389934p-45},
		{0x1.ba00000000000p-1, 0, 0x1.2d1610c868000p-3, 0x1.39d6ccb81b4a1p-47},
		{0x1.b800000000000p-1, 0, 0x1.365fcb0159000p-3, 0x1.62fa8234b7289p-51},
		{0x1.b400000000000p-1, 0, 0x1.4913d8333b000p-3, 0x1.5837954fdb678p-45},
		{0x1.b200000000000p-1, 0, 0x1.527e5e4a1b000p-3, 0x1.633e8e5697dc7p-45},
		{0x1.ae00000000000p-1, 0, 0x1.6574ebe8c1000p-3, 0x1.9cf8b2c3c2e78p-46},
		{0x1.ac00000000000p-1, 0, 0x1.6f0128b757000p-3, -0x1.5118de59c21e1p-45},
		{0x1.aa00000000000p-1, 0, 0x1.7898d85445000p-3, -0x1.c661070914305p-46},
		{0x1.a600000000000p-1, 0, 0x1.8beafeb390000p-3, -0x1.73d54aae92cd1p-47},
		{0x1.a400000000000p-1, 0, 0x1.95a5adcf70000p-3, 0x1.7f22858a0ff6fp-47},
		{0x1.a000000000000p-1, 0, 0x1.a93ed3c8ae000p-3, -0x1.8724350562169p-45},
		{0x1.9e00000000000p-1, 0, 0x1.b31d8575bd000p-3, -0x1.c358d4eace1aap-47},
		{0x1.9c00000000000p-1, 0, 0x1.bd087383be000p-3, -0x1.d4bc4595412b6p-45},
		{0x1.9a00000000000p-1, 0, 0x1.c6ffbc6f01000p-3, -0x1.1ec72c5962bd2p-48},
		{0x1.9600000000000p-1, 0, 0x1.db13db0d49000p-3, -0x1.aff2af715b035p-45},
		{0x1.9400000000000p-1, 0, 0x1.e530effe71000p-3, 0x1.212276041f430p-51},
		{0x1.9200000000000p-1, 0, 0x1.ef5ade4dd0000p-3, -0x1.a211565bb8e11p-51},
		{0x1.9000000000000p-1, 0, 0x1.f991c6cb3b000p-3, 0x1.bcbecca0cdf30p-46},
		{0x1.8c00000000000p-1, 0, 0x1.07138604d5800p-2, 0x1.89cdb16ed4e91p-48},
		{0x1.8a00000000000p-1, 0, 0x1.0c42d67616000p-2, 0x1.7188b163ceae9p-45},
		{0x1.8800000000000p-1, 0, 0x1.1178e8227e800p-2, -0x1.c210e63a5f01cp-45},
		{0x1.8600000000000p-1, 0, 0x1.16b5ccbacf800p-2, 0x1.b9acdf7a51681p-45},
		{0x1.8400000000000p-1, 0, 0x1.1bf99635a6800p-2, 0x1.ca6ed5147bdb7p-45},
		{0x1.8200000000000p-1, 0, 0x1.214456d0eb800p-2, 0x1.a87deba46baeap-47},
		{0x1.7e00000000000p-1, 0, 0x1.2bef07cdc9000p-2, 0x1.a9cfa4a5004f4p-45},
		{0x1.7c00000000000p-1, 0, 0x1.314f1e1d36000p-2, -0x1.8e27ad3213cb8p-45},
		{0x1.7a00000000000p-1, 0, 0x1.36b6776be1000p-2, 0x1.16ecdb0f177c8p-46},
		{0x1.7800000000000p-1, 0, 0x1.3c25277333000p-2, 0x1.83b54b606bd5cp-46},
		{0x1.7600000000000p-1, 0, 0x1.419b423d5e800p-2, 0x1.8e436ec90e09dp-47},
		{0x1.7400000000000p-1, 0, 0x1.4718dc271c800p-2, -0x1.f27ce0967d675p-45},
		{0x1.7200000000000p-1, 0, 0x1.4c9e09e173000p-2, -0x1.e20891b0ad8a4p-45},
		{0x1.7000000000000p-1, 0, 0x1.522ae0738a000p-2, 0x1.ebe708164c759p-45},
		{0x1.6e00000000000p-1, 0, 0x1.57bf753c8d000p-2, 0x1.fadedee5d40efp-46},
		{0x1.6c00000000000p-1, 0, 0x1.5d5bddf596000p-2, -0x1.a0b2a08a465dcp-47},
	},
}

// logInline computes y+*tail = log(x) where the rounded result is y and
// *tail has about 15 additional bits precision. ix is the bit representation
// of x, but normalized in the subnormal range using the sign bit for the
// exponent.
func logInline(ix uint64, tail *float64) float64 {
	var z, r, y, invc, logc, logctail, kd, hi, t1, t2, lo, lo1, lo2, p float64
	var iz, tmp uint64
	var k, i int

	// x = 2^k z; where z is in range [OFF,2*OFF) and exact.
	// The range is split into N subintervals.
	// The ith subinterval contains z and c is near its center.
	tmp = ix - 0x3fe6955500000000
	i = int((tmp >> (52 - powLogTableBits)) % 128)
	k = int(int64(tmp) >> 52) // arithmetic shift
	iz = ix - (tmp & (0xfff << 52))
	z = asFloat64(iz)
	kd = float64(k)

	// log(x) = k*Ln2 + log(c) + log1p(z/c-1).
	invc = powLogDataTable.tab[i].invc
	logc = powLogDataTable.tab[i].logc
	logctail = powLogDataTable.tab[i].logctail

	// Note: 1/c is j/N or j/N/2 where j is an integer in [N,2N) and
	// |z/c - 1| < 1/N, so r = z/c - 1 is exactly representible.
	r = math.FMA(z, invc, -1.0)

	// k*Ln2 + log(c) + r.
	t1 = kd*powLogDataTable.ln2hi + logc
	t2 = t1 + r
	lo1 = kd*powLogDataTable.ln2lo + logctail
	lo2 = t1 - t2 + r

	// Evaluation is optimized assuming superscalar pipelined execution.
	ar := powLogDataTable.poly[0] * r // poly[0] = -0.5.
	ar2 := r * ar
	ar3 := r * ar2
	// k*Ln2 + log(c) + r + poly[0]*r*r.
	hi = t2 + ar2
	lo3 := math.FMA(ar, r, -ar2)
	lo4 := t2 - hi + ar2

	// p = log1p(r) - r - poly[0]*r*r.
	p = ar3 * (powLogDataTable.poly[1] + r*powLogDataTable.poly[2] +
		ar2*(powLogDataTable.poly[3]+r*powLogDataTable.poly[4]+
			ar2*(powLogDataTable.poly[5]+r*powLogDataTable.poly[6])))
	lo = lo1 + lo2 + lo3 + lo4 + p
	y = hi + lo
	*tail = hi - y + lo
	return y
}

// powSpecialcase handles cases that may overflow or underflow when computing
// the result that is scale*(1+tmp) without intermediate rounding.
func powSpecialcase(tmp float64, sbits uint64, ki uint64) float64 {
	var scale, y float64

	if ki&0x80000000 == 0 {
		// k > 0, the exponent of scale might have overflowed by <= 460.
		sbits -= 1009 << 52
		scale = asFloat64(sbits)
		y = 0x1p1009 * (scale + scale*tmp)
		return evalAsDouble(y)
	}
	// k < 0, need special care in the subnormal range.
	sbits += 1022 << 52
	// Note: sbits is signed scale.
	scale = asFloat64(sbits)
	y = scale + scale*tmp
	if math.Abs(y) < 1.0 {
		// Round y to the right precision before scaling it into the subnormal
		// range to avoid double rounding that can cause 0.5+E/2 ulp error
		// where E is the worst-case ulp error outside the subnormal range.
		one := 1.0
		if y < 0.0 {
			one = -1.0
		}
		lo := scale - y + scale*tmp
		hi := one + y
		lo = one - hi + y + lo
		y = evalAsDouble(hi+lo) - one
		// Fix the sign of 0.
		if y == 0.0 {
			y = asFloat64(sbits & 0x8000000000000000)
		}
	}
	y = 0x1p-1022 * y
	return evalAsDouble(y)
}

// expInline computes sign*exp(x+xtail) where |xtail| < 2^-8/N and
// |xtail| <= |x|. The signBias argument is powSignBias or 0 and sets the
// sign to -1 or 1.
func expInline(x float64, xtail float64, signBias uint64) float64 {
	abstop := top12(x) & 0x7ff
	var ki, idx, top, sbits uint64
	var kd, z, r, r2, scale, tail, tmp float64

	if abstop-top12(0x1p-54) >= top12(512.0)-top12(0x1p-54) {
		if abstop-top12(0x1p-54) >= 0x80000000 {
			// Avoid spurious underflow for tiny x. Note: 0 is common input.
			one := 1.0 + x
			if signBias != 0 {
				return -one
			}
			return one
		}
		if abstop >= top12(1024.0) {
			// Note: inf and nan are already handled.
			if asUint64(x)>>63 != 0 {
				return uflow(signBias != 0)
			}
			return oflow(signBias != 0)
		}
		// Large x is special cased below.
		abstop = 0
	}

	// exp(x) = 2^(k/N) * exp(r), with exp(r) in [2^(-1/2N),2^(1/2N)].
	// x = ln2/N*k + r, with int k and r in [-ln2/2N, ln2/2N].
	z = expDataTable.invln2N * x
	// z - kd is in [-1, 1] in non-nearest rounding modes.
	kd = evalAsDouble(z + expDataTable.shift)
	ki = asUint64(kd)
	kd -= expDataTable.shift
	r = x + kd*expDataTable.negln2hiN + kd*expDataTable.negln2loN
	// The code assumes 2^-200 < |xtail| < 2^-8/N.
	r += xtail
	// 2^(k/N) ~= scale * (1 + tail).
	idx = 2 * (ki % 128)
	top = (ki + signBias) << (52 - expTableBits)
	tail = asFloat64(expDataTable.tab[idx])
	// This is only a valid scale when -1023*N < k < 1024*N.
	sbits = expDataTable.tab[idx+1] + top
	// exp(x) = 2^(k/N) * exp(r) ~= scale + scale * (tail + exp(r) - 1).
	// Evaluation is optimized assuming superscalar pipelined execution.
	r2 = r * r
	tmp = tail + r + r2*(expDataTable.poly[0]+r*expDataTable.poly[1]) +
		r2*r2*(expDataTable.poly[2]+r*expDataTable.poly[3])
	if abstop == 0 {
		return powSpecialcase(tmp, sbits, ki)
	}
	scale = asFloat64(sbits)
	return evalAsDouble(scale + scale*tmp)
}

// checkint returns 0 if not int, 1 if odd int, 2 if even int. The argument
// is the bit representation of a non-zero finite floating-point value.
func checkint(iy uint64) int {
	e := iy >> 52 & 0x7ff
	if e < 0x3ff {
		return 0
	}
	if e > 0x3ff+52 {
		return 2
	}
	if iy&((uint64(1)<<(0x3ff+52-e))-1) != 0 {
		return 0
	}
	if iy&(uint64(1)<<(0x3ff+52-e)) != 0 {
		return 1
	}
	return 2
}

// zeroinfnan returns true if i is the bit representation of 0, infinity or
// nan.
func zeroinfnan(i uint64) bool {
	return 2*i-1 >= 2*asUint64(inf)-1
}

// Pow returns x**y, the base-x exponential of y.
func Pow(x, y float64) float64 {
	var signBias uint64
	var ix, iy uint64
	var topx, topy uint64

	ix = asUint64(x)
	iy = asUint64(y)
	topx = top12(x)
	topy = top12(y)
	if topx-0x001 >= 0x7ff-0x001 ||
		(topy&0x7ff)-0x3be >= 0x43e-0x3be {
		// Note: if |y| > 1075 * ln2 * 2^53 ~= 0x1.749p62 then pow(x,y) =
		// inf/0 and if |y| < 2^-54 / 1075 ~= 0x1.e7b6p-65 then pow(x,y) = +-1.
		// Special cases: (x < 0x1p-126 or inf or nan) or
		// (|y| < 0x1p-65 or |y| >= 0x1p63 or nan).
		if zeroinfnan(iy) {
			if 2*iy == 0 {
				return 1.0
			}
			if ix == asUint64(1.0) {
				return 1.0
			}
			if 2*ix > 2*asUint64(inf) || 2*iy > 2*asUint64(inf) {
				return x + y
			}
			if 2*ix == 2*asUint64(1.0) {
				return 1.0
			}
			if (2*ix < 2*asUint64(1.0)) == (iy>>63 == 0) {
				return 0.0 // |x|<1 && y==inf or |x|>1 && y==-inf.
			}
			return y * y
		}
		if zeroinfnan(ix) {
			x2 := x * x
			if ix>>63 != 0 && checkint(iy) == 1 {
				x2 = -x2
			}
			if iy>>63 != 0 {
				return 1 / x2
			}
			return x2
		}
		// Here x and y are non-zero finite.
		if ix>>63 != 0 {
			// Finite x < 0.
			yint := checkint(iy)
			if yint == 0 {
				return nan
			}
			if yint == 1 {
				signBias = powSignBias
			}
			ix &= 0x7fffffffffffffff
			topx &= 0x7ff
		}
		if (topy&0x7ff)-0x3be >= 0x43e-0x3be {
			// Note: signBias == 0 here because y is not odd.
			if ix == asUint64(1.0) {
				return 1.0
			}
			if topy&0x7ff < 0x3be {
				// |y| < 2^-65, x^y ~= 1 + y*log(x).
				if ix > asUint64(1.0) {
					return 1.0 + y
				}
				return 1.0 - y
			}
			if (ix > asUint64(1.0)) == (topy < 0x800) {
				return oflow(false)
			}
			return uflow(false)
		}
		if topx == 0 {
			// Normalize subnormal x so exponent becomes negative.
			ix = asUint64(x * 0x1p52)
			ix &= 0x7fffffffffffffff
			ix -= 52 << 52
		}
	}

	var lo, hi float64
	hi = logInline(ix, &lo)
	var ehi, elo float64
	ehi = y * hi
	elo = y*lo + math.FMA(y, hi, -ehi)
	return expInline(ehi, elo, signBias)
}
