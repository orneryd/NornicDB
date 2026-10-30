// Log is a Go port of musl's double-precision log function,
// Copyright (c) 2018, Arm Limited. SPDX-License-Identifier: MIT.
// See NOTICES.md for the licence text.

package libm

import "math"

const logTableBits = 7

// top16 returns the top 16 bits of a double.
func top16(x float64) uint64 { return asUint64(x) >> 48 }

type logEntry struct {
	invc, logc float64
}

type logData struct {
	ln2hi float64
	ln2lo float64
	poly  [5]float64  // First coefficient is 1.
	poly1 [11]float64 // First coefficient is 1.
	tab   [128]logEntry
}

var logDataTable = logData{
	ln2hi: 0x1.62e42fefa3800p-1,
	ln2lo: 0x1.ef35793c76730p-45,
	poly1: [11]float64{
		-0x1p-1,
		0x1.5555555555577p-2,
		-0x1.ffffffffffdcbp-3,
		0x1.999999995dd0cp-3,
		-0x1.55555556745a7p-3,
		0x1.24924a344de3p-3,
		-0x1.fffffa4423d65p-4,
		0x1.c7184282ad6cap-4,
		-0x1.999eb43b068ffp-4,
		0x1.78182f7afd085p-4,
		-0x1.5521375d145cdp-4,
	},
	poly: [5]float64{
		-0x1.0000000000001p-1,
		0x1.555555551305bp-2,
		-0x1.fffffffeb459p-3,
		0x1.999b324f10111p-3,
		-0x1.55575e506c89fp-3,
	},
	tab: [128]logEntry{
		{0x1.734f0c3e0de9fp+0, -0x1.7cc7f79e69000p-2},
		{0x1.713786a2ce91fp+0, -0x1.76feec20d0000p-2},
		{0x1.6f26008fab5a0p+0, -0x1.713e31351e000p-2},
		{0x1.6d1a61f138c7dp+0, -0x1.6b85b38287800p-2},
		{0x1.6b1490bc5b4d1p+0, -0x1.65d5590807800p-2},
		{0x1.69147332f0cbap+0, -0x1.602d076180000p-2},
		{0x1.6719f18224223p+0, -0x1.5a8ca86909000p-2},
		{0x1.6524f99a51ed9p+0, -0x1.54f4356035000p-2},
		{0x1.63356aa8f24c4p+0, -0x1.4f637c36b4000p-2},
		{0x1.614b36b9ddc14p+0, -0x1.49da7fda85000p-2},
		{0x1.5f66452c65c4cp+0, -0x1.445923989a800p-2},
		{0x1.5d867b5912c4fp+0, -0x1.3edf439b0b800p-2},
		{0x1.5babccb5b90dep+0, -0x1.396ce448f7000p-2},
		{0x1.59d61f2d91a78p+0, -0x1.3401e17bda000p-2},
		{0x1.5805612465687p+0, -0x1.2e9e2ef468000p-2},
		{0x1.56397cee76bd3p+0, -0x1.2941b3830e000p-2},
		{0x1.54725e2a77f93p+0, -0x1.23ec58cda8800p-2},
		{0x1.52aff42064583p+0, -0x1.1e9e129279000p-2},
		{0x1.50f22dbb2bddfp+0, -0x1.1956d2b48f800p-2},
		{0x1.4f38f4734ded7p+0, -0x1.141679ab9f800p-2},
		{0x1.4d843cfde2840p+0, -0x1.0edd094ef9800p-2},
		{0x1.4bd3ec078a3c8p+0, -0x1.09aa518db1000p-2},
		{0x1.4a27fc3e0258ap+0, -0x1.047e65263b800p-2},
		{0x1.4880524d48434p+0, -0x1.feb224586f000p-3},
		{0x1.46dce1b192d0bp+0, -0x1.f474a7517b000p-3},
		{0x1.453d9d3391854p+0, -0x1.ea4443d103000p-3},
		{0x1.43a2744b4845ap+0, -0x1.e020d44e9b000p-3},
		{0x1.420b54115f8fbp+0, -0x1.d60a22977f000p-3},
		{0x1.40782da3ef4b1p+0, -0x1.cc00104959000p-3},
		{0x1.3ee8f5d57fe8fp+0, -0x1.c202956891000p-3},
		{0x1.3d5d9a00b4ce9p+0, -0x1.b81178d811000p-3},
		{0x1.3bd60c010c12bp+0, -0x1.ae2c9ccd3d000p-3},
		{0x1.3a5242b75dab8p+0, -0x1.a45402e129000p-3},
		{0x1.38d22cd9fd002p+0, -0x1.9a877681df000p-3},
		{0x1.3755bc5847a1cp+0, -0x1.90c6d69483000p-3},
		{0x1.35dce49ad36e2p+0, -0x1.87120a645c000p-3},
		{0x1.34679984dd440p+0, -0x1.7d68fb4143000p-3},
		{0x1.32f5cceffcb24p+0, -0x1.73cb83c627000p-3},
		{0x1.3187775a10d49p+0, -0x1.6a39a9b376000p-3},
		{0x1.301c8373e3990p+0, -0x1.60b3154b7a000p-3},
		{0x1.2eb4ebb95f841p+0, -0x1.5737d76243000p-3},
		{0x1.2d50a0219a9d1p+0, -0x1.4dc7b8fc23000p-3},
		{0x1.2bef9a8b7fd2ap+0, -0x1.4462c51d20000p-3},
		{0x1.2a91c7a0c1babp+0, -0x1.3b08abc830000p-3},
		{0x1.293726014b530p+0, -0x1.31b996b490000p-3},
		{0x1.27dfa5757a1f5p+0, -0x1.2875490a44000p-3},
		{0x1.268b39b1d3bbfp+0, -0x1.1f3b9f879a000p-3},
		{0x1.2539d838ff5bdp+0, -0x1.160c8252ca000p-3},
		{0x1.23eb7aac9083bp+0, -0x1.0ce7f57f72000p-3},
		{0x1.22a012ba940b6p+0, -0x1.03cdc49fea000p-3},
		{0x1.2157996cc4132p+0, -0x1.f57bdbc4b8000p-4},
		{0x1.201201dd2fc9bp+0, -0x1.e370896404000p-4},
		{0x1.1ecf4494d480bp+0, -0x1.d17983ef94000p-4},
		{0x1.1d8f5528f6569p+0, -0x1.bf9674ed8a000p-4},
		{0x1.1c52311577e7cp+0, -0x1.adc79202f6000p-4},
		{0x1.1b17c74cb26e9p+0, -0x1.9c0c3e7288000p-4},
		{0x1.19e010c2c1ab6p+0, -0x1.8a646b372c000p-4},
		{0x1.18ab07bb670bdp+0, -0x1.78d01b3ac0000p-4},
		{0x1.1778a25efbcb6p+0, -0x1.674f145380000p-4},
		{0x1.1648d354c31dap+0, -0x1.55e0e6d878000p-4},
		{0x1.151b990275fddp+0, -0x1.4485cdea1e000p-4},
		{0x1.13f0ea432d24cp+0, -0x1.333d94d6aa000p-4},
		{0x1.12c8b7210f9dap+0, -0x1.22079f8c56000p-4},
		{0x1.11a3028ecb531p+0, -0x1.10e4698622000p-4},
		{0x1.107fbda8434afp+0, -0x1.ffa6c6ad20000p-5},
		{0x1.0f5ee0f4e6bb3p+0, -0x1.dda8d4a774000p-5},
		{0x1.0e4065d2a9fcep+0, -0x1.bbcece4850000p-5},
		{0x1.0d244632ca521p+0, -0x1.9a1894012c000p-5},
		{0x1.0c0a77ce2981ap+0, -0x1.788583302c000p-5},
		{0x1.0af2f83c636d1p+0, -0x1.5715e67d68000p-5},
		{0x1.09ddb98a01339p+0, -0x1.35c8a49658000p-5},
		{0x1.08cabaf52e7dfp+0, -0x1.149e364154000p-5},
		{0x1.07b9f2f4e28fbp+0, -0x1.e72c082eb8000p-6},
		{0x1.06ab58c358f19p+0, -0x1.a55f152528000p-6},
		{0x1.059eea5ecf92cp+0, -0x1.63d62cf818000p-6},
		{0x1.04949cdd12c90p+0, -0x1.228fb8caa0000p-6},
		{0x1.038c6c6f0ada9p+0, -0x1.c317b20f90000p-7},
		{0x1.02865137932a9p+0, -0x1.419355daa0000p-7},
		{0x1.0182427ea7348p+0, -0x1.81203c2ec0000p-8},
		{0x1.008040614b195p+0, -0x1.0040979240000p-9},
		{0x1.fe01ff726fa1ap-1, 0x1.feff384900000p-9},
		{0x1.fa11cc261ea74p-1, 0x1.7dc41353d0000p-7},
		{0x1.f6310b081992ep-1, 0x1.3cea3c4c28000p-6},
		{0x1.f25f63ceeadcdp-1, 0x1.b9fc114890000p-6},
		{0x1.ee9c8039113e7p-1, 0x1.1b0d8ce110000p-5},
		{0x1.eae8078cbb1abp-1, 0x1.58a5bd001c000p-5},
		{0x1.e741aa29d0c9bp-1, 0x1.95c8340d88000p-5},
		{0x1.e3a91830a99b5p-1, 0x1.d276aef578000p-5},
		{0x1.e01e009609a56p-1, 0x1.07598e598c000p-4},
		{0x1.dca01e577bb98p-1, 0x1.253f5e30d2000p-4},
		{0x1.d92f20b7c9103p-1, 0x1.42edd8b380000p-4},
		{0x1.d5cac66fb5ccep-1, 0x1.606598757c000p-4},
		{0x1.d272caa5ede9dp-1, 0x1.7da76356a0000p-4},
		{0x1.cf26e3e6b2ccdp-1, 0x1.9ab434e1c6000p-4},
		{0x1.cbe6da2a77902p-1, 0x1.b78c7bb0d6000p-4},
		{0x1.c8b266d37086dp-1, 0x1.d431332e72000p-4},
		{0x1.c5894bd5d5804p-1, 0x1.f0a3171de6000p-4},
		{0x1.c26b533bb9f8cp-1, 0x1.067152b914000p-3},
		{0x1.bf583eeece73fp-1, 0x1.147858292b000p-3},
		{0x1.bc4fd75db96c1p-1, 0x1.2266ecdca3000p-3},
		{0x1.b951e0c864a28p-1, 0x1.303d7a6c55000p-3},
		{0x1.b65e2c5ef3e2cp-1, 0x1.3dfc33c331000p-3},
		{0x1.b374867c9888bp-1, 0x1.4ba366b7a8000p-3},
		{0x1.b094b211d304ap-1, 0x1.5933928d1f000p-3},
		{0x1.adbe885f2ef7ep-1, 0x1.66acd2418f000p-3},
		{0x1.aaf1d31603da2p-1, 0x1.740f8ec669000p-3},
		{0x1.a82e63fd358a7p-1, 0x1.815c0f51af000p-3},
		{0x1.a5740ef09738bp-1, 0x1.8e92954f68000p-3},
		{0x1.a2c2a90ab4b27p-1, 0x1.9bb3602f84000p-3},
		{0x1.a01a01393f2d1p-1, 0x1.a8bed1c2c0000p-3},
		{0x1.9d79f24db3c1bp-1, 0x1.b5b515c01d000p-3},
		{0x1.9ae2505c7b190p-1, 0x1.c2967ccbcc000p-3},
		{0x1.9852ef297ce2fp-1, 0x1.cf635d5486000p-3},
		{0x1.95cbaeea44b75p-1, 0x1.dc1bd3446c000p-3},
		{0x1.934c69de74838p-1, 0x1.e8c01b8cfe000p-3},
		{0x1.90d4f2f6752e6p-1, 0x1.f5509c0179000p-3},
		{0x1.8e6528effd79dp-1, 0x1.00e6c121fb800p-2},
		{0x1.8bfce9fcc007cp-1, 0x1.071b80e93d000p-2},
		{0x1.899c0dabec30ep-1, 0x1.0d46b9e867000p-2},
		{0x1.87427aa2317fbp-1, 0x1.13687334bd000p-2},
		{0x1.84f00acb39a08p-1, 0x1.1980d67234800p-2},
		{0x1.82a49e8653e55p-1, 0x1.1f8ffe0cc8000p-2},
		{0x1.8060195f40260p-1, 0x1.2595fd7636800p-2},
		{0x1.7e22563e0a329p-1, 0x1.2b9300914a800p-2},
		{0x1.7beb377dcb5adp-1, 0x1.3187210436000p-2},
		{0x1.79baa679725c2p-1, 0x1.377266dec1800p-2},
		{0x1.77907f2170657p-1, 0x1.3d54ffbaf3000p-2},
		{0x1.756cadbd6130cp-1, 0x1.432eee32fe000p-2},
	},
}

// Log returns the natural logarithm of x.
func Log(x float64) float64 {
	var w, z, r, r2, r3, y, invc, logc, kd, hi, lo float64
	var ix, iz, tmp uint64
	var top uint64
	var k, i int

	ix = asUint64(x)
	top = top16(x)
	loBound := asUint64(1.0 - 0x1p-4)
	hiBound := asUint64(1.0 + 0x1.09p-4)
	if ix-loBound < hiBound-loBound {
		// Handle close to 1.0 inputs separately.
		// Fix sign of zero with downward rounding when x==1.
		if ix == asUint64(1.0) {
			return 0
		}
		r = x - 1.0
		r2 = r * r
		r3 = r * r2
		y = r3 * (logDataTable.poly1[1] + r*logDataTable.poly1[2] +
			r2*logDataTable.poly1[3] +
			r3*(logDataTable.poly1[4]+r*logDataTable.poly1[5]+
				r2*logDataTable.poly1[6]+
				r3*(logDataTable.poly1[7]+r*logDataTable.poly1[8]+
					r2*logDataTable.poly1[9]+r3*logDataTable.poly1[10])))
		// Worst-case error is around 0.507 ULP.
		w = r * 0x1p27
		rhi := r + w - w
		rlo := r - rhi
		w = rhi * rhi * logDataTable.poly1[0] // poly1[0] == -0.5.
		hi = r + w
		lo = r - hi + w
		lo += logDataTable.poly1[0] * rlo * (rhi + r)
		y += lo
		y += hi
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
	i = int((tmp >> (52 - logTableBits)) % 128)
	k = int(int64(tmp) >> 52) // arithmetic shift
	iz = ix - (tmp & (0xfff << 52))
	invc = logDataTable.tab[i].invc
	logc = logDataTable.tab[i].logc
	z = asFloat64(iz)

	// log(x) = log1p(z/c-1) + log(c) + k*Ln2.
	// r ~= z/c - 1, |r| < 1/(2*N).
	// rounding error: 0x1p-55/N.
	r = math.FMA(z, invc, -1.0)
	kd = float64(k)

	// hi + lo = r + log(c) + k*Ln2.
	w = kd*logDataTable.ln2hi + logc
	hi = w + r
	lo = w - hi + r + kd*logDataTable.ln2lo

	// log(x) = lo + (log1p(r) - r) + hi.
	r2 = r * r // rounding error: 0x1p-54/N^2.
	y = lo + r2*logDataTable.poly[0] +
		r*r2*(logDataTable.poly[1]+r*logDataTable.poly[2]+
			r2*(logDataTable.poly[3]+r*logDataTable.poly[4])) + hi
	return evalAsDouble(y)
}
