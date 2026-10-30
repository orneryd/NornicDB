// Exp2 is a Go port of musl's double-precision 2**x function,
// Copyright (c) 2018, Arm Limited. SPDX-License-Identifier: MIT.
// See NOTICES.md for the licence text.

package libm

// exp2Specialcase handles cases that may overflow or underflow when computing
// the result that is scale*(1+tmp) without intermediate rounding.
func exp2Specialcase(tmp float64, sbits uint64, ki uint64) float64 {
	var scale, y float64

	if ki&0x80000000 == 0 {
		// k > 0, the exponent of scale might have overflowed by 1.
		sbits -= 1 << 52
		scale = asFloat64(sbits)
		y = 2 * (scale + scale*tmp)
		return evalAsDouble(y)
	}
	// k < 0, need special care in the subnormal range.
	sbits += 1022 << 52
	scale = asFloat64(sbits)
	y = scale + scale*tmp
	if y < 1.0 {
		// Round y to the right precision before scaling it into the subnormal
		// range to avoid double rounding that can cause 0.5+E/2 ulp error
		// where E is the worst-case ulp error outside the subnormal range.
		lo := scale - y + scale*tmp
		hi := 1.0 + y
		lo = 1.0 - hi + y + lo
		y = evalAsDouble(hi+lo) - 1.0
		// Avoid -0.0 with downward rounding.
		if y == 0.0 {
			y = 0.0
		}
	}
	y = 0x1p-1022 * y
	return evalAsDouble(y)
}

// Exp2 returns 2**x, the base-2 exponential of x.
func Exp2(x float64) float64 {
	abstop := top12(x) & 0x7ff
	if abstop-top12(0x1p-54) >= top12(512.0)-top12(0x1p-54) {
		if abstop-top12(0x1p-54) >= 0x80000000 {
			// Avoid spurious underflow for tiny x. Note: 0 is common input.
			return 1.0 + x
		}
		if abstop >= top12(1024.0) {
			if asUint64(x) == asUint64(-inf) {
				return 0.0
			}
			if abstop >= top12(inf) {
				return 1.0 + x
			}
			if asUint64(x)>>63 == 0 {
				return oflow(false)
			} else if asUint64(x) >= asUint64(-1075.0) {
				return uflow(false)
			}
		}
		if 2*asUint64(x) > 2*asUint64(928.0) {
			// Large x is special cased below.
			abstop = 0
		}
	}

	// exp2(x) = 2^(k/N) * 2^r, with 2^r in [2^(-1/2N),2^(1/2N)].
	// x = k/N + r, with int k and r in [-1/2N, 1/2N].
	kd := evalAsDouble(x + expDataTable.exp2Shift)
	ki := asUint64(kd)
	kd -= expDataTable.exp2Shift
	r := x - kd
	// 2^(k/N) ~= scale * (1 + tail).
	idx := 2 * (ki % 128)
	top := ki << (52 - expTableBits)
	tail := asFloat64(expDataTable.tab[idx])
	// This is only a valid scale when -1023*N < k < 1024*N.
	sbits := expDataTable.tab[idx+1] + top
	// exp2(x) = 2^(k/N) * 2^r ~= scale + scale * (tail + 2^r - 1).
	// Evaluation is optimized assuming superscalar pipelined execution.
	r2 := r * r
	tmp := tail + r*expDataTable.exp2Poly[0] +
		r2*(expDataTable.exp2Poly[1]+r*expDataTable.exp2Poly[2]) +
		r2*r2*(expDataTable.exp2Poly[3]+r*expDataTable.exp2Poly[4])
	if abstop == 0 {
		return exp2Specialcase(tmp, sbits, ki)
	}
	scale := asFloat64(sbits)
	return evalAsDouble(scale + scale*tmp)
}
