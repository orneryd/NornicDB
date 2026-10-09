package libm

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// The trigonometric functions and Log10 return musl 1.2.5's results bit for
// bit (#907). The port matched musl's libm on 20,030 seeded inputs per
// function (small, medium and huge arguments, subnormals, special values), on
// amd64 and arm64. The inputs pinned here, with musl's result bits, are a
// sample that reaches every branch: the argument reduction's rounding
// corrections and Payne-Hanek recomputation, and atan2's zero and infinite
// arguments.
func TestTrigMatchesMusl(t *testing.T) {
	for _, tc := range []struct {
		name          string
		in, want, in2 uint64
	}{
		{"sin", 0x0000000000000000, 0x0000000000000000, 0},
		{"sin", 0x000012688b70e62b, 0x000012688b70e62b, 0},
		{"sin", 0x0000000000000001, 0x0000000000000001, 0},
		{"sin", 0x412b28fda30cdeb0, 0x3feb8697c6a294a9, 0},
		{"sin", 0xbee251fd7fb2690c, 0xbee251fd7fb168d8, 0},
		{"sin", 0x4005008cc14c067c, 0x3fdf9890b58c02ca, 0},
		{"sin", 0x4021358fe2e76532, 0x3fe766775f4ff8c9, 0},
		{"sin", 0x41267d94058749a6, 0x3fdec96a9fcde42c, 0},
		{"sin", 0x3ea8c6089002db10, 0x3ea8c6089002d896, 0},
		{"sin", 0x4480f0cf064dd592, 0xbfeb453ab76bf397, 0},
		{"sin", 0x7e37e43c8800759c, 0xbfea2c16b010e385, 0},
		{"cos", 0x0000000000000000, 0x3ff0000000000000, 0},
		{"cos", 0x000012688b70e62b, 0x3ff0000000000000, 0},
		{"cos", 0x0000000000000001, 0x3ff0000000000000, 0},
		{"cos", 0x3fd34897eeedb0c0, 0x3fee8ef2e0e07251, 0},
		{"cos", 0x40233e4410e8264e, 0xbfef61d0ea1846e7, 0},
		{"cos", 0x57d3b064d88fbd62, 0x3fe4f26a21aa2126, 0},
		{"cos", 0x6d1b4e5fa2b9f249, 0xbfee5041b39485fc, 0},
		{"cos", 0x3e9595236f89de60, 0x3feffffffffffe2e, 0},
		{"cos", 0xc1078d24e520a428, 0x3fdba997a4a43f53, 0},
		{"cos", 0x4480f0cf064dd592, 0x3fe0be2cef01c8f4, 0},
		{"cos", 0x7e37e43c8800759c, 0xbfe2699022adc4c1, 0},
		{"tan", 0x0000000000000000, 0x0000000000000000, 0},
		{"tan", 0x000012688b70e62b, 0x000012688b70e62b, 0},
		{"tan", 0x0000000000000001, 0x0000000000000001, 0},
		{"tan", 0x78ebfae0fa718a1a, 0x3ff4646676e66e55, 0},
		{"tan", 0x41122ba72a2a8de0, 0xbff689cb0a0ba844, 0},
		{"tan", 0x3fe5f86d85ba9220, 0x3fea3a2016f66552, 0},
		{"tan", 0x4111b17ad60eabc0, 0xbfe66f9273d53f0f, 0},
		{"tan", 0xbedd85bffd0d9b52, 0xbedd85bffd0fb362, 0},
		{"tan", 0x4d9ec50cb9a58a1f, 0xbfdcdced008b71a6, 0},
		{"tan", 0x4480f0cf064dd592, 0xbffa0f79c1b6b258, 0},
		{"tan", 0x7e37e43c8800759c, 0x3ff6be411f37ac77, 0},
		{"asin", 0x0000000000000000, 0x0000000000000000, 0},
		{"asin", 0x000012688b70e62b, 0x000012688b70e62b, 0},
		{"asin", 0x0000000000000001, 0x0000000000000001, 0},
		{"asin", 0x3fd2b055cbfc28dc, 0x3fd2f714501643df, 0},
		{"asin", 0x3fe730ca6647bb1e, 0x3fe9f07f10cf102d, 0},
		{"asin", 0xbfe6fe44b5a12e90, 0xbfe9a7843f7b365a, 0},
		{"asin", 0x3fe398b063265972, 0x3fe5172c46b3cd25, 0},
		{"asin", 0xbfb1a9fcddad6d50, 0xbfb1ad956b68ea9c, 0},
		{"asin", 0xbfc9dac7d65cf6b8, 0xbfca08a286dceb87, 0},
		{"acos", 0x0000000000000000, 0x3ff921fb54442d18, 0},
		{"acos", 0x000012688b70e62b, 0x3ff921fb54442d18, 0},
		{"acos", 0x0000000000000001, 0x3ff921fb54442d18, 0},
		{"acos", 0x3fdee38c16078b5c, 0x3ff112f89fb297c5, 0},
		{"acos", 0x3fe67681f960ad44, 0x3fe95d5244c82256, 0},
		{"acos", 0x3fc639231c4dfd18, 0x3ff65737b1385828, 0},
		{"acos", 0xbfe578a7f601de28, 0x4002735a031fa29e, 0},
		{"acos", 0xbfdf16e09464a59c, 0x40009fd4546f1a5c, 0},
		{"acos", 0x3fe672f549632f30, 0x3fe9624dc1999928, 0},
		{"atan", 0x0000000000000000, 0x0000000000000000, 0},
		{"atan", 0x000012688b70e62b, 0x000012688b70e62b, 0},
		{"atan", 0x0000000000000001, 0x0000000000000001, 0},
		{"atan", 0x40203efda791ae10, 0x3ff72c42c18b060c, 0},
		{"atan", 0xc009fb3225862ac8, 0xbff45a7fa447e42c, 0},
		{"atan", 0x411217b37aad8e68, 0x3ff921f7cab3ab30, 0},
		{"atan", 0x401b9a4864820b78, 0x3ff6d484f065923b, 0},
		{"atan", 0xc00b22a83a8be2d0, 0xbff48bacd101ee04, 0},
		{"atan", 0x412dbd0abebc8caa, 0x3ff921fa40cc4843, 0},
		{"atan", 0x4480f0cf064dd592, 0x3ff921fb54442d18, 0},
		{"atan", 0x7e37e43c8800759c, 0x3ff921fb54442d18, 0},
		{"log10", 0x000012688b70e62b, 0xc073600000000000, 0},
		{"log10", 0x01a56e1fc2f8f359, 0xc072c00000000000, 0},
		{"log10", 0x3e50000000000000, 0xc01f4e9f6303263e, 0},
		{"log10", 0x544c9685d89302c2, 0x4058858e543be2a8, 0},
		{"log10", 0x09131eedad5beeab, 0xc07083a1961bb5aa, 0},
		{"log10", 0x51088d06571186e0, 0x4054977f2769c028, 0},
		{"log10", 0x2b5574f0183b8013, 0xc058cd98d7f34526, 0},
		{"log10", 0x53961c88b929a019, 0x4057aa7d99f3cd68, 0},
		{"log10", 0x37e0c33705c5a46d, 0xc0436804e2eb7a40, 0},
		{"log10", 0x4480f0cf064dd592, 0x4036000000000000, 0},
		{"log10", 0x7e37e43c8800759c, 0x4072c00000000000, 0},
		{"atan2", 0x402380c93bb1ef22, 0x4001757fe7711046, 0xc01b5a90336dcfb8},
		{"atan2", 0xc134831c5ad94fe6, 0xbff921fbb9f291ff, 0xbfe04b66c58a71d0},
		{"atan2", 0x410da8fbe7dea31e, 0x3ff921fb54446f1e, 0xbeae98d936fb83c0},
		{"atan2", 0x3ec0b28c4751e89c, 0x3ed09dc6aea2546d, 0x3fe014005cc237a0},
		{"atan2", 0xc015478713d2ce71, 0xbfe1e8f9fe5e7870, 0x4020fb7821173c68},
		{"atan2", 0xc021330859ccc288, 0xbf0df46ff3e2ec3a, 0x41025fa65560d8e0},
		{"sin", 0xbff0000000000000, 0xbfeaed548f090cee, 0},
		{"sin", 0x3ff921fb54442d18, 0x3ff0000000000000, 0},
		{"sin", 0x4012d97c7f3321d2, 0xbff0000000000000, 0},
		{"sin", 0x401921fb54442d18, 0xbcb1a62633145c07, 0},
		{"sin", 0xfe37e43c8800759c, 0x3fea2c16b010e385, 0},
		{"sin", 0x7fe0000000000000, 0x3fe205248cbdb760, 0},
		{"sin", 0xc019350a06d83546, 0xbf930e6a7b605ccd, 0},
		{"sin", 0xc014fb5d2f5d1569, 0x3feb8f4fb824f44b, 0},
		{"sin", 0xf8f9b5bfecbc5147, 0xbfe783b69f208b03, 0},
		{"sin", 0xc13e1f8358c72329, 0xbfedda58c9105338, 0},
		{"cos", 0x401921fb54442d18, 0x3ff0000000000000, 0},
		{"asin", 0x01a56e1fc2f8f359, 0x01a56e1fc2f8f359, 0},
		{"asin", 0x3fef333333333333, 0x3ff58c2b5ce0c3e5, 0},
		{"atan", 0x3ff0000000000000, 0x3fe921fb54442d18, 0},
		{"atan", 0xfe37e43c8800759c, 0xbff921fb54442d18, 0},
		{"atan2", 0xfe37e43c8800759c, 0xbff921fb54442d18, 0xc01e4f8a4dbf2544},
		{"atan2", 0x0000000000000000, 0x0000000000000000, 0x3ff8000000000000},
		{"atan2", 0x8000000000000000, 0x8000000000000000, 0x4000000000000000},
		{"sin", 0x40ebf085dcba563d, 0xbfe6a09e667f9bb5, 0},
		{"cos", 0x40ebf085dcba563d, 0x3fe6a09e667edbe4, 0},
		{"tan", 0x40ebf085dcba563d, 0xbff00000000087a3, 0},
		{"sin", 0x411bea4d132259dd, 0xbfe6a09e667ffd89, 0},
		{"cos", 0x411bea4d132259dd, 0xbfe6a09e667e7a10, 0},
		{"tan", 0x411bea4d132259dd, 0x3ff00000000111fc, 0},
		{"sin", 0x7a616710f95b9696, 0xbfeffffffffffff5, 0},
		{"cos", 0x7a616710f95b9696, 0x3e6a07f69b4de0bc, 0},
		{"tan", 0x7a616710f95b9696, 0xc173ab34e3793e20, 0},
		{"atan2", 0x3ff8000000000000, 0x3ff921fb54442d18, 0x0000000000000000},
		{"atan2", 0xbff8000000000000, 0xbff921fb54442d18, 0x0000000000000000},
		{"atan2", 0x3ff8000000000000, 0x3ff921fb54442d18, 0x8000000000000000},
		{"atan2", 0x7ff0000000000000, 0x3fe921fb54442d18, 0x7ff0000000000000},
		{"atan2", 0xfff0000000000000, 0xbfe921fb54442d18, 0x7ff0000000000000},
		{"atan2", 0x7ff0000000000000, 0x4002d97c7f3321d2, 0xfff0000000000000},
		{"atan2", 0xfff0000000000000, 0xc002d97c7f3321d2, 0xfff0000000000000},
		{"atan2", 0x3ff8000000000000, 0x0000000000000000, 0x7ff0000000000000},
		{"atan2", 0xbff8000000000000, 0x8000000000000000, 0x7ff0000000000000},
		{"atan2", 0x3ff8000000000000, 0x400921fb54442d18, 0xfff0000000000000},
		{"atan2", 0xbff8000000000000, 0xc00921fb54442d18, 0xfff0000000000000},
	} {
		x := math.Float64frombits(tc.in)
		var got float64
		switch tc.name {
		case "sin":
			got = Sin(x)
		case "cos":
			got = Cos(x)
		case "tan":
			got = Tan(x)
		case "asin":
			got = Asin(x)
		case "acos":
			got = Acos(x)
		case "atan":
			got = Atan(x)
		case "log10":
			got = Log10(x)
		case "atan2":
			got = Atan2(x, math.Float64frombits(tc.in2))
		}
		require.Equal(t, tc.want, math.Float64bits(got), "%s(%v)", tc.name, x)
	}
}

// Inputs where Go's math package differs from Neo4j 5.26.30 and the musl
// port returns Neo4j's value (#907).
func TestTrigMatchesNeo4j(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   float64
		want uint64
	}{
		{"sin", 6.459967238961659e+144, 0x3fc9180fdf511da5},
		{"sin", 3.6676042781272695e+98, 0xbfcbcea67b1c6791},
		{"cos", 1.0250403389434113, 0x3fe09c2cf62fd5a9},
		{"cos", 6.459967238961659e+144, 0xbfef6107cb9469a1},
		{"tan", 1.0250403389434113, 0x3ffa58d178163426},
		{"tan", 6.459967238961659e+144, 0xbfc99730de40f1fc},
		{"asin", -0.9009226407196964, 0xbff1f343d0a26bb8},
		{"asin", -0.697036542895461, 0xbfe8ae2186404725},
		{"acos", 0.5902813850065358, 0x3fee0f796280bbb5},
		{"acos", 0.7256143166564024, 0x3fe848a8dc952ea9},
		{"atan", 3.2031184288793755, 0x3ff44a802f41d490},
		{"atan", 9.37817260503695, 0x3ff76eddf4e4d9d5},
	} {
		fn := map[string]func(float64) float64{"sin": Sin, "cos": Cos, "tan": Tan, "asin": Asin, "acos": Acos, "atan": Atan}[tc.name]
		require.Equal(t, tc.want, math.Float64bits(fn(tc.in)), "%s(%v)", tc.name, tc.in)
	}
}

// Special values follow musl / IEEE 754.
func TestTrigSpecialValues(t *testing.T) {
	for _, fn := range []func(float64) float64{Sin, Cos, Tan} {
		require.True(t, math.IsNaN(fn(math.Inf(1))))
		require.True(t, math.IsNaN(fn(math.NaN())))
	}
	require.True(t, math.IsNaN(Asin(2)))
	require.True(t, math.IsNaN(Acos(-2)))
	require.Equal(t, math.Pi/2, Asin(1))
	require.Equal(t, math.Pi, Acos(-1))
	require.Equal(t, 0.0, Acos(1))
	require.Equal(t, math.Pi/2, Atan(math.Inf(1)))
	require.True(t, math.IsNaN(Atan(math.NaN())))
	require.Equal(t, math.Inf(-1), Log10(0))
	require.True(t, math.IsNaN(Log10(-1)))
	require.Equal(t, 0.0, Log10(1))
	require.Equal(t, 3.0, Log10(1000))
	require.Equal(t, math.Inf(1), Log10(math.Inf(1)))
	for _, c := range []struct{ y, x, want float64 }{
		{0, 1, 0}, {0, -1, math.Pi}, {math.Copysign(0, -1), -1, -math.Pi},
		{1, 0, math.Pi / 2}, {-1, 0, -math.Pi / 2},
		{math.Inf(1), math.Inf(1), math.Pi / 4}, {math.Inf(-1), math.Inf(1), -math.Pi / 4},
		{math.Inf(1), math.Inf(-1), 3 * math.Pi / 4}, {math.Inf(-1), math.Inf(-1), -3 * math.Pi / 4},
		{1, math.Inf(1), 0}, {1, math.Inf(-1), math.Pi}, {-1, math.Inf(-1), -math.Pi},
		{1e300, 1e-300, math.Pi / 2}, {-1e-300, -1e300, -math.Pi},
	} {
		require.Equal(t, c.want, Atan2(c.y, c.x), "atan2(%v, %v)", c.y, c.x)
	}
	require.True(t, math.Signbit(Atan2(-1, math.Inf(1))))
	require.True(t, math.IsNaN(Atan2(math.NaN(), 1)))
}
