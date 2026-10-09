package libm

import "math"

// This file makes the package a drop-in replacement for the standard library
// math package: every name imported from "math" can instead be imported from
// "github.com/orneryd/nornicdb/pkg/math/libm" (aliased as math) with no call
// site changes.
//
// The transcendental functions whose values can differ from Java's Math
// functions (Exp, Exp2, Log, Log2, Pow, the hyperbolic family, and the
// trigonometric functions with Log10) are ported from musl in this package.
// Everything else (exact functions, Log1p, Expm1, and the integer/float
// limits) is re-exported from the standard library.

const (
	E   = math.E
	Pi  = math.Pi
	Phi = math.Phi

	Sqrt2   = math.Sqrt2
	SqrtE   = math.SqrtE
	SqrtPi  = math.SqrtPi
	SqrtPhi = math.SqrtPhi

	Ln2    = math.Ln2
	Log2E  = math.Log2E
	Ln10   = math.Ln10
	Log10E = math.Log10E

	MaxFloat64             = math.MaxFloat64
	SmallestNonzeroFloat64 = math.SmallestNonzeroFloat64

	MaxInt8   = math.MaxInt8
	MinInt8   = math.MinInt8
	MaxInt16  = math.MaxInt16
	MinInt16  = math.MinInt16
	MaxInt32  = math.MaxInt32
	MinInt32  = math.MinInt32
	MaxInt64  = math.MaxInt64
	MinInt64  = math.MinInt64
	MaxUint8  = math.MaxUint8
	MaxUint16 = math.MaxUint16
	MaxUint32 = math.MaxUint32
	MaxUint64 = math.MaxUint64
	MaxUint   = math.MaxUint
)

// Exact and trivial functions: re-exported from the standard library. These
// are correctly rounded (or exact) by construction, so they never differ from
// Neo4j's results.
func Abs(x float64) float64                   { return math.Abs(x) }
func Sqrt(x float64) float64                  { return math.Sqrt(x) }
func IsNaN(x float64) bool                    { return math.IsNaN(x) }
func IsInf(x float64, sign int) bool          { return math.IsInf(x, sign) }
func Inf(sign int) float64                    { return math.Inf(sign) }
func NaN() float64                            { return math.NaN() }
func Floor(x float64) float64                 { return math.Floor(x) }
func Trunc(x float64) float64                 { return math.Trunc(x) }
func Ceil(x float64) float64                  { return math.Ceil(x) }
func Round(x float64) float64                 { return math.Round(x) }
func RoundToEven(x float64) float64           { return math.RoundToEven(x) }
func Min(x, y float64) float64                { return math.Min(x, y) }
func Max(x, y float64) float64                { return math.Max(x, y) }
func Copysign(x, y float64) float64           { return math.Copysign(x, y) }
func Signbit(x float64) bool                  { return math.Signbit(x) }
func Modf(f float64) (int, frac float64)      { return math.Modf(f) }
func Mod(x, y float64) float64                { return math.Mod(x, y) }
func Ldexp(frac float64, exp int) float64     { return math.Ldexp(frac, exp) }
func Pow10(n int) float64                     { return math.Pow10(n) }
func Frexp(f float64) (frac float64, exp int) { return math.Frexp(f) }

func Float64bits(f float64) uint64     { return math.Float64bits(f) }
func Float64frombits(b uint64) float64 { return math.Float64frombits(b) }
func Float32bits(f float32) uint32     { return math.Float32bits(f) }
func Float32frombits(b uint32) float32 { return math.Float32frombits(b) }

// Log1p and Expm1 are re-exported from the standard library. Sin, Cos, Tan,
// Asin, Acos, Atan, Atan2 and Log10 are ported from musl (trig.go,
// invtrig.go): Go's sin, cos, tan and atan come from Cephes, not fdlibm, and
// differed from Neo4j's results for a quarter to a third of inputs (#907).
func Log1p(x float64) float64 { return math.Log1p(x) }
func Expm1(x float64) float64 { return math.Expm1(x) }
