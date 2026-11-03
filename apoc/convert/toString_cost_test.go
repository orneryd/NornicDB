package convert

import (
	"fmt"
	"testing"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// toStringCostKinds are the numeric kinds apoc.convert.toString is measured on:
// an INTEGER, a whole FLOAT (its corrected text carries a fractional token) and
// a FLOAT with a fraction.
var toStringCostKinds = []struct {
	name  string
	value func(int) interface{}
}{
	{"integer", func(i int) interface{} { return int64(i + 1) }},
	{"wholeFLOAT", func(i int) interface{} { return float64(i + 1) }},
	{"fraction", func(i int) interface{} { return float64(i+1) + 0.5 }},
}

// toStringCostWidths are the result widths the numeric text contract is
// measured at.
var toStringCostWidths = []int{1, 8, 32, 64, 128}

// toStringCostLargeWhole are representative whole FLOATs beyond the immutable
// 0..99 token table, including a signed value and a scientific-range value.
// Their text is not a constant, so the string the conversion returns must be
// allocated; they are measured so that required output bytes are reported
// instead of being rounded into the token table.
var toStringCostLargeWhole = []struct {
	name  string
	value float64
}{
	{"100", 100},
	{"1000", 1000},
	{"negative10", -10},
	{"1e6", 1e6},
}

func toStringCostArguments(kind string, count int) []interface{} {
	for _, candidate := range toStringCostKinds {
		if candidate.name == kind {
			values := make([]interface{}, count)
			for i := range values {
				values[i] = candidate.value(i)
			}
			return values
		}
	}
	panic("unknown toString cost kind: " + kind)
}

var toStringCostSink string

// toStringCostLegacy is the committed pre-numeric-identity implementation of
// apoc.convert.toString: fmt's default verb. It is the allocation and byte
// control for the corrected text, not a typing oracle - it prints a whole
// FLOAT as "2" where the numeric contract now requires "2.0".
func toStringCostLegacy(values []interface{}) {
	for _, value := range values {
		toStringCostSink = fmt.Sprintf("%v", value)
	}
}

func toStringCostCurrent(values []interface{}) {
	for _, value := range values {
		toStringCostSink = ToString(value)
	}
}

func BenchmarkToStringCost(b *testing.B) {
	for _, kind := range toStringCostKinds {
		for _, count := range toStringCostWidths {
			values := toStringCostArguments(kind.name, count)
			legacy := fmt.Sprintf("legacy/%s/%d", kind.name, count)
			b.Run(legacy, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					toStringCostLegacy(values)
				}
			})
			current := fmt.Sprintf("current/%s/%d", kind.name, count)
			b.Run(current, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					toStringCostCurrent(values)
				}
			})
		}
	}
}

// BenchmarkToStringCostLargeWhole reports the bytes and allocations a single
// whole FLOAT outside the token table costs against fmt's text for the same
// value, so the delta can be attributed to the required fractional token.
func BenchmarkToStringCostLargeWhole(b *testing.B) {
	for _, whole := range toStringCostLargeWhole {
		values := []interface{}{whole.value}
		b.Run("legacy/"+whole.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				toStringCostLegacy(values)
			}
		})
		b.Run("current/"+whole.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				toStringCostCurrent(values)
			}
		})
	}
}

// TestToStringAllocationParity keeps apoc.convert.toString from costing more
// allocations than the text it replaced at every numeric kind and width. The
// corrected whole-FLOAT token is three characters where fmt printed one, so the
// FLOAT text owner must not turn that length difference into an allocation.
func TestToStringAllocationParity(t *testing.T) {
	for _, kind := range toStringCostKinds {
		for _, count := range toStringCostWidths {
			values := toStringCostArguments(kind.name, count)
			legacy := testing.AllocsPerRun(50, func() { toStringCostLegacy(values) })
			current := testing.AllocsPerRun(50, func() { toStringCostCurrent(values) })
			if current > legacy {
				t.Errorf("%s/%d: %.0f allocations, legacy text control %.0f", kind.name, count, current, legacy)
			}
		}
	}
}

// TestToStringWholeFloatTokenHasNoRuntimeCost holds the public conversion to the
// immutable token table: a whole FLOAT from 0 through 99 must reach the static
// spelling, so its corrected ".0" text costs no allocation at all rather than
// only costing no more than fmt's shorter text. Without this, the parity bound
// above would pass while every whole FLOAT still copied its text. The value is
// boxed once outside the measured loop, so the count is the text path and not
// the caller's interface conversion.
func TestToStringWholeFloatTokenHasNoRuntimeCost(t *testing.T) {
	for _, value := range []float64{0, 1, 9, 10, 42, 99} {
		want, numeric := valueconvert.AppendNumericText(nil, value)
		if !numeric {
			t.Fatalf("%v was not read as a number", value)
		}
		boxed := interface{}(value)
		var text string
		allocs := testing.AllocsPerRun(50, func() {
			text = ToString(boxed)
		})
		if text != string(want) {
			t.Fatalf("%v = %q, want %q", value, text, want)
		}
		if allocs != 0 {
			t.Errorf("%v: %.0f allocations for a static %q token", value, allocs, text)
		}
	}
}

// TestToStringLargeWholeTextCost pins the honest cost of a whole FLOAT outside
// the immutable table: the writer's exact bytes, and at most one allocation -
// the returned string itself. A second allocation would be runtime overhead the
// required fractional token does not explain.
func TestToStringLargeWholeTextCost(t *testing.T) {
	for _, whole := range toStringCostLargeWhole {
		want, numeric := valueconvert.AppendNumericText(nil, whole.value)
		if !numeric {
			t.Fatalf("%v was not read as a number", whole.value)
		}
		if text := ToString(whole.value); text != string(want) {
			t.Fatalf("%s = %q, want the writer's %q", whole.name, text, want)
		}
		if string(want)[len(want)-2:] != ".0" {
			t.Fatalf("%s text %q lost the required fractional token", whole.name, want)
		}
		values := []interface{}{whole.value}
		allocs := testing.AllocsPerRun(50, func() { toStringCostCurrent(values) })
		if allocs > 1 {
			t.Errorf("%s: %.0f allocations for %q, want at most the returned string", whole.name, allocs, want)
		}
	}
}
