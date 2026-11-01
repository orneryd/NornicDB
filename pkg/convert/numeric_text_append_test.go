// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Numeric token and prefix contract across string and append protocols; expected text is independent of production formatting.
package convert

import (
	"math"
	"testing"
)

func TestNumericTextAppendPreservesPrefixAndKind(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   interface{}
		want    string
		numeric bool
	}{
		{"small_integer", int64(42), "42", true},
		{"exact_integer", int64(9007199254740993), "9007199254740993", true},
		{"unsigned_integer", uint64(math.MaxUint64), "18446744073709551615", true},
		{"whole_float", float64(2), "2.0", true},
		{"float32", float32(1.2), "1.2", true},
		{"negative_zero", math.Copysign(0, -1), "-0.0", true},
		{"subnormal", math.SmallestNonzeroFloat64, "4.9E-324", true},
		{"infinity", math.Inf(1), "Infinity", true},
		{"negative_infinity", math.Inf(-1), "-Infinity", true},
		{"nan", math.NaN(), "NaN", true},
		{"string", "2", "", false},
		{"list", []float64{2}, "", false},
		{"nil", nil, "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, numeric := AppendNumericText([]byte("prefix="), test.value)
			if numeric != test.numeric || string(got) != "prefix="+test.want {
				t.Fatalf("append = %q, %t; want %q, %t", got, numeric, "prefix="+test.want, test.numeric)
			}
			text, numeric := NumericText(test.value)
			if numeric != test.numeric || text != test.want {
				t.Fatalf("string = %q, %t; want %q, %t", text, numeric, test.want, test.numeric)
			}
		})
	}
}
