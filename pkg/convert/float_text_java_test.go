package convert

import (
	"math"
	"testing"
)

// FormatFloat is Cypher's FLOAT text, Java's Double.toString: a one-digit
// shortest mantissa is chosen among two-digit candidates as Java does, so the
// smallest subnormals spell 4.9E-324 and 9.9E-324, as Neo4j's toString does.
func TestFormatFloatMatchesJavaDoubleToString(t *testing.T) {
	for value, want := range map[float64]string{
		math.SmallestNonzeroFloat64: "4.9E-324", -math.SmallestNonzeroFloat64: "-4.9E-324",
		2 * math.SmallestNonzeroFloat64: "9.9E-324", 1e7: "1.0E7", 1e-4: "1.0E-4", 2e-5: "2.0E-5",
		1.5e7: "1.5E7", 1.2345e-10: "1.2345E-10", -2.5e300: "-2.5E300", 0.001: "0.001", 9999999.0: "9999999.0",
	} {
		if got := FormatFloat(value, 64); got != want {
			t.Errorf("FormatFloat(%v) = %q, want %q", value, got, want)
		}
	}
}
