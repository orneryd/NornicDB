// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Validate the oracle against the failure mechanisms it must reject.
package numericoracle

import (
	"math"
	"testing"
)

func TestNumericOracleRejectsLostKindValueAndShape(t *testing.T) {
	for _, pair := range [][2]interface{}{
		{float64(2), int64(2)}, {int64(2), float64(2)},
		{int64(9007199254740993), int64(9007199254740992)},
		{uint64(math.MaxUint64), int64(-1)},
		{math.Copysign(0, -1), float64(0)},
		{[]interface{}{float64(2), int64(2)}, "[2 2]"},
		{map[string]interface{}{"v": float64(2)}, map[string]interface{}{"v": int64(2)}},
	} {
		if Compare(pair[0], pair[1]) == nil {
			t.Errorf("oracle accepted %T(%v) as %T(%v)", pair[1], pair[1], pair[0], pair[0])
		}
	}
	for _, pair := range [][2]interface{}{
		{uint64(7), int64(7)}, {[]interface{}{int64(7)}, []int64{7}},
		{[]interface{}{float64(2)}, []float64{2}},
		{float64(float32(1.2)), float32(1.2)},
		{uint64(math.MaxUint64), uint64(math.MaxUint64)},
		{math.Copysign(0, -1), math.Copysign(0, -1)},
	} {
		if err := Compare(pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
}
