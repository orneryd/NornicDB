package workcensus

import (
	"go/constant"
	"go/token"
	"go/types"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// The module census encounters ordinary string/float/bool comparisons while
// examining error-preserving callees; those are not integer cardinality facts.
func TestErrorProofIntegerRejectsOtherConstantKinds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value constant.Value
		typ   types.Type
		want  bool
	}{
		{"module_string_open", constant.MakeString("open"), types.Typ[types.String], false},
		{"boolean", constant.MakeBool(false), types.Typ[types.Bool], false},
		{"float_zero", constant.MakeFromLiteral("0.0", token.FLOAT, 0), types.Typ[types.Float64], false},
		{"imaginary", constant.MakeFromLiteral("2i", token.IMAG, 0), types.Typ[types.Complex128], false},
		{"out_of_range", constant.MakeUint64(^uint64(0)), types.Typ[types.Uint64], false},
		{"zero", constant.MakeInt64(0), types.Typ[types.Int], true},
		{"other_integer", constant.MakeInt64(1), types.Typ[types.Int], false},
		{"nil_pointer", nil, types.NewPointer(types.Typ[types.Int]), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := integer(ssa.NewConst(tc.value, tc.typ), 0); got != tc.want {
				t.Fatalf("integer(%v,0) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
