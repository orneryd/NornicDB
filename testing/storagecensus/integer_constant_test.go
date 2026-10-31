package storagecensus

import (
	"go/constant"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestIntegerSelectorLiteralBoundaries(t *testing.T) {
	overflow := ssa.NewConst(constant.MakeFromLiteral("18446744073709551616", token.INT, 0), types.Typ[types.UntypedInt])
	for _, tc := range []struct {
		name          string
		value         ssa.Value
		integer, fits bool
		number        int64
	}{
		{"negative", ssa.NewConst(constant.MakeInt64(-1), types.Typ[types.Int64]), true, true, -1},
		{"overflow", overflow, true, false, 0},
		{"boolean", ssa.NewConst(constant.MakeBool(true), types.Typ[types.Bool]), false, false, 0},
		{"float", ssa.NewConst(constant.MakeFloat64(1), types.Typ[types.Float64]), false, false, 0},
		{"nil value", nil, false, false, 0},
		{"nil literal", (*ssa.Const)(nil), false, false, 0},
		{"nil constant", ssa.NewConst(nil, types.Typ[types.UntypedNil]), false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			number, integer, fits := integerConstant(tc.value)
			require.Equal(t, tc.integer, integer)
			require.Equal(t, tc.fits, fits)
			if fits {
				require.Equal(t, tc.number, number)
			}
		})
	}
	// An out-of-domain literal must never borrow a narrower caller assumption.
	fact, ok := knownArgument(overflow, flowAssumption{selector: selectorFact{value: overflow, number: 1, op: token.EQL}})
	require.False(t, ok)
	require.Equal(t, selectorFact{}, fact)
}
