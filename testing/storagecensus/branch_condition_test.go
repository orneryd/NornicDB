package storagecensus

import (
	"github.com/stretchr/testify/require"
	"go/token"
	"golang.org/x/tools/go/ssa"
	"testing"
)

// The common parser normalizes NOT for both predicate kinds, while integer
// coverage must never treat a type-assertion boolean as a numeric selector.
func TestBranchConditionKeepsPredicateKinds(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 type writer interface{write()};type stagedKV struct{};func(*stagedKV)write(){}
 func integer(n int)bool{return !(n<3)}
 func typed(w writer)bool{_,ok:=w.(*stagedKV);return !ok}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	result := func(name string) ssa.Value {
		fn := pkg.Func(name)
		for _, b := range fn.Blocks {
			if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok {
				return ret.Results[0]
			}
		}
		t.Fatal("no result")
		return nil
	}
	integer := result("integer")
	known, ok := conditionKnown(integer, flowAssumption{selector: selectorFact{pkg.Func("integer").Params[0], 3, token.EQL}})
	require.True(t, ok)
	require.True(t, known)
	typed := result("typed")
	predicate, ok := readBranchCondition(typed)
	require.True(t, ok)
	require.NotNil(t, predicate.assertion)
	require.Equal(t, token.NEQ, predicate.op)
	_, ok = conditionKnown(typed, flowAssumption{selector: selectorFact{predicate.value, 1, token.EQL}})
	require.False(t, ok)
}
