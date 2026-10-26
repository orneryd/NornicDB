package storagecensus

import (
	"github.com/stretchr/testify/require"
	"go/token"
	"golang.org/x/tools/go/ssa"
	"testing"
)

func TestBranchConditionDoubleNotAndWholeStoreGuard(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type writer interface{write()};type stagedKV struct{};func(*stagedKV)write(){}
 func integer(n int)bool{return !!(n<3)}
 func typed(w writer)bool{_,ok:=w.(*stagedKV);return !!ok}
 func rawWrite(){}
 func whole(w writer){if _,staged:=w.(*stagedKV);!staged{rawWrite()}}
 `)
	for _, name := range []string{"integer", "typed"} {
		fn := pkg.Func(name)
		found := false
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				ret, ok := ins.(*ssa.Return)
				if !ok {
					continue
				}
				found = true
				pred, ok := readBranchCondition(ret.Results[0])
				require.True(t, ok)
				if name == "integer" {
					known, ok := conditionKnown(ret.Results[0], flowAssumption{selector: selectorFact{fn.Params[0], 3, token.EQL}})
					require.True(t, ok)
					require.False(t, known)
				} else {
					require.NotNil(t, pred.assertion)
					require.Equal(t, token.EQL, pred.op)
				}
			}
		}
		require.True(t, found, "predicate result must be inspected")
	}
	fn := pkg.Func("whole")
	a := &analysis{p: p, writes: map[string]write{}}
	found := false
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			call, ok := ins.(*ssa.Call)
			if !ok || call.Common().StaticCallee() != pkg.Func("rawWrite") {
				continue
			}
			found = true
			out := &summary{reqs: map[string]req{}, pends: map[string]pend{}}
			a.resolve(keyShape{whole: "badger.DB.DropAll", param: -1, free: -1}, "whole-write", []string{"whole whole-write"}, 0, true, fn, ins, out)
			require.Len(t, a.writes, 1)
			require.Len(t, out.reqs, 1)
			for _, write := range a.writes {
				require.Equal(t, "whole-store", write.Class)
			}
			for _, r := range out.reqs {
				require.Equal(t, "badger.DB.DropAll", r.space)
				require.True(t, r.later)
				require.Len(t, r.guards, 1)
				require.False(t, r.guards[0].succeeds)
			}
		}
	}
	require.True(t, found, "whole-store call must be inspected")
}
