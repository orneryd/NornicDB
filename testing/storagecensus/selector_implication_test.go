package storagecensus

import (
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// callTo is the one call of a function named name in fn.
func callTo(t *testing.T, fn *ssa.Function, name string) *ssa.Call {
	t.Helper()
	var found *ssa.Call
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(*ssa.Call)
			if !ok {
				continue
			}
			if callee := call.Common().StaticCallee(); callee != nil && callee.Name() == name {
				require.Nil(t, found, "the fixture function has one %s call", name)
				found = call
			}
		}
	}
	require.NotNil(t, found)
	return found
}

// The shared implication keeps the decisions the selector branch always made:
// a known equality decides, a known inequality decides its own number only,
// and every other value or comparison stays unknown.
func TestSelectorImpliesDecidesOnlyKnownComparisons(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 func probe(first, second int){}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	first, second := pkg.Func("probe").Params[0], pkg.Func("probe").Params[1]
	for _, tc := range []struct {
		name           string
		knownValue     ssa.Value
		knownNumber    int64
		knownOp        token.Token
		requiredValue  ssa.Value
		requiredNumber int64
		requiredOp     token.Token
		holds, decided bool
	}{
		{"known equality accepts the equal comparison", first, 1, token.EQL, first, 1, token.EQL, true, true},
		{"known equality rejects another number", first, 1, token.EQL, first, 2, token.EQL, false, true},
		{"known equality rejects the inequality", first, 1, token.EQL, first, 1, token.NEQ, false, true},
		{"known equality decides an ordered comparison", first, 3, token.EQL, first, 3, token.GEQ, true, true},
		{"known inequality rejects its own equality", first, 1, token.NEQ, first, 1, token.EQL, false, true},
		{"known inequality accepts its own inequality", first, 1, token.NEQ, first, 1, token.NEQ, true, true},
		{"known inequality leaves another number unknown", first, 1, token.NEQ, first, 2, token.EQL, false, false},
		{"known inequality leaves an ordering unknown", first, 1, token.NEQ, first, 1, token.GTR, false, false},
		{"another value stays unknown", first, 1, token.EQL, second, 1, token.EQL, false, false},
		{"no known value stays unknown", nil, 1, token.EQL, first, 1, token.EQL, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holds, decided := selectorImplies(
				selectorFact{tc.knownValue, tc.knownNumber, tc.knownOp},
				selectorFact{tc.requiredValue, tc.requiredNumber, tc.requiredOp})
			require.Equal(t, tc.holds, holds)
			require.Equal(t, tc.decided, decided)
		})
	}
}

// conditionKnown answers the same implication through the shared owner.
func TestConditionKnownKeepsSelectorDecisions(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 func compare(kind, other int)bool{return kind==1}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	fn := pkg.Func("compare")
	var value ssa.Value
	for _, block := range fn.Blocks {
		if ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); ok {
			value = ret.Results[0]
		}
	}
	require.NotNil(t, value)
	kind, other := fn.Params[0], fn.Params[1]
	for _, tc := range []struct {
		name           string
		knownValue     ssa.Value
		knownNumber    int64
		knownOp        token.Token
		known, decided bool
	}{
		{"matching equality holds", kind, 1, token.EQL, true, true},
		{"other equality fails", kind, 2, token.EQL, false, true},
		{"same inequality fails", kind, 1, token.NEQ, false, true},
		{"other inequality stays unknown", kind, 2, token.NEQ, false, false},
		{"another value stays unknown", other, 1, token.EQL, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			known, decided := conditionKnown(value, flowAssumption{selector: selectorFact{tc.knownValue, tc.knownNumber, tc.knownOp}})
			require.Equal(t, tc.known, known)
			require.Equal(t, tc.decided, decided)
		})
	}
}

const selectorFixture = `package storage
 type trackedRWMutex struct{}
 type holder struct{ first, second trackedRWMutex }
 func (h *holder) single() *trackedRWMutex { return &h.first }
 func (h *holder) choose(kind int) *trackedRWMutex {
  if kind == 1 { return &h.first }
  return &h.second
 }
 func literalOne(h *holder) *trackedRWMutex { return h.choose(1) }
 func literalTwo(h *holder) *trackedRWMutex { return h.choose(2) }
 func probe(h *holder, kind, other int) {
  single := h.single()
  _ = single
  chosen := h.choose(kind)
  _ = chosen
 }
`

// One return resolves its field; two returns naming different fields stay
// ambiguous, and a known selector prunes a return only when the shared
// implication decides it.
func TestMutexFieldPrunesOnlyDecidedReturns(t *testing.T) {
	p, pkg := ssaFixture(t, selectorFixture, "pkg/storage", ssa.SanityCheckFunctions)
	q := &acquirer{p: p}
	fn := pkg.Func("probe")
	choose := callTo(t, fn, "choose")
	callee := choose.Common().StaticCallee()
	require.NotNil(t, callee)
	returns := 0
	for _, block := range callee.Blocks {
		if _, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); ok {
			require.Len(t, blockSelectors(callee, block, nil), 1, "each return must depend on the kind predicate")
			returns++
		}
	}
	require.Equal(t, 2, returns, "the fixture must have two conditional returns")

	require.Equal(t, "pkg/storage.holder.first", q.mutexField(callTo(t, fn, "single"), flowAssumption{}, 0))
	require.Empty(t, q.mutexField(choose, flowAssumption{}, 0),
		"two possible returns naming different fields grant no field")

	kind, other := fn.Params[1], fn.Params[2]
	for _, tc := range []struct {
		name     string
		selector selectorFact
		want     string
	}{
		{"known equality selects the matching return", selectorFact{kind, 1, token.EQL}, "pkg/storage.holder.first"},
		{"known equality selects the other return", selectorFact{kind, 2, token.EQL}, "pkg/storage.holder.second"},
		{"known inequality excludes its own equality", selectorFact{kind, 1, token.NEQ}, "pkg/storage.holder.second"},
		{"unknown value keeps both returns", selectorFact{other, 1, token.EQL}, ""},
		{"known inequality on another number keeps both returns", selectorFact{kind, 2, token.NEQ}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, q.mutexField(choose, flowAssumption{selector: tc.selector}, 0))
		})
	}

	// The callee's predicate names its parameter; the argument passed for it
	// is what the implication must be given.
	require.Equal(t, "pkg/storage.holder.first", q.mutexField(callTo(t, pkg.Func("literalOne"), "choose"), flowAssumption{}, 0))
	require.Equal(t, "pkg/storage.holder.second", q.mutexField(callTo(t, pkg.Func("literalTwo"), "choose"), flowAssumption{}, 0))
}
