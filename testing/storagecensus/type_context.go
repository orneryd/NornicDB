package storagecensus

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"go/token"
	"go/types"
	"golang.org/x/tools/go/ssa"
	"sort"
	"strings"
)

// typeGuard is a necessary branch condition, never an acquisition. Unknown
// interface values keep the write obligation; only an impossible concrete
// argument removes that call path.
type typeGuard struct {
	parameter int
	free      bool          // index names a captured binding, rather than a call parameter
	deref     bool          // the binding is a cell whose immutable value must be established
	callee    *ssa.Function // alternative condition: this exact function target
	asserted  types.Type
	succeeds  bool
}

func (g typeGuard) id() string {
	if g.callee != nil {
		return fmt.Sprintf("%d:%t:%t:func:%s:%d:%t", g.parameter, g.free, g.deref, g.callee.String(), g.callee.Pos(), g.succeeds)
	}
	return fmt.Sprintf("%d:%t:%t:%s:%t", g.parameter, g.free, g.deref, types.TypeString(g.asserted, nil), g.succeeds)
}
func guardID(guards []typeGuard) string {
	var ids []string
	for _, g := range guards {
		ids = append(ids, g.id())
	}
	sort.Strings(ids)
	return strings.Join(ids, "|")
}
func combineGuards(first, second []typeGuard) []typeGuard {
	var out []typeGuard
	seen := map[string]bool{}
	for _, list := range [][]typeGuard{first, second} {
		for _, g := range list {
			if !seen[g.id()] {
				seen[g.id()] = true
				out = append(out, g)
			}
		}
	}
	return out
}

func typeGuardsAt(fn *ssa.Function, at ssa.Instruction) []typeGuard {
	var guards []typeGuard
	for _, block := range fn.Blocks {
		branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok || block == at.Block() || !block.Dominates(at.Block()) {
			continue
		}
		predicate, known := readBranchCondition(branch.Cond)
		assertion := predicate.assertion
		if !known || assertion == nil {
			continue
		}
		positive := predicate.op == token.EQL
		// Only a concrete type test of a parameter can be refined by callers.
		if _, it := assertion.AssertedType.Underlying().(*types.Interface); it {
			continue
		}
		parameter, ok := assertion.X.(*ssa.Parameter)
		if !ok {
			continue
		}
		index := -1
		for i, p := range fn.Params {
			if p == parameter {
				index = i
				break
			}
		}
		if index < 0 {
			continue
		}
		// A join reached by both arms is not guarded. A loop backedge
		// starts another iteration, so it cannot establish this one's fact.
		sides := [2]bool{}
		for i, succ := range block.Succs {
			if !succ.Dominates(block) {
				sides[i] = reachable(succ)[at.Block()]
			}
		}
		yes, no := sides[0], sides[1]
		if yes == no {
			continue
		}
		if no {
			positive = !positive
		}
		guards = append(guards, typeGuard{parameter: index, asserted: assertion.AssertedType, succeeds: positive})
	}
	return guards
}

// concreteAssertion follows exact interface construction and every Phi arm.
// A mixed or external interface remains unknown; a lookalike concrete type
// never becomes the asserted type through its method names.
func concreteAssertion(value ssa.Value, asserted types.Type, seen map[ssa.Value]bool) (bool, bool) {
	if seen[value] {
		return false, false
	}
	seen[value] = true
	switch v := value.(type) {
	case *ssa.MakeInterface:
		return concreteAssertion(v.X, asserted, seen)
	case *ssa.ChangeInterface:
		return concreteAssertion(v.X, asserted, seen)
	case *ssa.Phi:
		var answer bool
		for i, edge := range v.Edges {
			armSeen := map[ssa.Value]bool{}
			for x := range seen {
				armSeen[x] = true
			}
			yes, known := concreteAssertion(edge, asserted, armSeen)
			if !known {
				return false, false
			}
			if i > 0 && yes != answer {
				return false, false
			}
			answer = yes
		}
		return answer, len(v.Edges) > 0
	}
	if _, ok := value.Type().Underlying().(*types.Interface); ok {
		return false, false
	}
	if it, ok := asserted.Underlying().(*types.Interface); ok {
		return types.Implements(value.Type(), it), true
	}
	return types.Identical(value.Type(), asserted), true
}

// transferGuards normalizes each condition to a caller parameter index.
// Local/opaque values lose their condition but retain the write obligation.
func transferGuards(guards []typeGuard, args, captures []ssa.Value, at ssa.Instruction) ([]typeGuard, bool) {
	var out []typeGuard
	for _, g := range guards {
		values := args
		if g.free {
			values = captures
		}
		if g.parameter < 0 || g.parameter >= len(values) {
			continue
		}
		value := values[g.parameter]
		if g.deref {
			actual, known := workcensus.ImmutableCellValueAt(value, at)
			if !known {
				continue
			} // unknown keeps the effect, never rejects it
			value = actual
		}
		var yes, known bool
		if g.callee != nil {
			yes, known = workcensus.MatchesFunctionValue(value, g.callee)
		} else {
			yes, known = concreteAssertion(value, g.asserted, map[ssa.Value]bool{})
		}
		if known {
			if yes != g.succeeds {
				return nil, false
			}
			continue
		}
		for {
			changed, ok := value.(*ssa.ChangeInterface)
			if !ok {
				break
			}
			value = changed.X
		}
		if binding, ok := guardBinding(value, g.asserted); ok {
			binding.succeeds = g.succeeds
			binding.callee = g.callee
			out = combineGuards(out, []typeGuard{binding})
		}
	}
	return out, true
}

func functionDispatchGuards(site ssa.CallInstruction, target *ssa.Function) ([]typeGuard, bool) {
	c := site.Common()
	if c.IsInvoke() || c.StaticCallee() != nil {
		return nil, true
	}
	if yes, known := workcensus.MatchesFunctionValue(c.Value, target); known {
		return nil, yes
	}
	if g, ok := guardBinding(c.Value, nil); ok {
		g.callee = target
		return []typeGuard{g}, true
	}
	return nil, true
}

// A resolved interface target is possible only for its actual receiver type.
// This is a path condition, never acquisition credit or a name exemption.
func dispatchGuards(site ssa.CallInstruction, target *types.Func) ([]typeGuard, bool) {
	c := site.Common()
	if !c.IsInvoke() {
		return nil, true
	}
	sig, ok := target.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return nil, true
	}
	recv := sig.Recv().Type()
	if yes, known := concreteAssertion(c.Value, recv, map[ssa.Value]bool{}); known {
		return nil, yes
	}
	if g, ok := guardBinding(c.Value, recv); ok {
		return []typeGuard{g}, true
	}
	return nil, true // unknown receiver keeps the obligation
}

func guardBinding(value ssa.Value, asserted types.Type) (typeGuard, bool) {
	for {
		changed, ok := value.(*ssa.ChangeInterface)
		if !ok {
			break
		}
		value = changed.X
	}
	if load, ok := value.(*ssa.UnOp); ok && load.Op == token.MUL {
		if free, ok := load.X.(*ssa.FreeVar); ok {
			if g, ok := guardBinding(free, asserted); ok {
				g.deref = true
				return g, true
			}
		}
		if actual, known := workcensus.ImmutableCellValueAt(load.X, load); known {
			return guardBinding(actual, asserted)
		}
	}
	switch v := value.(type) {
	case *ssa.Parameter:
		for i, p := range v.Parent().Params {
			if p == v {
				return typeGuard{parameter: i, asserted: asserted, succeeds: true}, true
			}
		}
	case *ssa.FreeVar:
		for i, f := range v.Parent().FreeVars {
			if f == v {
				return typeGuard{parameter: i, free: true, asserted: asserted, succeeds: true}, true
			}
		}
	}
	return typeGuard{}, false
}
