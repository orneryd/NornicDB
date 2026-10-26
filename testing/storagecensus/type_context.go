package storagecensus

import (
	"fmt"
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
	asserted  types.Type
	succeeds  bool
}

func (g typeGuard) id() string {
	return fmt.Sprintf("%d:%s:%t", g.parameter, types.TypeString(g.asserted, nil), g.succeeds)
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
		guards = append(guards, typeGuard{index, assertion.AssertedType, positive})
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
func transferGuards(guards []typeGuard, args []ssa.Value) ([]typeGuard, bool) {
	var out []typeGuard
	for _, g := range guards {
		if g.parameter < 0 || g.parameter >= len(args) {
			continue
		}
		value := args[g.parameter]
		yes, known := concreteAssertion(value, g.asserted, map[ssa.Value]bool{})
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
		parameter, ok := value.(*ssa.Parameter)
		if !ok {
			continue
		}
		for i, p := range parameter.Parent().Params {
			if p == parameter {
				g.parameter = i
				out = combineGuards(out, []typeGuard{g})
				break
			}
		}
	}
	return out, true
}
