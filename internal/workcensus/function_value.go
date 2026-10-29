package workcensus

import (
	"go/token"
	"golang.org/x/tools/go/ssa"
)

// MatchesFunctionValue checks the identity of a compiled function value against
// one conservative call target. It follows exact values, aliases, immutable
// local cells and unanimous phi arms. Unknown/escaping values return unknown;
// this never establishes a receiver instance, lifetime or acquisition.
func MatchesFunctionValue(value ssa.Value, target *ssa.Function) (matches, known bool) {
	return matchesFunctionValue(value, target, map[ssa.Value]bool{})
}

func matchesFunctionValue(value ssa.Value, target *ssa.Function, seen map[ssa.Value]bool) (bool, bool) {
	if value == nil || target == nil || seen[value] {
		return false, false
	}
	seen[value] = true
	switch v := value.(type) {
	case *ssa.Function:
		return v == target, true
	case *ssa.MakeClosure:
		return v.Fn == target, true
	case *ssa.ChangeType:
		return matchesFunctionValue(v.X, target, seen)
	case *ssa.Convert:
		return matchesFunctionValue(v.X, target, seen)
	case *ssa.Const:
		if v.IsNil() {
			return false, true
		}
	case *ssa.UnOp:
		if v.Op == token.MUL {
			if actual, ok := ImmutableCellValueAt(v.X, v); ok {
				return matchesFunctionValue(actual, target, seen)
			}
		}
	case *ssa.Phi:
		var answer bool
		for i, arm := range v.Edges {
			branch := map[ssa.Value]bool{}
			for x := range seen {
				branch[x] = true
			}
			yes, ok := matchesFunctionValue(arm, target, branch)
			if !ok || i > 0 && yes != answer {
				return false, false
			}
			answer = yes
		}
		return answer, len(v.Edges) > 0
	}
	return false, false
}
