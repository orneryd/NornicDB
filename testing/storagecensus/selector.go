package storagecensus

import (
	"go/constant"
	"go/token"

	"github.com/orneryd/nornicdb/internal/lockeffect"

	"golang.org/x/tools/go/ssa"
)

// selectorFact is a predicate on one actual SSA value, not a function name.
// It couples a selected lock and selected key only when both use that value.
type selectorFact struct {
	value  ssa.Value
	number int64
	op     token.Token
}
type flowAssumption struct {
	nonEmpty *ssa.Parameter
	selector selectorFact
}

// integerComparison is the common reader of the CFG's integer comparisons,
// including the zero-length guards used by callback coverage.
func integerComparison(value ssa.Value) (selectorFact, bool) {
	if neg, ok := value.(*ssa.UnOp); ok && neg.Op == token.NOT {
		f, ok := integerComparison(neg.X)
		if !ok {
			return f, false
		}
		switch f.op {
		case token.EQL:
			f.op = token.NEQ
		case token.NEQ:
			f.op = token.EQL
		case token.LSS:
			f.op = token.GEQ
		case token.GTR:
			f.op = token.LEQ
		case token.LEQ:
			f.op = token.GTR
		case token.GEQ:
			f.op = token.LSS
		default:
			return f, false
		}
		return f, true
	}
	comparison, ok := value.(*ssa.BinOp)
	if !ok {
		return selectorFact{}, false
	}
	subject := comparison.X
	literal, ok := comparison.Y.(*ssa.Const)
	op := comparison.Op
	if !ok {
		literal, ok = comparison.X.(*ssa.Const)
		subject = comparison.Y
		switch op {
		case token.LSS:
			op = token.GTR
		case token.GTR:
			op = token.LSS
		case token.LEQ:
			op = token.GEQ
		case token.GEQ:
			op = token.LEQ
		}
	}
	if !ok || literal.Value == nil || literal.Value.Kind() != constant.Int {
		return selectorFact{}, false
	}
	number, ok := constant.Int64Val(literal.Value)
	if !ok {
		return selectorFact{}, false
	}
	switch op {
	case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ:
		return selectorFact{subject, number, op}, true
	}
	return selectorFact{}, false
}

func compareInteger(value, number int64, op token.Token) bool {
	switch op {
	case token.EQL:
		return value == number
	case token.NEQ:
		return value != number
	case token.LSS:
		return value < number
	case token.GTR:
		return value > number
	case token.LEQ:
		return value <= number
	case token.GEQ:
		return value >= number
	}
	return false
}

func conditionKnown(value ssa.Value, assumption flowAssumption) (bool, bool) {
	test, ok := integerComparison(value)
	if !ok {
		return false, false
	}
	if length, ok := test.value.(*ssa.Call); ok && assumption.nonEmpty != nil && test.number == 0 {
		if builtin, ok := length.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "len" && len(length.Common().Args) == 1 && length.Common().Args[0] == assumption.nonEmpty {
			switch test.op {
			case token.GTR, token.NEQ:
				return true, true
			case token.EQL, token.LEQ:
				return false, true
			}
		}
	}
	known := assumption.selector
	if known.value == nil || known.value != test.value {
		return false, false
	}
	if known.op == token.EQL {
		return compareInteger(known.number, test.number, test.op), true
	}
	if known.op == token.NEQ && known.number == test.number {
		switch test.op {
		case token.EQL:
			return false, true
		case token.NEQ:
			return true, true
		}
	}
	return false, false
}

// blockSelectors records predicates implied by reaching block (or its edge
// to next). Both positive and negative branches keep the same typed identity.
func blockSelectors(fn *ssa.Function, block, next *ssa.BasicBlock) []selectorFact {
	var out []selectorFact
	for _, candidate := range fn.Blocks {
		branch, ok := candidate.Instrs[len(candidate.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		side := -1
		if candidate == block && next != nil {
			for i, succ := range candidate.Succs {
				if succ == next {
					side = i
				}
			}
		} else if candidate != block && candidate.Dominates(block) {
			left, right := candidate.Succs[0].Dominates(block), candidate.Succs[1].Dominates(block)
			if left != right {
				if left {
					side = 0
				} else {
					side = 1
				}
			}
		}
		if side < 0 {
			continue
		}
		fact, ok := integerComparison(branch.Cond)
		if !ok {
			continue
		}
		if fact.op != token.EQL && fact.op != token.NEQ {
			continue
		}
		if side == 1 {
			if fact.op == token.EQL {
				fact.op = token.NEQ
			} else {
				fact.op = token.EQL
			}
		}
		out = append(out, fact)
	}
	return out
}

// selectedValues are actual arguments of a returned-mutex selector that is
// subsequently locked. Constants stay constants; no selector-name allowlist.
func (q *acquirer) selectedValues(fn *ssa.Function) map[ssa.Value]bool {
	values := map[ssa.Value]bool{}
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(*ssa.Call)
			if !ok {
				continue
			}
			callee := call.Common().StaticCallee()
			if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg.Path() != "sync" || len(call.Common().Args) == 0 {
				continue
			}
			if action, exclusive := lockeffect.Classify(callee.Name(), false); !exclusive || action != "lock" {
				continue
			}
			source, ok := call.Common().Args[0].(*ssa.Call)
			if !ok {
				continue
			}
			selector := source.Common().StaticCallee()
			if selector == nil || !inModule(selector) {
				continue
			}
			for _, arg := range source.Common().Args {
				values[arg] = true
			}
		}
	}
	return values
}

// mutexField resolves only returns consistent with the caller's predicate.
// Ambiguous fields, interface calls and unknown return values grant no lock.
func (q *acquirer) mutexField(value ssa.Value, assumption flowAssumption, depth int) string {
	if depth > 8 {
		return ""
	}
	if field, ok := value.(*ssa.FieldAddr); ok {
		name, _ := q.p.field(field.X.Type(), field.Field)
		return name
	}
	call, ok := value.(*ssa.Call)
	if !ok {
		return ""
	}
	callee := call.Common().StaticCallee()
	if callee == nil || callee.Blocks == nil || !inModule(callee) {
		return ""
	}
	result := ""
	found := false
	for _, block := range callee.Blocks {
		ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
		if !ok || len(ret.Results) != 1 {
			continue
		}
		possible := true
		for _, predicate := range blockSelectors(callee, block, nil) {
			parameter, ok := predicate.value.(*ssa.Parameter)
			if !ok {
				continue
			}
			index := -1
			for i, p := range callee.Params {
				if p == parameter {
					index = i
				}
			}
			if index < 0 || index >= len(call.Common().Args) {
				continue
			}
			argument := call.Common().Args[index]
			if literal, ok := argument.(*ssa.Const); ok && literal.Value != nil && literal.Value.Kind() == constant.Int {
				number, ok := constant.Int64Val(literal.Value)
				if ok && !compareInteger(number, predicate.number, predicate.op) {
					possible = false
				}
			} else if argument == assumption.selector.value && assumption.selector.value != nil {
				known := assumption.selector
				if known.op == token.EQL && !compareInteger(known.number, predicate.number, predicate.op) {
					possible = false
				}
				if known.op == token.NEQ && known.number == predicate.number && predicate.op == token.EQL {
					possible = false
				}
			}
		}
		if !possible {
			continue
		}
		field := q.mutexField(ret.Results[0], flowAssumption{}, depth+1)
		if field == "" || found && field != result {
			return ""
		}
		result = field
		found = true
	}
	return result
}

// selectedByteCases preserves each finite closed-world selector alternative as
// its own key and lock proof. Unknown/external values remain pending/fail closed.
func (a *analysis) selectedByteCases(shape keyShape, fn *ssa.Function) ([]keyShape, bool) {
	selected := a.acq.selectedValues(fn)
	for index, source := range shape.bytes {
		if source.known || source.param < 0 || source.param >= len(fn.Params) {
			continue
		}
		parameter := fn.Params[source.param]
		if !selected[parameter] {
			continue
		}
		values, why := a.k.byteVals(parameter, map[ssa.Value]bool{})
		if why != "" || len(values) == 0 {
			return nil, false
		}
		var out []keyShape
		for _, value := range values {
			next := shape
			next.bytes = append([]byteSrc(nil), shape.bytes...)
			next.bytes[index] = byteSrc{known: true, val: value, param: -1}
			next.selectors = append(append([]selectorFact(nil), shape.selectors...), selectorFact{parameter, int64(value), token.EQL})
			out = append(out, next)
		}
		return out, true
	}
	return nil, false
}
