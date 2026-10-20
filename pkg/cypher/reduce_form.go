package cypher

import (
	"strings"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// reduceForm is a call written with reduce's own syntax:
// reduce(accumulator = initial, variable IN list | step) and Cypher 25's
// allReduce(accumulator = initial, variable IN list | step, predicate). The
// accumulator and variable are bound inside step and predicate, shadowing
// the scope. Every evaluator and check reads the form with parseReduceForm.
type reduceForm struct {
	accumulator, initial, variable, list, step, predicate string
	all                                                   bool
}

// isReduceFormFunction reports reduce and allReduce, case-insensitively,
// without allocating: every call name the checks scan passes through it.
func isReduceFormFunction(name string) bool {
	return strings.EqualFold(name, "reduce") || strings.EqualFold(name, "allReduce")
}

// parseReduceForm reads the arguments of a reduce or allReduce call; ok is
// false when they don't have the function's form (allReduce takes a
// predicate, reduce doesn't).
func parseReduceForm(function, arguments string) (reduceForm, bool) {
	form := reduceForm{all: strings.EqualFold(function, "allReduce")}
	syntax, parsed := fabric.ReduceSyntax(form.all, arguments)
	if !parsed {
		return form, false
	}
	read := func(span fabric.ExpressionSpan) string {
		if span.End <= span.Start {
			return ""
		}
		return arguments[span.Start:span.End]
	}
	form.accumulator, form.variable = syntax.Declarations[0].Name, syntax.Declarations[1].Name
	form.initial, form.list, form.step, form.predicate = read(syntax.Declarations[0].Source), read(syntax.Input), read(syntax.Projection), read(syntax.Predicate)
	return form, true
}

// reduceAppendPlan is a step of the form accumulator + t1 + … + tn (every
// operator +, the accumulator leftmost): while the accumulator is a list,
// the step's value is the list with each operand's value appended
// (appendListOperand), or null when one is null, so the fold can append
// the operands to a list it owns instead of copying the whole accumulator
// at every step, which made reduce quadratic in the list it builds.
//
// The owned list is shared with nothing, from the fold's start to its
// result: no operand names the accumulator (a step like a + [x] + [a]
// would keep a view of the list the fold goes on appending to, so it has
// no plan), the predicate's value is read as a truth value and dropped,
// and the result is handed over as a list of its own length
// (runReduceForm).
type reduceAppendPlan struct {
	terms []string // t1 … tn
	// bound are the texts of the step's sums: a row binding of the same name
	// would answer one of them instead (bound), so the plan is not used then.
	bound []string
}

// reduceAppendPlans caches each step's plan; nil when the step has none.
var reduceAppendPlans = newBoundedCache[string, *reduceAppendPlan](1024, boundedCacheStringShardIndex)

// reduceAppendDisabled turns the plans off, so that every step is
// evaluated whole. Tests use it to compare the two.
var reduceAppendDisabled atomic.Bool

// appendPlan returns the step's plan, read from the row compiler's shape of
// the step so that it splits the step as every evaluator does; nil when
// the step isn't such a sum.
func (form reduceForm) appendPlan() *reduceAppendPlan {
	key := form.accumulator + "\x00" + form.step
	if plan, cached := reduceAppendPlans.get(key); cached {
		return plan
	}
	var plan *reduceAppendPlan
	var node rowValueNode
	// A CASE is the one form that holds a + outside brackets, which the
	// arithmetic split doesn't skip; a step with one has no plan.
	if !containsIdentifierToken(strings.ToLower(form.step), "case") {
		node = rowValueProgramForFacts(form.step).root
	}
	var terms, texts []string
	for node != nil {
		node = rowValueShape(node)
		if bound, ok := node.(*rowBoundNode); ok {
			texts = append(texts, bound.text)
			node = bound.inner
			continue
		}
		sum, ok := node.(*rowArithmeticNode)
		if !ok || sum.operator != '+' || sum.rightText == "" {
			break
		}
		terms = append(terms, sum.rightText)
		if sum.leftText == form.accumulator {
			for i, j := 0, len(terms)-1; i < j; i, j = i+1, j-1 {
				terms[i], terms[j] = terms[j], terms[i]
			}
			if !termsNameAccumulator(terms, form.accumulator) {
				plan = &reduceAppendPlan{terms: terms, bound: texts}
			}
			break
		}
		node = sum.left
	}
	reduceAppendPlans.put(key, plan)
	return plan
}

// termsNameAccumulator reports whether an operand refers to the accumulator,
// as a name (referencesVariable) or quoted (`a`): such an operand can keep
// the accumulator's value.
func termsNameAccumulator(terms []string, accumulator string) bool {
	for _, term := range terms {
		if referencesVariable(term, accumulator) || strings.Contains(term, "`"+accumulator+"`") {
			return true
		}
	}
	return false
}

// usable reports whether the plan applies in a scope: none of the step's
// sums is a name the scope binds.
func (plan *reduceAppendPlan) usable(scope map[string]interface{}) bool {
	if plan == nil || reduceAppendDisabled.Load() {
		return false
	}
	for _, text := range plan.bound {
		if _, bound := scope[text]; bound {
			return false
		}
	}
	return true
}

// reduceAppender evaluates a plan's operands: term evaluates operand index
// with the accumulator and the item bound, as step evaluates the step.
type reduceAppender struct {
	plan *reduceAppendPlan
	term func(index int, accumulator, item interface{}) (interface{}, error)
}

// runReduceForm evaluates a reduce form over items: step folds each item
// into the accumulator; for allReduce, predicate is evaluated after each
// step, and the result is false when it is false once, else null when it
// is null once, else true (an empty list is true). The callbacks get the
// accumulator and the item to bind. With an appender, a step folding into
// a list appends its operands to a list the fold owns; the accumulator
// bound for the operands and the predicate is that list capped at its
// length, so nothing else appends into it, and the result is a copy of its
// own length (reduceAppendPlan).
func runReduceForm(form reduceForm, accumulator interface{}, items []interface{},
	step func(accumulator, item interface{}) (interface{}, error),
	predicate func(accumulator, item interface{}) (interface{}, error),
	appender *reduceAppender) (interface{}, error) {
	var result interface{} = true
	var owned, operands []interface{}
	if appender != nil {
		operands = make([]interface{}, len(appender.plan.terms))
	}
	for _, item := range items {
		var err error
		if list, isList := accumulator.([]interface{}); isList && appender != nil {
			// Every operand is evaluated, as the sum evaluates them, before
			// they are appended.
			for index := range operands {
				if operands[index], err = appender.term(index, accumulator, item); err != nil {
					return nil, err
				}
			}
			if owned == nil || len(list) != len(owned) || (len(list) > 0 && &list[0] != &owned[0]) {
				owned = append(make([]interface{}, 0, 2*len(list)+8), list...)
			}
			accumulator = nil
			for _, operand := range operands {
				if operand == nil {
					owned = nil
					break
				}
				owned = appendListOperand(owned, operand)
			}
			if owned != nil {
				accumulator = owned[:len(owned):len(owned)]
			}
		} else if accumulator, err = step(accumulator, item); err != nil {
			return nil, err
		}
		if !form.all {
			continue
		}
		holds, err := predicate(accumulator, item)
		if err != nil {
			return nil, err
		}
		switch typed := holds.(type) {
		case nil:
			result = nil
		case bool:
			if !typed {
				return false, nil
			}
		default:
			// A parameter predicate is checked with the statement, as Neo4j
			// types parameters; any other value fails when it is met.
			if parameter := strings.TrimSpace(form.predicate); parameter[0] == '$' && simpleSemanticIdentifier(parameter[1:]) != "" {
				operand := staticParameterOperand(holds)
				operand.parameter = parameter[1:]
				return nil, operandMismatch(operand, "Boolean")
			}
			return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
				localization.CypherCorePredicateNotBoolean(neo4jValueRepr(holds)))
		}
	}
	if form.all {
		return result, nil
	}
	// The fold's list goes to its next owner (a row, a list, a property) as a
	// list of its own length: no spare capacity of the fold's list stays
	// live behind it.
	if list, isList := accumulator.([]interface{}); isList && owned != nil && len(list) == len(owned) &&
		(len(list) == 0 || &list[0] == &owned[0]) && cap(owned) > len(owned) {
		return append(make([]interface{}, 0, len(owned)), owned...), nil
	}
	return accumulator, nil
}
