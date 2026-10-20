package cypher

import (
	"strings"

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

// runReduceForm evaluates a reduce form over items: step folds each item
// into the accumulator; for allReduce, predicate is evaluated after each
// step, and the result is false when it is false once, else null when it
// is null once, else true (an empty list is true). The callbacks get the
// accumulator and the item to bind.
//
// The accumulator counts against the statement's memory limit as it grows
// (budget, #1343): checked after each step, so a step that takes it over
// the limit fails the statement before the next step; given back at the end,
// where the row or list that keeps the result counts it.
func runReduceForm(budget *expressionBudget, form reduceForm, accumulator interface{}, items []interface{},
	step func(accumulator, item interface{}) (interface{}, error),
	predicate func(accumulator, item interface{}) (interface{}, error)) (interface{}, error) {
	defer budget.release()
	var result interface{} = true
	for _, item := range items {
		var err error
		if accumulator, err = step(accumulator, item); err != nil {
			return nil, err
		}
		if err := budget.holdAccumulator(accumulator); err != nil {
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
	return accumulator, nil
}
