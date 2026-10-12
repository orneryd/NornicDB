package cypher

import (
	"strings"

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
	form := reduceForm{all: lowerASCII(function) == "allreduce"}
	parts := splitTopLevelComma(arguments)
	if want := map[bool]int{false: 2, true: 3}[form.all]; len(parts) != want {
		return form, false
	}
	assignment := strings.TrimSpace(parts[0])
	accumulator, end, ok := scanIdentifierToken(assignment, 0)
	if !ok {
		return form, false
	}
	equals := skipSpaces(assignment, end)
	if equals >= len(assignment) || assignment[equals] != '=' {
		return form, false
	}
	form.accumulator, form.initial = accumulator, strings.TrimSpace(assignment[equals+1:])
	iteration := strings.TrimSpace(parts[1])
	variable, end, ok := scanIdentifierToken(iteration, 0)
	if !ok {
		return form, false
	}
	rest := strings.TrimLeft(iteration[end:], " \t\r\n")
	if !startsWithKeywordFold(rest, "IN") {
		return form, false
	}
	rest = rest[len("IN"):]
	pipe := rowTopLevelPipeIndex(rest)
	if pipe < 0 {
		return form, false
	}
	form.variable, form.list, form.step = variable, strings.TrimSpace(rest[:pipe]), strings.TrimSpace(rest[pipe+1:])
	if form.initial == "" || form.list == "" || form.step == "" {
		return form, false
	}
	if form.all {
		form.predicate = parts[2]
	}
	return form, true
}

// runReduceForm evaluates a reduce form over items: step folds each item
// into the accumulator; for allReduce, predicate is evaluated after each
// step, and the result is false when it is false once, else null when it
// is null once, else true (an empty list is true). The callbacks get the
// accumulator and the item to bind.
func runReduceForm(form reduceForm, accumulator interface{}, items []interface{},
	step func(accumulator, item interface{}) (interface{}, error),
	predicate func(accumulator, item interface{}) (interface{}, error)) (interface{}, error) {
	var result interface{} = true
	for _, item := range items {
		var err error
		if accumulator, err = step(accumulator, item); err != nil {
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
