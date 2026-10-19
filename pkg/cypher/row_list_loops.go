package cypher

import "context"

// The element loops of the row evaluator's list forms, one implementation
// each for the text path (evaluateRowValueText) and the compiled programs
// (row_value_program.go) (#1214). The caller binds the row in scope once and
// evaluates each part for the element the loop binds; the loop decides the
// form's value, and when an element leaves it unresolved or fails.

// rowElementScope is the one row map a list form's element parts see: the
// row copied once, its variables (extra of them) rebound per element. A body
// with a subquery (localSubqueries) evaluates it per element, only where
// reached, so the scope is a local subquery scope (localSubqueryScope).
func rowElementScope(values map[string]interface{}, extra int, localSubqueries bool) map[string]interface{} {
	scope := make(map[string]interface{}, len(values)+extra)
	for name, value := range values {
		scope[name] = value
	}
	if localSubqueries {
		if ctx := rowStatementContext(values); ctx != nil && !inLocalSubqueryScope(ctx) {
			scope[rowStatementContextKey] = context.WithValue(ctx, localSubqueryScopeKey{}, true)
		}
	}
	return scope
}

// rowQuantifierLoop is all / any / none / single(variable IN items WHERE
// predicate), folded by quantifierFold. An unevaluated predicate leaves the
// quantifier unresolved; an evaluated value uses the shared predicate contract.
func rowQuantifierLoop(function string, items []interface{}, bind func(interface{}), predicate func() (interface{}, bool, error)) (interface{}, bool, error) {
	fold := quantifierFold{function: function}
	for _, item := range items {
		bind(item)
		result, evaluated, err := predicate()
		if err != nil {
			return nil, false, err
		}
		if !evaluated {
			return nil, false, nil
		}
		truth, err := cypherPredicateTruth(result)
		if err != nil {
			return nil, true, err
		}
		if value, decided := fold.add(truth.value()); decided {
			return value, true, nil
		}
	}
	return fold.result(), true, nil
}

// rowComprehensionLoop is [variable IN items WHERE predicate | projection]:
// holds (nil without a predicate) whether an element is kept, project (nil
// without a projection) its value. An element either can't evaluate leaves
// the comprehension unresolved.
//
// The result counts against the statement's memory limit while it is built
// (budget, #1343): its capacity before it is made, its values as they are
// added, checked every comprehensionCheckEvery elements. It is given back
// once built; the row or list that keeps it counts it there.
func rowComprehensionLoop(budget *expressionBudget, items []interface{}, bind func(interface{}), holds func() (bool, bool, error), project func() (interface{}, bool, error)) (interface{}, bool, error) {
	if budget.active() {
		if err := budget.beginList(len(items)); err != nil {
			return nil, false, err
		}
		defer budget.release()
	}
	result := make([]interface{}, 0, len(items))
	for index, item := range items {
		if index > 0 && index%comprehensionCheckEvery == 0 {
			if err := budget.checkList(); err != nil {
				return nil, false, err
			}
		}
		bind(item)
		if holds != nil {
			kept, evaluated, err := holds()
			if err != nil {
				return nil, false, err
			}
			if !evaluated {
				return nil, false, nil
			}
			if !kept {
				continue
			}
		}
		value := item
		if project != nil {
			var evaluated bool
			var err error
			value, evaluated, err = project()
			if err != nil {
				return nil, false, err
			}
			if !evaluated {
				return nil, false, nil
			}
		}
		budget.addElement(value)
		result = append(result, value)
	}
	return result, true, nil
}

// comprehensionCheckEvery is how many elements a comprehension adds between
// checks of what it built against the statement's limits.
const comprehensionCheckEvery = 1024

// rowConditionHolds is a comprehension condition's value read as kept: only
// true keeps the element; a value that isn't a boolean or null is the
// statement's error.
func rowConditionHolds(condition interface{}, evaluated bool, err error) (bool, bool, error) {
	if err != nil || !evaluated {
		return false, evaluated, err
	}
	truth, err := cypherPredicateTruth(condition)
	if err != nil {
		return false, true, err
	}
	return truth == truthTrue, true, nil
}

// rowReduceLoop folds reduce / allReduce (runReduceForm) over listValue from
// accumulator: step and predicate evaluate the form's step and predicate
// with the accumulator and the variable bound in scope. A step or predicate
// that can't be evaluated leaves the form unresolved.
func rowReduceLoop(form reduceForm, accumulator, listValue interface{}, scope map[string]interface{}, step, predicate func() (interface{}, bool, error)) (interface{}, bool, error) {
	probe := rowPipelineCancellation(scope)
	items := coerceToUnwindItemsWithCancellation(listValue, probe)
	if probe != nil && probe.failed() != nil {
		return nil, false, probe.failed()
	}
	bind := func(evaluate func() (interface{}, bool, error)) func(accumulator, item interface{}) (interface{}, error) {
		return func(accumulator, item interface{}) (interface{}, error) {
			scope[form.accumulator], scope[form.variable] = accumulator, item
			value, resolved, err := evaluate()
			if err == nil && !resolved {
				err = errRowArgumentUnresolved
			}
			return value, err
		}
	}
	budget := rowExpressionBudget(scope)
	result, err := runReduceFormWithCancellation(&budget, form, accumulator, items, bind(step), bind(predicate), probe)
	if err == errRowArgumentUnresolved {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}
