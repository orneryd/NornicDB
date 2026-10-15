package cypher

// The element loops of the row evaluator's list forms, one implementation
// each for the text path (evaluateRowValueText) and the compiled programs
// (row_value_program.go) (#1214). The caller binds the row in scope once and
// evaluates each part for the element the loop binds; the loop decides the
// form's value, and when an element leaves it unresolved or fails.

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
func rowComprehensionLoop(items []interface{}, bind func(interface{}), holds func() (bool, bool, error), project func() (interface{}, bool, error)) (interface{}, bool, error) {
	result := make([]interface{}, 0, len(items))
	for _, item := range items {
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
		result = append(result, value)
	}
	return result, true, nil
}

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
	result, err := runReduceForm(form, accumulator, coerceToUnwindItems(listValue), bind(step), bind(predicate))
	if err == errRowArgumentUnresolved {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}
