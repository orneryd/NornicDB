package cypher

import "context"

// evaluateRowReduce evaluates reduce and allReduce (parseReduceForm) for a
// row, every part evaluated by evaluate (the row evaluator, or the context
// evaluator for a reduce whose expressions hold subqueries); resolved is false
// when a part can't be evaluated.
func (e *StorageExecutor) evaluateRowReduce(ctx context.Context, function, argument string, values map[string]interface{}, evaluate rowValueEvaluator) (interface{}, bool, error) {
	form, ok := parseReduceForm(function, argument)
	if !ok {
		return nil, false, nil
	}
	accumulator, resolved, err := evaluate(ctx, form.initial, values)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := evaluate(ctx, form.list, values)
	if err != nil || !resolved {
		return nil, false, err
	}
	if listValue == nil {
		return nil, true, nil
	}
	scope := make(map[string]interface{}, len(values)+2)
	for name, value := range values {
		scope[name] = value
	}
	plan := form.appendPlan(ctx)
	return rowReduceLoop(ctx, form, accumulator, listValue, scope,
		func() (interface{}, bool, error) { return evaluate(ctx, form.step, scope) },
		func() (interface{}, bool, error) { return evaluate(ctx, form.predicate, scope) },
		func(index int) (interface{}, bool, error) { return evaluate(ctx, plan.terms[index], scope) })
}

func rowTopLevelPipeIndex(expression string) int {
	return findTopLevelOperator(expression, "|", false, true)
}
