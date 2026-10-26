package cypher

// evaluateRowReduce evaluates reduce and allReduce (parseReduceForm) for a
// row, every part evaluated by evaluate (the row evaluator, or the context
// evaluator for a reduce whose expressions hold subqueries); resolved is false
// when a part can't be evaluated.
func (e *StorageExecutor) evaluateRowReduce(function string, argument braceView, values map[string]interface{}, evaluate rowValueEvaluator) (interface{}, bool, error) {
	form, ok := parseReduceForm(function, argument)
	if !ok {
		return nil, false, nil
	}
	accumulator, resolved, err := evaluate(argument.spanWindow(form.initialSpan), values)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := evaluate(argument.spanWindow(form.listSpan), values)
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
	plan := form.appendPlan()
	return rowReduceLoop(form, accumulator, listValue, scope,
		func() (interface{}, bool, error) { return evaluate(argument.spanWindow(form.stepSpan), scope) },
		func() (interface{}, bool, error) { return evaluate(argument.spanWindow(form.predicateSpan), scope) },
		func(index int) (interface{}, bool, error) { return evaluate(plan.views[index], scope) })
}

func rowTopLevelPipeIndex(expression string) int {
	return findTopLevelSymbol(expression, "|", false, true)
}
