package cypher

// evaluateRowReduce evaluates reduce and allReduce (parseReduceForm) for a
// row, every part evaluated by evaluate (the row evaluator, or the context
// evaluator for a reduce whose expressions hold subqueries); resolved is false
// when a part can't be evaluated.
func (e *StorageExecutor) evaluateRowReduce(function, argument string, values map[string]interface{}, evaluate rowValueEvaluator) (interface{}, bool, error) {
	form, ok := parseReduceForm(function, argument)
	if !ok {
		return nil, false, nil
	}
	accumulator, resolved, err := evaluate(form.initial, values)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := evaluate(form.list, values)
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
		func() (interface{}, bool, error) { return evaluate(form.step, scope) },
		func() (interface{}, bool, error) { return evaluate(form.predicate, scope) },
		func(index int) (interface{}, bool, error) { return evaluate(plan.terms[index], scope) })
}

func rowTopLevelPipeIndex(expression string, views ...expressionLexicalView) int {
	v := borrowedExpressionView(expression, views)
	tokens := v.tokens()
	if len(tokens) > 0 {
		for _, token := range tokens {
			if token.kind == lexicalPunctuation && token.depth == tokens[0].depth && v.facts.text[token.start] == '|' {
				return token.start - v.span.start
			}
		}
	}
	return -1
}
