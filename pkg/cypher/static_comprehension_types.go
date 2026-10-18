package cypher

// checkListComprehension validates the source before execution and checks the
// body with the iterator's element type, shadowing any outer binding.
func (checker staticOperatorChecker) checkListComprehension(variable, list, predicate, projection string, probe *pipelineCancellation) (staticOperand, error) {
	source, err := checker.check(list, probe)
	if err != nil {
		return staticOperand{}, err
	}
	if source.known() && !source.list() && source.kind != "Null" {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	types := typeScopeOf(checker.scope)
	types[variable] = typedOperand(elementOfType(source.typeName()))
	nested := staticOperatorChecker{scope: types.static(checker.scope.cypher25), params: checker.params}
	nested.scope.complete = checker.scope.complete
	nested.scope.params = checker.scope.params
	var element staticOperand
	for index, expression := range []string{predicate, projection} {
		if err := validateStaticFunctionVariables(expression, nested.scope, probe); err != nil {
			return staticOperand{}, err
		}
		operand, err := nested.check(expression, probe)
		if err != nil {
			return staticOperand{}, err
		}
		if index == 0 && predicate != "" {
			if err := requireBooleanOperand(operand); err != nil {
				return staticOperand{}, err
			}
		}
		if index == 1 {
			element = operand
		}
	}
	if projection == "" {
		return source, nil
	}
	if name := element.typeName(); name != "" {
		return knownOperand(listOfType(name)), nil
	}
	return knownOperand("List<T>"), nil
}

// checkReduceTypes binds the source element and initial accumulator once, then
// validates the step with the same local function-argument owner as comprehensions.
func (checker staticOperatorChecker) checkReduceTypes(form reduceForm, probe *pipelineCancellation) (staticOperand, error) {
	source, err := checker.check(form.list, probe)
	if err != nil {
		return staticOperand{}, err
	}
	if source.known() && !source.list() && source.kind != "Null" {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	initial, err := checker.check(form.initial, probe)
	if err != nil {
		return staticOperand{}, err
	}
	types := typeScopeOf(checker.scope)
	types[form.variable] = typedOperand(elementOfType(source.kind))
	types[form.accumulator] = initial
	nested := staticOperatorChecker{scope: types.static(checker.scope.cypher25), params: checker.params}
	nested.scope.complete = checker.scope.complete
	nested.scope.params = checker.scope.params
	for index, text := range []string{form.step, form.predicate} {
		if text == "" {
			continue
		}
		if err := validateStaticFunctionVariables(text, nested.scope, probe); err != nil {
			return staticOperand{}, err
		}
		operand, err := nested.check(text, probe)
		if err != nil {
			return staticOperand{}, err
		}
		if index == 1 {
			if err := requireBooleanOperand(operand); err != nil {
				return staticOperand{}, err
			}
		}
	}
	if form.all {
		return knownOperand("Boolean"), nil
	}
	return initial, nil
}
