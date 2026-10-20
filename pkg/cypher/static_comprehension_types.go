package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// bindingOperandViews come directly from a native local-scope descriptor.
// Standalone evaluator adapters omit them; statement admission always retains
// the parent and its operand spans through recursive type checks.
type bindingOperandViews struct {
	source string
	spans  [4]fabric.ExpressionSpan
}

func (checker staticOperatorChecker) bindingPart(text string, views []bindingOperandViews, index int) (staticOperand, error) {
	if len(views) == 0 {
		return checker.check(text)
	}
	span := views[0].spans[index]
	if span.End <= span.Start {
		return staticOperand{}, nil
	}
	return checker.checkRange(views[0].source, span.Start, span.End)
}

func bindingPartScope(scope staticTypeScope, views []bindingOperandViews, index int) staticTypeScope {
	if len(views) == 0 {
		return scope
	}
	span := views[0].spans[index]
	return scope.nativeRange(views[0].source, span.Start, span.End)
}

// checkListComprehension validates the source before execution and checks the
// body with the iterator's element type, shadowing any outer binding.
func (checker staticOperatorChecker) checkListComprehension(variable, list, predicate, projection string, views ...bindingOperandViews) (staticOperand, error) {
	source, err := checker.bindingPart(list, views, 0)
	if err != nil {
		return staticOperand{}, err
	}
	if source.known() && !source.list() && source.kind != "Null" {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	types := typeScopeOf(checker.scope)
	types[variable] = typedOperand(elementOfType(source.typeName()))
	nested := checker.scope.withTypes(types).operatorChecker()
	var element staticOperand
	for index, expression := range []string{predicate, projection} {
		if err := validateStaticFunctionVariables(expression, bindingPartScope(nested.scope, views, index+1)); err != nil {
			return staticOperand{}, err
		}
		operand, err := nested.bindingPart(expression, views, index+1)
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
func (checker staticOperatorChecker) checkReduceTypes(form reduceForm, views ...bindingOperandViews) (staticOperand, error) {
	source, err := checker.bindingPart(form.list, views, 0)
	if err != nil {
		return staticOperand{}, err
	}
	if source.known() && !source.list() && source.kind != "Null" {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	initial, err := checker.bindingPart(form.initial, views, 1)
	if err != nil {
		return staticOperand{}, err
	}
	types := typeScopeOf(checker.scope)
	types[form.variable] = typedOperand(elementOfType(source.kind))
	types[form.accumulator] = initial
	nested := checker.scope.withTypes(types).operatorChecker()
	for index, text := range []string{form.step, form.predicate} {
		if text == "" {
			continue
		}
		if err := validateStaticFunctionVariables(text, bindingPartScope(nested.scope, views, index+2)); err != nil {
			return staticOperand{}, err
		}
		operand, err := nested.bindingPart(text, views, index+2)
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
