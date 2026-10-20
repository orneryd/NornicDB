package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// checkNativeBindingScope checks operands with the native declaration and span
// descriptor before ordinary map/function decomposition can discard its scope.
func (checker staticOperatorChecker) checkNativeBindingScope(expression string, scope fabric.ExpressionScope) (staticOperand, bool, error) {
	read := func(span fabric.ExpressionSpan) string {
		if span.End <= span.Start {
			return ""
		}
		return expression[span.Start:span.End]
	}
	if scope.Kind == fabric.ExpressionMapScope {
		if _, err := checker.check(read(scope.Input)); err != nil {
			return staticOperand{}, true, err
		}
		local := withLocalTypes(checker.scope, expression, scope.Visible)
		nested := staticOperatorChecker{scope: local, params: checker.params}
		for index, part := range []string{read(scope.Predicate), read(scope.Key), read(scope.Projection)} {
			if part == "" {
				continue
			}
			if err := validateStaticFunctionVariables(part, local); err != nil {
				return staticOperand{}, true, err
			}
			operand, err := nested.check(part)
			if err != nil {
				return staticOperand{}, true, err
			}
			if index == 0 {
				if err := requireBooleanOperand(operand); err != nil {
					return staticOperand{}, true, err
				}
			}
		}
		return knownOperand("Map"), true, nil
	}
	if scope.Kind != fabric.ExpressionLocalScope {
		return staticOperand{}, false, nil
	}
	switch scope.Function {
	case "", "filter":
		operand, err := checker.checkListComprehension(scope.Declarations[0].Name, read(scope.Input), read(scope.Predicate), read(scope.Projection))
		return operand, true, err
	case "all", "any", "none", "single":
		_, err := checker.checkListComprehension(scope.Declarations[0].Name, read(scope.Input), read(scope.Predicate), "")
		return knownOperand("Boolean"), true, err
	case "reduce", "allreduce":
		form, ok := parseReduceForm(scope.Function, expression[scope.Open+1:scope.Close])
		if !ok {
			return staticOperand{}, false, nil
		}
		operand, err := checker.checkReduceTypes(form)
		return operand, true, err
	}
	return staticOperand{}, false, nil
}
