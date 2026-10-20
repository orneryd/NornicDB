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
		if _, err := checker.checkRange(expression, scope.Input.Start, scope.Input.End); err != nil {
			return staticOperand{}, true, err
		}
		local := withLocalTypes(checker.scope, expression, scope.Visible)
		nested := local.operatorChecker()
		for index, span := range []fabric.ExpressionSpan{scope.Predicate, scope.Key, scope.Projection} {
			part := read(span)
			if part == "" {
				continue
			}
			if err := validateStaticFunctionVariables(part, local.nativeRange(expression, span.Start, span.End)); err != nil {
				return staticOperand{}, true, err
			}
			operand, err := nested.checkRange(expression, span.Start, span.End)
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
	if scope.Kind == fabric.ExpressionPatternScope {
		// Pattern bindings and expression-local shadowing come from the same
		// admitted native scope. Its projection is still checked on empty input.
		local := withPatternComprehensionBindings(withLocalTypes(checker.scope, expression, scope.Visible), expression, scope.Visible)
		nested := local.operatorChecker()
		for index, span := range []fabric.ExpressionSpan{scope.Predicate, scope.Projection} {
			if read(span) == "" {
				continue
			}
			operand, err := nested.checkRange(expression, span.Start, span.End)
			if err != nil {
				return staticOperand{}, true, err
			}
			if index == 0 {
				if err := requireBooleanOperand(operand); err != nil {
					return staticOperand{}, true, err
				}
			}
		}
		return knownOperand("List<T>"), true, nil
	}
	if scope.Kind != fabric.ExpressionLocalScope {
		return staticOperand{}, false, nil
	}
	switch scope.Function {
	case "", "filter":
		operand, err := checker.checkListComprehension(scope.Declarations[0].Name, read(scope.Input), read(scope.Predicate), read(scope.Projection), bindingOperandViews{source: expression, spans: [4]fabric.ExpressionSpan{scope.Input, scope.Predicate, scope.Projection}})
		return operand, true, err
	case "all", "any", "none", "single":
		_, err := checker.checkListComprehension(scope.Declarations[0].Name, read(scope.Input), read(scope.Predicate), "", bindingOperandViews{source: expression, spans: [4]fabric.ExpressionSpan{scope.Input, scope.Predicate}})
		return knownOperand("Boolean"), true, err
	case "reduce", "allreduce":
		form := reduceForm{all: scope.Function == "allreduce", accumulator: scope.Declarations[0].Name, variable: scope.Declarations[1].Name, initial: read(scope.Declarations[0].Source), list: read(scope.Input), step: read(scope.Projection), predicate: read(scope.Predicate)}
		operand, err := checker.checkReduceTypes(form, bindingOperandViews{source: expression, spans: [4]fabric.ExpressionSpan{scope.Input, scope.Declarations[0].Source, scope.Projection, scope.Predicate}})
		return operand, true, err
	}
	return staticOperand{}, false, nil
}
