package cypher

import (
	"context"
	"strings"
)

// checkScoped types text in a local scope (a list or pattern
// comprehension's, reduce's): the function signatures the statement's
// visitor leaves to the scope that binds their variables, then its operand.
//
// Both passes read the scope's texts through the scope's own results
// (checked), so an argument the signature check types is not typed again by
// the operator check, nor its nested scopes.
func (checker staticOperatorChecker) checkScoped(ctx context.Context, text string) (staticOperand, error) {
	if checker.checked == nil {
		checker.checked = make(map[string]staticCheckResult)
	}
	// The signature check types without parameter values (operandOf).
	typer := staticOperatorChecker{scope: checker.scope, checked: checker.checked}
	if checker.params != nil {
		typer.checked = make(map[string]staticCheckResult)
	}
	if err := validateStaticFunctionVariablesTyped(ctx, text, func() staticTypeScope { return checker.scope }, func(scope staticTypeScope, expression string) string {
		return typer.argumentType(ctx, scope, expression)
	}); err != nil {
		return staticOperand{}, err
	}
	return checker.check(ctx, text)
}

// argumentType is staticExpressionType through the checker's results.
func (checker staticOperatorChecker) argumentType(ctx context.Context, scope staticTypeScope, expression string) string {
	if len(scope.params) > 0 && strings.HasPrefix(strings.TrimSpace(expression), "$") {
		return scope.parameterExpressionType(ctx, expression)
	}
	operand, err := checker.check(ctx, expression)
	if err != nil {
		return ""
	}
	return operand.typeName()
}

// boundLocalOperand admits an isolated expression once through the same
// constructor used by statements. No type checker installs a lexical binding.
func (checker staticOperatorChecker) boundLocalOperand(ctx context.Context, expression string) (staticOperand, error) {
	if checker.scope.admitted != nil {
		return staticOperand{}, unresolvedStatementError()
	}
	input := typeScopeOf(checker.scope)
	if !checker.scope.complete {
		// A signature-only caller supplies unknown external operands, not an
		// admitted query namespace. Grammar supplies its free input signature.
		for _, name := range expressionFreeVariables(ctx, expression) {
			if _, known := input[name]; !known {
				input[name] = staticOperand{}
			}
		}
	}
	values, model, err := (&StorageExecutor{}).enrichBoundTypes(ctx, "RETURN "+expression, input, checker.scope.bindings, "", checker.scope.cypher25)
	if err != nil {
		return staticOperand{}, err
	}
	if values == nil || model == nil {
		return staticOperand{}, unresolvedStatementError()
	}
	for _, value := range model.returned {
		return value, nil
	}
	return staticOperand{}, nil
}

// reduceAccumulatorOperand is the static type of reduce's accumulator in its
// step, as Neo4j types it (rule R11): a single-type initial value's type;
// for a union initial value (a CASE's, an operator signature's, another
// reduce's) the least upper bound of each pair of its alternatives (proven
// for direct use only: composable is false with an Any alternative);
// a property value, accumulated (labels(reduce(a = n.k, x IN l | a)) is
// rejected as labels(n.k) is, labels(v) after UNWIND [reduce(a = n.k, …)] AS
// v is not); unknown for an initial value that may be null or isn't typed.
func reduceAccumulatorOperand(initial staticOperand) staticOperand {
	if initial.proven && initial.isPropertyValue() {
		initial.accumulated = true
		return initial
	}
	if !initial.proven || initial.mayBeNull() || initial.typeName() == "" {
		return staticOperand{}
	}
	if !initial.union {
		return initial
	}
	return typedOperand(leastUpperBoundClosure(initial.typeName())).proved()
}
