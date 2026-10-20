package cypher

import "github.com/orneryd/nornicdb/internal/admittedcheck"

// checkOperand is the only general typed-checker entrance. The checker type is
// private to admittedcheck: only its constructor can initialize a usable
// callback. That callback receives this complete frame, including parameters.
func (scope staticTypeScope) checkOperand(expression string) (staticOperand, error) {
	return admittedcheck.New(scope, staticTypeScope.checkExpression).Check(expression)
}

type staticPostfixRequest struct {
	receiver, inner string
	subscript       bool
	source          []string
}

// Isolated property/subscript consumers use the same constructor contract.
func (scope staticTypeScope) checkPostfixOperand(receiver, inner string, subscript bool, source ...string) (staticOperand, error) {
	input := staticPostfixRequest{receiver: receiver, inner: inner, subscript: subscript, source: source}
	return admittedcheck.New(scope, staticTypeScope.checkPostfixRequest).Check(input)
}

func (scope staticTypeScope) checkPostfixRequest(input staticPostfixRequest) (staticOperand, error) {
	return scope.checkPostfix(input.receiver, input.inner, input.subscript, input.source...)
}

// admittedOperandFact preserves derived facts, including nested list elements,
// exclusions, alternatives and parameter identity. Literal-only approximation
// fills a missing fact; it never replaces one produced by the admitted frame.
func admittedOperandFact(expression string, admitted staticOperand) staticOperand {
	if admitted.typeName() != "" || admitted.nonBoolean || admitted.nonInteger || admitted.nonEntity {
		return admitted
	}
	return typedOperand(staticLiteralTypeName(expression))
}

// withTypes changes local bindings while retaining this invocation and source.
func (scope staticTypeScope) withTypes(types typeScope) staticTypeScope {
	local := types.static(scope.cypher25)
	local.params = scope.params
	local.complete = scope.complete
	local.native = scope.native
	local.noLocalScopes = scope.noLocalScopes
	return local
}

// bindingOperand reads all retained facts. No consumer reconstructs a binding
// from its display/category string and thereby drops exclusions or deferral.
func (scope staticTypeScope) bindingOperand(name string) (staticOperand, bool) {
	if operand, found := scope.derived[name]; found {
		return operand, true
	}
	if kind := scope.typeOf(name); kind != "" {
		operand := typedOperand(kind)
		operand.members = scope.members[name]
		return operand, true
	}
	return staticOperand{}, scope.bound(name)
}
