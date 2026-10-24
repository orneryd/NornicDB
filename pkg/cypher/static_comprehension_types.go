package cypher

import (
	"fmt"
	"maps"
	"strings"
)

// checkScoped types text in a local scope (a list or pattern
// comprehension's, reduce's): the function signatures the statement's
// visitor leaves to the scope that binds their variables, then its operand.
func (checker staticOperatorChecker) checkScoped(text string) (staticOperand, error) {
	if err := validateStaticFunctionVariables(text, checker.scope); err != nil {
		return staticOperand{}, err
	}
	return checker.check(text)
}

// checkListComprehension validates the source before execution and checks the
// body with the iterator's element type, shadowing any outer binding.
func (checker staticOperatorChecker) checkListComprehension(variable, list, predicate, projection string) (staticOperand, error) {
	source, err := checker.check(list)
	if err != nil {
		return staticOperand{}, err
	}
	if source.excludes(isListTypeName) {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	types := typeScopeOf(checker.scope)
	types[variable] = source.elementOperand()
	nested := staticOperatorChecker{scope: types.static(checker.scope.cypher25), params: checker.params}
	nested.scope.complete = checker.scope.complete
	nested.scope.params = checker.scope.params
	var element staticOperand
	for index, expression := range []string{predicate, projection} {
		operand, err := nested.checkScoped(expression)
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
		// A filter is its source's list type, or a list.
		if composable(source) && source.list() {
			return source, nil
		}
		return provenOperand("List<T>"), nil
	}
	if name := element.typeName(); name != "" && composable(element) {
		return typedOperand(listOfType(name)).proved(), nil
	}
	return provenOperand("List<T>"), nil
}

// checkPatternComprehension types [pattern WHERE predicate | projection]:
// the pattern binds its variables, the predicate must be a boolean, and the
// result is a list of the projection's type.
func (checker staticOperatorChecker) checkPatternComprehension(pattern, projection string) (staticOperand, error) {
	predicate := ""
	if where := topLevelKeywordIndex(pattern, "WHERE"); where >= 0 {
		pattern, predicate = strings.TrimSpace(pattern[:where]), strings.TrimSpace(pattern[where+len("WHERE"):])
	}
	bindings := make(matchSemanticScope)
	addMatchPatternBindingKinds(bindings, pattern)
	nested := checker
	nested.scope.kinds = maps.Clone(checker.scope.kinds)
	if nested.scope.kinds == nil {
		nested.scope.kinds = make(matchSemanticScope)
	}
	for name, kind := range bindings {
		if _, bound := nested.scope.kinds[name]; bound && kind != matchBindingPath {
			continue // the pattern reads the outer node or relationship
		}
		// A new local, or a path the comprehension assigns ([p = (a)-->(b) |
		// length(p)]), which shadows an outer p of any type.
		nested.scope.kinds[name] = kind
		if _, shadowed := nested.scope.derived[name]; shadowed {
			nested.scope.derived = maps.Clone(nested.scope.derived)
			delete(nested.scope.derived, name)
		}
		if _, shadowed := nested.scope.values[name]; shadowed {
			nested.scope.values = maps.Clone(nested.scope.values)
			delete(nested.scope.values, name)
		}
		if _, shadowed := nested.scope.members[name]; shadowed {
			nested.scope.members = maps.Clone(nested.scope.members)
			delete(nested.scope.members, name)
		}
	}
	if predicate != "" {
		operand, err := nested.checkScoped(predicate)
		if err != nil {
			return staticOperand{}, err
		}
		if err := requireBooleanOperand(operand); err != nil {
			return staticOperand{}, err
		}
	}
	element, err := nested.checkScoped(projection)
	if err != nil {
		return staticOperand{}, err
	}
	if name := element.typeName(); name != "" && composable(element) {
		return typedOperand(listOfType(name)).proved(), nil
	}
	return provenOperand("List<T>"), nil
}

// checkReduceTypes binds the source element and initial accumulator once, then
// validates the step with the same local function-argument owner as comprehensions.
func (checker staticOperatorChecker) checkReduceTypes(form reduceForm) (staticOperand, error) {
	source, err := checker.check(form.list)
	if err != nil {
		return staticOperand{}, err
	}
	if source.excludes(isListTypeName) {
		return staticOperand{}, operandMismatch(source, "List<T>")
	}
	initial, err := checker.check(form.initial)
	if err != nil {
		return staticOperand{}, err
	}
	types := typeScopeOf(checker.scope)
	types[form.variable] = source.elementOperand()
	types[form.accumulator] = reduceAccumulatorOperand(initial)
	nested := staticOperatorChecker{scope: types.static(checker.scope.cypher25), params: checker.params}
	nested.scope.complete = checker.scope.complete
	nested.scope.params = checker.scope.params
	var step staticOperand
	for index, text := range []string{form.step, form.predicate} {
		if text == "" {
			continue
		}
		operand, err := nested.checkScoped(text)
		if err != nil {
			return staticOperand{}, err
		}
		if index == 0 {
			step = operand
		}
		if index == 1 {
			if err := requireBooleanOperand(operand); err != nil {
				return staticOperand{}, err
			}
		}
	}
	if form.all {
		return provenOperand("Boolean"), nil
	}
	// The step gives the accumulator's next value: Neo4j requires one of its
	// initial value's types (reduce(t = '', x IN [1] | x) is "Type mismatch:
	// accumulator is String but expression has type Integer", whatever reads
	// the result).
	if initial.typeName() != "" && step.typeName() != "" && composable(initial) && step.excludes(accumulatorAccepts(initial)) {
		return staticOperand{}, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType",
			fmt.Sprintf("Type mismatch: accumulator is %s but expression has type %s", initial.typeName(), step.typeName()))
	}
	// Neo4j types reduce as its accumulator (reduceAccumulatorOperand); the
	// step only has to fit it: reduce(a = 1, x IN [n.k] | a + x) is an
	// Integer, reduce(t = CASE … 1 … 'a' END, x IN l | t) "Any, Integer or
	// String", reduce(a = null, …) of no known type.
	return reduceAccumulatorOperand(initial), nil
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
