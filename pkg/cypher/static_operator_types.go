package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Compile-time operator types.
//
// Neo4j rejects an arithmetic operand whose static type no signature of the
// operator accepts, when it compiles the statement, with a SyntaxError "Type
// mismatch: expected X but was Y" ('a' + true, n.v - 'x', -[1], n + 1 for a
// node n, …), wherever the expression is. Which types an operand position
// accepts depends on the other operand's type when that is known (1 + x
// accepts Float, Integer, String or List<T>; true + x only List<T>). An
// operand whose type isn't known statically (a property, a function result)
// is never rejected here; if the data turns out wrong, the evaluator raises a
// TypeError (runtimeArithmeticTypeError).
//
// The same check covers the other operators whose operand types Neo4j fixes
// at compile time (#907): AND, OR, XOR and NOT take booleans; IN takes a list
// on its right; =~ takes strings; unary - and + take numbers (+ also a
// temporal value or a duration); a subscript takes a list with an integer
// index or a map, node or relationship with a string key; a slice takes a
// list; a property access takes a map, node, relationship, point, duration or
// temporal value; and a WHERE or CASE WHEN condition must be a boolean.
// Null passes everywhere; STARTS WITH, ENDS WITH and CONTAINS accept any
// types (they return null).

// staticFunctionResultTypes are the result types of functions whose result
// type doesn't depend on their arguments.
var staticFunctionResultTypes = map[string]string{
	"size": "Integer", "length": "Integer", "tointeger": "Integer", "char_length": "Integer",
	"character_length": "Integer", "timestamp": "Integer", "sign": "Integer",
	"tofloat": "Float", "sqrt": "Float", "exp": "Float", "log": "Float", "log10": "Float",
	"sin": "Float", "cos": "Float", "tan": "Float", "cot": "Float", "asin": "Float",
	"acos": "Float", "atan": "Float", "atan2": "Float", "degrees": "Float", "radians": "Float",
	"haversin": "Float", "rand": "Float", "pi": "Float", "e": "Float", "round": "Float",
	"ceil": "Float", "floor": "Float", "stdev": "Float", "stdevp": "Float", "abs": "Float or Integer",
	"tostring": "String", "toupper": "String", "tolower": "String", "upper": "String",
	"lower": "String", "trim": "String", "ltrim": "String", "rtrim": "String",
	"btrim": "String", "replace": "String", "substring": "String", "left": "String",
	"right": "String", "type": "String", "elementid": "String",
	"toboolean": "Boolean", "allreduce": "Boolean", "property_exists": "Boolean",
	"range": "List<Integer>", "keys": "List<String>", "labels": "List<String>", "split": "List<String>",
	"date": "Date", "datetime": "DateTime", "localdatetime": "LocalDateTime",
	"time": "Time", "localtime": "LocalTime", "duration": "Duration",
	"point": "Point", "properties": "Map", "count": "Integer",
	"nodes": "List<Node>", "relationships": "List<Relationship>", "startnode": "Node", "endnode": "Node",
	// The temporal namespaces (Neo4j 5.26, #907).
	"duration.between": "Duration", "duration.inmonths": "Duration", "duration.indays": "Duration",
	"duration.inseconds": "Duration",
	"date.truncate":      "Date", "date.realtime": "Date", "date.statement": "Date", "date.transaction": "Date",
	"datetime.truncate": "DateTime", "datetime.realtime": "DateTime", "datetime.statement": "DateTime",
	"datetime.transaction": "DateTime", "datetime.fromepoch": "DateTime", "datetime.fromepochmillis": "DateTime",
	"localdatetime.truncate": "LocalDateTime", "localdatetime.realtime": "LocalDateTime",
	"localdatetime.statement": "LocalDateTime", "localdatetime.transaction": "LocalDateTime",
	"time.truncate": "Time", "time.realtime": "Time", "time.statement": "Time", "time.transaction": "Time",
	"localtime.truncate": "LocalTime", "localtime.realtime": "LocalTime", "localtime.statement": "LocalTime",
	"localtime.transaction": "LocalTime",
	// VECTOR and UUID (Cypher 25, #907), under their internal names too
	// (vector_call_rewrite.go).
	"vector": "Vector", "__nornic_vector": "Vector", "vector_distance": "Float", "__nornic_vector_distance": "Float",
	"vector_norm": "Float", "__nornic_vector_norm": "Float", "vector_dimension_count": "Integer",
	"uuid": "UUID", "uuid.mostsignificantbits": "Integer", "uuid.leastsignificantbits": "Integer", "uuid.v5": "UUID",
}

// staticFunctionResultType is the result type of a call to function with
// arguments, when it doesn't depend on the arguments: the function's own
// (staticFunctionResultTypes). reduce's type depends on its initial value
// and its step, so only the deriver types it (checkReduceTypes): a caller
// that reads a call's result here reads reduce through the deriver instead.
func staticFunctionResultType(function, arguments string) string {
	result, _ := lookupLowerASCII(staticFunctionResultTypes, function)
	return result
}

// staticOperatorChecker infers expression types for the operator checks, from
// a clause's variables and, when checking with parameter values, from them.
type staticOperatorChecker struct {
	scope  staticTypeScope
	params map[string]interface{}
	// checked holds the results of one local scope's checks by text
	// (checkScoped): its function-argument check and its operator check
	// type the same arguments, and each nested scope is typed once.
	checked map[string]staticCheckResult
}

// staticCheckResult is one check's result.
type staticCheckResult struct {
	operand staticOperand
	err     error
}

// operandMismatch is the SyntaxError for an operand the operator can't take.
func operandMismatch(operand staticOperand, expected string) error {
	if operand.parameter != "" {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidArgumentType",
			fmt.Sprintf("Type mismatch for parameter '%s': expected %s but was %s", operand.parameter, expected, operand.display),
		)
	}
	return typeNameMismatchError(expected, operand.display)
}

// requireBooleanOperand rejects an operand in a boolean position (AND, OR,
// XOR, NOT, WHERE, CASE WHEN) whose static type isn't Boolean: a list with
// Neo4j's coercion error, any other known type with a type mismatch. Null
// and unknown types pass.
func requireBooleanOperand(operand staticOperand) error {
	switch {
	case operand.list():
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreListCoercionToBoolean())
	case operand.excludes(isBooleanOrListTypeName):
		// A union with a list alternative passes the type check; only a
		// value that is a list on every alternative is the coercion error.
		return operandMismatch(operand, "Boolean")
	}
	return nil
}

// checkOperator applies Neo4j's operand rules for left op right and returns
// the result type. cypher25 applies Neo4j 2026.09's rules for a Cypher 25
// statement: a string joins any value but a map or a graph entity ('a' +
// true, date('2020-01-02') + 's').
func checkOperator(op byte, left, right staticOperand, cypher25 bool) (staticOperand, error) {
	result, err := checkOperatorAlternatives(op, left, right, cypher25)
	if err != nil {
		return staticOperand{}, err
	}
	if op == '+' && !cypher25 && !left.list() && !right.list() && (temporalListUnion(left) || temporalListUnion(right)) {
		// Neo4j 5.26 adds a temporal or duration alternative of a union
		// with a list to a value that isn't a list as a value of no common
		// type, Any: NOT (CASE … duration(…) … [1] END + 1) is admitted,
		// its .day is not (+ [1] stays a list).
		return joinedOperand(result.proved(), provenOperand("Any")), nil
	}
	// The operands are proven or unknown (check's gate), and a null, Any or
	// property-value alternative is read as unknown: the result follows
	// the operator's signatures from proven facts only (n.k ^ 2 is a Float;
	// a null alternative's result joins every other one as unknown).
	return result.proved(), nil
}

// temporalListUnion reports a union of only temporal values or durations
// and a list (CASE … duration(…) … [1] END; withoutTemporalListElements
// leaves the list as List<T>). An operator signature's union (n.k + n.j
// may also be a number or a string) is typed as any other.
func temporalListUnion(operand staticOperand) bool {
	if !operand.union {
		return false
	}
	temporal, list := false, false
	for _, choice := range staticTypeChoices(operand.typeName()) {
		switch {
		case choice == "List<T>":
			list = true
		case choice == "Duration" || isTemporalTypeName(choice):
			temporal = true
		default:
			return false
		}
	}
	return temporal && list
}

// checkOperatorAlternatives applies the operand rules to each combination
// of the operands' alternatives; a null alternative is admitted as an
// operand of unknown type (null - 2 is null), an Any alternative matches no
// signature (head([1] + ['a']) - 1 is Neo4j's "expected Float, Integer, …
// but was Any").
func checkOperatorAlternatives(op byte, left, right staticOperand, cypher25 bool) (staticOperand, error) {
	if !left.union && !right.union {
		return checkSingleOperator(op, admittedAlternative(left), admittedAlternative(right), cypher25)
	}
	// A union operand is checked alternative by alternative: the expression
	// is accepted when one combination is, with the join of their results.
	var result staticOperand
	var first error
	accepted := false
	for _, leftAlternative := range left.alternatives() {
		for _, rightAlternative := range right.alternatives() {
			operand, err := checkSingleOperator(op, admittedAlternative(leftAlternative), admittedAlternative(rightAlternative), cypher25)
			// Each combination's result follows its signature from proven
			// facts (check's gate), so its display joins the others'.
			operand = operand.proved()
			switch {
			case err != nil:
				if first == nil {
					first = err
				}
			case !accepted:
				result, accepted = operand, true
			default:
				result = joinedOperand(result, operand)
			}
		}
	}
	if !accepted {
		return staticOperand{}, first
	}
	return result, nil
}

// admittedAlternative is the operand an operator reads for one alternative:
// null is admitted as unknown.
func admittedAlternative(operand staticOperand) staticOperand {
	if operand.is("Null") {
		return staticOperand{}
	}
	return operand
}

// checkSingleOperator is checkOperator for operands of one type each: -, *,
// /, % and ^ by their signatures (signatureOperator), + by its own rules.
func checkSingleOperator(op byte, left, right staticOperand, cypher25 bool) (staticOperand, error) {
	if op != '+' {
		return signatureOperator(op, left, right)
	}
	if cypher25 && (left.is("String") && right.joinsStringStatically() || right.is("String") && left.joinsStringStatically()) {
		return knownOperand("String"), nil
	}
	switch {
	case left.is("Vector"):
		// A vector (Cypher 25) joins a string or a list, as Neo4j
		// 2026.09 types it.
		switch {
		case right.is("String"):
			return knownOperand("String"), nil
		case right.list():
			return knownOperand("List<T>"), nil
		case right.excludes(noStaticType):
			return staticOperand{}, operandMismatch(right, "String or List<T>")
		}
		return staticOperand{}, nil
	case left.is("String") && right.is("Vector"):
		return knownOperand("String"), nil
	case left.is("String") && right.is("UUID"):
		// UUID is a Cypher 25 type: Neo4j 2026.09's list of what a
		// string joins.
		return staticOperand{}, operandMismatch(right, "Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime, DateTime, Vector or List<T>")
	case left.temporal():
		if right.list() {
			return concatenatedOperand(left, right), nil
		}
		if right.excludes(isDurationOrNumberTypeName) {
			return staticOperand{}, operandMismatch(right, "Duration or List<T>")
		}
		return left, nil
	case left.duration():
		if right.temporal() || right.duration() {
			return right, nil
		}
		if right.list() {
			return concatenatedOperand(left, right), nil
		}
		if right.excludes(noStaticType) {
			return staticOperand{}, operandMismatch(right, "Duration, Date, Time, LocalTime, LocalDateTime, DateTime or List<T>")
		}
	case left.numeric() || left.is("String"):
		if right.excludes(isAddableToNumberOrString) {
			return staticOperand{}, operandMismatch(right, "Float, Integer, String or List<T>")
		}
	case left.known() && !left.list():
		if right.known() && !right.list() {
			return staticOperand{}, operandMismatch(right, "List<T>")
		}
	}
	switch {
	case left.numeric() && right.numeric():
		return numericOperatorResult(left, right), nil
	case (left.is("String") && (right.numeric() || right.is("String"))) || (right.is("String") && left.numeric()):
		return knownOperand("String"), nil
	case left.list() || right.list():
		return concatenatedOperand(left, right), nil
	}
	// One operand's type isn't known: the result is every type Neo4j's
	// addition gives a value of the other's type and any value, the list
	// a concatenation makes included (a predicate admits it, so WHERE
	// n.k + 'a' is the row's).
	return signatureOperand(additionResultTypes(left, right)), nil
}

// staticAdditionWithUnknown is Neo4j 5.26's result type of left + right
// when one side's type isn't known, by the other side's type, as Neo4j names
// it (n.k + 1 is "Float, Integer, String or List<Float>", whose head is a
// Float).
var staticAdditionWithUnknown = map[string][]string{
	"Integer":       {"Float", "Integer", "String", "List<Float>"},
	"Float":         {"Float", "String", "List<Any>", "List<Float>", "List<Number>"},
	"String":        {"String", "List<Any>", "List<String>"},
	"Boolean":       {"List<Any>", "List<Boolean>"},
	"Map":           {"List<Any>"},
	"List<Integer>": {"List<Any>", "List<Float>", "List<Integer>", "List<Number>"},
	"List<Float>":   {"List<Any>", "List<Float>", "List<Number>"},
	"List<String>":  {"List<Any>", "List<String>"},
}

// additionResultTypes are the result types of left + right when one of them
// isn't of a known type: Neo4j's for a scalar or a list of the other's type
// (staticAdditionWithUnknown); otherwise a string joins a string or a
// number, a duration adds to a duration or a temporal value, and either side
// can be a list.
func additionResultTypes(left, right staticOperand) []string {
	known := left
	if !left.known() {
		known = right
	}
	if types, listed := staticAdditionWithUnknown[known.typeName()]; listed && known.known() && !known.emptyList {
		return types
	}
	switch {
	case !known.known():
		return []string{"Float", "Integer", "String", "Duration", "Date", "Time", "LocalTime", "LocalDateTime", "DateTime", "List<T>"}
	case known.numeric():
		return []string{"Float", "Integer", "String", "List<T>"}
	case known.duration():
		return []string{"Duration", "Date", "Time", "LocalTime", "LocalDateTime", "DateTime", "List<T>"}
	case known.temporal():
		return []string{known.typeName(), "List<T>"}
	}
	return []string{"List<T>"}
}

// staticSignature is one signature of an operator: the types of its
// operands and of its result (Integer - Float is a Float).
type staticSignature struct{ left, right, result string }

// staticOperatorSignatures are Neo4j 5.26's signatures of -, *, /, % and ^
// (^ takes Floats, an Integer coerced to one).
var staticOperatorSignatures = func() map[byte][]staticSignature {
	numeric := []staticSignature{{"Integer", "Integer", "Integer"}, {"Integer", "Float", "Float"}, {"Float", "Integer", "Float"}, {"Float", "Float", "Float"}}
	subtract := append(append([]staticSignature{}, numeric...), staticSignature{"Duration", "Duration", "Duration"})
	for _, temporal := range []string{"Date", "Time", "LocalTime", "LocalDateTime", "DateTime"} {
		subtract = append(subtract, staticSignature{temporal, "Duration", temporal})
	}
	multiply := append(append([]staticSignature{}, numeric...),
		staticSignature{"Duration", "Float", "Duration"}, staticSignature{"Duration", "Integer", "Duration"},
		staticSignature{"Float", "Duration", "Duration"}, staticSignature{"Integer", "Duration", "Duration"})
	divide := append(append([]staticSignature{}, numeric...),
		staticSignature{"Duration", "Float", "Duration"}, staticSignature{"Duration", "Integer", "Duration"})
	return map[byte][]staticSignature{'-': subtract, '*': multiply, '/': divide, '%': numeric, '^': {{"Float", "Float", "Float"}}}
}()

// signatureOperator checks left op right against op's signatures as Neo4j
// does, operand by operand: the signatures whose left type the left operand
// may have remain, the right operand must then have one of their right
// types, and the result is every result type of the signatures both take
// (n.k - 2 is a Float or an Integer, n.k * 1.5 a Float or a Duration). An
// operand of no known type takes every signature; a Number is an Integer or
// a Float. A mismatch names the types the position took.
func signatureOperator(op byte, left, right staticOperand) (staticOperand, error) {
	if left.numeric() && right.numeric() && op != '^' {
		// Two numbers: a Float with a Float, an Integer of two Integers, a
		// Number otherwise (v * 10 for v of [1, 2.5] is a Number, which a
		// temporal value's + admits).
		return numericOperatorResult(left, right), nil
	}
	takes := func(operand staticOperand, typeName string) bool {
		return operand.fits(func(choice string) bool {
			return choice == typeName || choice == "Number" && (typeName == "Integer" || typeName == "Float") ||
				op == '^' && choice == "Integer" && typeName == "Float"
		})
	}
	var expected, results []string
	var remaining []staticSignature
	for _, signature := range staticOperatorSignatures[op] {
		expected = appendTypeChoice(expected, signature.left)
		if takes(left, signature.left) {
			remaining = append(remaining, signature)
		}
	}
	if len(remaining) == 0 {
		return staticOperand{}, operandMismatch(left, joinTypeNames(canonicalTypeOrder(expected)))
	}
	expected = nil
	for _, signature := range remaining {
		if takes(right, signature.right) {
			results = appendTypeChoice(results, signature.result)
		}
	}
	// A mismatch names the right types of the signatures that remain; for
	// * Neo4j names every right type its signatures take (duration(…) * 'a'
	// is "expected Float, Integer or Duration").
	names := remaining
	if op == '*' {
		names = staticOperatorSignatures[op]
	}
	for _, signature := range names {
		expected = appendTypeChoice(expected, signature.right)
	}
	if len(results) == 0 {
		return staticOperand{}, operandMismatch(right, joinTypeNames(canonicalTypeOrder(expected)))
	}
	return signatureOperand(results), nil
}

// staticComparisonOperators split a predicate into the operands whose
// arithmetic is checked; the comparison itself accepts any types.
var staticComparisonOperators = []string{"<>", "<=", ">=", "=~", "=", "<", ">"}

var staticComparisonKeywords = []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "IN", "IS NOT NULL", "IS NULL"}

// check returns the static type of expression and the first operator type
// error in it. Its result is proven or unknown (gateProven): a type only a
// verified rule produced can reach a consumer.
func (checker staticOperatorChecker) check(expression string) (staticOperand, error) {
	if checker.checked == nil {
		operand, err := checker.derive(expression)
		return gateProven(operand), err
	}
	expression = strings.TrimSpace(expression)
	if result, done := checker.checked[expression]; done {
		return result.operand, result.err
	}
	operand, err := checker.derive(expression)
	operand = gateProven(operand)
	checker.checked[expression] = staticCheckResult{operand: operand, err: err}
	return operand, err
}

// derive is check before its gate. CASE, list comprehensions, quantifiers,
// pattern expressions and subqueries are not looked into: they bind their
// own variables.
func (checker staticOperatorChecker) derive(expression string) (staticOperand, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return staticOperand{}, nil
	}
	if receiver, projected := staticMapProjectionReceiver(expression); projected && receiver != "" {
		if _, _, projection := mapProjectionSplit(expression, mapProjectionStatic, nil); !projection {
			return staticOperand{}, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
				localization.CypherCoreMapProjectionReceiver(receiver))
		}
	}
	if _, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		return checker.checkAtom(expression)
	}
	if expression[0] == '{' && cyphertext.FindMatchingDelimiter(expression, 0, '{', '}', cyphertext.OpeningDelimiter) == len(expression)-1 {
		return checker.checkAtom(expression)
	}
	if _, _, projection := mapProjectionSplit(expression, mapProjectionStatic, &checker.scope); projection {
		return checker.checkAtom(expression)
	}
	// A map projection is an operand: an expression around one (s{.a} IS
	// NULL, toString(s{.a})) is typed like any other.
	if !expressionTextAccepted(checker.maskMapProjections(expression), expressionTextOperator) {
		// A function call or subquery expression over a pattern still has
		// its result type (size([(n)-->() | 1]) is an Integer).
		return staticPatternExpressionType(expression), nil
	}
	if inner, enclosed := stripEnclosingExpressionParentheses(expression); enclosed {
		return checker.check(inner)
	}
	if startsWithKeywordFold(expression, "CASE") {
		end := leadingCaseExpressionEnd(expression)
		if isCaseExpression(expression) && end == len(expression) {
			if parsed, err := parseCaseExpression(expression); err == nil {
				return checker.checkCase(parsed)
			}
			return staticOperand{}, nil
		}
		if end <= 0 || end >= len(expression) {
			return staticOperand{}, nil
		}
		// A CASE is an operand of the operators after it (CASE … END + 1).
	}
	// The expression's own operators are found outside its CASE blocks
	// ([x] + CASE WHEN a OR b = c THEN [] END is a list, not a Boolean).
	scan := maskCaseBlocks(expression)
	for _, keyword := range []string{"OR", "XOR", "AND"} {
		if index := infixKeywordIndex(scan, keyword); index > 0 {
			for _, side := range [2]string{expression[:index], expression[keywordEndAt(expression, index, keyword):]} {
				operand, err := checker.check(side)
				if err != nil {
					return staticOperand{}, err
				}
				if err := requireBooleanOperand(operand); err != nil {
					return staticOperand{}, err
				}
			}
			return provenOperand("Boolean"), nil
		}
	}
	// A bare NOT is a variable named not (WITH [1] AS not ... x IN not).
	if startsWithKeywordFold(expression, "NOT") && strings.TrimSpace(expression[len("NOT"):]) != "" {
		operand, err := checker.check(expression[len("NOT"):])
		if err != nil {
			return staticOperand{}, err
		}
		if err := requireBooleanOperand(operand); err != nil {
			return staticOperand{}, err
		}
		return provenOperand("Boolean"), nil
	}
	// A type or normalization predicate (x IS :: LIST<INTEGER>, x IS NOT
	// TYPED STRING, x IS NFC NORMALIZED) is a Boolean; its type text isn't
	// an expression.
	if index := infixKeywordIndex(scan, "IS"); index > 0 {
		rest := strings.TrimSpace(expression[index+len("IS"):])
		if !startsWithKeywordFold(rest, "NULL") && !startsWithKeywordFold(strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(rest), "NOT")), "NULL") {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			return provenOperand("Boolean"), nil
		}
	}
	for _, keyword := range staticComparisonKeywords {
		if index := infixKeywordIndex(scan, keyword); index > 0 {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			right, err := checker.check(expression[keywordEndAt(expression, index, keyword):])
			if err != nil {
				return staticOperand{}, err
			}
			if keyword == "IN" && right.excludes(isListTypeName) {
				return staticOperand{}, operandMismatch(right, "List<T>")
			}
			return provenOperand("Boolean"), nil
		}
	}
	for _, operator := range staticComparisonOperators {
		if left, right, found := splitAtOperator(expression, operator, findTopLevelOperator(scan, operator, false, true)); found && left != "" && right != "" {
			for _, side := range [2]string{left, right} {
				operand, err := checker.check(side)
				if err != nil {
					return staticOperand{}, err
				}
				if operator == "=~" && operand.excludes(isStringTypeName) {
					return staticOperand{}, operandMismatch(operand, "String")
				}
			}
			return provenOperand("Boolean"), nil
		}
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		left, right, operator, arithmetic := splitRowArithmeticTier(scan, tier)
		if !arithmetic {
			continue
		}
		// CASE bodies are atomic to the enclosing expression. Use the masked
		// boundary, then derive operands from their original source.
		position := len(left) + strings.IndexByte(scan[len(left):], operator)
		left, right = strings.TrimSpace(expression[:position]), strings.TrimSpace(expression[position+1:])
		if !staticArithmeticOperandText(left) || !staticArithmeticOperandText(right) {
			return staticOperand{}, nil
		}
		leftType, err := checker.check(left)
		if err != nil {
			return staticOperand{}, err
		}
		rightType, err := checker.check(right)
		if err != nil {
			return staticOperand{}, err
		}
		return checkOperator(operator, leftType, rightType, checker.scope.cypher25)
	}
	if (expression[0] == '-' || expression[0] == '+') && len(expression) > 1 {
		operand, err := checker.check(expression[1:])
		if err != nil {
			return staticOperand{}, err
		}
		// Unary minus takes a number. Neo4j 5.26's plus also takes a
		// temporal value or a duration; Neo4j 2026.09, which runs Cypher
		// 25, only a number.
		accepts := isNumericTypeName
		if expression[0] == '+' && !checker.scope.cypher25 {
			accepts = func(typeName string) bool {
				return isNumericTypeName(typeName) || isTemporalTypeName(typeName) || typeName == "Duration"
			}
		}
		if operand.excludes(accepts) {
			return staticOperand{}, operandMismatch(operand, "Float or Integer")
		}
		return unaryOperand(operand), nil
	}
	return checker.checkAtom(expression)
}

// checkCase type-checks a CASE expression: a searched CASE's conditions must
// be booleans. Its result retains every possible branch type, so a function
// signature is rejected only when none of the alternatives fits.
func (checker staticOperatorChecker) checkCase(parsed *caseExpression) (staticOperand, error) {
	if parsed.isSimple {
		if _, err := checker.check(parsed.testExpression); err != nil {
			return staticOperand{}, err
		}
	}
	var result staticOperand
	var choices []string
	unknown := false
	// single is the one result of a CASE with no ELSE (CASE WHEN c THEN v
	// END): the CASE is of its type, as written. A null branch, written or
	// an ELSE, makes the CASE nullable as Neo4j types it (head(CASE … null
	// … [1] END) is any type).
	var single staticOperand
	branches := 0
	merge := func(text string) error {
		operand, err := checker.check(text)
		if err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(text), "null") {
			operand = provenOperand("Null")
		} else {
			single, branches = operand, branches+1
			// An Any alternative is a type of its own, as Neo4j unions
			// it (CASE WHEN c THEN v ELSE 1 END for v of Any, Integer or
			// String is Any, Integer or String).
			if !operand.joinable() {
				// A branch whose alternatives compose differently (null,
				// Any, a property value) leaves a CASE of several
				// branches untyped.
				unknown = true
				return nil
			}
		}
		name := operand.typeName()
		if name == "" {
			unknown = true
			return nil
		}
		if result.typeName() == "" {
			result = operand
			return nil
		}
		if name == result.typeName() {
			return nil
		}
		if choices == nil {
			choices = staticTypeChoices(result.typeName())
		}
		for _, choice := range staticTypeChoices(name) {
			if !containsString(choices, choice) {
				choices = append(choices, choice)
			}
		}
		return nil
	}
	for _, when := range parsed.whenClauses {
		if parsed.isSimple {
			if _, err := checker.check(when.value); err != nil {
				return staticOperand{}, err
			}
		} else {
			condition, err := checker.check(when.condition)
			if err != nil {
				return staticOperand{}, err
			}
			if err := requireBooleanOperand(condition); err != nil {
				return staticOperand{}, err
			}
		}
		if err := merge(when.result); err != nil {
			return staticOperand{}, err
		}
	}
	if strings.TrimSpace(parsed.elseResult) != "" {
		if err := merge(parsed.elseResult); err != nil {
			return staticOperand{}, err
		}
	}
	if branches == 1 && len(parsed.whenClauses) == 1 && strings.TrimSpace(parsed.elseResult) == "" && single.typeName() != "" {
		return single, nil
	}
	if unknown {
		return staticOperand{}, nil
	}
	if len(choices) > 1 {
		if choices = withoutTemporalListElements(choices); len(choices) == 1 {
			return provenOperand(choices[0]), nil
		}
		return typedOperand(joinTypeNames(choices)).proved(), nil
	}
	return result, nil
}

// maskCaseBlocks blanks, keeping byte offsets, the inside of every CASE … END
// block in expression (its CASE and END stay), so an operator inside one isn't
// taken for one of the expression's own. Quoted text and a property named
// case (n.case) are not blocks.
// infixKeywordIndex is the first top-level occurrence of an infix keyword
// (AND, OR, XOR) that follows a complete operand: a word of its name where
// an operand is expected is a variable (or > 1 OR or < 3).
func infixKeywordIndex(s, keyword string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for from := 0; from < len(s); {
		index := keywordIndexFrom(s, keyword, from, opts)
		if index < 0 {
			return -1
		}
		if functionNameStartsAt(s, index) && !caseOperandExpectedBefore(s, index) {
			return index
		}
		from = index + len(keyword)
	}
	return -1
}

func maskCaseBlocks(expression string) string {
	if !mayContainCaseKeyword(expression) {
		return expression
	}
	var masked []byte
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
			continue
		case 'C', 'c':
		default:
			continue
		}
		if !matchKeywordAt(expression, index, "CASE") || (index > 0 && (cyphertext.NamePartAt(expression, index-1) || expression[index-1] == '.')) {
			continue
		}
		end := leadingCaseExpressionEnd(expression[index:])
		if end <= len("CASE END") {
			continue
		}
		if masked == nil {
			masked = []byte(expression)
		}
		for position := index + len("CASE"); position < index+end-len("END"); position++ {
			masked[position] = ' '
		}
		index += end - 1
	}
	if masked == nil {
		return expression
	}
	return string(masked)
}

// staticPostfixSplit splits the trailing subscript or property access off an
// operand: the receiver and either the bracket contents (subscript) or the
// property key. ok is false when the operand doesn't end in one at its top
// level. A '.' between digits belongs to a number.
func staticPostfixSplit(expression string) (receiver, inner string, subscript, ok bool) {
	last := -1
	depth := 0
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
		case '/':
			if end := queryCommentEnd(expression, index); end >= 0 {
				index = end - 1
			}
		case '(', '{':
			depth++
		case ')', '}':
			depth--
		case '[':
			if depth == 0 && index > 0 {
				last, subscript = index, true
			}
			depth++
		case ']':
			depth--
		case '.':
			if depth == 0 && index > 0 && index+1 < len(expression) && cyphertext.NameStartAt(expression, index+1) {
				last, subscript = index, false
			}
		}
	}
	if last <= 0 {
		return "", "", false, false
	}
	// expression is trimmed and last > 0, so the receiver isn't empty.
	receiver = strings.TrimSpace(expression[:last])
	if subscript {
		if cyphertext.FindMatchingDelimiter(expression, last, '[', ']', cyphertext.OpeningDelimiter) != len(expression)-1 {
			return "", "", false, false
		}
		return receiver, expression[last+1 : len(expression)-1], true, true
	}
	key := expression[last+1:]
	if simpleSemanticIdentifier(key) == "" {
		return "", "", false, false
	}
	return receiver, key, false, true
}

// staticSliceBounds splits a subscript's contents at a top-level "..": a
// ".." nested in brackets or parentheses (m[head(l[0..1])]) or quoted
// (r['a..b']) belongs to the key. It is the one reader of slice bounds, for
// the static checks and every evaluator (evaluateRowValue,
// validateRowSubscriptTypes, the context evaluator, applyArraySuffix).
func staticSliceBounds(inner string) (from, to string, slice bool) {
	if separator := findTopLevelOperator(inner, "..", false, true); separator >= 0 {
		return inner[:separator], inner[separator+2:], true
	}
	return "", "", false
}

// checkPostfix type-checks a subscript (receiver[inner]), a slice
// (receiver[from..to]) or a property access (receiver.key).
func (checker staticOperatorChecker) checkPostfix(receiverText, inner string, subscript bool) (staticOperand, error) {
	receiver, err := checker.check(receiverText)
	if err != nil {
		return staticOperand{}, err
	}
	if !subscript {
		if receiver.excludes(acceptsPropertyAccess) {
			return staticOperand{}, operandMismatch(receiver, "Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime")
		}
		if member, found := receiver.staticMapMember(symbolicNameValue(strings.TrimSpace(inner))); found {
			return member, nil
		}
		if receiver.is("Node") || receiver.is("Relationship") {
			// A node's or relationship's property may hold any property type.
			return typedOperand(staticPropertyValueType).proved(), nil
		}
		return staticOperand{}, nil
	}
	if from, to, slice := staticSliceBounds(inner); slice {
		for _, bound := range [2]string{from, to} {
			operand, err := checker.check(bound)
			if err != nil {
				return staticOperand{}, err
			}
			if operand.excludes(isIntegerOrNumberTypeName) {
				return staticOperand{}, operandMismatch(operand, "Integer")
			}
		}
		if receiver.excludes(isListTypeName) {
			return staticOperand{}, operandMismatch(receiver, "List<T>")
		}
		if list := receiver.listPart(); list.typeName() != "" {
			return list, nil
		}
		// A slice is a list at compile time, whatever its receiver
		// (WHERE n.x[1..] is a list coercion error in Neo4j).
		return provenOperand("List<T>"), nil
	}
	key, err := checker.check(inner)
	if err != nil {
		return staticOperand{}, err
	}
	keyKnown := key.known() && !key.is("Null")
	invalidIntegerKey := key.excludes(isIntegerOrNumberTypeName)
	invalidStringKey := key.excludes(isStringTypeName)
	const syntaxError, detail = "Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType"
	if receiver.union && !receiver.list() && !receiver.is("Map") {
		// A union of different kinds takes the key when one of its
		// alternatives does: reject one no alternative can be subscripted
		// as. A union of lists or maps is subscripted as that kind below.
		if receiver.excludes(func(typeName string) bool {
			return typeName == "Any" || isListTypeName(typeName) && !invalidIntegerKey || acceptsStringKey(typeName, checker.scope.cypher25) && !invalidStringKey
		}) {
			return staticOperand{}, operandMismatch(receiver, "List<T>, Map, Node or Relationship")
		}
		if receiver.hasAnyAlternative() {
			// Any may be subscripted; the row decides ((reduce(a = CASE …
			// [1] … true END, x IN l | a))[0] is any type to Neo4j).
			return staticOperand{}, nil
		}
		if element := receiver.elementOperand(); element.typeName() != "" && !invalidIntegerKey && !key.is("String") {
			return element, nil
		}
		return staticOperand{}, nil
	}
	switch {
	case receiver.is("Any"):
		// Any may be subscripted; the row decides.
	case !receiver.known():
		// A node's or relationship's property can't be a map, so it takes
		// only a list index (#882).
		if base, _, property := rowPropertyChainShape(receiverText); property && invalidIntegerKey {
			if kind := checker.scope.typeOf(base); kind == "Node" || kind == "Relationship" {
				return staticOperand{}, operandMismatch(key, "Integer")
			}
		}
	case receiver.is("Null"):
	case receiver.list():
		if invalidIntegerKey {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreListIndexTypeMismatch(key.display))
		}
		return receiver.elementOperand(), nil
	case receiver.is("Map") || receiver.is("Map, Node or Relationship"):
		if invalidStringKey {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreMapKeyTypeMismatch(key.display))
		}
		// A string literal key reads a known member (m['a']); a key from
		// a variable is read at run time, as in Neo4j 2026.09.
		if text, literal := cyphertext.DecodeString(strings.TrimSpace(inner), cyphertext.AcceptedSource); literal {
			if member, found := receiver.staticMapMember(text); found {
				return member, nil
			}
		}
	case receiver.is("Node") || receiver.is("Relationship"):
		if invalidStringKey {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreEntityPropertyKeyTypeMismatch(key.display))
		}
	case key.is("String"):
		// In Neo4j 5.26 a temporal value or duration takes a string key at
		// compile time (reading a field it doesn't have is a runtime error);
		// Neo4j 2026.09, which runs Cypher 25, takes none.
		if !receiver.temporal() && !receiver.duration() || checker.scope.cypher25 {
			return staticOperand{}, operandMismatch(receiver, "Map, Node or Relationship")
		}
	case key.is("Integer"):
		return staticOperand{}, operandMismatch(receiver, "List<T>")
	case !keyKnown && (!receiver.temporal() && !receiver.duration() || checker.scope.cypher25):
		return staticOperand{}, operandMismatch(receiver, "List<T>, Map, Node or Relationship")
	}
	return staticOperand{}, nil
}

// staticArithmeticOperandText reports whether text is a whole operand of
// arithmetic (expressionTextOperand policy), a subscript or slice of one
// ([1, 2.5][0] + 'a' is a String), a whole CASE or a COUNT { } / COLLECT { } /
// EXISTS { } expression: an operator split at a pattern's arrow is none.
func staticArithmeticOperandText(text string) bool {
	text = strings.TrimSpace(text)
	if expressionTextAccepted(text, expressionTextOperand) {
		return true
	}
	if receiver, _, _, postfix := staticPostfixSplit(text); postfix {
		return staticArithmeticOperandText(receiver)
	}
	if _, subquery := staticSubqueryExpressionOperand(text); subquery {
		return true
	}
	return isCaseExpression(text) && leadingCaseExpressionEnd(text) == len(text)
}

// staticSubqueryExpressionTypes are the result types of COUNT { }, COLLECT { }
// and EXISTS { }.
var staticSubqueryExpressionTypes = map[string]string{"COUNT": "Integer", "COLLECT": "List<T>", "EXISTS": "Boolean"}

// staticPatternExpressionType is the result type of an expression the
// operator check doesn't read (it holds a pattern): a whole function call's
// or subquery expression's, or unknown.
func staticPatternExpressionType(expression string) staticOperand {
	var callFacts1 functionCallFacts
	if call := inspectFunctionCall(expression, qualifiedFunctionCall, "", &callFacts1); call && callFacts1.name != "" {
		function, arguments := callFacts1.name, callFacts1.inner

		return provenOperand(staticFunctionResultType(function, arguments))
	}
	operand, _ := staticSubqueryExpressionOperand(expression)
	return operand
}

// staticSubqueryExpressionOperand is the result type of a whole COUNT { },
// COLLECT { } or EXISTS { } expression (staticSubqueryExpressionTypes);
// subquery is false for any other expression. Neo4j types COLLECT { … } as
// List<T> whatever its body returns (toUpper(head(COLLECT { RETURN 1 AS x }))
// is admitted).
func staticSubqueryExpressionOperand(expression string) (operand staticOperand, subquery bool) {
	name, ok := cyphertext.ScanIdentifier(expression, 0)
	if !ok {
		return staticOperand{}, false
	}
	typeName, known := staticSubqueryExpressionTypes[upperASCII(name.Value)]
	if !known {
		return staticOperand{}, false
	}
	open := name.End
	for open < len(expression) && cyphertext.IsWhitespaceAt(expression, open) {
		open++
	}
	if open < len(expression) && expression[open] == '{' && isSubqueryBrace(expression, open) && cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter) == len(expression)-1 {
		return provenOperand(typeName), true
	}
	return staticOperand{}, false
}

// staticMapProjectionReceiver returns what precedes the brace of an
// expression that ends in a braced item list (receiver{…}); projected is false
// when it doesn't. Neo4j projects only a variable: any other receiver (a
// literal, a call, n.prop, a map literal) is a SyntaxError.
func staticMapProjectionReceiver(expression string) (receiver string, projected bool) {
	if !strings.HasSuffix(expression, "}") {
		return "", false
	}
	for open := 0; open < len(expression); open++ {
		switch cypherStringLexicalByte(expression[open]) {
		case cypherStringQuoteClass:
			open = skipQuotedSemanticText(expression, open) - 1
		case '(', '[':
			opening, closingDelimiter := '(', ')'
			if expression[open] == '[' {
				opening, closingDelimiter = '[', ']'
			}
			closing := cyphertext.FindMatchingDelimiter(expression, open, opening, closingDelimiter, cyphertext.OpeningDelimiter)
			if closing < 0 {
				return "", false
			}
			open = closing
		case '{':
			if cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter) == len(expression)-1 {
				receiver = strings.TrimSpace(expression[:open])
				_, subqueryName := staticSubqueryExpressionTypes[upperASCII(receiver)]
				if !endsWithOperand(receiver) && (!subqueryName || isSubqueryBrace(expression, open)) {
					// After an operator, a comma or a keyword ({a: 1} + x,
					// x = {a: 1}, THEN {a: 1}, and EXISTS / COUNT / COLLECT
					// { … } subqueries) the brace opens a map literal or a
					// subquery.
					return "", false
				}
				return receiver, true
			}
			closing := cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter)
			if closing < 0 {
				return "", false
			}
			open = closing
		}
	}
	return "", false
}

// staticProjectable reports whether a value of a static type, or of one of
// its choices ("Map, Node or Relationship"), can be map-projected: a map,
// node, relationship or null, and in a Cypher 5 statement a temporal value
// or duration too (Neo4j 5.26: date(…){.year}); Neo4j 2026.09 rejects
// those in a Cypher 25 statement.
func staticProjectable(typeName string, cypher25 bool) bool {
	for _, choice := range staticTypeChoices(typeName) {
		operand := knownOperand(strings.TrimSpace(choice))
		switch {
		case operand.is("Map"), operand.is("Node"), operand.is("Relationship"), operand.is("Null"),
			!cypher25 && (operand.temporal() || operand.duration()):
			return true
		}
	}
	return false
}

// endsWithOperand reports whether text ends with an operand: a literal, a
// closing bracket, or a name that isn't a keyword (true, false and null are
// operands).
func endsWithOperand(text string) bool {
	if text == "" {
		return false
	}
	last := text[len(text)-1]
	switch {
	case isCypherQuoteByte(last) || last == ')' || last == ']' || last == '}':
		return true
	case !cyphertext.NamePartAt(text, len(text)-1):
		return false
	}
	start := len(text)
	for start > 0 && cyphertext.NamePartAt(text, start-1) {
		start--
	}
	word := text[start:]
	return !isCypherKeyword(word) || isLiteralKeyword(word)
}

// checkedWithoutUnaryPlus runs a statement's type check, and for a Cypher 5
// statement with a unary plus runs it again with each +x rewritten to x, as
// Neo4j 5.26 checks a statement's types again after that rewrite: a unary
// plus over a duration is a number to the first check ((+d).day is
// rejected: "Float or Integer") and the duration itself to the second
// (abs(+d) and +d - 1 are rejected as a Duration).
//
// Only a type mismatch of the second check counts: a plus read as unary
// that wasn't (a variable named distinct before a binary plus) leaves text
// that can only fail to parse, so it can't reject a valid statement.
func checkedWithoutUnaryPlus(cypher string, cypher25 bool, check func(string) error) error {
	if err := check(cypher); err != nil || cypher25 {
		return err
	}
	rewritten, unaryPlus := withoutUnaryPlus(cypher)
	if !unaryPlus {
		return nil
	}
	err := check(rewritten)
	var detailed interface{ BoltErrorDetail() string }
	if errors.As(err, &detailed) && detailed.BoltErrorDetail() == "InvalidArgumentType" {
		return err
	}
	return nil
}

// withoutUnaryPlus is cypher with each unary plus blanked (+x read as x),
// and whether it had one (checkedWithoutUnaryPlus).
func withoutUnaryPlus(cypher string) (string, bool) {
	if strings.IndexByte(cypher, '+') < 0 {
		return cypher, false
	}
	var rewritten []byte
	for index := 0; index < len(cypher); index++ {
		switch character := cypher[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(cypher, index, character) - 1
			continue
		case '/':
			if end := queryCommentEnd(cypher, index); end >= 0 {
				index = end - 1
			}
			continue
		case '+':
		default:
			continue
		}
		if index+1 < len(cypher) && cypher[index+1] == '=' {
			continue // SET n += m
		}
		if !unaryPlusAt(strings.TrimRightFunc(cypher[:index], cyphertext.IsWhitespace)) {
			continue
		}
		if rewritten == nil {
			rewritten = []byte(cypher)
		}
		rewritten[index] = ' '
	}
	if rewritten == nil {
		return cypher, false
	}
	return string(rewritten), true
}

// unaryPlusAt reports whether a + after before is a unary plus: one after
// an operator, an opening bracket, a comma or a keyword that an expression
// follows. After a name (a variable named skip too), a literal or a closing
// bracket it is a binary plus; a plus that may be either is read as binary,
// which only skips the second check.
func unaryPlusAt(before string) bool {
	if before == "" {
		return true
	}
	switch last := before[len(before)-1]; {
	case strings.IndexByte("(,[{:=<>+-*/%^|", last) >= 0:
		return true
	case !cyphertext.NamePartAt(before, len(before)-1):
		return false
	}
	for _, keyword := range [...]string{"RETURN", "WITH", "WHERE", "AND", "OR", "XOR", "NOT", "THEN", "ELSE", "WHEN", "CASE", "IN", "BY", "UNWIND", "DISTINCT"} {
		if endsWithKeywordFold(before, keyword) {
			return true
		}
	}
	return false
}

// endsWithKeywordFold reports whether text ends with the word keyword.
func endsWithKeywordFold(text, keyword string) bool {
	if len(text) < len(keyword) || !strings.EqualFold(text[len(text)-len(keyword):], keyword) {
		return false
	}
	return len(text) == len(keyword) || !cyphertext.NamePartAt(text, len(text)-len(keyword)-1)
}

// maskMapProjections blanks the item list of every map projection in
// expression (a name, a keyword's only when the scope binds it, then a brace
// that doesn't open a subquery), so its braces don't hide the operators
// around it.
func (checker staticOperatorChecker) maskMapProjections(expression string) string {
	if strings.IndexByte(expression, '{') < 0 {
		return expression
	}
	masked := []byte(expression)
	for index := 0; index < len(expression); index++ {
		switch c := expression[index]; {
		case isCypherStringQuoteByte(c):
			index = skipQuotedSemanticText(expression, index) - 1
		case isCypherBacktickByte(c) || cyphertext.NameStartAt(expression, index) && (index == 0 || !cyphertext.NamePartAt(expression, index-1) && expression[index-1] != '$' && expression[index-1] != '.'):
			name, next, ok := scanSymbolicName(expression, index)
			if !ok {
				continue
			}
			index = next - 1
			open := skipSpaces(expression, next)
			if open >= len(expression) || expression[open] != '{' || isSubqueryBrace(expression, open) {
				continue
			}
			if isCypherKeyword(name) && !checker.scope.bound(normalizeProjectionColumnName(name)) {
				continue
			}
			end := cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter)
			if end < 0 {
				return expression
			}
			for blank := open; blank <= end; blank++ {
				masked[blank] = ' '
			}
			index = end
		}
	}
	return string(masked)
}

// checkAtom types a literal, parameter, variable, list or map literal or
// function call, checking the expressions inside it.
func (checker staticOperatorChecker) checkAtom(expression string) (staticOperand, error) {
	if expression[0] == '$' && simpleSemanticIdentifier(strings.TrimSpace(expression[1:])) != "" {
		name := strings.TrimSpace(expression[1:])
		if checker.params == nil || simpleSemanticIdentifier(name) == "" {
			return staticOperand{}, nil
		}
		value, bound := checker.params[name]
		if !bound {
			return staticOperand{}, nil
		}
		operand := staticParameterOperand(value)
		operand.parameter = name
		return operand.proved(), nil
	}
	if operand, subquery := staticSubqueryExpressionOperand(expression); subquery {
		return operand, nil
	}
	if receiver, inner, subscript, postfix := staticPostfixSplit(expression); postfix {
		return checker.checkPostfix(receiver, inner, subscript)
	}
	if pattern, projection, comprehension := splitPatternComprehension(expression); comprehension {
		return checker.checkPatternComprehension(pattern, projection)
	}
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if variable, list, predicate, projection, comprehension := parseListComprehension(inner); comprehension {
			return checker.checkListComprehension(variable, list, predicate, projection)
		}
		var elements []staticOperand
		if strings.TrimSpace(inner) != "" {
			for _, item := range splitTopLevelComma(inner) {
				element, err := checker.check(item)
				if err != nil {
					return staticOperand{}, err
				}
				elements = append(elements, element)
			}
		}
		return listLiteralOperand(elements), nil
	}
	if expression[0] == '{' && cyphertext.FindMatchingDelimiter(expression, 0, '{', '}', cyphertext.OpeningDelimiter) == len(expression)-1 {
		mapOperand := provenOperand("Map")
		for _, pair := range splitTopLevelComma(expression[1 : len(expression)-1]) {
			if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
				value, err := checker.check(pair[separator+1:])
				if err != nil {
					return staticOperand{}, err
				}
				if checker.scope.cypher25 && value.typeName() != "" {
					if mapOperand.members == nil {
						mapOperand.members = make(map[string]staticOperand)
					}
					mapOperand.members[normalizePropertyKey(pair[:separator])] = value
				}
			}
		}
		return mapOperand, nil
	}
	if variable, items, projection := mapProjectionSplit(expression, mapProjectionStatic, &checker.scope); projection {
		// In Cypher 5 a temporal value or duration projects its fields;
		// reading one it doesn't have is a runtime error, as for d.field.
		if receiver := knownOperand(checker.scope.typeOf(variable)); receiver.known() && !staticProjectable(receiver.typeName(), checker.scope.cypher25) {
			return staticOperand{}, operandMismatch(receiver, "Map, Node or Relationship")
		}
		for _, item := range splitTopLevelComma(items) {
			if separator := findTopLevelMapKeyValueSeparator(item); separator > 0 {
				if _, err := checker.check(item[separator+1:]); err != nil {
					return staticOperand{}, err
				}
			}
		}
		return provenOperand("Map"), nil
	}
	var callFacts2 functionCallFacts
	if call := inspectFunctionCall(expression, qualifiedFunctionCall, "", &callFacts2); call && callFacts2.name != "" {
		function, arguments := callFacts2.name, callFacts2.inner

		if isQuantifierFunction(lowerASCII(function)) {
			if variable, list, predicate, ok := parseQuantifierArguments(arguments); ok {
				_, err := checker.checkListComprehension(variable, list, predicate, "")
				return provenOperand("Boolean"), err
			}
		}
		if isReduceFormFunction(function) {
			if form, ok := parseReduceForm(function, arguments); ok {
				return checker.checkReduceTypes(form)
			}
		}
		var operands []staticOperand
		if !isQuantifierOrReduceFunction(function) {
			for index, argument := range splitTopLevelComma(arguments) {
				// normalize(s, NFC) names its normal form, not a variable.
				if index == 1 && lowerASCII(function) == "normalize" {
					continue
				}
				// An aggregate's DISTINCT isn't part of its argument.
				argument, _ = cutDistinct(strings.TrimSpace(argument))
				operand, err := checker.check(argument)
				if err != nil {
					return staticOperand{}, err
				}
				operands = append(operands, operand)
			}
		}
		// A result typed from its arguments keeps each of their
		// alternatives (staticFunctionDerivations).
		if operand, derived := derivedFunctionOperand(lowerASCII(function), operands); derived && operand.typeName() != "" {
			return operand, nil
		}
		if _, _, _, aggregate := parsePipelineAggregate(expression); aggregate {
			if result := staticFunctionResultType(function, arguments); result != "" {
				return provenOperand(result), nil
			}
			return staticOperand{nonEntity: true, proven: true}, nil
		}
		return provenOperand(staticFunctionResultType(function, arguments)), nil
	}
	if typeName := staticLiteralType(expression, scalarLiteralOnly); typeName != "" {
		return provenOperand(typeName), nil
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		if operand, ok := checker.scope.derived[variable]; ok {
			return operand, nil
		}
		if typeName := checker.scope.typeOf(variable); typeName != "" {
			// The scope's type names come only from proven operands.
			operand := typedOperand(typeName).proved()
			if typeName == "Map" {
				operand.members = checker.scope.members[variable]
			}
			return operand, nil
		}
		if checker.scope.complete && !checker.scope.bound(variable) && !isLiteralKeyword(variable) {
			return staticOperand{}, createUndefinedVariableError(variable)
		}
	}
	return staticOperand{}, nil
}

func isQuantifierOrReduceFunction(name string) bool {
	switch lowerASCII(name) {
	case "all", "any", "none", "single", "reduce", "allreduce", "exists":
		return true
	}
	return false
}

// staticParameterOperand is a parameter value's static type, as Neo4j names
// it when it type-checks a statement with its parameters: a list of strings is
// List<String>, any other list List<T>, a map "Map, Node or Relationship".
func staticParameterOperand(value interface{}) staticOperand {
	if number, numeric := valueconvert.ReadNumber(value); numeric {
		if number.IsInteger() {
			return knownOperand("Integer")
		}
		return knownOperand("Float")
	}
	switch value.(type) {
	case nil:
		return staticOperand{}
	case string:
		return knownOperand("String")
	case bool:
		return knownOperand("Boolean")
	}
	if isAllStringList(value) {
		return knownOperand("List<String>")
	}
	if isRuntimeList(value) {
		return knownOperand("List<T>")
	}
	if _, isMap := toStringAnyMap(value); isMap {
		return displayedOperand("Map", "Map, Node or Relationship", "")
	}
	return staticOperand{}
}

// mayContainArithmetic is the cheap precheck before an expression is
// type-checked: an arithmetic operator needs one of these characters.
func mayContainArithmetic(text string) bool {
	return strings.ContainsAny(text, "+-*/%^")
}

// mayNeedStaticTypeCheck is the cheap precheck before a clause's expressions
// are type-checked: an operator, subscript or property access that can reject
// an operand's static type needs one of these characters or keywords.
func mayNeedStaticTypeCheck(text string) bool {
	if strings.ContainsAny(text, "+-*/%^[.~{") {
		return true
	}
	return containsFold(text, "AND") || containsFold(text, "OR") || containsFold(text, "NOT") ||
		containsFold(text, " IN ") || containsFold(text, "CASE")
}

// clauseTextUntil is text[from:] up to the first of keywords at its top
// level. Each keyword is read in the whole text's context, so a variable
// named like one right after the clause word that takes it (WHERE skip{.a}
// IS NULL, ORDER BY limit.x) isn't taken for that clause.
func clauseTextUntil(text string, from int, keywords ...string) string {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	end := len(text)
	for _, keyword := range keywords {
		if index := keywordIndexFrom(text, keyword, from, opts); index >= 0 && index < end {
			end = index
		}
	}
	return text[from:end]
}

// forEachClauseOperatorExpression visits the expressions of one clause whose
// operators are type-checked (projections, WHERE, ORDER BY, SET values,
// UNWIND lists, pattern property values) and that may contain arithmetic.
// afterProjection is set for the WHERE and ORDER BY of a RETURN / WITH, which
// see the projection's aliases. It allocates only for clauses with arithmetic.
func (e *StorageExecutor) forEachClauseOperatorExpression(ctx context.Context, clause pipelineClause, visit func(expression string, afterProjection bool) error) error {
	text := strings.TrimSpace(clause.text)
	if !mayNeedStaticTypeCheck(text) {
		return nil
	}
	visitPatternValues := func(pattern string) error {
		for index := 0; index < len(pattern); index++ {
			switch cypherStringLexicalByte(pattern[index]) {
			case cypherStringQuoteClass:
				index = skipQuotedSemanticText(pattern, index) - 1
			case '{':
				closing := cyphertext.FindMatchingDelimiter(pattern, index, '{', '}', cyphertext.OpeningDelimiter)
				if closing < 0 {
					return nil
				}
				if body := pattern[index+1 : closing]; mayNeedStaticTypeCheck(body) {
					for _, pair := range splitTopLevelComma(body) {
						if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
							if err := visit(pair[separator+1:], false); err != nil {
								return err
							}
						}
					}
				}
				index = closing
			}
		}
		return nil
	}
	visitPredicate := func(body string, afterProjection bool) error {
		where := topLevelKeywordIndex(body, "WHERE")
		if where < 0 {
			return nil
		}
		predicate := clauseTextUntil(body, where+len("WHERE"), "ORDER BY", "SKIP", "LIMIT")
		if !mayNeedStaticTypeCheck(predicate) {
			return nil
		}
		return visit(predicate, afterProjection)
	}
	switch clause.kind {
	case pipelineClauseLet:
		projections, err := parsePipelineLet(clause.text)
		if err != nil {
			return err
		}
		for _, projection := range projections {
			if err := visit(projection.expression, false); err != nil {
				return err
			}
		}
	case pipelineClauseFilter:
		return visit(pipelineFilterExpression(clause.text), false)
	case pipelineClauseReturn, pipelineClauseWith:
		keyword := "RETURN"
		if clause.kind == pipelineClauseWith {
			keyword = "WITH"
		}
		projection := clauseTextUntil(text, len(keyword), "WHERE", "ORDER BY", "SKIP", "LIMIT")
		rest := text[len(keyword)+len(projection):]
		projection, _ = cutDistinct(projection)
		if mayNeedStaticTypeCheck(projection) {
			for _, item := range splitTopLevelComma(projection) {
				expression := readProjectionItemSource(ctx, strings.TrimSpace(item), projectionAdmitted).expression
				if mayNeedStaticTypeCheck(expression) {
					if err := visit(expression, false); err != nil {
						return err
					}
				}
			}
		}
		if mayNeedStaticTypeCheck(rest) {
			return forEachProjectedTailExpression(ctx, projection, rest, func(expression string) error {
				if !mayNeedStaticTypeCheck(expression) {
					return nil
				}
				return visit(expression, true)
			})
		}
	case pipelineClauseMatch, pipelineClauseOptionalMatch:
		pattern := text
		if where := topLevelKeywordIndex(text, "WHERE"); where >= 0 {
			pattern = text[:where]
		}
		if err := visitPatternValues(pattern); err != nil {
			return err
		}
		return visitPredicate(text, false)
	case pipelineClauseCreate, pipelineClauseMerge:
		pattern := text
		for _, keyword := range [...]string{"ON CREATE SET", "ON MATCH SET"} {
			if index := findKeywordIndexInContext(pattern, keyword); index >= 0 {
				pattern = pattern[:index]
			}
		}
		return visitPatternValues(pattern)
	case pipelineClauseSet:
		for _, assignment := range splitSetAssignments(strings.TrimSpace(text[len("SET"):])) {
			if operator := strings.Index(assignment, "="); operator > 0 && mayNeedStaticTypeCheck(assignment[operator+1:]) {
				if err := visit(assignment[operator+1:], false); err != nil {
					return err
				}
			}
		}
	case pipelineClauseUnwind:
		return visit(unwindSourceExpression(text), false)
	}
	return nil
}

// forEachProjectedTailExpression calls visit with each expression of a
// projection's tail (rest, from splitWithProjection: WHERE, ORDER BY, SKIP,
// LIMIT) that is evaluated with the projected scope: the WHERE predicate and
// the ORDER BY terms. A term repeating a projection item's expression is the
// projected column (orderedProjectionExpressions, as the rows are ordered)
// and is left out; the item itself is checked with the incoming scope
// (RETURN size(s) AS s ORDER BY size(s)). projection is the item list, with
// or without its RETURN / WITH [DISTINCT] keyword.
func forEachProjectedTailExpression(ctx context.Context, projection, rest string, visit func(expression string) error) error {
	if where := topLevelKeywordIndex(rest, "WHERE"); where >= 0 {
		if err := visit(clauseTextUntil(rest, where+len("WHERE"), "ORDER BY", "SKIP", "LIMIT")); err != nil {
			return err
		}
	}
	order := topLevelKeywordIndex(rest, "ORDER BY")
	if order < 0 {
		return nil
	}
	projection = strings.TrimSpace(projection)
	for _, keyword := range [...]string{"RETURN", "WITH"} {
		if startsWithKeywordFold(projection, keyword) {
			projection = projection[len(keyword):]
			break
		}
	}
	projection, _ = cutDistinct(strings.TrimSpace(projection))
	items := splitTopLevelComma(projection)
	item := func(index int) (string, string) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(items[index]), projectionAdmitted)
		return itemSource.expression, itemSource.alias
	}
	for _, term := range parseOrderByClause(clauseTextUntil(rest, order+len("ORDER BY"), "SKIP", "LIMIT", "WHERE")) {
		if orderedProjectionExpressions([]orderByTerm{term}, len(items), item) != nil {
			continue
		}
		if err := visit(term.column); err != nil {
			return err
		}
	}
	return nil
}

// validateStaticOperatorTypes type-checks the operators of one clause's
// expressions with the clause's variable types; the WHERE and ORDER BY after
// a projection use projectedScope (built only when needed), where the
// projection's aliases replace the variables they rename
// (RETURN n.num AS n ORDER BY n + 2).
func (e *StorageExecutor) validateStaticOperatorTypes(ctx context.Context, clause pipelineClause, scope staticTypeScope, projectedScope func() staticTypeScope, params map[string]interface{}) error {
	if clause.kind == pipelineClauseDelete {
		if err := staticDeleteTargetTypeError(clause.text, scope); err != nil {
			return err
		}
	}
	if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
		predicate := clauseTextUntil(clause.text, where+len("WHERE"), "ORDER BY", "SKIP", "LIMIT")
		predicateScope := scope
		if projectedScope != nil {
			predicateScope = projectedScope()
		}
		checker := staticOperatorChecker{scope: predicateScope, params: params}
		operand, err := checker.check(predicate)
		if err != nil {
			return err
		}
		if err := requireBooleanOperand(operand); err != nil {
			return err
		}
	}
	if clause.kind == pipelineClauseFilter {
		predicate := pipelineFilterExpression(clause.text)
		checker := staticOperatorChecker{scope: scope, params: params}
		operand, err := checker.check(predicate)
		if err != nil {
			return err
		}
		if err := requireBooleanOperand(operand); err != nil {
			return err
		}
	}
	var projected *staticOperatorChecker
	return e.forEachClauseOperatorExpression(ctx, clause, func(expression string, afterProjection bool) error {
		checker := staticOperatorChecker{scope: scope, params: params}
		if afterProjection && projectedScope != nil {
			if projected == nil {
				projected = &staticOperatorChecker{scope: projectedScope(), params: params}
			}
			checker = *projected
		}
		_, err := checker.check(expression)
		return err
	})
}

// parameterMayMismatchOperator reports whether a $parameter next to an
// arithmetic operator in cypher holds a value that operator could reject.
func parameterMayMismatchOperator(cypher string, params map[string]interface{}) bool {
	var previous byte
	for index := 0; index < len(cypher); index++ {
		switch cypherStringLexicalByte(cypher[index]) {
		case cypherStringQuoteClass:
			previous = cypher[index]
			index = skipQuotedSemanticText(cypher, index) - 1
			continue
		case '/':
			if end := queryCommentEnd(cypher, index); end >= 0 {
				index = end - 1
				continue
			}
		}
		if cypher[index] != '$' {
			// A parenthesised parameter is its operator's operand too:
			// ($m) + 'a'.
			if !cyphertext.IsWhitespaceAt(cypher, index) && cypher[index] != '(' {
				previous = cypher[index]
			}
			continue
		}
		start := index + 1
		end := start
		end = cyphertext.NamePartEnd(cypher, end)
		if end == start {
			previous = '$'
			continue
		}
		before := previous
		previous = '$'
		after := queryGapEnd(cypher, end)
		for after < len(cypher) && cypher[after] == ')' {
			after = queryGapEnd(cypher, after+1)
		}
		var operators [2]byte
		if strings.IndexByte("+-*/%^", before) >= 0 {
			operators[0] = before
		}
		if after < len(cypher) && strings.IndexByte("+-*/%^", cypher[after]) >= 0 {
			operators[1] = cypher[after]
		}
		if operators[0] == 0 && operators[1] == 0 {
			index = end - 1
			continue
		}
		value, bound := params[cypher[start:end]]
		if !bound || value == nil || isRuntimeNumber(value) {
			index = end - 1
			continue
		}
		_, isString := value.(string)
		for _, operator := range operators {
			if operator == 0 {
				continue
			}
			if operator == '+' && (isString || isRuntimeList(value)) {
				continue
			}
			return true
		}
		index = end - 1
	}
	return false
}

// numericOperatorResult retains Number when an Integer or Float remains possible.
func numericOperatorResult(left, right staticOperand) staticOperand {
	if left.is("Float") || right.is("Float") {
		return knownOperand("Float")
	}
	if left.is("Integer") && right.is("Integer") {
		return knownOperand("Integer")
	}
	return knownOperand("Number")
}
