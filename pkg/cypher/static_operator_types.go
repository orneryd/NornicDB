package cypher

import (
	"fmt"
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

// staticOperand is an operand's static type: kind drives the operator rules,
// display is how the error names it, parameter the parameter it comes from.
type staticOperand struct {
	kind       string
	display    string
	parameter  string
	nonBoolean bool
	// members are the static types of a map literal's entries by key, in a
	// Cypher 25 statement (Neo4j 2026.09 types {a: 2}.a as an Integer;
	// Neo4j 5.26 doesn't): staticMapMember reads them.
	members map[string]staticOperand
}

// staticMapMember is the operand a map with known members gives for key
// (m.key, m['key']), and whether it has one.
func (operand staticOperand) staticMapMember(key string) (staticOperand, bool) {
	member, found := operand.members[key]
	return member, found
}

func knownOperand(kind string) staticOperand {
	return staticOperand{kind: kind, display: kind}
}

func (operand staticOperand) known() bool { return operand.kind != "" }

func (operand staticOperand) numeric() bool {
	return operand.kind == "Integer" || operand.kind == "Float"
}

func (operand staticOperand) list() bool { return strings.HasPrefix(operand.kind, "List<") }

func (operand staticOperand) duration() bool { return operand.kind == "Duration" }

// joinsStringStatically reports whether a Cypher 25 string + operand is the
// string joined with the operand's text: a string, number, boolean, point,
// duration or temporal value (a list is list concatenation instead).
func (operand staticOperand) joinsStringStatically() bool {
	switch operand.kind {
	case "String", "Integer", "Float", "Boolean", "Point", "Duration":
		return true
	}
	return operand.temporal()
}

func (operand staticOperand) temporal() bool {
	switch operand.kind {
	case "Date", "Time", "LocalTime", "LocalDateTime", "DateTime":
		return true
	}
	return false
}

// staticFunctionResultTypes are the result types of functions whose result
// type doesn't depend on their arguments.
var staticFunctionResultTypes = map[string]string{
	"size": "Integer", "length": "Integer", "tointeger": "Integer", "char_length": "Integer",
	"character_length": "Integer", "timestamp": "Integer", "sign": "Integer",
	"tofloat": "Float", "sqrt": "Float", "exp": "Float", "log": "Float", "log10": "Float",
	"sin": "Float", "cos": "Float", "tan": "Float", "cot": "Float", "asin": "Float",
	"acos": "Float", "atan": "Float", "atan2": "Float", "degrees": "Float", "radians": "Float",
	"haversin": "Float", "rand": "Float", "pi": "Float", "e": "Float", "round": "Float",
	"ceil": "Float", "floor": "Float", "stdev": "Float", "stdevp": "Float",
	"tostring": "String", "toupper": "String", "tolower": "String", "upper": "String",
	"lower": "String", "trim": "String", "ltrim": "String", "rtrim": "String",
	"btrim": "String", "replace": "String", "substring": "String", "left": "String",
	"right": "String", "type": "String", "elementid": "String",
	"toboolean": "Boolean", "allreduce": "Boolean", "property_exists": "Boolean",
	"keys": "List<String>", "labels": "List<String>", "split": "List<String>",
	"date": "Date", "datetime": "DateTime", "localdatetime": "LocalDateTime",
	"time": "Time", "localtime": "LocalTime", "duration": "Duration",
	"point": "Point", "properties": "Map",
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
	"uuid": "UUID", "uuid.mostsignificantbits": "Integer", "uuid.leastsignificantbits": "Integer",
}

// staticFunctionResultType is the result type of a call to function with
// arguments, when it is known before the statement runs: the function's own
// (staticFunctionResultTypes), or, for reduce(acc = init, x IN list | step),
// its accumulator's when init is a literal (reduce(a = 0, …) is an Integer,
// as Neo4j types it).
func staticFunctionResultType(function, arguments string) string {
	if strings.EqualFold(function, "reduce") {
		if form, ok := parseReduceForm("reduce", arguments); ok {
			return staticLiteralTypeName(form.initial)
		}
		return ""
	}
	result, _ := lookupLowerASCII(staticFunctionResultTypes, function)
	return result
}

// staticValueCallType is the static type of expression when it is one whole
// call to a function returning a VECTOR or a UUID (toBoolean(vector(…)),
// [x IN uuid() | x]). These Cypher 25 types are checked as Neo4j 2026.09
// does; other function results stay unchecked in argument and list
// positions.
func staticValueCallType(expression string) string {
	expression = strings.TrimSpace(expression)
	name, end, ok := scanIdentifierToken(expression, 0)
	if !ok {
		return ""
	}
	open := queryGapEnd(expression, end)
	if open >= len(expression) || expression[open] != '(' || findMatchingParen(expression, open) != len(expression)-1 {
		return ""
	}
	switch result := staticFunctionResultType(name, expression[open+1:len(expression)-1]); result {
	case "Vector", "UUID":
		return result
	}
	return ""
}

// staticOperatorChecker infers expression types for the operator checks, from
// a clause's variables and, when checking with parameter values, from them.
type staticOperatorChecker struct {
	scope  staticTypeScope
	params map[string]interface{}
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
	case operand.nonBoolean, operand.known() && operand.kind != "Boolean" && operand.kind != "Null":
		return operandMismatch(operand, "Boolean")
	}
	return nil
}

// checkOperator applies Neo4j's operand rules for left op right and returns
// the result type. cypher25 applies Neo4j 2026.09's rules for a Cypher 25
// statement: a string joins any value but a map or a graph entity ('a' +
// true, date('2020-01-02') + 's').
func checkOperator(op byte, left, right staticOperand, cypher25 bool) (staticOperand, error) {
	switch op {
	case '+':
		if cypher25 && (left.kind == "String" && right.joinsStringStatically() || right.kind == "String" && left.joinsStringStatically()) {
			return knownOperand("String"), nil
		}
		switch {
		case left.kind == "Vector":
			// A vector (Cypher 25) joins a string or a list, as Neo4j
			// 2026.09 types it.
			switch {
			case right.kind == "String":
				return knownOperand("String"), nil
			case right.list():
				return knownOperand("List<T>"), nil
			case right.known() && right.kind != "Null":
				return staticOperand{}, operandMismatch(right, "String or List<T>")
			}
			return staticOperand{}, nil
		case left.kind == "String" && right.kind == "Vector":
			return knownOperand("String"), nil
		case left.kind == "String" && right.kind == "UUID":
			// UUID is a Cypher 25 type: Neo4j 2026.09's list of what a
			// string joins.
			return staticOperand{}, operandMismatch(right, "Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime, DateTime, Vector or List<T>")
		case left.temporal():
			if right.list() {
				return knownOperand("List<T>"), nil
			}
			if right.known() && !right.duration() {
				return staticOperand{}, operandMismatch(right, "Duration or List<T>")
			}
			return left, nil
		case left.duration():
			if right.temporal() || right.duration() {
				return right, nil
			}
			if right.list() {
				return knownOperand("List<T>"), nil
			}
			if right.known() && right.kind != "Null" {
				return staticOperand{}, operandMismatch(right, "Duration, Date, Time, LocalTime, LocalDateTime, DateTime or List<T>")
			}
			return staticOperand{}, nil
		case left.numeric() || left.kind == "String":
			if right.known() && !right.numeric() && right.kind != "String" && !right.list() {
				return staticOperand{}, operandMismatch(right, "Float, Integer, String or List<T>")
			}
		case left.known() && !left.list():
			if right.known() && !right.list() {
				return staticOperand{}, operandMismatch(right, "List<T>")
			}
		}
		switch {
		case left.numeric() && right.numeric():
			if left.kind == "Integer" && right.kind == "Integer" {
				return knownOperand("Integer"), nil
			}
			return knownOperand("Float"), nil
		case (left.kind == "String" && (right.numeric() || right.kind == "String")) || (right.kind == "String" && left.numeric()):
			return knownOperand("String"), nil
		case left.list() || right.list():
			return knownOperand("List<T>"), nil
		}
		if left.numeric() || right.numeric() {
			return staticOperand{display: "Float, Integer, String or List<T>", nonBoolean: true}, nil
		}
		return staticOperand{}, nil
	case '-':
		if left.temporal() || left.duration() {
			if right.known() && !right.duration() {
				return staticOperand{}, operandMismatch(right, "Duration")
			}
			return left, nil
		}
		if left.known() && !left.numeric() {
			return staticOperand{}, operandMismatch(left, "Float, Integer, Duration, Date, Time, LocalTime, LocalDateTime or DateTime")
		}
		if right.known() && !right.numeric() {
			if left.numeric() {
				return staticOperand{}, operandMismatch(right, "Float or Integer")
			}
			// An unknown left operand may be a temporal value or a duration.
			if !right.duration() {
				return staticOperand{}, operandMismatch(right, "Float, Integer or Duration")
			}
		}
	case '*':
		if left.duration() && (!right.known() || right.numeric()) || right.duration() && (!left.known() || left.numeric()) {
			return knownOperand("Duration"), nil
		}
		if left.known() && !left.numeric() && !left.duration() {
			return staticOperand{}, operandMismatch(left, "Float, Integer or Duration")
		}
		if right.known() && !right.numeric() {
			return staticOperand{}, operandMismatch(right, "Float, Integer or Duration")
		}
	case '/':
		if left.duration() {
			if right.known() && !right.numeric() {
				return staticOperand{}, operandMismatch(right, "Float or Integer")
			}
			return left, nil
		}
		if left.known() && !left.numeric() {
			return staticOperand{}, operandMismatch(left, "Float, Integer or Duration")
		}
		if right.known() && !right.numeric() {
			return staticOperand{}, operandMismatch(right, "Float or Integer")
		}
	case '%':
		if left.known() && !left.numeric() {
			return staticOperand{}, operandMismatch(left, "Float or Integer")
		}
		if right.known() && !right.numeric() {
			return staticOperand{}, operandMismatch(right, "Float or Integer")
		}
	case '^':
		if left.known() && !left.numeric() {
			return staticOperand{}, operandMismatch(left, "Float")
		}
		if right.known() && !right.numeric() {
			return staticOperand{}, operandMismatch(right, "Float")
		}
		// A power is a Float whatever its operands are (m.a ^ 2 too).
		return knownOperand("Float"), nil
	}
	if left.numeric() && right.numeric() {
		if left.kind == "Integer" && right.kind == "Integer" {
			return knownOperand("Integer"), nil
		}
		return knownOperand("Float"), nil
	}
	return staticOperand{display: "Float, Integer or Duration", nonBoolean: true}, nil
}

// staticComparisonOperators split a predicate into the operands whose
// arithmetic is checked; the comparison itself accepts any types.
var staticComparisonOperators = []string{"<>", "<=", ">=", "=~", "=", "<", ">"}

var staticComparisonKeywords = []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "IN", "IS NOT NULL", "IS NULL"}

// check returns the static type of expression and the first operator type
// error in it. CASE, list comprehensions, quantifiers, pattern expressions
// and subqueries are not looked into: they bind their own variables.
func (checker staticOperatorChecker) check(expression string) (staticOperand, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return staticOperand{}, nil
	}
	if receiver, projected := staticMapProjectionReceiver(expression); projected && receiver != "" {
		if _, _, projection := staticMapProjectionSplit(expression); !projection {
			return staticOperand{}, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
				localization.CypherCoreMapProjectionReceiver(receiver))
		}
	}
	if _, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		return checker.checkAtom(expression)
	}
	if expression[0] == '{' && findMatchingDelimiter(expression, 0, '{', '}') == len(expression)-1 {
		return checker.checkAtom(expression)
	}
	if _, _, projection := staticMapProjectionSplit(expression); projection {
		return checker.checkAtom(expression)
	}
	if !isOperatorExpressionText(expression) {
		// A function call or subquery expression over a pattern still has
		// its result type (size([(n)-->() | 1]) is an Integer).
		return staticPatternExpressionType(expression), nil
	}
	if inner, enclosed := stripEnclosingExpressionParentheses(expression); enclosed {
		return checker.check(inner)
	}
	if startsWithKeywordFold(expression, "CASE") {
		if isCaseExpression(expression) && leadingCaseExpressionEnd(expression) == len(expression) {
			if parsed, err := parseCaseExpression(expression); err == nil {
				return checker.checkCase(parsed)
			}
		}
		return staticOperand{}, nil
	}
	for _, keyword := range []string{"OR", "XOR", "AND"} {
		if index := topLevelKeywordIndex(expression, keyword); index > 0 {
			for _, side := range [2]string{expression[:index], expression[index+len(keyword):]} {
				operand, err := checker.check(side)
				if err != nil {
					return staticOperand{}, err
				}
				if err := requireBooleanOperand(operand); err != nil {
					return staticOperand{}, err
				}
			}
			return knownOperand("Boolean"), nil
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
		return knownOperand("Boolean"), nil
	}
	// A type or normalization predicate (x IS :: LIST<INTEGER>, x IS NOT
	// TYPED STRING, x IS NFC NORMALIZED) is a Boolean; its type text isn't
	// an expression.
	if index := topLevelKeywordIndex(expression, "IS"); index > 0 {
		rest := strings.TrimSpace(expression[index+len("IS"):])
		if !startsWithKeywordFold(rest, "NULL") && !startsWithKeywordFold(strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(rest), "NOT")), "NULL") {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			return knownOperand("Boolean"), nil
		}
	}
	for _, keyword := range staticComparisonKeywords {
		if index := topLevelKeywordIndex(expression, keyword); index > 0 {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			right, err := checker.check(expression[index+len(keyword):])
			if err != nil {
				return staticOperand{}, err
			}
			if keyword == "IN" && right.known() && !right.list() && right.kind != "Null" {
				return staticOperand{}, operandMismatch(right, "List<T>")
			}
			return knownOperand("Boolean"), nil
		}
	}
	for _, operator := range staticComparisonOperators {
		if left, right, found := splitByOperatorWithOptions(expression, operator, false, true); found && left != "" && right != "" {
			for _, side := range [2]string{left, right} {
				operand, err := checker.check(side)
				if err != nil {
					return staticOperand{}, err
				}
				if operator == "=~" && operand.known() && operand.kind != "String" && operand.kind != "Null" {
					return staticOperand{}, operandMismatch(operand, "String")
				}
			}
			return knownOperand("Boolean"), nil
		}
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		left, right, operator, arithmetic := splitRowArithmeticTier(expression, tier)
		if !arithmetic {
			continue
		}
		if !isOperandExpressionText(left) || !isOperandExpressionText(right) {
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
	if expression[0] == '-' && len(expression) > 1 {
		operand, err := checker.check(expression[1:])
		if err != nil {
			return staticOperand{}, err
		}
		if operand.known() && !operand.numeric() && operand.kind != "Null" {
			return staticOperand{}, operandMismatch(operand, "Float or Integer")
		}
		if !operand.known() || operand.kind == "Null" {
			// -null and -n.x are numbers to Neo4j's type check.
			return staticOperand{display: "Float or Integer", nonBoolean: true}, nil
		}
		return operand, nil
	}
	if expression[0] == '+' && len(expression) > 1 {
		operand, err := checker.check(expression[1:])
		if err != nil {
			return staticOperand{}, err
		}
		// Neo4j 5.26 also takes a temporal value or a duration (+date(…) is
		// the date); Neo4j 2026.09, which runs Cypher 25, only a number.
		temporal := (operand.temporal() || operand.duration()) && !checker.scope.cypher25
		if operand.known() && !operand.numeric() && !temporal && operand.kind != "Null" {
			return staticOperand{}, operandMismatch(operand, "Float or Integer")
		}
		if !operand.known() || operand.kind == "Null" {
			// +null and +n.x are numbers to Neo4j's type check.
			return staticOperand{display: "Float or Integer", nonBoolean: true}, nil
		}
		return operand, nil
	}
	return checker.checkAtom(expression)
}

// checkCase type-checks a CASE expression: a searched CASE's conditions must
// be booleans. Its type is the one type all its results share, if they do.
func (checker staticOperatorChecker) checkCase(parsed *caseExpression) (staticOperand, error) {
	if parsed.isSimple {
		if _, err := checker.check(parsed.testExpression); err != nil {
			return staticOperand{}, err
		}
	}
	var result staticOperand
	agree := true
	merge := func(text string) error {
		operand, err := checker.check(text)
		if err != nil {
			return err
		}
		switch {
		case !operand.known():
			agree = false
		case !result.known():
			result = operand
		case result.kind != operand.kind:
			agree = false
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
	if !agree {
		return staticOperand{}, nil
	}
	return result, nil
}

// staticPostfixSplit splits the trailing subscript or property access off an
// operand: the receiver and either the bracket contents (subscript) or the
// property key. ok is false when the operand doesn't end in one at its top
// level. A '.' between digits belongs to a number.
func staticPostfixSplit(expression string) (receiver, inner string, subscript, ok bool) {
	last := -1
	depth := 0
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; character {
		case '\'', '"', '`':
			index = skipQuotedSemanticText(expression, index) - 1
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
			if depth == 0 && index > 0 && index+1 < len(expression) && isIdentStartByte(expression[index+1]) {
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
		if findMatchingDelimiter(expression, last, '[', ']') != len(expression)-1 {
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

// staticSliceBounds splits a subscript's contents at a top-level "..".
func staticSliceBounds(inner string) (from, to string, slice bool) {
	depth := 0
	for index := 0; index+1 < len(inner); index++ {
		switch inner[index] {
		case '\'', '"', '`':
			index = skipQuotedSemanticText(inner, index) - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '.':
			if depth == 0 && inner[index+1] == '.' {
				return inner[:index], inner[index+2:], true
			}
		}
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
		if receiver.known() && receiver.kind != "Null" && rejectsPropertyAccess(receiver.kind) {
			return staticOperand{}, operandMismatch(receiver, "Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime")
		}
		if member, found := receiver.staticMapMember(symbolicNameValue(strings.TrimSpace(inner))); found {
			return member, nil
		}
		return staticOperand{}, nil
	}
	if from, to, slice := staticSliceBounds(inner); slice {
		for _, bound := range [2]string{from, to} {
			if _, err := checker.check(bound); err != nil {
				return staticOperand{}, err
			}
		}
		if receiver.known() && !receiver.list() && receiver.kind != "Null" {
			return staticOperand{}, operandMismatch(receiver, "List<T>")
		}
		if receiver.list() {
			return receiver, nil
		}
		// A slice is a list at compile time, whatever its receiver
		// (WHERE n.x[1..] is a list coercion error in Neo4j).
		return knownOperand("List<T>"), nil
	}
	key, err := checker.check(inner)
	if err != nil {
		return staticOperand{}, err
	}
	keyKnown := key.known() && key.kind != "Null"
	const syntaxError, detail = "Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType"
	switch {
	case !receiver.known():
		// A node's or relationship's property can't be a map, so it takes
		// only a list index (#882).
		if base, _, property := rowPropertyChainShape(receiverText); property && keyKnown && key.kind != "Integer" {
			if kind := checker.scope.typeOf(base); kind == "Node" || kind == "Relationship" {
				return staticOperand{}, operandMismatch(key, "Integer")
			}
		}
	case receiver.kind == "Null":
	case receiver.list():
		if keyKnown && key.kind != "Integer" {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreListIndexTypeMismatch(key.display))
		}
		if element := strings.TrimSuffix(strings.TrimPrefix(receiver.kind, "List<"), ">"); element != "T" && !strings.ContainsAny(element, "<,") {
			return knownOperand(element), nil
		}
	case receiver.kind == "Map":
		if keyKnown && key.kind != "String" {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreMapKeyTypeMismatch(key.display))
		}
		// A string literal key reads a known member (m['a']); a key from
		// a variable is read at run time, as in Neo4j 2026.09.
		if text, literal := decodeCypherQuotedString(strings.TrimSpace(inner)); literal {
			if member, found := receiver.staticMapMember(text); found {
				return member, nil
			}
		}
	case receiver.kind == "Node" || receiver.kind == "Relationship":
		if keyKnown && key.kind != "String" {
			return staticOperand{}, localizedStatusError(syntaxError, detail, localization.CypherCoreEntityPropertyKeyTypeMismatch(key.display))
		}
	case key.kind == "String":
		// In Neo4j 5.26 a temporal value or duration takes a string key at
		// compile time (reading a field it doesn't have is a runtime error);
		// Neo4j 2026.09, which runs Cypher 25, takes none.
		if !receiver.temporal() && !receiver.duration() || checker.scope.cypher25 {
			return staticOperand{}, operandMismatch(receiver, "Map, Node or Relationship")
		}
	case key.kind == "Integer":
		return staticOperand{}, operandMismatch(receiver, "List<T>")
	case !keyKnown && (!receiver.temporal() && !receiver.duration() || checker.scope.cypher25):
		return staticOperand{}, operandMismatch(receiver, "List<T>, Map, Node or Relationship")
	}
	return staticOperand{}, nil
}

// staticSubqueryExpressionTypes are the result types of COUNT { }, COLLECT { }
// and EXISTS { }.
var staticSubqueryExpressionTypes = map[string]string{"COUNT": "Integer", "COLLECT": "List<T>", "EXISTS": "Boolean"}

// staticPatternExpressionType is the result type of an expression the
// operator check doesn't read (it holds a pattern): a whole function call's
// or subquery expression's, or unknown.
func staticPatternExpressionType(expression string) staticOperand {
	if function, arguments, call := parseFunctionCallWS(expression); call && function != "" {
		return knownOperand(staticFunctionResultType(function, arguments))
	}
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok {
		return staticOperand{}
	}
	open := next
	for open < len(expression) && isASCIIWhitespace(expression[open]) {
		open++
	}
	if open < len(expression) && expression[open] == '{' && findMatchingDelimiter(expression, open, '{', '}') == len(expression)-1 {
		return knownOperand(staticSubqueryExpressionTypes[upperASCII(name)])
	}
	return staticOperand{}
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
		switch expression[open] {
		case '\'', '"':
			open = skipQuotedSemanticText(expression, open) - 1
		case '(', '[':
			opening, closingDelimiter := '(', ')'
			if expression[open] == '[' {
				opening, closingDelimiter = '[', ']'
			}
			closing := findMatchingDelimiter(expression, open, opening, closingDelimiter)
			if closing < 0 {
				return "", false
			}
			open = closing
		case '{':
			if findMatchingDelimiter(expression, open, '{', '}') == len(expression)-1 {
				receiver = strings.TrimSpace(expression[:open])
				if !endsWithOperand(receiver) {
					// After an operator, a comma or a keyword ({a: 1} + x,
					// x = {a: 1}, THEN {a: 1}, and EXISTS / COUNT / COLLECT
					// { … } subqueries) the brace opens a map literal or a
					// subquery.
					return "", false
				}
				return receiver, true
			}
			closing := findMatchingDelimiter(expression, open, '{', '}')
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
		case operand.kind == "Map", operand.kind == "Node", operand.kind == "Relationship", operand.kind == "Null",
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
	case last == '\'' || last == '"' || last == '`' || last == ')' || last == ']' || last == '}':
		return true
	case !isIdentByte(last):
		return false
	}
	start := len(text)
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	word := text[start:]
	return !isCypherKeyword(word) || isLiteralKeyword(word)
}

// staticMapProjectionSplit splits a map projection (variable{.a, k: v})
// into its variable and item list.
func staticMapProjectionSplit(expression string) (variable, items string, projection bool) {
	open := strings.IndexByte(expression, '{')
	if open <= 0 || findMatchingDelimiter(expression, open, '{', '}') != len(expression)-1 {
		return "", "", false
	}
	variable = simpleSemanticIdentifier(strings.TrimSpace(expression[:open]))
	if variable == "" || isCypherKeyword(variable) {
		return "", "", false
	}
	return variable, expression[open+1 : len(expression)-1], true
}

// checkAtom types a literal, parameter, variable, list or map literal or
// function call, checking the expressions inside it.
func (checker staticOperatorChecker) checkAtom(expression string) (staticOperand, error) {
	if expression[0] == '$' {
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
		return operand, nil
	}
	if receiver, inner, subscript, postfix := staticPostfixSplit(expression); postfix {
		return checker.checkPostfix(receiver, inner, subscript)
	}
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if _, _, _, _, comprehension := parseListComprehension(inner); comprehension {
			// The comprehension binds its own variable; only its type is known.
			return knownOperand("List<T>"), nil
		}
		for _, item := range splitTopLevelComma(inner) {
			if _, err := checker.check(item); err != nil {
				return staticOperand{}, err
			}
		}
		return knownOperand(staticLiteralTypeNameOr(expression, "List<T>")), nil
	}
	if expression[0] == '{' && findMatchingDelimiter(expression, 0, '{', '}') == len(expression)-1 {
		mapOperand := knownOperand("Map")
		for _, pair := range splitTopLevelComma(expression[1 : len(expression)-1]) {
			if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
				value, err := checker.check(pair[separator+1:])
				if err != nil {
					return staticOperand{}, err
				}
				if checker.scope.cypher25 && value.known() {
					if mapOperand.members == nil {
						mapOperand.members = make(map[string]staticOperand)
					}
					mapOperand.members[normalizePropertyKey(pair[:separator])] = value
				}
			}
		}
		return mapOperand, nil
	}
	if variable, items, projection := staticMapProjectionSplit(expression); projection {
		// In Cypher 5 a temporal value or duration projects its fields;
		// reading one it doesn't have is a runtime error, as for d.field.
		if receiver := knownOperand(checker.scope.typeOf(variable)); receiver.kind != "" && !staticProjectable(receiver.kind, checker.scope.cypher25) {
			return staticOperand{}, operandMismatch(receiver, "Map, Node or Relationship")
		}
		for _, item := range splitTopLevelComma(items) {
			if separator := findTopLevelMapKeyValueSeparator(item); separator > 0 {
				if _, err := checker.check(item[separator+1:]); err != nil {
					return staticOperand{}, err
				}
			}
		}
		return knownOperand("Map"), nil
	}
	if function, arguments, call := parseFunctionCallWS(expression); call && function != "" {
		if !isQuantifierOrReduceFunction(function) {
			for index, argument := range splitTopLevelComma(arguments) {
				// normalize(s, NFC) names its normal form, not a variable.
				if index == 1 && lowerASCII(function) == "normalize" {
					continue
				}
				if _, err := checker.check(argument); err != nil {
					return staticOperand{}, err
				}
			}
		}
		return knownOperand(staticFunctionResultType(function, arguments)), nil
	}
	if typeName := staticLiteralTypeName(expression); typeName != "" {
		return knownOperand(typeName), nil
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		if typeName := checker.scope.typeOf(variable); typeName != "" {
			operand := knownOperand(typeName)
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

// staticLiteralTypeNameOr is staticLiteralTypeName, or fallback when it has
// none.
func staticLiteralTypeNameOr(expression, fallback string) string {
	if typeName := staticLiteralTypeName(expression); typeName != "" {
		return typeName
	}
	return fallback
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
	switch value.(type) {
	case nil:
		return staticOperand{}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return knownOperand("Integer")
	case float32, float64:
		return knownOperand("Float")
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
		return staticOperand{kind: "Map", display: "Map, Node or Relationship"}
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

// forEachClauseOperatorExpression visits the expressions of one clause whose
// operators are type-checked (projections, WHERE, ORDER BY, SET values,
// UNWIND lists, pattern property values) and that may contain arithmetic.
// afterProjection is set for the WHERE and ORDER BY of a RETURN / WITH, which
// see the projection's aliases. It allocates only for clauses with arithmetic.
func (e *StorageExecutor) forEachClauseOperatorExpression(clause pipelineClause, visit func(expression string, afterProjection bool) error) error {
	text := strings.TrimSpace(clause.text)
	if !mayNeedStaticTypeCheck(text) {
		return nil
	}
	visitPatternValues := func(pattern string) error {
		for index := 0; index < len(pattern); index++ {
			switch pattern[index] {
			case '\'', '"':
				index = skipQuotedSemanticText(pattern, index) - 1
			case '{':
				closing := findMatchingDelimiter(pattern, index, '{', '}')
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
		predicate := body[where+len("WHERE"):]
		for _, keyword := range [...]string{"ORDER BY", "SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(predicate, keyword); index >= 0 {
				predicate = predicate[:index]
			}
		}
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
		body := strings.TrimSpace(text[len(keyword):])
		projection, rest := splitWithProjection(body)
		projection, _ = cutDistinct(projection)
		if mayNeedStaticTypeCheck(projection) {
			for _, item := range splitTopLevelComma(projection) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
				if mayNeedStaticTypeCheck(expression) {
					if err := visit(expression, false); err != nil {
						return err
					}
				}
			}
		}
		if mayNeedStaticTypeCheck(rest) {
			return forEachProjectedTailExpression(projection, rest, func(expression string) error {
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
func forEachProjectedTailExpression(projection, rest string, visit func(expression string) error) error {
	cutAt := func(text string, keywords ...string) string {
		for _, keyword := range keywords {
			if index := topLevelKeywordIndex(text, keyword); index >= 0 {
				text = text[:index]
			}
		}
		return text
	}
	if where := topLevelKeywordIndex(rest, "WHERE"); where >= 0 {
		if err := visit(cutAt(rest[where+len("WHERE"):], "ORDER BY", "SKIP", "LIMIT")); err != nil {
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
		return parseProjectionExprAlias(strings.TrimSpace(items[index]))
	}
	for _, term := range parseOrderByClause(cutAt(rest[order+len("ORDER BY"):], "SKIP", "LIMIT", "WHERE")) {
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
func (e *StorageExecutor) validateStaticOperatorTypes(clause pipelineClause, scope staticTypeScope, projectedScope func() staticTypeScope, params map[string]interface{}) error {
	if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
		predicate := clause.text[where+len("WHERE"):]
		for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(predicate, keyword); index >= 0 {
				predicate = predicate[:index]
			}
		}
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
	return e.forEachClauseOperatorExpression(clause, func(expression string, afterProjection bool) error {
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

// validateStaticOperatorParameters type-checks the operators of a statement
// against its parameter values, as Neo4j does when it compiles a statement
// with its parameters ("Type mismatch for parameter 's': …"). It parses the
// statement only when a parameter next to an arithmetic operator has a value
// that operator could reject: numbers never are, and neither are strings or
// lists next to +, so parameterized arithmetic on hot paths costs one scan.
func (e *StorageExecutor) validateStaticOperatorParameters(cypher string, params map[string]interface{}, cypher25 bool) error {
	if len(params) == 0 || !parameterMayMismatchOperator(cypher, params) {
		return nil
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		if err := e.validateStaticOperatorTypes(clause, staticTypeScope{cypher25: cypher25}, nil, params); err != nil {
			return err
		}
	}
	return nil
}

// parameterMayMismatchOperator reports whether a $parameter next to an
// arithmetic operator in cypher holds a value that operator could reject.
func parameterMayMismatchOperator(cypher string, params map[string]interface{}) bool {
	var previous byte
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"':
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
			if !isWhitespace(cypher[index]) {
				previous = cypher[index]
			}
			continue
		}
		start := index + 1
		end := start
		for end < len(cypher) && isIdentByte(cypher[end]) {
			end++
		}
		if end == start {
			previous = '$'
			continue
		}
		before := previous
		previous = '$'
		after := queryGapEnd(cypher, end)
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
