package cypher

import (
	"fmt"
	"strings"
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

// staticOperand is an operand's static type: kind drives the operator rules,
// display is how the error names it, parameter the parameter it comes from.
type staticOperand struct {
	kind      string
	display   string
	parameter string
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
	"toboolean": "Boolean",
	"keys":      "List<String>", "labels": "List<String>", "split": "List<String>",
	"date": "Date", "datetime": "DateTime", "localdatetime": "LocalDateTime",
	"time": "Time", "localtime": "LocalTime", "duration": "Duration",
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

// checkOperator applies Neo4j's operand rules for left op right and returns
// the result type.
func checkOperator(op byte, left, right staticOperand) (staticOperand, error) {
	switch op {
	case '+':
		switch {
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
			return staticOperand{}, operandMismatch(right, "Float, Integer or Duration")
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
		if left.known() && right.known() {
			return knownOperand("Float"), nil
		}
		return staticOperand{}, nil
	}
	if left.numeric() && right.numeric() {
		if left.kind == "Integer" && right.kind == "Integer" {
			return knownOperand("Integer"), nil
		}
		return knownOperand("Float"), nil
	}
	return staticOperand{}, nil
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
	if _, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		return checker.checkAtom(expression)
	}
	if expression[0] == '{' && findMatchingDelimiter(expression, 0, '{', '}') == len(expression)-1 {
		return checker.checkAtom(expression)
	}
	if !isOperatorExpressionText(expression) {
		return staticOperand{}, nil
	}
	if inner, enclosed := stripEnclosingExpressionParentheses(expression); enclosed {
		return checker.check(inner)
	}
	if startsWithKeywordFold(expression, "CASE") {
		return staticOperand{}, nil
	}
	for _, keyword := range []string{"OR", "XOR", "AND"} {
		if index := topLevelKeywordIndex(expression, keyword); index > 0 {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			if _, err := checker.check(expression[index+len(keyword):]); err != nil {
				return staticOperand{}, err
			}
			return knownOperand("Boolean"), nil
		}
	}
	if startsWithKeywordFold(expression, "NOT") {
		if _, err := checker.check(expression[len("NOT"):]); err != nil {
			return staticOperand{}, err
		}
		return knownOperand("Boolean"), nil
	}
	for _, keyword := range staticComparisonKeywords {
		if index := topLevelKeywordIndex(expression, keyword); index > 0 {
			if _, err := checker.check(expression[:index]); err != nil {
				return staticOperand{}, err
			}
			if _, err := checker.check(expression[index+len(keyword):]); err != nil {
				return staticOperand{}, err
			}
			return knownOperand("Boolean"), nil
		}
	}
	for _, operator := range staticComparisonOperators {
		if left, right, found := splitByOperatorWithOptions(expression, operator, false, true); found && left != "" && right != "" {
			if _, err := checker.check(left); err != nil {
				return staticOperand{}, err
			}
			if _, err := checker.check(right); err != nil {
				return staticOperand{}, err
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
		return checkOperator(operator, leftType, rightType)
	}
	if expression[0] == '-' && len(expression) > 1 {
		operand, err := checker.check(expression[1:])
		if err != nil {
			return staticOperand{}, err
		}
		if operand.known() && !operand.numeric() {
			return staticOperand{}, operandMismatch(operand, "Float or Integer")
		}
		return operand, nil
	}
	return checker.checkAtom(expression)
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
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if _, _, _, _, comprehension := parseListComprehension(inner); comprehension {
			return staticOperand{}, nil
		}
		for _, item := range splitTopLevelComma(inner) {
			if _, err := checker.check(item); err != nil {
				return staticOperand{}, err
			}
		}
		return knownOperand(staticLiteralTypeNameOr(expression, "List<T>")), nil
	}
	if expression[0] == '{' && findMatchingDelimiter(expression, 0, '{', '}') == len(expression)-1 {
		for _, pair := range splitTopLevelComma(expression[1 : len(expression)-1]) {
			if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
				if _, err := checker.check(pair[separator+1:]); err != nil {
					return staticOperand{}, err
				}
			}
		}
		return knownOperand("Map"), nil
	}
	if function, arguments, call := parseFunctionCallWS(expression); call && function != "" {
		if !isQuantifierOrReduceFunction(function) {
			for _, argument := range splitTopLevelComma(arguments) {
				if _, err := checker.check(argument); err != nil {
					return staticOperand{}, err
				}
			}
		}
		return knownOperand(staticFunctionResultTypes[strings.ToLower(function)]), nil
	}
	if typeName := staticLiteralTypeName(expression); typeName != "" {
		return knownOperand(typeName), nil
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		if typeName := checker.scope.typeOf(variable); typeName != "" {
			return knownOperand(typeName), nil
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
	switch strings.ToLower(name) {
	case "all", "any", "none", "single", "reduce", "exists":
		return true
	}
	return false
}

// staticParameterOperand is a parameter value's static type, as Neo4j names
// it when it type-checks a statement with its parameters: a list parameter is
// List<T>, a map parameter "Map, Node or Relationship".
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
	if isRuntimeList(value) {
		return knownOperand("List<T>")
	}
	if _, isMap := toStringAnyMap(value); isMap {
		return staticOperand{kind: "Map", display: "Map, Node or Relationship"}
	}
	return staticOperand{}
}

// clauseOperatorExpressions returns the expressions of one clause whose
// operators are type-checked: projections, WHERE, ORDER BY, SET values,
// UNWIND lists and pattern property values.
func (e *StorageExecutor) clauseOperatorExpressions(clause pipelineClause) []string {
	text := strings.TrimSpace(clause.text)
	var expressions []string
	addPatternValues := func(pattern string) {
		for index := 0; index < len(pattern); index++ {
			switch pattern[index] {
			case '\'', '"':
				index = skipQuotedSemanticText(pattern, index) - 1
			case '{':
				closing := findMatchingDelimiter(pattern, index, '{', '}')
				if closing < 0 {
					return
				}
				for _, pair := range splitTopLevelComma(pattern[index+1 : closing]) {
					if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
						expressions = append(expressions, pair[separator+1:])
					}
				}
				index = closing
			}
		}
	}
	addPredicate := func(body string) {
		if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
			predicate := body[where+len("WHERE"):]
			for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
				if index := topLevelKeywordIndex(predicate, keyword); index >= 0 {
					predicate = predicate[:index]
				}
			}
			expressions = append(expressions, predicate)
		}
	}
	switch clause.kind {
	case pipelineClauseReturn, pipelineClauseWith:
		keyword := "RETURN"
		if clause.kind == pipelineClauseWith {
			keyword = "WITH"
		}
		body := strings.TrimSpace(text[len(keyword):])
		projection, _ := splitWithProjection(body)
		if startsWithKeywordFold(projection, "DISTINCT") {
			projection = projection[len("DISTINCT"):]
		}
		for _, item := range splitTopLevelComma(projection) {
			expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
			expressions = append(expressions, expression)
		}
		addPredicate(body)
		if order := topLevelKeywordIndex(body, "ORDER BY"); order >= 0 {
			orderBody := body[order+len("ORDER BY"):]
			for _, keyword := range []string{"SKIP", "LIMIT", "WHERE"} {
				if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
					orderBody = orderBody[:index]
				}
			}
			for _, term := range parseOrderByClause(orderBody) {
				expressions = append(expressions, term.column)
			}
		}
	case pipelineClauseMatch, pipelineClauseOptionalMatch:
		pattern := text
		if where := topLevelKeywordIndex(text, "WHERE"); where >= 0 {
			pattern = text[:where]
		}
		addPatternValues(pattern)
		addPredicate(text)
	case pipelineClauseCreate, pipelineClauseMerge:
		pattern := text
		for _, keyword := range []string{"ON CREATE SET", "ON MATCH SET"} {
			if index := findKeywordIndexInContext(pattern, keyword); index >= 0 {
				pattern = pattern[:index]
			}
		}
		addPatternValues(pattern)
	case pipelineClauseSet:
		for _, assignment := range e.splitSetAssignments(strings.TrimSpace(text[len("SET"):])) {
			if operator := strings.Index(assignment, "="); operator > 0 {
				expressions = append(expressions, assignment[operator+1:])
			}
		}
	case pipelineClauseUnwind:
		body := strings.TrimSpace(text[len("UNWIND"):])
		if as := findKeywordIndexInContext(body, "AS"); as >= 0 {
			expressions = append(expressions, body[:as])
		}
	}
	return expressions
}

// validateStaticOperatorTypes type-checks the operators of one clause's
// expressions with the clause's variable types.
func (e *StorageExecutor) validateStaticOperatorTypes(clause pipelineClause, scope staticTypeScope, params map[string]interface{}) error {
	checker := staticOperatorChecker{scope: scope, params: params}
	for _, expression := range e.clauseOperatorExpressions(clause) {
		if _, err := checker.check(expression); err != nil {
			return err
		}
	}
	return nil
}

// validateStaticOperatorParameters type-checks the operators of a statement
// against its parameter values, as Neo4j does when it compiles a statement
// with its parameters ("Type mismatch for parameter 's': …"). It parses the
// statement only when a parameter next to an arithmetic operator has a value
// that operator could reject: numbers never are, and neither are strings or
// lists next to +, so parameterized arithmetic on hot paths costs one scan.
func (e *StorageExecutor) validateStaticOperatorParameters(cypher string, params map[string]interface{}) error {
	if len(params) == 0 || !parameterMayMismatchOperator(cypher, params) {
		return nil
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		if err := e.validateStaticOperatorTypes(clause, staticTypeScope{}, params); err != nil {
			return err
		}
	}
	return nil
}

// parameterMayMismatchOperator reports whether a $parameter next to an
// arithmetic operator in cypher holds a value that operator could reject.
func parameterMayMismatchOperator(cypher string, params map[string]interface{}) bool {
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"':
			index = skipQuotedSemanticText(cypher, index) - 1
			continue
		case '$':
		default:
			continue
		}
		start := index + 1
		end := start
		for end < len(cypher) && isIdentByte(cypher[end]) {
			end++
		}
		if end == start {
			continue
		}
		before := index - 1
		for before >= 0 && isWhitespace(cypher[before]) {
			before--
		}
		after := skipSpaces(cypher, end)
		var operators [2]byte
		if before >= 0 && strings.IndexByte("+-*/%^", cypher[before]) >= 0 {
			operators[0] = cypher[before]
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
