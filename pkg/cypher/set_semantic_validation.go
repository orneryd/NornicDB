package cypher

import (
	"fmt"
	"math/bits"
	"reflect"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validateSetSemanticScopes validates SET expressions against the clause
// horizon before execution. This is deliberately independent of row count:
// an undefined variable is a compile-time error even when MATCH yields no rows.
func (e *StorageExecutor) validateSetSemanticScopes(cypher string) error {
	if !containsKeywordOutsideStrings(cypher, "SET") && !containsKeywordOutsideStrings(cypher, "REMOVE") &&
		!containsKeywordOutsideStrings(cypher, "DELETE") && !containsKeywordOutsideStrings(cypher, "FOREACH") {
		return nil
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	return e.validateMutationClauseScopes(clauses, newSemanticBindingScope())
}

func (e *StorageExecutor) validateMutationClauseScopes(clauses []pipelineClause, scope *semanticBindingScope) error {
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseMerge, pipelineClauseCreate:
			for _, name := range extractNodeVariables(clause.text) {
				scope.bind(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bind(name)
			}
			// Path variables (p = (…)), with the pipeline's own binder.
			patternBindings := make(map[string]struct{})
			addPipelinePatternBindings(e, patternBindings, clause.text, pipelineClauseKeyword(clause.kind))
			for name := range patternBindings {
				scope.bind(name)
			}
		case pipelineClauseWith:
			scope = projectedBindingScope(scope, clause.text)
		case pipelineClauseLet:
			if err := bindPipelineLet(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope.bind(alias)
			}
		case pipelineClauseSet:
			if err := e.validateSetClauseScope(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseForeach:
			variable, _, updates, err := parsePipelineForeach(clause.text)
			if err != nil {
				return err
			}
			child := newSemanticBindingScope()
			for name := range scope.names {
				child.bind(name)
			}
			child.bind(variable)
			if err := e.validateMutationClauseScopes(updates, child); err != nil {
				return err
			}
		case pipelineClauseRemove:
			if err := validateRemoveClauseScope(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseDelete:
			if err := validateDeleteClauseScope(scope, clause.text); err != nil {
				return err
			}
		}
	}
	return nil
}

// pipelineClauseKeyword is the leading keyword of a pattern clause kind.
func pipelineClauseKeyword(kind pipelineClauseKind) string {
	switch kind {
	case pipelineClauseOptionalMatch:
		return "OPTIONAL MATCH"
	case pipelineClauseMerge:
		return "MERGE"
	case pipelineClauseCreate:
		return "CREATE"
	default:
		return "MATCH"
	}
}

// validateRemoveClauseScope rejects a REMOVE item (m.x, m:L, m[k],
// m:$(e)) whose variable is not bound, as SET does and as Neo4j does
// ("Variable `m` not defined"), and checks the expressions of its dynamic
// keys and labels like a SET value's (validateWriteExpressionScope).
func validateRemoveClauseScope(scope *semanticBindingScope, clause string) error {
	items, err := parseRemoveItems(strings.TrimSpace(clause[len("REMOVE"):]))
	if err != nil {
		return err
	}
	for _, item := range items {
		if !scope.contains(item.variable) {
			return createUndefinedVariableError(item.variable)
		}
		expressions := make([]string, 0, len(item.labels)+1)
		if item.key != "" {
			expressions = append(expressions, item.key)
		}
		for _, label := range item.labels {
			if label.expression != "" {
				expressions = append(expressions, label.expression)
			}
		}
		for _, expression := range expressions {
			if err := validateWriteExpressionScope(expression, scope); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateDeleteClauseScope rejects a DELETE target whose root variable is not
// bound (deleteExpressionRootIdentifier), with the error the pipeline DELETE
// step raises at run time, so a subquery body is checked before it runs.
func validateDeleteClauseScope(scope *semanticBindingScope, clause string) error {
	targets, _, _ := deleteClauseTargets(clause)
	for _, expression := range targets {
		if root := deleteExpressionRootIdentifier(expression); root != "" && !scope.contains(root) {
			return deleteUndefinedVariableError(expression)
		}
	}
	return nil
}

// deleteClauseTargets splits a [DETACH] DELETE clause into its target
// expressions, for every check and the pipeline step that read them. ok is
// false for text that isn't a DELETE clause.
func deleteClauseTargets(clause string) (targets []string, detach, ok bool) {
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "DETACH DELETE") {
		detach, body = true, strings.TrimSpace(body[len("DETACH"):])
	}
	if !startsWithKeywordFold(body, "DELETE") {
		return nil, false, false
	}
	return splitTopLevelComma(strings.TrimSpace(body[len("DELETE"):])), detach, true
}

// deleteUndefinedVariableError is the error for a DELETE target that refers to
// an undefined variable, raised by the statement check and the pipeline step.
func deleteUndefinedVariableError(expression string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UndefinedVariable",
		fmt.Sprintf("DELETE expression %q refers to an undefined variable", strings.TrimSpace(expression)),
	)
}

// validateSetClauseScope is the statement-level check for one SET clause,
// shared by every route (MATCH, CREATE, MERGE, pipeline, fast paths): the
// assignment shapes (validatePipelineSetAssignments), bound target and value
// variables, and known functions in the assigned values.
func (e *StorageExecutor) validateSetClauseScope(scope *semanticBindingScope, clause string) error {
	body := strings.TrimSpace(clause[len("SET"):])
	assignments := splitSetAssignments(body)
	if err := validatePipelineSetAssignments(assignments); err != nil {
		return err
	}
	for _, assignment := range assignments {
		target, property, operator, expression := splitSetAssignment(assignment)
		if operator == ":" {
			// Label assignment `n:Label[:Label2]`: the variable before the
			// first colon must be bound, as for a property assignment, and so
			// must the variables a dynamic label's expression reads.
			if target = normalizeProjectionColumnName(target); isValidIdentifier(target) && !scope.contains(target) {
				return createUndefinedVariableError(target)
			}
			// The chain was read above (validatePipelineSetAssignments).
			items, _ := setLabelChainItems(expression)
			for _, item := range items {
				if item.expression == "" {
					continue
				}
				if err := validateWriteExpressionScope(item.expression, scope); err != nil {
					return err
				}
			}
			continue
		}
		if operator == "" {
			continue
		}
		if target != "" && !scope.contains(target) {
			return createUndefinedVariableError(target)
		}
		if operator == "[]=" {
			// x[key] = v: the key is an expression too.
			if err := validateWriteExpressionScope(property, scope); err != nil {
				return err
			}
		}
		if err := validateWriteExpressionScope(expression, scope); err != nil {
			return err
		}
		// Literal sources here; the statement walker
		// (validatePropertyAccessClauses) also knows WITH / UNWIND types.
		if err := setSourceTypeError(property, operator, expression, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// validateWriteExpressionScope checks an expression a SET or REMOVE item
// evaluates (a value, a dynamic key or label): complete operands, bound
// variables and known functions.
func validateWriteExpressionScope(expression string, scope *semanticBindingScope) error {
	if err := validateExpressionOperandCompleteness(expression); err != nil {
		return err
	}
	if missing := firstUndefinedSetExpressionVariable(expression, scope); missing != "" {
		return createUndefinedVariableError(missing)
	}
	return validateKnownFunctionsInExpression(expression)
}

func firstUndefinedSetExpressionVariable(expression string, scope *semanticBindingScope) string {
	for _, name := range expressionFreeVariables(expression) {
		if _, exists := scope.names[name]; !exists {
			return name
		}
	}
	return ""
}

// expressionFreeVariables returns the variables an expression reads, in order
// of appearance: identifiers outside string literals that are not parameters,
// property or map keys, function names, keywords, type names after :: or TYPED,
// or names the expression binds itself (list comprehension iterators, reduce /
// all / any / none / single).
// It is the one reference scanner for the static SET and CREATE checks.
func expressionFreeVariables(expression string) []string {
	expression = maskTypePredicateTypes(maskPathFunctionCalls(expression))
	locals := make(map[string]struct{})
	collectListComprehensionBindings(expression, locals)
	collectFunctionExpressionBindings(expression, locals)
	var names []string
	delimiters := make([]byte, 0, 8)
	for index := 0; index < len(expression); {
		character := expression[index]
		if character == '[' {
			if closing := findMatchingBracket(expression, index); closing >= 0 {
				if pattern, projection, comprehension := splitPatternComprehension(expression[index : closing+1]); comprehension {
					bindings := make(matchSemanticScope)
					addMatchPatternBindingKinds(bindings, pattern)
					if where := topLevelKeywordIndex(pattern, "WHERE"); where >= 0 {
						projection += ", " + pattern[where+len("WHERE"):]
					}
					for _, reference := range expressionFreeVariables(projection) {
						if _, bound := bindings[reference]; bound {
							continue
						}
						if _, local := locals[reference]; !local {
							names = append(names, reference)
						}
					}
					index = closing + 1
					continue
				}
			}
		}
		if character == '\'' || character == '"' || character == '`' {
			quote := character
			index++
			for index < len(expression) {
				if expression[index] == quote {
					if quote == '`' && index+1 < len(expression) && expression[index+1] == '`' {
						index += 2
						continue
					}
					index++
					break
				}
				if expression[index] == '\\' && quote != '`' && index+1 < len(expression) {
					index += 2
					continue
				}
				index++
			}
			continue
		}
		if character == '(' {
			// A pattern predicate ((n)-[:R]->(m:L)) reads the variables its
			// nodes and relationships name, never its labels, types or
			// property keys; it can't introduce one (Neo4j: "PatternExpressions
			// are not allowed to introduce new variables").
			if chainEnd, chain := relationshipChainEnd(expression, index, len(expression)); chain {
				bindings := make(matchSemanticScope)
				addMatchPatternBindingKinds(bindings, expression[index:chainEnd])
				references := make([]string, 0, len(bindings))
				for reference := range bindings {
					references = append(references, reference)
				}
				sort.Strings(references)
				for _, reference := range references {
					if _, local := locals[reference]; !local {
						names = append(names, reference)
					}
				}
				index = chainEnd
				continue
			}
		}
		switch character {
		case '(', '[', '{':
			delimiters = append(delimiters, character)
		case ')', ']', '}':
			if len(delimiters) > 0 {
				delimiters = delimiters[:len(delimiters)-1]
			}
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		if index > 0 && expression[index-1] >= '0' && expression[index-1] <= '9' {
			index = next
			continue
		}
		previous := previousSetExpressionByte(expression, index)
		following := nextSetExpressionByte(expression, next)
		upper := upperASCII(name)
		if following == '(' && (upper == "TRIM" || upper == "NORMALIZE") {
			open := skipSpaces(expression, next)
			if closing := findMatchingParen(expression, open); closing >= 0 {
				inner := expression[open+1 : closing]
				parameters, grammar := trimFromArguments(inner)
				if upper == "NORMALIZE" {
					parameters = splitTopLevelComma(inner)
					grammar = false
					if len(parameters) == 2 {
						parameters, grammar = parameters[:1], true
					}
				}
				if grammar {
					for _, parameter := range parameters {
						for _, reference := range expressionFreeVariables(parameter) {
							if _, local := locals[reference]; !local {
								names = append(names, reference)
							}
						}
					}
					index = closing + 1
					continue
				}
			}
		}
		if following == '.' {
			functionEnd := skipSpaces(expression, next)
			for functionEnd < len(expression) && expression[functionEnd] == '.' {
				_, end, identifier := scanIdentifierToken(expression, skipSpaces(expression, functionEnd+1))
				if !identifier {
					break
				}
				functionEnd = skipSpaces(expression, end)
			}
			if functionEnd < len(expression) && expression[functionEnd] == '(' {
				delimiters = append(delimiters, '(')
				index = functionEnd + 1
				continue
			}
		}
		// A subquery expression (EXISTS / COUNT / COLLECT { … }) binds its own
		// variables and sees the outer ones; its body isn't an expression.
		if following == '{' && (upper == "EXISTS" || upper == "COUNT" || upper == "COLLECT") {
			if closing := findMatchingDelimiter(expression, skipSpaces(expression, next), '{', '}'); closing > next {
				index = closing + 1
				continue
			}
		}
		mapKey := following == ':' && len(delimiters) > 0 && delimiters[len(delimiters)-1] == '{' && (previous == '{' || previous == ',')
		// x IS NFC NORMALIZED: the normal form names a form, not a variable.
		normalForm := false
		switch upper {
		case "NFC", "NFD", "NFKC", "NFKD":
			form, _, word := scanIdentifierToken(expression, skipSpaces(expression, next))
			normalForm = word && strings.EqualFold(form, "NORMALIZED")
		}
		if previous != '$' && previous != '.' && following != '(' && !mapKey && !normalForm && !setExpressionKeyword(upper) {
			if _, exists := locals[name]; !exists {
				names = append(names, name)
			}
		}
		index = next
		if following == ':' && !mapKey {
			// The labels of n:A:B and of a label expression n:A|B&!C or
			// n:%: names of labels, not variables.
			labelStart := skipSpaces(expression, next)
			for labelStart < len(expression) && strings.IndexByte(":|&", expression[labelStart]) >= 0 {
				labelName := skipSpaces(expression, labelStart+1)
				for labelName < len(expression) && expression[labelName] == '!' {
					labelName = skipSpaces(expression, labelName+1)
				}
				labelEnd := labelName + 1
				if labelName >= len(expression) || expression[labelName] != '%' {
					var valid bool
					if _, labelEnd, valid = scanIdentifierToken(expression, labelName); !valid {
						break
					}
				}
				labelStart = skipSpaces(expression, labelEnd)
				index = labelStart
			}
		}
	}
	return names
}

func collectFunctionExpressionBindings(expression string, bindings map[string]struct{}) {
	lower := lowerASCII(expression)
	for _, functionName := range []string{"reduce", "all", "any", "none", "single", "filter"} {
		searchFrom := 0
		for searchFrom < len(expression) {
			relative := strings.Index(lower[searchFrom:], functionName)
			if relative < 0 {
				break
			}
			nameStart := searchFrom + relative
			open := nameStart + len(functionName)
			for open < len(expression) && isWhitespace(expression[open]) {
				open++
			}
			if open >= len(expression) || expression[open] != '(' {
				searchFrom = nameStart + len(functionName)
				continue
			}
			close := findMatchingParen(expression, open)
			if close < 0 {
				break
			}
			inner := strings.TrimSpace(expression[open+1 : close])
			if functionName == "reduce" {
				parts := splitTopLevelComma(inner)
				if len(parts) == 2 {
					if accumulator, _, ok := scanIdentifierToken(strings.TrimSpace(parts[0]), 0); ok {
						bindings[accumulator] = struct{}{}
					}
					collectLeadingIteratorBinding(parts[1], bindings)
				}
			} else {
				collectLeadingIteratorBinding(inner, bindings)
			}
			searchFrom = open + 1
		}
	}
}

func collectLeadingIteratorBinding(expression string, bindings map[string]struct{}) {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok {
		return
	}
	for next < len(expression) && isWhitespace(expression[next]) {
		next++
	}
	if next+2 <= len(expression) && strings.EqualFold(expression[next:next+2], "IN") &&
		(next+2 == len(expression) || isWhitespace(expression[next+2]) || expression[next+2] == '(') {
		bindings[name] = struct{}{}
	}
}

func collectListComprehensionBindings(expression string, bindings map[string]struct{}) {
	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' {
			continue
		}
		start := index + 1
		for start < len(expression) && isWhitespace(expression[start]) {
			start++
		}
		name, next, ok := scanIdentifierToken(expression, start)
		if !ok {
			continue
		}
		for next < len(expression) && isWhitespace(expression[next]) {
			next++
		}
		if next+2 <= len(expression) && strings.EqualFold(expression[next:next+2], "IN") &&
			(next+2 == len(expression) || isWhitespace(expression[next+2])) {
			bindings[name] = struct{}{}
		}
	}
}

func previousSetExpressionByte(text string, index int) byte {
	for index--; index >= 0; index-- {
		if !isWhitespace(text[index]) {
			return text[index]
		}
	}
	return 0
}

func nextSetExpressionByte(text string, index int) byte {
	for index < len(text) {
		if !isWhitespace(text[index]) {
			return text[index]
		}
		index++
	}
	return 0
}

func setExpressionKeyword(token string) bool {
	if isLiteralKeyword(token) {
		return true
	}
	switch token {
	case "CASE", "WHEN", "THEN", "ELSE", "END",
		"IN", "WHERE", "AND", "OR", "XOR", "NOT", "IS", "STARTS", "ENDS",
		"WITH", "CONTAINS", "DISTINCT", "AS", "NORMALIZED":
		return true
	default:
		return false
	}
}

func validateSetPropertyValue(value interface{}) error {
	if value == nil {
		return nil
	}
	if _, invalid := value.(*storage.Node); invalid {
		return invalidSetPropertyType(value)
	}
	if _, invalid := value.(*storage.Edge); invalid {
		return invalidSetPropertyType(value)
	}
	if _, invalid := value.(*PathResult); invalid {
		return invalidSetPropertyType(value)
	}
	switch value.(type) {
	case []float64, []float32, []int64, []int, []int32, []string, []bool:
		return nil // typed lists of primitives (e.g. embeddings) are valid as-is
	}
	typeOf := reflect.TypeOf(value)
	if typeOf != nil && typeOf.Kind() == reflect.Map {
		return invalidSetPropertyType(value)
	}
	items, list := toInterfaceSlice(value)
	if !list {
		return nil
	}
	if len(items) == 0 {
		return nil
	}
	// Neo4j's array property rule (#643): elements must share one primitive
	// or temporal kind, null is not storable, and an int/float mix is stored
	// as floats. The backing []interface{} is mutated in place so the coerced
	// floats are the values that get stored. Kinds are a bitmask so the rule
	// is allocation-free per write.
	var kinds uint16
	for _, item := range items {
		if item == nil {
			return newSemanticError(
				"Neo.ClientError.Statement.TypeError",
				"InvalidPropertyType",
				"Collections containing null values can not be stored in properties.",
			)
		}
		itemType := reflect.TypeOf(item)
		if itemType != nil && (itemType.Kind() == reflect.Map || itemType.Kind() == reflect.Slice || itemType.Kind() == reflect.Array) {
			return invalidSetPropertyType(value)
		}
		if _, invalid := item.(*storage.Node); invalid {
			return invalidSetPropertyType(value)
		}
		if _, invalid := item.(*storage.Edge); invalid {
			return invalidSetPropertyType(value)
		}
		kinds |= 1 << propertyArrayElementOf(item)
	}
	if kinds&(1<<arrayElemOther) != 0 {
		return invalidSetPropertyType(value)
	}
	numIntFloat := uint16(1<<arrayElemInt) | uint16(1<<arrayElemFloat)
	if bits.OnesCount16(kinds) > 2 ||
		(bits.OnesCount16(kinds) == 2 && kinds&numIntFloat != numIntFloat) {
		return newSemanticError(
			"Neo.ClientError.Statement.TypeError",
			"InvalidPropertyType",
			"Neo4j only supports a subset of Cypher types for storage as singleton or array properties.",
		)
	}
	if kinds == 1<<arrayElemPoint {
		first, _ := pointValue(items[0])
		for _, item := range items[1:] {
			if point, _ := pointValue(item); point.SRID != first.SRID {
				return newSemanticError(
					"Neo.ClientError.Statement.TypeError",
					"InvalidPropertyType",
					"Collections containing point values with different CRS can not be stored in properties.",
				)
			}
		}
	}
	if kinds&(1<<arrayElemFloat) != 0 {
		for i, item := range items {
			if f, ok := toFloat64(item); ok {
				items[i] = f
			}
		}
	}
	return nil
}

// propertyArrayElement categorizes an array element for the Neo4j property
// array rule (#643).
type propertyArrayElement uint8

const (
	arrayElemEmpty propertyArrayElement = iota
	arrayElemInt
	arrayElemFloat
	arrayElemString
	arrayElemBool
	arrayElemDate
	arrayElemTime
	arrayElemLocalTime
	arrayElemDateTime
	arrayElemLocalDateTime
	arrayElemDuration
	arrayElemPoint
	arrayElemOther
)

func propertyArrayElementOf(item interface{}) propertyArrayElement {
	switch item.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrayElemInt
	case float32, float64:
		return arrayElemFloat
	case string:
		return arrayElemString
	case bool:
		return arrayElemBool
	case CypherDate, *CypherDate:
		return arrayElemDate
	case CypherTime, *CypherTime:
		return arrayElemTime
	case CypherLocalTime, *CypherLocalTime:
		return arrayElemLocalTime
	case CypherDateTime, *CypherDateTime:
		return arrayElemDateTime
	case CypherLocalDateTime, *CypherLocalDateTime:
		return arrayElemLocalDateTime
	case CypherDuration, *CypherDuration:
		return arrayElemDuration
	case CypherPoint, *CypherPoint:
		return arrayElemPoint
	default:
		return arrayElemOther
	}
}

// validatePropertyValues applies the property value rule
// (validateSetPropertyValue: no maps, no nested lists, no entities) to every
// value a CREATE or MERGE is about to store, as SET does (b968c6a2). It runs
// at every CREATE / MERGE write: CREATE node preparation (also the auto-commit
// node fast path) and relationships, MERGE node and relationship creation,
// and the UNWIND batch creators.
func validatePropertyValues(properties map[string]interface{}) error {
	for _, value := range properties {
		if err := validateSetPropertyValue(value); err != nil {
			return err
		}
	}
	return nil
}

func invalidSetPropertyType(value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidPropertyType",
		fmt.Sprintf("property value has unsupported type %s", cypherTypeName(value)),
	)
}
