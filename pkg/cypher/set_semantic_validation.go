// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"math/bits"
	"reflect"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// validateSetSemanticScopes validates SET expressions against the clause
// horizon before execution. This is deliberately independent of row count:
// an undefined variable is a compile-time error even when MATCH yields no rows.
func (e *StorageExecutor) validateSetSemanticScopes(ctx context.Context, cypher string, imports ...string) error {
	if !containsKeywordOutsideStrings(cypher, "SET") && !containsKeywordOutsideStrings(cypher, "REMOVE") &&
		!containsKeywordOutsideStrings(cypher, "DELETE") && !containsKeywordOutsideStrings(cypher, "FOREACH") {
		return nil
	}
	return e.validateSetSemanticBody(ctx, readQueryBodyText(ctx, cypher, 0, len(cypher)), newSemanticBindingScope(), imports...)
}

func (e *StorageExecutor) validateSetSemanticBody(ctx context.Context, body queryBody, scope *semanticBindingScope, imports ...string) error {
	clauses, ok := splitPipelineClauses(ctx, body)
	if !ok {
		return nil
	}
	return e.validateMutationClauseScopes(ctx, clauses, scope, imports...)
}

func (e *StorageExecutor) validateMutationClauseScopes(ctx context.Context, clauses []pipelineClause, scope *semanticBindingScope, imports ...string) error {
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
			addPipelinePatternBindings(patternBindings, clause)
			for name := range patternBindings {
				scope.bind(name)
			}
		case pipelineClauseWith:
			scope = projectedBindingScope(ctx, scope, clause)
			for _, name := range imports {
				scope.bind(name)
			}
		case pipelineClauseLet:
			if err := bindPipelineLet(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause); alias != "" {
				scope.bind(alias)
			}
		case pipelineClauseSet:
			if err := e.validateSetClauseScope(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseForeach:
			body := readQueryBody(clause.syntax)
			if body.status != queryBodyRead || body.foreach == nil {
				return invalidForeachError()
			}
			child := newSemanticBindingScope()
			for name := range scope.names {
				child.bind(name)
			}
			child.bind(body.foreach.variable)
			if err := e.validateMutationClauseScopes(ctx, body.foreach.updates, child); err != nil {
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
		if word, ok := trailingWordAfterOperand(expression); ok {
			// One expression per target: DETACH DELETE n COMMIT is Neo4j
			// 5.26.30's "Invalid input 'COMMIT'"; the root variable was
			// deleted and the rest ignored (#741).
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInput(word))
		}
		if root := deleteExpressionRootIdentifier(expression); root != "" && !scope.contains(root) {
			return deleteUndefinedVariableError(expression)
		}
	}
	return nil
}

// trailingWordAfterOperand returns the word that follows a complete operand
// at the top level of expression with no operator between (n COMMIT,
// a.b c), which no expression has; operator keywords (AND, IN, IS, …) are
// not such words.
func trailingWordAfterOperand(expression string) (string, bool) {
	depth := 0
	operandEnded := false
	for i := 0; i < len(expression); {
		c := expression[i]
		switch {
		case isCypherQuoteByte(c):
			i = skipCypherQuotedText(expression, i, c)
			operandEnded = depth == 0
			continue
		case c == '(' || c == '[' || c == '{':
			depth++
			operandEnded = false
		case c == ')' || c == ']' || c == '}':
			depth--
			operandEnded = depth == 0
		case cyphertext.IsWhitespaceAt(expression, i):
		case depth == 0 && cyphertext.NameStartAt(expression, i):
			word, _ := cyphertext.ScanIdentifier(expression, i)
			if operandEnded && !isExpressionOperatorWord(word.Value) {
				return word.Value, true
			}
			operandEnded = !isExpressionOperatorWord(word.Value)
			i = word.End
			continue
		case depth == 0 && c == '.':
			// a property name follows: part of the operand
			operandEnded = false
		case depth == 0 && isDigitByte(c):
			operandEnded = true
		default:
			operandEnded = false
		}
		i++
	}
	return "", false
}

// isExpressionOperatorWord reports whether word is an operator or CASE
// keyword that may follow an operand.
func isExpressionOperatorWord(word string) bool {
	switch upperASCII(word) {
	case "AND", "OR", "XOR", "NOT", "IN", "IS", "CONTAINS", "STARTS", "ENDS", "WITH", "NULL", "AS",
		"CASE", "WHEN", "THEN", "ELSE", "END":
		return true
	}
	return false
}

// deleteClauseTargets splits a [DETACH] DELETE clause into its target
// expressions, for every check and the pipeline step that read them. ok is
// false for text that isn't a DELETE clause.
func deleteClauseTargets(clause string) (targets []string, detach, ok bool) {
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "DETACH") {
		detach, body = true, body[queryGapEnd(body, len("DETACH")):]
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
			if target = normalizeProjectionColumnName(target); cyphertext.IsIdentifier(target) && !scope.contains(target) {
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
		// A literal source here, in item order; the shared clause validator
		// checks every source with the statement's types
		// (staticSetItemsTokenError).
		if err := setSourceTypeError(property, operator, (staticTypeScope{}).operandOf(expression)); err != nil {
			return err
		}
	}
	return nil
}

// validateWriteExpressionScope checks an expression a SET or REMOVE item
// evaluates (a value, a dynamic key or label): complete operands and bound
// variables. Its functions are checked with the statement's.
func validateWriteExpressionScope(expression string, scope *semanticBindingScope) error {
	if err := validateExpressionOperandCompleteness(expression); err != nil {
		return err
	}
	if missing := firstUndefinedSetExpressionVariable(expression, scope); missing != "" {
		return createUndefinedVariableError(missing)
	}
	return nil
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
	// Both maskers blank in place: offsets are the expression's.
	locals := newLocalReader(expression)
	var names []string
	delimiters := make([]byte, 0, 8)
	for index := 0; index < len(expression); {
		if end := queryGapEnd(expression, index); end > index {
			index = end
			continue
		}
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
						if !locals.shadows(reference, index) {
							names = append(names, reference)
						}
					}
					index = closing + 1
					continue
				}
			}
		}
		if end, skipped := skipCypherLexicalToken(expression, index); skipped {
			index = end
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
					if !locals.shadows(reference, index) {
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
		name, ok := cyphertext.ScanIdentifier(expression, index)
		if !ok {
			index++
			continue
		}
		if index > 0 && cyphertext.IsDigit(expression[index-1]) {
			index = name.End
			continue
		}
		previous := cyphertext.NeighborByte(expression, index, cyphertext.Before)
		following := cyphertext.NeighborByte(expression, name.End, cyphertext.AtOrAfter)
		upper := upperASCII(name.Value)
		if following == '(' && (upper == "TRIM" || upper == "NORMALIZE") {
			open := skipSpaces(expression, name.End)
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
							if !locals.shadows(reference, open) {
								names = append(names, reference)
							}
						}
					}
					index = closing + 1
					continue
				}
			}
		}
		if functionEnd, call := qualifiedFunctionNameEnd(expression, name.End); call {
			delimiters = append(delimiters, '(')
			index = functionEnd + 1
			continue
		}
		// A subquery expression (EXISTS / COUNT / COLLECT { … }) binds its own
		// variables and sees the outer ones; its body isn't an expression.
		if _, open, subquery := subqueryExpressionKeywordAt(expression, index); subquery {
			if closing := cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter); closing > name.End {
				index = closing + 1
				continue
			}
		}
		mapKey := following == ':' && len(delimiters) > 0 && delimiters[len(delimiters)-1] == '{' && (previous == '{' || previous == ',')
		// x IS NFC NORMALIZED: the normal form names a form, not a variable.
		normalForm := false
		switch upper {
		case "NFC", "NFD", "NFKC", "NFKD":
			form, word := cyphertext.ScanIdentifier(expression, skipSpaces(expression, name.End))
			normalForm = word && strings.EqualFold(form.Value, "NORMALIZED")
		}
		keyword := setExpressionKeyword(upper)
		// END is also a legal variable, notably the operand in NOT end +
		// another CASE. Preserve the full expression when asking the CASE
		// grammar owner, before evaluation splits it into atomic operands.
		if upper == "END" && !caseKeywordAt(expression, index, "END") {
			keyword = false
		}
		if previous != '$' && previous != '.' && following != '(' && !mapKey && !normalForm && !keyword {
			if !locals.shadows(name.Value, index) {
				names = append(names, name.Value)
			}
		}
		index = name.End
		if following == ':' && !mapKey {
			// The labels of n:A:B and of a label expression n:A|B&!C or
			// n:%: names of labels, not variables.
			labelStart := skipSpaces(expression, name.End)
			for labelStart < len(expression) && strings.IndexByte(":|&", expression[labelStart]) >= 0 {
				labelName := skipSpaces(expression, labelStart+1)
				for labelName < len(expression) && expression[labelName] == '!' {
					labelName = skipSpaces(expression, labelName+1)
				}
				labelEnd := labelName + 1
				if labelName >= len(expression) || expression[labelName] != '%' {
					label, valid := cyphertext.ScanIdentifier(expression, labelName)
					if !valid {
						break
					}
					labelEnd = label.End
				}
				labelStart = skipSpaces(expression, labelEnd)
				index = labelStart
			}
		}
	}
	return names
}

// expressionLocalBindings returns the variables an expression binds itself,
// at any depth: every declaration localBindingAt reads (list and named
// pattern comprehensions, reduce / all / any / none / single / filter). It
// is the names-only view of the one declaration reader, for the caches and
// pre-filters in front of localVariableShadowsAt.
func expressionLocalBindings(expression string) map[string]struct{} {
	locals := make(map[string]struct{})
	for index := 0; index < len(expression); index++ {
		switch c := expression[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, c) - 1
		case '/':
			if end := queryCommentEnd(expression, index); end >= 0 {
				index = end - 1
			}
		case '[', '(':
			if binding, ok := localBindingAt(expression, index); ok {
				for declared := 0; declared < binding.count; declared++ {
					locals[binding.names[declared]] = struct{}{}
				}
			}
		}
	}
	return locals
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

// storablePropertyValue is the value a property write stores, or the
// TypeError of a value a property cannot store: value normalized first
// (normalizedPropValue: Go integers and times become Cypher values, in lists
// too), then checked by the property value rule (validateSetPropertyValue),
// whose int/float list coercion copies only a mixed numeric list. rewritten
// reports whether stored is not value itself. Every SET route checks its
// values here, so a value is storable or not whichever route writes it: a
// map entry as a direct property value (#1055).
func storablePropertyValue(value interface{}) (stored interface{}, rewritten bool, err error) {
	stored, rewritten = normalizedPropValue(value)
	validated, coerced, err := validateSetPropertyValue(stored)
	if err != nil {
		return nil, false, err
	}
	return validated, rewritten || coerced, nil
}

func validateSetPropertyValue(value interface{}) (stored interface{}, rewritten bool, err error) {
	if value == nil {
		return value, false, nil
	}
	if _, invalid := value.(*storage.Node); invalid {
		return nil, false, invalidSetPropertyType(value)
	}
	if _, invalid := value.(*storage.Edge); invalid {
		return nil, false, invalidSetPropertyType(value)
	}
	if _, invalid := value.(*PathResult); invalid {
		return nil, false, invalidSetPropertyType(value)
	}
	switch value.(type) {
	case []float64, []float32, []int64, []int, []int32, []string, []bool:
		return value, false, nil // typed lists of primitives (e.g. embeddings) are valid as-is
	}
	typeOf := reflect.TypeOf(value)
	if typeOf != nil && typeOf.Kind() == reflect.Map {
		return nil, false, invalidSetPropertyType(value)
	}
	items, list := toInterfaceSlice(value)
	if !list {
		return value, false, nil
	}
	if len(items) == 0 {
		return value, false, nil
	}
	// Neo4j's array property rule (#643): elements must share one primitive
	// or temporal kind, null is not storable, and an int/float mix is stored
	// as floats. A mixed numeric list is copied so the coerced
	// floats are stored without changing the caller's list. The kind bitmask
	// keeps homogeneous-list validation allocation-free.
	var kinds uint16
	for _, item := range items {
		if item == nil {
			return nil, false, newSemanticError(
				"Neo.ClientError.Statement.TypeError",
				"InvalidPropertyType",
				"Collections containing null values can not be stored in properties.",
			)
		}
		itemType := reflect.TypeOf(item)
		if itemType != nil && (itemType.Kind() == reflect.Map || itemType.Kind() == reflect.Slice || itemType.Kind() == reflect.Array) {
			return nil, false, invalidSetPropertyType(value)
		}
		if _, invalid := item.(*storage.Node); invalid {
			return nil, false, invalidSetPropertyType(value)
		}
		if _, invalid := item.(*storage.Edge); invalid {
			return nil, false, invalidSetPropertyType(value)
		}
		kinds |= 1 << propertyArrayElementOf(item)
	}
	if kinds&(1<<arrayElemOther) != 0 {
		return nil, false, invalidSetPropertyType(value)
	}
	numIntFloat := uint16(1<<arrayElemInt) | uint16(1<<arrayElemFloat)
	if bits.OnesCount16(kinds) > 2 ||
		(bits.OnesCount16(kinds) == 2 && kinds&numIntFloat != numIntFloat) {
		return nil, false, newSemanticError(
			"Neo.ClientError.Statement.TypeError",
			"InvalidPropertyType",
			"Neo4j only supports a subset of Cypher types for storage as singleton or array properties.",
		)
	}
	if kinds == 1<<arrayElemPoint {
		first, _ := pointValue(items[0])
		for _, item := range items[1:] {
			if point, _ := pointValue(item); point.SRID != first.SRID {
				return nil, false, newSemanticError(
					"Neo.ClientError.Statement.TypeError",
					"InvalidPropertyType",
					"Collections containing point values with different CRS can not be stored in properties.",
				)
			}
		}
	}
	if kinds == numIntFloat {
		items = append([]interface{}(nil), items...)
		for i, item := range items {
			if f, ok := toFloat64(item); ok {
				items[i] = f
			}
		}
		return items, true, nil
	}
	return value, false, nil
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
	for key, value := range properties {
		stored, _, err := storablePropertyValue(value)
		if err != nil {
			return err
		}
		properties[key] = stored
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
