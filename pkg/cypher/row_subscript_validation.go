package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validateStaticPropertySubscripts type-checks every subscript in text
// (receiver[key], receiver[from..to]), wherever it appears, with scope's
// static types. The rules are staticOperatorChecker.checkPostfix's.
func validateStaticPropertySubscripts(text string, scope staticTypeScope) error {
	if !strings.ContainsRune(text, '[') {
		return nil
	}
	// The scan reads raw text, local variables included ([x IN l | x[0]]),
	// so it can't tell an undefined variable.
	scope.complete = false
	checker := staticOperatorChecker{scope: scope, params: scope.params}
	syntax := readFunctionSyntax(text)
	for _, group := range syntax.groups {
		if group.open < 0 || text[group.open] != '[' {
			continue
		}
		index, end := group.open, group.close
		if end < 0 {
			return nil
		}
		start := rowSubscriptReceiverStart(text, index)
		if start < 0 {
			continue
		}
		receiver := strings.TrimSpace(text[start:index])
		// A bracket after a keyword (RETURN [..], x IN [..]) opens a list,
		// not a subscript; true, false and null are values.
		if !isOperandExpressionText(receiver) || (isCypherKeyword(receiver) && !isLiteralKeyword(receiver)) {
			continue
		}
		if _, err := checker.checkPostfix(receiver, text[index+1:end], true); err != nil {
			// The subscript may read a variable an enclosing expression
			// binds itself ([r IN l | r[0]]), not the clause's.
			local, shadowed := withoutLocalVariables(scope, text, index)
			if !shadowed {
				return err
			}
			if _, err := (staticOperatorChecker{scope: local, params: scope.params}).checkPostfix(receiver, text[index+1:end], true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validatePipelineProjectionSubscripts(rows []pipelineRow, clause, keyword string) error {
	for _, expression := range projectionExpressions(clause, keyword) {
		for _, row := range rows {
			if err := e.validateRowSubscriptTypes(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRowSubscriptTypes(expression string, row pipelineRow) error {
	expression = strings.TrimSpace(expression)
	if !strings.HasSuffix(expression, "]") {
		return nil
	}
	// The subscript is the bracket group that closes the expression; a list
	// literal or comprehension that is the whole expression ([x IN l | …])
	// is not one.
	open := closingBracketGroupStart(expression)
	if open <= 0 {
		return nil
	}
	receiverStart := rowSubscriptReceiverStart(expression, open)
	if receiverStart < 0 || receiverStart >= open {
		return nil
	}
	baseExpression := strings.TrimSpace(expression[receiverStart:open])
	indexExpression := strings.TrimSpace(expression[open+1 : len(expression)-1])
	if err := e.validateRowSubscriptTypes(baseExpression, row); err != nil {
		return err
	}
	base, baseOK, err := e.evaluateRowValue(baseExpression, row)
	if err != nil {
		return err
	}
	if from, to, slice := staticSliceBounds(indexExpression); slice {
		if !baseOK || base == nil {
			return nil
		}
		// A value that isn't a list is sliced as a list of it
		// (cypherListSlice); a type known before the statement runs is
		// checked statically.
		for _, boundExpression := range []string{strings.TrimSpace(from), strings.TrimSpace(to)} {
			if boundExpression == "" {
				continue
			}
			bound, ok, err := e.evaluateRowValue(boundExpression, row)
			if err != nil {
				return err
			}
			if ok && bound != nil && !isCypherInteger(bound) {
				return invalidSubscriptTypeError("list slice bound requires an INTEGER", bound)
			}
		}
		return nil
	}
	index, indexOK, err := e.evaluateRowValue(indexExpression, row)
	if err != nil {
		return err
	}
	if !baseOK || !indexOK || base == nil || index == nil {
		return nil
	}

	if object, isMap := toStringAnyMap(base); isMap {
		_ = object
		if _, valid := index.(string); !valid {
			return mapElementAccessByNonStringError(index)
		}
		return nil
	}
	switch base.(type) {
	case *storage.Node, *storage.Edge, storage.RemoteEntityValue:
		if _, valid := index.(string); !valid {
			return invalidSubscriptTypeError("entity subscript requires a STRING property key", index)
		}
		return nil
	}

	if key, isKey := index.(string); isKey && (isRuntimeTemporal(base) || isRuntimeDuration(base)) {
		// A temporal value's or duration's fields read like properties:
		// d['year'] is d.year, and a field it doesn't have is a TypeError.
		if _, _, supported := evaluateTemporalProperty(base, key); !supported {
			return temporalNoSuchFieldError(key)
		}
		return nil
	}
	baseType := reflect.TypeOf(base)
	if baseType == nil || (baseType.Kind() != reflect.Slice && baseType.Kind() != reflect.Array) {
		return subscriptReceiverError(base, index)
	}
	if !isCypherInteger(index) {
		return invalidSubscriptTypeError("list subscript requires an INTEGER index", index)
	}
	return nil
}

// rowSubscriptStart returns where the subscript or slice that ends expr
// starts (expr[open:] is [key] or [from..to] applied to expr[:open]), or -1:
// the bracket group that closes expr (its key may hold brackets of its own:
// m[r[k]], m[l[0].id]) on a receiver that is the whole rest of expr. The one
// reader of the form, for the row evaluator (evaluateRowValueText) and its
// compiled programs (rowValueCompiler.compile).
func rowSubscriptStart(expr string) int {
	if open := closingBracketGroupStart(expr); open > 0 && rowSubscriptReceiverStart(expr, open) == 0 {
		return open
	}
	return -1
}

// closingBracketGroupStart returns where the [ … ] group that ends
// expression starts, reading brackets outside string literals and quoted
// names; -1 when expression doesn't end with a balanced group.
func closingBracketGroupStart(expression string) int {
	for _, group := range readFunctionSyntax(expression).groups {
		if group.open >= 0 && expression[group.open] == '[' && group.close >= 0 && queryGapEnd(expression, group.close+1) == len(expression) {
			return group.open
		}
	}
	return -1
}

// rowSubscriptReceiverStart finds the primary expression immediately to the
// left of a postfix subscript. It prevents validators from treating a whole
// lower-precedence expression such as `3 IN values[0..1]` as the slice
// receiver while retaining chained, parenthesized, literal, and property
// receivers.
func rowSubscriptReceiverStart(expression string, open int) int {
	end := queryGapStart(expression, open) - 1
	if end < 0 {
		return -1
	}
	if !isRowReceiverIdentifierByte(expression[end]) && !strings.ContainsRune(")]'\"`}", rune(expression[end])) {
		return -1
	}
	start := end
	switch expression[end] {
	case ']':
		start = matchingRowDelimiterStart(expression, end, '[', ']')
		// A bracket group after an operator or a comma is a list literal,
		// the whole receiver (... + [5, 6][1]).
		before := start - 1
		for before >= 0 && isASCIIWhitespace(expression[before]) {
			before--
		}
		if before >= 0 && !isRowReceiverIdentifierByte(expression[before]) && !strings.ContainsRune(")]'\"`", rune(expression[before])) {
			return start
		}
		if start > 0 {
			receiverStart := rowSubscriptReceiverStart(expression, start)
			if receiverStart >= 0 {
				switch upperASCII(strings.TrimSpace(expression[receiverStart:start])) {
				case "RETURN", "WITH", "UNWIND", "WHERE", "IN", "AS", "THEN", "ELSE", "WHEN", "DISTINCT":
					return start
				default:
					return receiverStart
				}
			}
		}
		return start
	case ')':
		start = matchingRowDelimiterStart(expression, end, '(', ')')
		if start < 0 {
			return -1
		}
		for start > 0 && isRowReceiverIdentifierByte(expression[start-1]) {
			start--
		}
		return start
	case '\'', '"':
		quote := expression[end]
		for start--; start >= 0; start-- {
			if expression[start] == quote && !isBackslashEscaped(expression, start) {
				return start
			}
		}
		return -1
	case '}':
		open := matchingRowDelimiterStart(expression, end, '{', '}')
		if open < 0 {
			return -1
		}
		wordEnd := open - 1
		for wordEnd >= 0 && isASCIIWhitespace(expression[wordEnd]) {
			wordEnd--
		}
		word := wordEnd
		for word >= 0 && isRowReceiverIdentifierByte(expression[word]) {
			word--
		}
		// EXISTS / COUNT / COLLECT { … }: the receiver starts at its keyword.
		if _, keywordOpen, ok := subqueryExpressionKeywordAt(expression, word+1); ok && keywordOpen == open {
			return word + 1
		}
		// A map projection (n {.k}) starts at its variable; a map literal
		// ({b: true}[k], RETURN {b: true}[k]) at its brace.
		if word < wordEnd && !isCypherKeyword(expression[word+1:wordEnd+1]) {
			return word + 1
		}
		return open
	default:
		for start > 0 && isRowReceiverIdentifierByte(expression[start-1]) {
			start--
		}
		return start
	}
}

func matchingRowDelimiterStart(expression string, closeIndex int, open, close byte) int {
	for _, group := range readFunctionSyntax(expression).groups {
		if group.open >= 0 && group.close == closeIndex && expression[group.open] == open && expression[closeIndex] == close {
			return group.open
		}
	}
	return -1
}

func isRowReceiverIdentifierByte(value byte) bool {
	return value == '_' || value == '.' || value == '`' || value == '$' ||
		(value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
		(value >= '0' && value <= '9') || value >= 0x80
}

func isCypherInteger(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func invalidSubscriptTypeError(message string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentType",
		fmt.Sprintf("%s, got %s", message, cypherTypeName(value)),
	)
}

func mapElementAccessByNonStringError(value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"MapElementAccessByNonString",
		fmt.Sprintf("map subscript requires a STRING key, got %T", value),
	)
}

func (e *StorageExecutor) validatePipelineSizeArguments(rows []pipelineRow, clause, keyword string) error {
	for _, expression := range projectionExpressions(clause, keyword) {
		name, argument, functionCall := parseFunctionCallWS(expression)
		if !functionCall || !strings.EqualFold(name, "size") {
			continue
		}
		argument = strings.TrimSpace(argument)
		if containsRelationshipPattern(argument) && strings.HasPrefix(argument, "(") {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UnexpectedSyntax",
				"size() does not accept a pattern predicate; use a pattern comprehension",
			)
		}
		for _, row := range rows {
			value, ok, err := e.evaluateRowValue(argument, row)
			if err != nil {
				return err
			}
			if !ok || value == nil {
				continue
			}
			if path, isMap := toStringAnyMap(value); isMap {
				if _, isPath := path["_pathResult"]; isPath {
					return sizeArgumentError(value)
				}
			}
		}
	}
	return nil
}
