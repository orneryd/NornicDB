package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validatePipelineProjectionSubscripts applies Cypher's runtime type rules to
// every postfix subscript before a WITH or RETURN projection materializes it.
// The expression evaluator and optimized physical plans therefore share one
// validation boundary instead of silently converting invalid access to null.
func validateStaticPropertySubscripts(text string, scope staticTypeScope) error {
	for index := 0; index < len(text); index++ {
		if text[index] == '\'' || text[index] == '"' || text[index] == '`' {
			index = skipCypherQuotedText(text, index, text[index]) - 1
			continue
		}
		if text[index] != '[' {
			continue
		}
		end := findMatchingDelimiter(text, index, '[', ']')
		if end < 0 {
			return nil
		}
		start := rowSubscriptReceiverStart(text, index)
		if start < 0 {
			continue
		}
		base, _, property := rowPropertyChainShape(strings.TrimSpace(text[start:index]))
		if !property || scope.typeOf(base) != "Node" && scope.typeOf(base) != "Relationship" {
			continue
		}
		key := strings.TrimSpace(text[index+1 : end])
		if strings.Contains(key, "..") {
			continue
		}
		if typeName := scope.staticExpressionType(key); typeName != "" && typeName != "Integer" {
			return typeNameMismatchError("Integer", typeName)
		}
	}
	return nil
}

func (e *StorageExecutor) validatePipelineProjectionSubscripts(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	body, _ = cutDistinct(body)
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	if body == "" || body == "*" {
		return nil
	}
	for _, item := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
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
	if rangeIndex := strings.Index(indexExpression, ".."); rangeIndex >= 0 {
		if !baseOK || base == nil {
			return nil
		}
		baseType := reflect.TypeOf(base)
		if baseType == nil || (baseType.Kind() != reflect.Slice && baseType.Kind() != reflect.Array) {
			return invalidSubscriptTypeError("slice receiver must be a LIST", base)
		}
		for _, boundExpression := range []string{strings.TrimSpace(indexExpression[:rangeIndex]), strings.TrimSpace(indexExpression[rangeIndex+2:])} {
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
	case *storage.Node, *storage.Edge:
		if _, valid := index.(string); !valid {
			return invalidSubscriptTypeError("entity subscript requires a STRING property key", index)
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

// closingBracketGroupStart returns where the [ … ] group that ends
// expression starts, reading brackets outside string literals and quoted
// names; -1 when expression doesn't end with a balanced group.
func closingBracketGroupStart(expression string) int {
	var opens []int
	for index := 0; index < len(expression); {
		switch c := expression[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(expression, index, c)
			continue
		case '[':
			opens = append(opens, index)
		case ']':
			if len(opens) == 0 {
				return -1
			}
			start := opens[len(opens)-1]
			opens = opens[:len(opens)-1]
			if index == len(expression)-1 {
				return start
			}
		}
		index++
	}
	return -1
}

// rowSubscriptReceiverStart finds the primary expression immediately to the
// left of a postfix subscript. It prevents validators from treating a whole
// lower-precedence expression such as `3 IN values[0..1]` as the slice
// receiver while retaining chained, parenthesized, literal, and property
// receivers.
func rowSubscriptReceiverStart(expression string, open int) int {
	end := open - 1
	for end >= 0 && isASCIIWhitespace(expression[end]) {
		end--
	}
	if end < 0 {
		return -1
	}
	start := end
	switch expression[end] {
	case ']':
		start = matchingRowDelimiterStart(expression, end, '[', ']')
		if start > 0 {
			return rowSubscriptReceiverStart(expression, start)
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
	default:
		for start > 0 && isRowReceiverIdentifierByte(expression[start-1]) {
			start--
		}
		return start
	}
}

func matchingRowDelimiterStart(expression string, closeIndex int, open, close byte) int {
	depth := 0
	for index := closeIndex; index >= 0; index-- {
		switch expression[index] {
		case close:
			depth++
		case open:
			depth--
			if depth == 0 {
				return index
			}
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
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	for _, item := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		name, argument, functionCall := parseFunctionCallWS(expression)
		if !functionCall || !strings.EqualFold(name, "size") {
			continue
		}
		argument = strings.TrimSpace(argument)
		if containsRelExistencePattern(argument) && strings.HasPrefix(argument, "(") {
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
					return typeMismatchError(sizeArgumentTypes, pathTypeMarker{})
				}
			}
		}
	}
	return nil
}
