package cypher

import (
	"fmt"
	"strings"
)

// validateStaticPropertySubscripts type-checks every subscript in text
// (receiver[key], receiver[from..to]), wherever it appears, with scope's
// static types. The rules are staticOperatorChecker.checkPostfix's.
func validateStaticPropertySubscripts(text string, scope staticTypeScope) error {
	// The scan reads raw text, local variables included ([x IN l | x[0]]),
	// so it can't tell an undefined variable.
	scope.complete = false
	checker := staticOperatorChecker{scope: scope, params: scope.params}
	for index := 0; index < len(text); index++ {
		if isCypherQuoteByte(text[index]) {
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
	return matchingRowDelimiterStart(expression, len(expression)-1, '[', ']')
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
	if !isRowReceiverIdentifierByte(expression[end]) && !strings.ContainsRune(")]}", rune(expression[end])) && !isCypherQuoteByte(expression[end]) {
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
		if before >= 0 && !isRowReceiverIdentifierByte(expression[before]) && !strings.ContainsRune(")]}", rune(expression[before])) && !isCypherQuoteByte(expression[before]) {
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
		if isCypherStringQuoteByte(expression[end]) {
			return quotedRowReceiverStart(expression, end)
		}
		for start > 0 && isRowReceiverIdentifierByte(expression[start-1]) {
			start--
		}
		return start
	}
}

func matchingRowDelimiterStart(expression string, closeIndex int, open, close byte) int {
	if closeIndex < 0 || closeIndex >= len(expression) || expression[closeIndex] != close {
		return -1
	}
	for index := 0; index <= closeIndex; index++ {
		switch character := expression[index]; {
		case isCypherQuoteByte(character):
			end, closed := scanCypherQuotedText(expression, index, character)
			if !closed {
				return -1
			}
			index = end - 1
		case character == '/':
			if end := queryCommentEnd(expression, index); end >= 0 {
				index = end - 1
			}
		case character == open:
			end := findMatchingDelimiter(expression, index, rune(open), rune(close))
			if end == closeIndex {
				return index
			}
			if end < 0 {
				return -1
			}
			if end < closeIndex {
				index = end
			}
		}
	}
	return -1
}

// quotedRowReceiverStart finds a string/name token ending at end. It reads
// forward through canonical quotation tokens; reverse quote matching loses
// doubled quotes and escapes.
func quotedRowReceiverStart(expression string, end int) int {
	for index := 0; index <= end; index++ {
		switch character := expression[index]; {
		case isCypherQuoteByte(character):
			finish, closed := scanCypherQuotedText(expression, index, character)
			if !closed {
				return -1
			}
			if finish == end+1 {
				return index
			}
			index = finish - 1
		case character == '/':
			if finish := queryCommentEnd(expression, index); finish >= 0 {
				index = finish - 1
			}
		}
	}
	return -1
}

func isRowReceiverIdentifierByte(value byte) bool {
	return value == '_' || value == '.' || isCypherBacktickByte(value) || value == '$' ||
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

func validateProjectionSizeSyntax(clause, keyword string) error {
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

	}
	return nil
}
