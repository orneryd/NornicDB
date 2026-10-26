package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validateStaticPropertySubscripts type-checks every subscript in text
// (receiver[key], receiver[from..to]), wherever it appears, with scope's
// static types. The rules are staticOperatorChecker.checkPostfix's.
func validateStaticPropertySubscripts(view braceView, scope staticTypeScope) error {
	text := view.text()
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
		end := cyphertext.FindMatchingDelimiter(text, index, '[', ']', cyphertext.OpeningDelimiter)
		if end < 0 {
			return nil
		}
		start := rowSubscriptReceiverStart(view, index)
		if start < 0 {
			continue
		}
		receiverView := view.window(start, index).trim()
		receiver := receiverView.text()
		// A bracket after a keyword (RETURN [..], x IN [..]) opens a list,
		// not a subscript; true, false and null are values.
		if !expressionTextAccepted(receiverView, expressionTextOperand) || (isCypherKeyword(receiver) && !isLiteralKeyword(receiver)) {
			continue
		}
		if _, err := checker.checkPostfix(receiverView, view.window(index+1, end), true); err != nil {
			// The subscript may read a variable an enclosing expression
			// binds itself ([r IN l | r[0]]), not the clause's.
			local, shadowed := withoutLocalVariables(scope, text, index)
			if !shadowed {
				return err
			}
			if _, err := (staticOperatorChecker{scope: local, params: scope.params}).checkPostfix(receiverView, view.window(index+1, end), true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRowSubscriptTypes(view braceView, row pipelineRow) error {
	view = view.trim()
	expression := view.text()
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
	receiverStart := rowSubscriptReceiverStart(view, open)
	if receiverStart < 0 || receiverStart >= open {
		return nil
	}
	baseView := view.window(receiverStart, open).trim()
	baseExpression := baseView.text()
	// The trailing bracket must be a postfix access, not a list following
	// an operator or clause keyword. Validate syntax before evaluating it.
	if !expressionTextAccepted(baseView, expressionTextOperand) || (isCypherKeyword(baseExpression) && !isLiteralKeyword(baseExpression)) {
		return nil
	}
	indexView := view.window(open+1, len(expression)-1).trim()
	indexExpression := indexView.text()
	if err := e.validateRowSubscriptTypes(baseView, row); err != nil {
		return err
	}
	base, baseOK, err := e.evaluateRowValue(baseView, row)
	if err != nil {
		return err
	}
	if !baseOK {
		return nil
	}
	if from, to, slice := staticSliceBounds(indexExpression); slice {
		if !baseOK || base == nil {
			return nil
		}
		// A value that isn't a list is sliced as a list of it
		// (cypherListSlice); a type known before the statement runs is
		// checked statically.
		for _, boundExpression := range []braceView{indexView.window(0, len(from)).trim(), indexView.window(len(indexExpression)-len(to), len(indexExpression)).trim()} {
			if boundExpression.empty() {
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
	index, indexOK, err := e.evaluateRowValue(indexView, row)
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
	if _, list := valueconvert.ReadList(base); !list {
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
func rowSubscriptStart(view braceView) int {
	expr := view.text()
	if open := closingBracketGroupStart(expr); open > 0 && rowSubscriptReceiverStart(view, open) == 0 {
		return open
	}
	return -1
}

// closingBracketGroupStart returns where the [ … ] group that ends
// expression starts, reading brackets outside string literals and quoted
// names; -1 when expression doesn't end with a balanced group.
func closingBracketGroupStart(expression string) int {
	return cyphertext.FindMatchingDelimiter(expression, len(expression)-1, '[', ']', cyphertext.ClosingDelimiter)
}

// rowSubscriptReceiverStart finds the primary expression immediately to the
// left of a postfix subscript. It prevents validators from treating a whole
// lower-precedence expression such as `3 IN values[0..1]` as the slice
// receiver while retaining chained, parenthesized, literal, and property
// receivers.
func rowSubscriptReceiverStart(view braceView, open int) int {
	expression := view.text()
	end := open - 1
	for end >= 0 && cyphertext.IsWhitespaceAt(expression, end) {
		end--
	}
	if end < 0 {
		return -1
	}
	if !isRowReceiverIdentifierAt(expression, end) && !strings.ContainsRune(")]}", rune(expression[end])) && !isCypherQuoteByte(expression[end]) {
		return -1
	}
	start := end
	switch {
	case expression[end] == ']':
		start = cyphertext.FindMatchingDelimiter(expression, end, '[', ']', cyphertext.ClosingDelimiter)
		// A bracket group after an operator or a comma is a list literal,
		// the whole receiver (... + [5, 6][1]).
		before := start - 1
		for before >= 0 && cyphertext.IsWhitespaceAt(expression, before) {
			before--
		}
		if before >= 0 && !isRowReceiverIdentifierAt(expression, before) && !strings.ContainsRune(")]}", rune(expression[before])) && !isCypherQuoteByte(expression[before]) {
			return start
		}
		if start > 0 {
			receiverStart := rowSubscriptReceiverStart(view, start)
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
	case expression[end] == ')':
		start = cyphertext.FindMatchingDelimiter(expression, end, '(', ')', cyphertext.ClosingDelimiter)
		if start < 0 {
			return -1
		}
		for start > 0 && isRowReceiverIdentifierAt(expression, start-1) {
			start--
		}
		return start
	case cyphertext.IsCloseBrace(expression[end]):
		opening, fact, found := view.braceEndingAt(end)
		if !found {
			return -1
		}
		switch fact.kind {
		case subqueryBrace:
			start := -1
			for _, span := range view.source.ProjectFrom(fact.source, fact.introducer) {
				if span.End <= opening && span.Start > start {
					start = span.Start
				}
			}
			return start
		case projectionBrace:
			start := opening - 1
			for start >= 0 && cyphertext.IsWhitespaceAt(expression, start) {
				start--
			}
			for start >= 0 && isRowReceiverIdentifierAt(expression, start) {
				start--
			}
			return start + 1
		case literalBrace:
			return opening
		default:
			return -1
		}
	default:
		if isCypherStringQuoteByte(expression[end]) {
			return quotedRowReceiverStart(expression, end)
		}
		for start > 0 && isRowReceiverIdentifierAt(expression, start-1) {
			start--
		}
		return start
	}
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

func isRowReceiverIdentifierAt(text string, index int) bool {
	return cyphertext.NamePartAt(text, index) || text[index] == '.' || isCypherBacktickByte(text[index])
}

func isCypherInteger(value interface{}) bool {
	return valueconvert.IsInteger(value)
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

func (e *StorageExecutor) validatePipelineSizeArguments(ctx context.Context, rows []pipelineRow, clause braceView, keyword string) error {
	for _, expression := range projectionExpressions(ctx, clause, keyword) {
		var callFacts1 functionCallFacts
		functionCall := inspectFunctionCall(expression.text(), qualifiedFunctionCall, "", &callFacts1)
		name, argument := callFacts1.name, callFacts1.inner
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
		// Aggregates have no value in an input row. Their argument is checked
		// by the aggregate projection after its group has been evaluated.
		if containsAggregateFunc(expression.functionArguments().trim()) || needsRowStatementContext(argument) {
			continue
		}
		for _, row := range rows {
			value, ok, err := e.evaluateRowValue(expression.functionArguments().trim(), row)
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
