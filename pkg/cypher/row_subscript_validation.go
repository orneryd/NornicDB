package cypher

import (
	"context"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
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
	var failure error
	lexicalPostfixWalk(text, func(access lexicalPostfix) bool {
		if !access.subscript {
			return true
		}
		index, end := access.open, access.close
		receiver := lexicalExpressionText(text[access.receiver.start:access.receiver.end])
		if _, err := checker.checkPostfix(receiver, text[index+1:end], true); err != nil {
			local, shadowed := withoutLocalVariables(scope, text, index)
			if !shadowed {
				failure = err
				return false
			}
			if _, err := (staticOperatorChecker{scope: local, params: scope.params}).checkPostfix(receiver, text[index+1:end], true); err != nil {
				failure = err
				return false
			}
		}
		return true
	})
	return failure
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
	baseExpression := lexicalExpressionText(expression[receiverStart:open])
	// The trailing bracket must be a postfix access, not a list following
	// an operator or clause keyword. Validate syntax before evaluating it.
	if !isOperandExpressionText(baseExpression) || (isCypherKeyword(baseExpression) && !isLiteralKeyword(baseExpression)) {
		return nil
	}
	indexExpression := strings.TrimSpace(expression[open+1 : len(expression)-1])
	if err := e.validateRowSubscriptTypes(baseExpression, row); err != nil {
		return err
	}
	base, baseOK, err := e.evaluateRowValue(baseExpression, row)
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
func rowSubscriptStart(expr string) int {
	if access, ok := lexicalTrailingPostfix(expr); ok && access.subscript {
		return access.open
	}
	return -1
}

// closingBracketGroupStart consumes the shared forward group events. List
// literals remain groups even when they have no postfix receiver.
func closingBracketGroupStart(expression string) int {
	token, ok := readExpressionLexicalFacts(expression).tokenBefore(len(expression))
	if ok && token.opened >= 0 && expression[token.opened] == '[' {
		return token.opened
	}
	return -1
}

// rowSubscriptReceiverStart is a compatibility adapter over the primary span
// recorded by the shared forward reader; it never guesses from closing text.
func rowSubscriptReceiverStart(expression string, open int) int {
	if access, ok := lexicalPostfixAt(expression, open); ok && access.subscript {
		return access.receiver.start
	}
	return -1
}

func matchingRowDelimiterStart(expression string, closeIndex int, open, close byte) int {
	if closeIndex < 0 || closeIndex >= len(expression) || expression[closeIndex] != close {
		return -1
	}
	if start := lexicalGroupOpening(expression, closeIndex); start >= 0 && expression[start] == open {
		return start
	}
	return -1
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

func (e *StorageExecutor) validatePipelineSizeArguments(ctx context.Context, rows []pipelineRow, clause, keyword string) error {
	for _, expression := range projectionExpressions(ctx, clause, keyword) {
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
		// Aggregates have no value in an input row. Their argument is checked
		// by the aggregate projection after its group has been evaluated.
		if containsAggregateFunc(argument) || needsRowStatementContext(argument) {
			continue
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
