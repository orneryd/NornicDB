package cypher

import (
	"fmt"
	"strings"
)

// validateStaticQuantifierTypes rejects predicates that apply numeric
// operators to statically non-numeric list elements. This is a compile-time
// Cypher type error rather than a null-producing runtime expression.
func validateStaticQuantifierTypes(cypher string) error {
	var failure error
	lexicalFunctionCallSpans(cypher, func(call functionCallSpan) bool {
		if !isQuantifierFunction(call.normalized) {
			return true
		}
		if close := call.close(cypher); close >= 0 {
			failure = validateStaticQuantifierCall(call.syntax.source(syntaxSpan{call.open + 1, close}))
		}
		return failure == nil
	})
	return failure
}

func validateStaticQuantifierCall(inner string) error {
	inner = StripComments(inner)
	inIndex := findKeywordIndexInContext(inner, "IN")
	if inIndex <= 0 {
		return nil
	}
	rest := inner[inIndex+len("IN"):]
	whereIndex := findKeywordIndexInContext(rest, "WHERE")
	if whereIndex < 0 {
		return nil
	}
	variable := strings.TrimSpace(StripComments(inner[:inIndex]))
	listExpression := strings.TrimSpace(StripComments(rest[:whereIndex]))
	predicate := strings.TrimSpace(rest[whereIndex+len("WHERE"):])
	if !isValidIdentifier(variable) || !quantifierPredicateRequiresNumbers(predicate, variable) {
		return nil
	}
	list, ok := parseLiteralValueForPipeline(listExpression)
	if !ok {
		return nil
	}
	for _, item := range toAnySlice(list) {
		if item == nil {
			continue
		}
		if _, numeric := toFloat64(item); !numeric {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("quantifier predicate applies a numeric operator to %T", item),
			)
		}
	}
	return nil
}

func quantifierPredicateRequiresNumbers(predicate, variable string) bool {
	var previous byte
	for index := 0; index < len(predicate); index++ {
		switch cypherLexicalByte(predicate[index]) {
		case cypherQuoteClass:
			previous = predicate[index]
			index = skipQuotedSemanticText(predicate, index) - 1
			continue
		case '/':
			if end := queryCommentEnd(predicate, index); end >= 0 {
				index = end - 1
				continue
			}
		}
		if !identifierAt(predicate, index, variable) {
			if !isWhitespace(predicate[index]) {
				previous = predicate[index]
			}
			continue
		}
		after := queryGapEnd(predicate, index+len(variable))
		if isNumericOnlyOperatorByte(previous) || after < len(predicate) && isNumericOnlyOperatorByte(predicate[after]) {
			return true
		}
		previous = predicate[index+len(variable)-1]
		index += len(variable) - 1
	}
	return false
}

func identifierAt(text string, index int, identifier string) bool {
	if index < 0 || index+len(identifier) > len(text) || !strings.EqualFold(text[index:index+len(identifier)], identifier) {
		return false
	}
	return (index == 0 || !isIdentByte(text[index-1])) &&
		(index+len(identifier) == len(text) || !isIdentByte(text[index+len(identifier)]))
}

func isNumericOnlyOperatorByte(character byte) bool {
	switch character {
	case '%', '*', '/', '-':
		return true
	default:
		return false
	}
}
