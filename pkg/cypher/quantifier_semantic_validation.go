package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// validateStaticQuantifierTypes rejects predicates that apply numeric
// operators to statically non-numeric list elements. This is a compile-time
// Cypher type error rather than a null-producing runtime expression.
func validateStaticQuantifierTypes(cypher string) error {
	options := defaultKeywordScanOpts()
	options.SkipParens = false
	options.SkipBrackets = false
	options.SkipBraces = false
	for _, name := range []string{"all", "any", "none", "single"} {
		for from := 0; from < len(cypher); {
			index := keywordIndexFrom(cypher, name, from, options)
			if index < 0 {
				break
			}
			from = index + len(name)
			open := queryGapEnd(cypher, index+len(name))
			if open >= len(cypher) || cypher[open] != '(' {
				continue
			}
			close := findMatchingParen(cypher, open)
			if close < 0 {
				continue
			}
			if err := validateStaticQuantifierCall(cypher[open+1 : close]); err != nil {
				return err
			}
		}
	}
	return nil
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
	if !cyphertext.IsIdentifier(variable) || !quantifierPredicateRequiresNumbers(predicate, variable) {
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
		if cyphertext.FindIdentifier(predicate, variable, index, cyphertext.IdentifierFoldAt) != index {
			if !cyphertext.IsWhitespaceAt(predicate, index) {
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

func isNumericOnlyOperatorByte(character byte) bool {
	switch character {
	case '%', '*', '/', '-':
		return true
	default:
		return false
	}
}
