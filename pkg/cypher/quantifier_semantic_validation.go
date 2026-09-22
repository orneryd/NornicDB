package cypher

import (
	"fmt"
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
			open := skipSpaces(cypher, index+len(name))
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
	inIndex := findKeywordIndexInContext(inner, "IN")
	if inIndex <= 0 {
		return nil
	}
	rest := inner[inIndex+len("IN"):]
	whereIndex := findKeywordIndexInContext(rest, "WHERE")
	if whereIndex < 0 {
		return nil
	}
	variable := strings.TrimSpace(inner[:inIndex])
	listExpression := strings.TrimSpace(rest[:whereIndex])
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
	for index := 0; index < len(predicate); index++ {
		if !identifierAt(predicate, index, variable) {
			continue
		}
		before := quantifierPreviousNonSpaceByte(predicate, index)
		after := quantifierNextNonSpaceByte(predicate, index+len(variable))
		if isNumericOnlyOperatorByte(before) || isNumericOnlyOperatorByte(after) {
			return true
		}
	}
	return false
}

func identifierAt(text string, index int, identifier string) bool {
	if index < 0 || index+len(identifier) > len(text) || !strings.EqualFold(text[index:index+len(identifier)], identifier) {
		return false
	}
	return (index == 0 || !isIdentChar(text[index-1])) &&
		(index+len(identifier) == len(text) || !isIdentChar(text[index+len(identifier)]))
}

func quantifierPreviousNonSpaceByte(text string, index int) byte {
	for index--; index >= 0; index-- {
		if !isWhitespace(text[index]) {
			return text[index]
		}
	}
	return 0
}

func quantifierNextNonSpaceByte(text string, index int) byte {
	for ; index < len(text); index++ {
		if !isWhitespace(text[index]) {
			return text[index]
		}
	}
	return 0
}

func isNumericOnlyOperatorByte(character byte) bool {
	switch character {
	case '%', '*', '/', '-':
		return true
	default:
		return false
	}
}
