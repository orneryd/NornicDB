package cypher

import (
	"fmt"
	"strings"
)

var builtInCypherFunctions = map[string]struct{}{
	"abs": {}, "acos": {}, "all": {}, "allshortestpaths": {}, "any": {}, "asin": {}, "atan": {}, "atan2": {}, "avg": {},
	"ceil": {}, "coalesce": {}, "collect": {}, "cos": {}, "cosh": {}, "cot": {}, "count": {}, "coth": {},
	"date": {}, "datetime": {}, "day": {}, "dayofweek": {}, "dayofyear": {}, "degrees": {}, "distance": {}, "duration": {},
	"e": {}, "elementid": {}, "endnode": {}, "exists": {}, "exp": {},
	"floor": {}, "format": {},
	"haversin": {}, "head": {}, "hour": {},
	"id": {}, "isempty": {},
	"keys":   {},
	"labels": {}, "last": {}, "left": {}, "length": {}, "linestring": {}, "localdatetime": {}, "localtime": {}, "log": {}, "log10": {}, "lower": {}, "lpad": {}, "ltrim": {},
	"max": {}, "min": {}, "minute": {}, "month": {},
	"nodes": {}, "none": {},
	"ordinalday":     {},
	"percentilecont": {}, "percentiledisc": {}, "pi": {}, "point": {}, "polygon": {}, "power": {}, "properties": {},
	"quarter": {},
	"rand":    {}, "randomuuid": {}, "range": {}, "reduce": {}, "relationships": {}, "replace": {}, "reverse": {}, "right": {}, "round": {}, "rpad": {}, "rtrim": {},
	"second": {}, "shortestpath": {}, "sign": {}, "sin": {}, "sinh": {}, "single": {}, "size": {}, "split": {}, "sqrt": {}, "startnode": {}, "stdev": {}, "stdevp": {}, "substring": {}, "sum": {},
	"tail": {}, "tan": {}, "tanh": {}, "time": {}, "timestamp": {}, "toboolean": {}, "tobooleanornull": {}, "tofloat": {}, "tofloatornull": {}, "toint": {}, "tointeger": {}, "tointegerornull": {}, "tolower": {}, "tostring": {}, "tostringornull": {}, "toupper": {}, "trim": {}, "type": {},
	"week": {}, "weekyear": {},
	"year": {},
}

func validateKnownFunctionsInExpression(expression string) error {
	for index := 0; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := next
		for cursor < len(expression) && expression[cursor] == '.' {
			part, partEnd, partOK := scanIdentifierToken(expression, cursor+1)
			if !partOK {
				break
			}
			name += "." + part
			cursor = partEnd
		}
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' {
			index = next
			continue
		}
		normalized := strings.ToLower(normalizeProjectionColumnName(name))
		// Clause and predicate keywords can legally be followed by a parenthesized
		// pattern/expression (MATCH (n), STARTS WITH (...)). They are syntax, not
		// function invocations, and must stay in the converged expression scanner.
		if isNonFunctionSemanticKeyword(normalized) {
			index = cursor + 1
			continue
		}
		if _, known := builtInCypherFunctions[normalized]; known {
			index = cursor + 1
			continue
		}
		if strings.Contains(normalized, ".") {
			parts := strings.Split(normalized, ".")
			if _, builtInFallback := builtInCypherFunctions[parts[len(parts)-1]]; builtInFallback {
				index = cursor + 1
				continue
			}
			if PluginFunctionLookup == nil {
				index = cursor + 1
				continue
			}
			if _, found := PluginFunctionLookup(normalized); found {
				index = cursor + 1
				continue
			}
		}
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"UnknownFunction",
			fmt.Sprintf("unknown function: %s", name),
		)
	}
	return nil
}

func isNonFunctionSemanticKeyword(name string) bool {
	switch name {
	case "and", "call", "case", "create", "delete", "detach", "distinct", "do", "else", "end", "foreach", "in", "match", "merge", "not", "on", "optional", "or", "remove", "return", "set", "starts", "then", "unwind", "when", "where", "with", "xor":
		return true
	default:
		return false
	}
}

func skipQuotedSemanticText(expression string, start int) int {
	quote := expression[start]
	for index := start + 1; index < len(expression); index++ {
		if expression[index] == quote && !isBackslashEscaped(expression, index) {
			return index + 1
		}
	}
	return len(expression)
}
