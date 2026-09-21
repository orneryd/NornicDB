package cypher

import (
	"context"
	"fmt"
	"strings"
)

func extractProcedureInvocationArguments(ctx context.Context, spec ProcedureSpec, callCypher string) ([]interface{}, error) {
	if procedureCallContainsAggregation(callCypher) {
		return nil, newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"procedure arguments cannot contain aggregate expressions",
		)
	}
	if strings.Index(callCypher, "(") >= 0 {
		args, err := extractCallArguments(callCypher)
		if err != nil {
			return nil, err
		}
		if err := validateProcedureArgCount(spec, args); err != nil {
			return nil, err
		}
		return args, nil
	}

	params := getParamsFromContext(ctx)
	args := make([]interface{}, 0, len(spec.Params))
	for index, parameter := range spec.Params {
		value, exists := params[parameter.Name]
		if !exists {
			if index >= spec.MinArgs && parameter.Optional {
				continue
			}
			return nil, newSemanticError(
				"Neo.ClientError.Statement.ParameterMissing",
				"MissingParameter",
				fmt.Sprintf("missing implicit procedure parameter %s", parameter.Name),
			)
		}
		args = append(args, value)
	}
	if err := validateProcedureArgCount(spec, args); err != nil {
		return nil, err
	}
	return args, nil
}

func procedureCallContainsAggregation(callCypher string) bool {
	open := strings.Index(callCypher, "(")
	if open < 0 {
		return false
	}
	close := findMatchingCallParen(callCypher, open)
	if close < 0 {
		return false
	}
	body := callCypher[open+1 : close]
	for _, name := range []string{"count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp", "percentilecont", "percentiledisc"} {
		if findKeywordIndexInContext(body, name) >= 0 && strings.Contains(strings.ToLower(body), name+"(") {
			return true
		}
	}
	return false
}
