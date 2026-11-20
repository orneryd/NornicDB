package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/auth"
)

type authRuleBindings struct {
	parent *authRuleBindings
	name   string
	value  any
}

type authRuleValue func(auth.FineGrainedAuthRuleInput, *authRuleBindings) any

type authRuleProvidersKey struct{}

type authRuleProviders struct {
	native bool
	oidc   bool
}

// CompileFineGrainedAuthRuleCondition compiles a Cypher 25 authorization
// expression using the same scalar equality, membership and boolean rules as
// graph queries. The input exposes identity attributes and time, with no graph
// or procedure execution capabilities.
func CompileFineGrainedAuthRuleCondition(condition string) (auth.FineGrainedAuthRuleCondition, error) {
	providers := authRuleProviders{}
	ctx := context.WithValue(context.Background(), authRuleProvidersKey{}, &providers)
	value, err := compileAuthRuleValue(ctx, StripComments(condition), nil, 0)
	if err != nil {
		return nil, err
	}
	return func(input auth.FineGrainedAuthRuleInput) bool {
		if providers.native && input.NativeTags == nil || providers.oidc && input.Claims == nil {
			return false
		}
		result, _ := value(input, nil).(bool)
		return result
	}, nil
}

func compileAuthRuleValue(ctx context.Context, expression string, variables []string, depth int) (authRuleValue, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" || depth > 128 {
		return nil, fineGrainedPrivilegeSyntaxError("invalid authorization rule expression")
	}
	compile := func(text string) (authRuleValue, error) {
		return compileAuthRuleValue(ctx, text, variables, depth+1)
	}
	if expression[0] == '(' && findMatchingParenAt(expression, 0) == len(expression)-1 {
		return compile(expression[1 : len(expression)-1])
	}
	for _, keyword := range []string{"OR", "XOR", "AND"} {
		if at := topLevelKeywordIndex(ctx, expression, keyword); at >= 0 {
			return compileAuthRuleBinary(compile, expression[:at], expression[at+len(keyword):], keyword)
		}
	}
	if operand, ok := logicalNotOperand(expression); ok {
		value, err := compile(operand)
		if err != nil {
			return nil, err
		}
		return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
			if v, ok := value(input, bindings).(bool); ok {
				return !v
			}
			return nil
		}, nil
	}
	for _, keyword := range []string{"IS NOT NULL", "IS NULL"} {
		if at := findTopLevelOperator(expression, keyword, true, true); at >= 0 &&
			strings.TrimSpace(expression[at+len(keyword):]) == "" {
			value, err := compile(expression[:at])
			if err != nil {
				return nil, err
			}
			return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
				return (value(input, bindings) == nil) == (keyword == "IS NULL")
			}, nil
		}
	}
	for _, operator := range []string{"STARTS WITH", "ENDS WITH", "CONTAINS"} {
		if left, right, ok := splitByOperatorOutsideCase(expression, " "+operator+" ", true, true); ok {
			return compileAuthRuleBinary(compile, left, right, operator)
		}
	}
	if at := topLevelKeywordIndex(ctx, expression, "IN"); at >= 0 {
		return compileAuthRuleBinary(compile, expression[:at], expression[at+2:], "IN")
	}
	for _, operator := range []string{"<>", "<=", ">=", "=~", "=", "<", ">"} {
		if at := findTopLevelOperator(expression, operator, false, true); at >= 0 {
			return compileAuthRuleBinary(compile, expression[:at], expression[at+len(operator):], operator)
		}
	}
	if literal, ok := authRuleLiteral(expression); ok {
		return func(auth.FineGrainedAuthRuleInput, *authRuleBindings) any { return literal }, nil
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		if left, right, operator, ok := splitRowArithmeticTier(expression, tier); ok {
			return compileAuthRuleBinary(compile, left, right, string(operator))
		}
	}
	if isListLiteral(expression) {
		var items []authRuleValue
		for _, entry := range splitTopLevelComma(expression[1 : len(expression)-1]) {
			if strings.TrimSpace(entry) == "" && strings.TrimSpace(expression[1:len(expression)-1]) == "" {
				continue
			}
			value, err := compile(entry)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
			list := make([]any, len(items))
			for i, item := range items {
				list[i] = item(input, bindings)
			}
			return list
		}, nil
	}
	if name, ok := isOneSymbolicName(expression); ok {
		for _, variable := range variables {
			if name == variable {
				return func(_ auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
					for entry := bindings; entry != nil; entry = entry.parent {
						if entry.name == name {
							return entry.value
						}
					}
					return nil
				}, nil
			}
		}
	}
	return compileAuthRuleFunction(ctx, expression, variables, depth)
}

func authRuleLiteral(expression string) (any, bool) {
	if text, ok := cyphertext.DecodeString(expression, cyphertext.AcceptedSource); ok {
		return text, true
	}
	switch strings.ToUpper(expression) {
	case "TRUE":
		return true, true
	case "FALSE":
		return false, true
	case "NULL":
		return nil, true
	}
	if whole, err := strconv.ParseInt(expression, 10, 64); err == nil {
		return whole, true
	}
	if fraction, err := strconv.ParseFloat(expression, 64); err == nil {
		return fraction, true
	}
	return nil, false
}

func compileAuthRuleBinary(compile func(string) (authRuleValue, error), left, right, operator string) (authRuleValue, error) {
	lhs, err := compile(left)
	if err != nil {
		return nil, err
	}
	rhs, err := compile(right)
	if err != nil {
		return nil, err
	}
	return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
		leftValue := lhs(input, bindings)
		if operator == "AND" || operator == "OR" {
			decisive := operator == "OR"
			if truth, ok := leftValue.(bool); ok && truth == decisive {
				return decisive
			}
		}
		rightValue := rhs(input, bindings)
		switch operator {
		case "AND", "OR":
			decisive := operator == "OR"
			leftBool, leftKnown := leftValue.(bool)
			rightBool, rightKnown := rightValue.(bool)
			if leftKnown && leftBool == decisive || rightKnown && rightBool == decisive {
				return decisive
			}
			if leftKnown && rightKnown {
				return !decisive
			}
			return nil
		case "XOR":
			l, a := leftValue.(bool)
			r, b := rightValue.(bool)
			if a && b {
				return l != r
			}
			return nil
		case "IN":
			value, _ := cypherMembership(leftValue, rightValue)
			return value
		case "CONTAINS", "STARTS WITH", "ENDS WITH":
			leftText, leftOK := leftValue.(string)
			rightText, rightOK := rightValue.(string)
			if !leftOK || !rightOK {
				return nil
			}
			switch operator {
			case "STARTS WITH":
				return strings.HasPrefix(leftText, rightText)
			case "ENDS WITH":
				return strings.HasSuffix(leftText, rightText)
			default:
				return strings.Contains(leftText, rightText)
			}
		case "+", "-", "*", "/", "%", "^":
			value, _, err := (&StorageExecutor{}).evaluateRowArithmeticValues(operator[0], leftValue, rightValue)
			if err != nil {
				return nil
			}
			return value
		default:
			return compareCypherPredicateValue(leftValue, rightValue, operator)
		}
	}, nil
}

func compileAuthRuleFunction(ctx context.Context, expression string, variables []string, depth int) (authRuleValue, error) {
	open := strings.IndexByte(expression, '(')
	if open <= 0 {
		return nil, fineGrainedPrivilegeSyntaxError("unsupported value in authorization rule")
	}
	close := findMatchingParenAt(expression, open)
	if close <= open {
		return nil, fineGrainedPrivilegeSyntaxError("invalid authorization rule function")
	}
	function := strings.ToLower(strings.TrimSpace(expression[:open]))
	arg := strings.TrimSpace(expression[open+1 : close])
	tail := strings.TrimSpace(expression[close+1:])
	switch function {
	case "abac.native.user_tags":
		if arg == "" && tail == "" {
			ctx.Value(authRuleProvidersKey{}).(*authRuleProviders).native = true
			return func(input auth.FineGrainedAuthRuleInput, _ *authRuleBindings) any {
				return input.NativeTags
			}, nil
		}
	case "abac.oidc.user_attribute":
		if key, ok := cyphertext.DecodeString(arg, cyphertext.AcceptedSource); ok && tail == "" {
			ctx.Value(authRuleProvidersKey{}).(*authRuleProviders).oidc = true
			return func(input auth.FineGrainedAuthRuleInput, _ *authRuleBindings) any {
				return input.Claims[key]
			}, nil
		}
	case "date.transaction", "datetime.transaction", "localdatetime.transaction", "localtime.transaction", "time.transaction":
		return compileAuthRuleClock(strings.TrimSuffix(function, ".transaction"), arg, tail)
	case "all", "any", "none", "single":
		if tail == "" {
			return compileAuthRuleQuantifier(ctx, function, arg, variables, depth)
		}
	case "reduce":
		if tail == "" {
			return compileAuthRuleReduce(ctx, arg, variables, depth)
		}
	case "range", "reverse", "tail", "tobooleanlist", "tofloatlist", "tointegerlist", "tostringlist",
		"abs", "ceil", "floor", "isnan", "round", "sign", "isempty", "char_length", "character_length",
		"coalesce", "head", "last", "nullif", "size", "toboolean", "tobooleanornull", "tofloat", "tofloatornull",
		"tointeger", "tointegerornull", "btrim", "left", "lower", "ltrim", "replace", "right", "rtrim",
		"split", "substring", "tolower", "tostring", "tostringornull", "toupper", "trim", "upper",
		"duration", "duration.between", "duration.indays", "duration.inmonths", "duration.inseconds",
		"date", "date.truncate", "datetime", "datetime.truncate", "localdatetime", "localdatetime.truncate",
		"localtime", "localtime.truncate", "time", "time.truncate":
		if arg != "" {
			return compileAuthRuleScalar(ctx, function, arg, tail, variables, depth)
		}
	}
	return nil, fineGrainedPrivilegeSyntaxError("unsupported authorization rule function or argument")
}

// compileAuthRuleScalar runs the closed set of ordinary Cypher scalar
// functions on compiled values. Only the function identifier and generated
// argument names reach the scalar evaluator.
func compileAuthRuleScalar(ctx context.Context, function, rawArgs, field string, variables []string, depth int) (authRuleValue, error) {
	var args []authRuleValue
	var names []string
	for i, raw := range splitTopLevelComma(rawArgs) {
		value, err := compileAuthRuleValue(ctx, raw, variables, depth+1)
		if err != nil {
			return nil, err
		}
		args = append(args, value)
		names = append(names, "authRuleArg"+strconv.Itoa(i))
	}
	if field != "" {
		name, valid := isOneSymbolicName(strings.TrimPrefix(field, "."))
		if !strings.HasPrefix(field, ".") || !valid || name == "" {
			return nil, fineGrainedPrivilegeSyntaxError("invalid scalar field in authorization rule")
		}
	}
	call := function + "(" + strings.Join(names, ", ") + ")" + field
	usesClock := containsStatementClockCall(call)
	executor := &StorageExecutor{}
	return func(input auth.FineGrainedAuthRuleInput, scope *authRuleBindings) any {
		values := make(map[string]any, len(args))
		for i, arg := range args {
			values[names[i]] = arg(input, scope)
		}
		evalContext := input.Context
		if evalContext == nil {
			evalContext = ctx
		}
		if usesClock {
			evalContext = context.WithValue(evalContext, temporalStatementTimeKey{}, &input.Now)
		}
		value, resolved, err := executor.evaluateExpressionFromValuesResult(evalContext, call, values)
		if !resolved || err != nil {
			return nil
		}
		return value
	}, nil
}

// compileAuthRuleClock uses the captured admission time and the same
// temporal type and property owners as ordinary Cypher expressions.
func compileAuthRuleClock(kind, argument, field string) (authRuleValue, error) {
	var location *time.Location
	var zoneID string
	if argument != "" {
		var ok bool
		zoneID, ok = cyphertext.DecodeString(argument, cyphertext.AcceptedSource)
		if !ok {
			return nil, fineGrainedPrivilegeSyntaxError("expected a literal time zone")
		}
		var err error
		location, err = time.LoadLocation(zoneID)
		if err != nil {
			return nil, fineGrainedPrivilegeSyntaxError("unknown time zone")
		}
	}
	property := strings.TrimPrefix(field, ".")
	if field != "" {
		if !strings.HasPrefix(field, ".") {
			return nil, fineGrainedPrivilegeSyntaxError("invalid transaction time field")
		}
		value, _ := temporalClockValue(kind, time.Unix(0, 0).UTC(), zoneID)
		if _, _, supported := evaluateTemporalProperty(value, property); !supported {
			return nil, fineGrainedPrivilegeSyntaxError("unsupported transaction time field")
		}
	}
	return func(input auth.FineGrainedAuthRuleInput, _ *authRuleBindings) any {
		instant := input.Now
		if location != nil {
			instant = instant.In(location)
		}
		value, _ := temporalClockValue(kind, instant, zoneID)
		if field != "" {
			value, _, _ = evaluateTemporalProperty(value, property)
		}
		return value
	}, nil
}

func compileAuthRuleQuantifier(ctx context.Context, function, arguments string, variables []string, depth int) (authRuleValue, error) {
	name, list, predicate, ok := parseQuantifierArguments(ctx, arguments)
	if !ok {
		return nil, fmt.Errorf("invalid %s authorization rule predicate", function)
	}
	values, err := compileAuthRuleValue(ctx, list, variables, depth+1)
	if err != nil {
		return nil, err
	}
	test, err := compileAuthRuleValue(ctx, predicate, append(append([]string(nil), variables...), name), depth+1)
	if err != nil {
		return nil, err
	}
	return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
		items, ok := cypherListValue(values(input, bindings))
		if !ok {
			return nil
		}
		fold := quantifierFold{function: function}
		frame := authRuleBindings{parent: bindings, name: name}
		for _, item := range items {
			if input.Context != nil && input.Context.Err() != nil {
				return nil
			}
			frame.value = item
			if result, settled := fold.add(test(input, &frame)); settled {
				return result
			}
		}
		return fold.result()
	}, nil
}

func compileAuthRuleReduce(ctx context.Context, arguments string, variables []string, depth int) (authRuleValue, error) {
	reduce, ok := parseReduceForm("reduce", arguments)
	if !ok {
		return nil, fineGrainedPrivilegeSyntaxError("invalid reduce authorization rule expression")
	}
	initial, err := compileAuthRuleValue(ctx, reduce.initial, variables, depth+1)
	if err != nil {
		return nil, err
	}
	values, err := compileAuthRuleValue(ctx, reduce.list, variables, depth+1)
	if err != nil {
		return nil, err
	}
	step, err := compileAuthRuleValue(ctx, reduce.step, append(append([]string(nil), variables...), reduce.accumulator, reduce.variable), depth+1)
	if err != nil {
		return nil, err
	}
	return func(input auth.FineGrainedAuthRuleInput, bindings *authRuleBindings) any {
		items, ok := cypherListValue(values(input, bindings))
		if !ok {
			return nil
		}
		accumulator := authRuleBindings{parent: bindings, name: reduce.accumulator, value: initial(input, bindings)}
		current := authRuleBindings{parent: &accumulator, name: reduce.variable}
		for _, value := range items {
			if input.Context != nil && input.Context.Err() != nil {
				return nil
			}
			current.value = value
			accumulator.value = step(input, &current)
		}
		return accumulator.value
	}, nil
}
