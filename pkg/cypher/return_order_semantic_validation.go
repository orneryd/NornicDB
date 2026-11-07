package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

import "strings"

func validateReturnOrderBySemanticScope(ctx context.Context, clause string) error {
	return validateReturnOrderBySemanticScopeIn(ctx, clause, nil)
}

// Retained imports are available throughout a subquery, including a DISTINCT
// or aggregating horizon's ORDER BY. They remain outside its projection key.
func validateReturnOrderBySemanticScopeIn(ctx context.Context, clause string, retained typeScope) error {
	body := strings.TrimSpace(clause[len("RETURN"):])
	orderIndex := topLevelKeywordIndex(ctx, body, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	projectionBody := strings.TrimSpace(body[:orderIndex])
	orderBody := strings.TrimSpace(body[keywordEndAt(body, orderIndex, "ORDER BY"):])
	projectionBody, distinct := cutDistinct(projectionBody)

	aliases := make(map[string]struct{})
	directExpressions := make(map[semanticReference]struct{})
	complexReferences := make(map[semanticReference]struct{})
	projectedAggregates := make(map[string]struct{})
	hasProjectionAggregate := false
	// star: a * item projects every variable in scope, so ORDER BY may use
	// any of them (RETURN DISTINCT *, x AS y ORDER BY z, #883).
	star := false
	for _, item := range splitTopLevelComma(projectionBody) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(item), projectionAdmitted)
		expression, alias := itemSource.expression, itemSource.alias
		if expression == "*" {
			star = true
			continue
		}
		if alias != "" && alias != expression {
			aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		}
		if containsAggregateFunc(expression) {
			hasProjectionAggregate = true
			for _, call := range semanticAggregateCalls(expression) {
				projectedAggregates[canonicalSemanticExpression(call)] = struct{}{}
			}
			continue
		}
		if reference, ok := directSemanticReference(expression); ok {
			directExpressions[reference] = struct{}{}
			continue
		}
		forEachFreeSemanticReference(expression, expression, func(reference semanticReference) bool {
			complexReferences[reference] = struct{}{}
			return true
		})
	}

	for _, term := range parseOrderByClause(ctx, orderBody) {
		expression := term.column
		hasOrderAggregate := containsAggregateFunc(expression)
		if hasOrderAggregate && !hasProjectionAggregate {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidAggregation",
				"ORDER BY cannot introduce an aggregate after a non-aggregating RETURN",
			)
		}
		if !distinct && !hasOrderAggregate {
			continue
		}
		if hasOrderAggregate {
			for _, call := range semanticAggregateCalls(expression) {
				if _, projected := projectedAggregates[canonicalSemanticExpression(call)]; projected {
					continue
				}
				var undefined error
				forEachFreeSemanticReference(call, call, func(reference semanticReference) bool {
					undefined = createUndefinedVariableError(reference.variable)
					return false
				})
				if undefined != nil {
					return undefined
				}
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"InvalidAggregation",
					"ORDER BY aggregate must be projected by the same horizon",
				)
			}
		}
		scalarExpression := expression
		if hasOrderAggregate {
			scalarExpression = removeAggregateCalls(expression)
		}
		var referenceErr error
		forEachFreeSemanticReference(expression, scalarExpression, func(reference semanticReference) bool {
			if _, projectedAlias := aliases[reference.variable]; !reference.hasProperty && projectedAlias {
				return true
			}
			if _, projectedDirectly := directExpressions[reference]; projectedDirectly {
				return true
			}
			if _, global := retained[reference.variable]; global {
				return true
			}
			if _, projectedEntity := directExpressions[semanticReference{variable: reference.variable}]; projectedEntity || star {
				return true
			}
			if hasOrderAggregate {
				if _, partOfComplexProjection := complexReferences[reference]; partOfComplexProjection {
					referenceErr = newSemanticError("Neo.ClientError.Statement.SyntaxError", "AmbiguousAggregationExpression", "ORDER BY mixes aggregation with a non-grouping subexpression")
					return false
				}
			}
			referenceErr = createUndefinedVariableError(reference.variable)
			return false
		})
		if referenceErr != nil {
			return referenceErr
		}
	}
	return nil
}

// semanticAggregateCalls is the text of expression's aggregate calls
// (findAggregateSpans).
func semanticAggregateCalls(expression string) []string {
	spans := findAggregateSpans(expression)
	calls := make([]string, 0, len(spans))
	for _, span := range spans {
		calls = append(calls, strings.TrimSpace(expression[span.start:span.end]))
	}
	return calls
}

func canonicalSemanticExpression(expression string) string {
	var normalized strings.Builder
	normalized.Grow(len(expression))
	for index := 0; index < len(expression); index++ {
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			normalized.WriteString(expression[index:end])
			index = end - 1
			continue
		}
		current := expression[index]
		if cyphertext.IsWhitespaceAt(expression, index) {
			continue
		}
		if current >= 'a' && current <= 'z' {
			current -= 'a' - 'A'
		}
		normalized.WriteByte(current)
	}
	return normalized.String()
}

func validateReturnAggregationSemantics(ctx context.Context, body string) error {
	return validateReturnAggregationSemanticsIn(ctx, body, nil)
}

// validateReturnAggregationSemanticsIn is validateReturnAggregationSemantics
// with outer's variables constants: in an EXISTS / COUNT / COLLECT body the
// enclosing query's variables aren't grouping keys.
func validateReturnAggregationSemanticsIn(ctx context.Context, body string, outer typeScope) error {
	end := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(ctx, body, keyword); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	body, _ = cutDistinct(body)
	directExpressions := make(map[semanticReference]struct{})
	aliases := make(map[string]struct{})
	expressions := make([]string, 0)
	for _, item := range splitTopLevelComma(body) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(item), projectionAdmitted)
		expression, alias := itemSource.expression, itemSource.alias
		expressions = append(expressions, expression)
		if alias != "" && alias != expression {
			aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		}
		if !containsAggregateFunc(expression) {
			if reference, ok := directSemanticReference(expression); ok {
				directExpressions[reference] = struct{}{}
			}
		}
	}
	for _, expression := range expressions {
		if !containsAggregateFunc(expression) {
			continue
		}
		if aggregateContains(expression, containsAggregateFunc) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"NestedAggregation",
				"aggregate functions cannot contain aggregate functions",
			)
		}
		if aggregateContains(expression, func(inner string) bool {
			return containsFunctionCallNamed(inner, "rand")
		}) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"NonConstantExpression",
				"non-deterministic expressions cannot be arguments to aggregate functions",
			)
		}
		// What the projection reads outside its aggregates: removeAggregateCalls
		// blanks them in place, so a local's scope is read where it is
		// (x + size(collect([x IN l | x])) reads the outer x).
		var referenceErr error
		forEachFreeSemanticReference(expression, removeAggregateCalls(expression), func(reference semanticReference) bool {
			if _, constant := outer[reference.variable]; constant {
				return true
			}
			if _, projected := directExpressions[reference]; projected {
				return true
			}
			if _, projectedAlias := aliases[reference.variable]; !reference.hasProperty && projectedAlias {
				return true
			}
			referenceErr = newSemanticError("Neo.ClientError.Statement.SyntaxError", "AmbiguousAggregationExpression", "aggregate expression contains an implicit grouping expression")
			return false
		})
		if referenceErr != nil {
			return referenceErr
		}
	}
	return nil
}

// aggregateContains reports whether predicate holds for the arguments of one
// of expression's aggregate calls (findAggregateSpans: a qualified function
// such as apoc.coll.sum is not an aggregate, and a subquery's aggregates are
// its own).
func aggregateContains(expression string, predicate func(string) bool) bool {
	for _, span := range findAggregateSpans(expression) {
		call := expression[span.start:span.end]
		open := strings.IndexByte(call, '(')
		if predicate(call[open+1 : len(call)-1]) {
			return true
		}
	}
	return false
}

func containsFunctionCallNamed(expression, expected string) bool {
	for index := 0; index < len(expression); {
		name, ok := cyphertext.ScanIdentifier(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := name.End
		for cursor < len(expression) && cyphertext.IsWhitespaceAt(expression, cursor) {
			cursor++
		}
		if strings.EqualFold(normalizeProjectionColumnName(name.Value), expected) && cursor < len(expression) && expression[cursor] == '(' {
			return true
		}
		index = name.End
	}
	return false
}

// semanticReference retains the decoded boundaries of an entity reference.
// A variable named `n.x` differs from property x of variable n.
type semanticReference struct {
	variable    string
	property    string
	hasProperty bool
}

func directSemanticReference(expression string) (semanticReference, bool) {
	expression = strings.TrimSpace(expression)
	if identifier := simpleSemanticIdentifier(expression); identifier != "" {
		return semanticReference{variable: identifier}, true
	}
	if variable, property, ok := splitPostfixPropertyAccess(expression); ok && isPatternVariableName(variable) {
		return semanticReference{variable: normalizeProjectionColumnName(variable), property: property, hasProperty: true}, true
	}
	return semanticReference{}, false
}

// forEachSemanticExpressionReference visits each decoded variable and its first
// property separately, preserving dots inside quoted identities. at is the
// variable's source offset. Returning false stops the walk; true means complete.
func forEachSemanticExpressionReference(expression string, visit func(variable, property string, hasProperty bool, at int) bool) bool {
	for index := 0; index < len(expression); {
		if end := queryGapEnd(expression, index); end > index {
			index = end
			continue
		}
		if isCypherStringQuoteByte(expression[index]) {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		if expression[index] == '$' {
			next, ok := cyphertext.ScanIdentifier(expression, index+1)
			if ok {
				index = next.End
				continue
			}
		}
		name, next, ok := scanSymbolicName(expression, index)
		if !ok {
			index++
			continue
		}
		// A name after a dot is a property key, not a variable: the receiver
		// may be a list element or a call's result (collect(n)[0].id,
		// head(xs).id), which the scan doesn't read as a reference itself.
		start := index
		before := index
		for before > 0 && cyphertext.IsWhitespaceAt(expression, before-1) {
			before--
		}
		index = next
		if before > 0 && expression[before-1] == '.' && (before < 2 || expression[before-2] != '.') {
			continue
		}
		cursor := next
		for cursor < len(expression) && cyphertext.IsWhitespaceAt(expression, cursor) {
			cursor++
		}
		if cursor < len(expression) && expression[cursor] == '(' {
			continue
		}
		if cursor < len(expression) && expression[cursor] == ':' {
			continue
		}
		if end, call := qualifiedFunctionNameEnd(expression, cursor); call {
			// apoc.coll.sort(…): a namespaced function's name, not a
			// variable and its property.
			index = end
			continue
		}
		normalized := normalizeProjectionColumnName(name)
		propertyAccess := cursor < len(expression) && expression[cursor] == '.'
		// A keyword-named variable (where.id, #894) is still a variable when
		// a property access follows it; a keyword is never followed by '.'.
		if !isCypherBacktickByte(name[0]) && !propertyAccess && isSemanticLiteralWord(normalized) {
			continue
		}
		propertyName, hasProperty := "", false
		if propertyAccess {
			property, propertyEnd, propertyOK := scanSymbolicName(expression, cursor+1)
			if propertyOK {
				propertyName, hasProperty = normalizePropertyKey(property), true
				index = propertyEnd
			}
		}
		if !visit(normalized, propertyName, hasProperty, start) {
			return false
		}
	}
	return true
}

// qualifiedFunctionNameEnd reports whether the name before at continues as
// a namespace path (.name …) that a call's '(' follows, and where the path
// ends.
func qualifiedFunctionNameEnd(expression string, at int) (int, bool) {
	parts, end := dottedNameParts(expression, at, true, dottedNameIdentity)
	cursor := queryGapEnd(expression, end)
	if parts == "" || cursor >= len(expression) || expression[cursor] != '(' {
		return at, false
	}
	return cursor, true
}

// forEachFreeSemanticReference visits references not shadowed by expression-local
// bindings. scopes retains the original offsets when text has aggregate calls
// blanked out. Returning false stops the walk, without allocating a result list.
func forEachFreeSemanticReference(scopes, text string, visit func(semanticReference) bool) bool {
	locals := newLocalReader(scopes)
	return forEachSemanticExpressionReference(maskSemanticBraceBodies(text), func(variable, property string, hasProperty bool, at int) bool {
		if locals.shadows(variable, at) {
			return true
		}
		return visit(semanticReference{variable: variable, property: property, hasProperty: hasProperty})
	})
}

// maskSemanticBraceBodies blanks every top-level {...} of expression, and the
// COUNT / EXISTS / COLLECT keyword of a subquery before one.
func maskSemanticBraceBodies(expression string) string {
	if strings.IndexByte(expression, '{') < 0 {
		return expression
	}
	// Classify operands before blanking their bodies, using the same lexical
	// reader as evaluation so a comment cannot expose COUNT as a variable.
	expression = maskSubqueryBodies(expression)
	masked := []byte(expression)
	for index := 0; index < len(expression); index++ {
		switch cypherStringLexicalByte(expression[index]) {
		case cypherStringQuoteClass:
			index = skipQuotedSemanticText(expression, index) - 1
			continue
		case '{':
		default:
			continue
		}
		close := cyphertext.FindMatchingDelimiter(expression, index, '{', '}', cyphertext.OpeningDelimiter)
		if close < 0 {
			break
		}
		for blank := index; blank <= close; blank++ {
			masked[blank] = ' '
		}
		end := index
		for end > 0 && cyphertext.IsWhitespaceAt(expression, end-1) {
			end--
		}
		start := end
		for start > 0 && cyphertext.NamePartAt(expression, start-1) {
			start--
		}
		switch upperASCII(expression[start:end]) {
		case "COUNT", "EXISTS", "COLLECT":
			for blank := start; blank < end; blank++ {
				masked[blank] = ' '
			}
		}
		index = close
	}
	return string(masked)
}

// isSemanticLiteralWord reports whether an identifier-shaped word is a
// literal or an operator / expression keyword, not a variable.
func isSemanticLiteralWord(value string) bool {
	if isLiteralKeyword(value) {
		return true
	}
	switch upperASCII(value) {
	case "ASC", "ASCENDING", "DESC", "DESCENDING",
		"AND", "IN", "NOT", "OR", "WHERE", "XOR", "IS", "STARTS", "ENDS", "WITH", "CONTAINS",
		"CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT":
		return true
	default:
		return false
	}
}

// removeAggregateCalls blanks expression's aggregate calls
// (findAggregateSpans), leaving what the projection reads outside them.
func removeAggregateCalls(expression string) string {
	result := []byte(expression)
	for _, span := range findAggregateSpans(expression) {
		for blank := span.start; blank < span.end; blank++ {
			result[blank] = ' '
		}
	}
	return string(result)
}
