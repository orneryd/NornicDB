package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

import "strings"

func validateReturnOrderBySemanticScope(ctx context.Context, clause braceView) error {
	return validateReturnOrderBySemanticScopeIn(ctx, clause, nil)
}

// Retained imports are available throughout a subquery, including a DISTINCT
// or aggregating horizon's ORDER BY. They remain outside its projection key.
func validateReturnOrderBySemanticScopeIn(ctx context.Context, view braceView, retained typeScope) error {
	bodyView := view.clauseBody("RETURN")
	body := bodyView.text()
	orderIndex := topLevelKeywordIndex(bodyView, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	projectionBody := bodyView.window(0, orderIndex).trim()
	orderBody := bodyView.window(keywordEndAt(body, orderIndex, "ORDER BY"), len(body)).trim()
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
			orderBody = orderBody.window(0, index).trim()
		}
	}
	projectionBody, distinct := cutDistinct(projectionBody)

	aliases := make(map[string]struct{})
	directExpressions := make(map[string]struct{})
	complexReferences := make(map[string]struct{})
	projectedAggregates := make(map[string]struct{})
	hasProjectionAggregate := false
	// star: a * item projects every variable in scope, so ORDER BY may use
	// any of them (RETURN DISTINCT *, x AS y ORDER BY z, #883).
	star := false
	for _, item := range projectionBody.commaViews() {
		product := readProjectionItemSource(ctx, item, projectionAdmitted)
		expression, alias := product.expression, product.alias
		if expression == "*" {
			star = true
			continue
		}
		if alias != "" && alias != expression {
			aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		}
		if containsAggregateFunc(product.view) {
			hasProjectionAggregate = true
			for _, call := range semanticAggregateCalls(product.view) {
				projectedAggregates[canonicalSemanticExpression(call)] = struct{}{}
			}
			continue
		}
		if reference := directSemanticReference(expression); reference != "" {
			directExpressions[reference] = struct{}{}
			continue
		}
		for _, reference := range semanticFreeReferences(product.view) {
			complexReferences[reference] = struct{}{}
		}
	}

	for _, term := range parseOrderByClause(orderBody) {
		expression := term.view.text()
		hasOrderAggregate := containsAggregateFunc(term.view)
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
			for _, span := range findAggregateSpans(term.view) {
				call := strings.TrimSpace(expression[span.start:span.end])
				if _, projected := projectedAggregates[canonicalSemanticExpression(call)]; projected {
					continue
				}
				references := semanticFreeReferences(term.view.window(span.start, span.end).trim())
				if len(references) > 0 {
					return createUndefinedVariableError(strings.SplitN(references[0], ".", 2)[0])
				}
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"InvalidAggregation",
					"ORDER BY aggregate must be projected by the same horizon",
				)
			}
		}
		scalarExpression := term.view
		if hasOrderAggregate {
			scalarExpression = term.view.mask(removeAggregateCalls(term.view))
		}
		for _, reference := range semanticFreeReferencesIn(term.view, scalarExpression) {
			if _, projectedAlias := aliases[reference]; projectedAlias {
				continue
			}
			if _, projectedDirectly := directExpressions[reference]; projectedDirectly {
				continue
			}
			baseReference := strings.SplitN(reference, ".", 2)[0]
			if _, global := retained[baseReference]; global {
				continue
			}
			if _, projectedEntity := directExpressions[baseReference]; projectedEntity || star {
				continue
			}
			if hasOrderAggregate {
				if _, partOfComplexProjection := complexReferences[reference]; partOfComplexProjection {
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"AmbiguousAggregationExpression",
						"ORDER BY mixes aggregation with a non-grouping subexpression",
					)
				}
			}
			return createUndefinedVariableError(baseReference)
		}
	}
	return nil
}

// semanticAggregateCalls is the text of expression's aggregate calls
// (findAggregateSpans).
func semanticAggregateCalls(view braceView) []string {
	expression := view.text()
	spans := findAggregateSpans(view)
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

func validateReturnAggregationSemantics(ctx context.Context, body braceView) error {
	return validateReturnAggregationSemanticsIn(ctx, body, nil)
}

// validateReturnAggregationSemanticsIn is validateReturnAggregationSemantics
// with outer's variables constants: in an EXISTS / COUNT / COLLECT body the
// enclosing query's variables aren't grouping keys.
func validateReturnAggregationSemanticsIn(ctx context.Context, bodyView braceView, outer typeScope) error {
	body := bodyView.text()
	end := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(bodyView, keyword); index >= 0 && index < end {
			end = index
		}
	}
	bodyView = bodyView.window(0, end).trim()
	bodyView, _ = cutDistinct(bodyView)
	directExpressions := make(map[string]struct{})
	aliases := make(map[string]struct{})
	expressions := make([]braceView, 0)
	for _, item := range bodyView.commaViews() {
		product := readProjectionItemSource(ctx, item, projectionAdmitted)
		expression, alias := product.expression, product.alias
		expressions = append(expressions, product.view)
		if alias != "" && alias != expression {
			aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		}
		if !containsAggregateFunc(product.view) {
			if reference := directSemanticReference(expression); reference != "" {
				directExpressions[reference] = struct{}{}
			}
		}
	}
	for _, input := range expressions {
		if !containsAggregateFunc(input) {
			continue
		}
		if aggregateContains(input, containsAggregateFunc) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"NestedAggregation",
				"aggregate functions cannot contain aggregate functions",
			)
		}
		if aggregateContains(input, func(inner braceView) bool {
			return containsFunctionCallNamed(inner.text(), "rand")
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
		for _, reference := range semanticFreeReferencesIn(input, input.mask(removeAggregateCalls(input))) {
			if _, constant := outer[strings.SplitN(reference, ".", 2)[0]]; constant {
				continue
			}
			if _, projected := directExpressions[reference]; projected {
				continue
			}
			if _, projectedAlias := aliases[reference]; projectedAlias {
				continue
			}
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"AmbiguousAggregationExpression",
				"aggregate expression contains an implicit grouping expression",
			)
		}
	}
	return nil
}

// aggregateContains reports whether predicate holds for the arguments of one
// of expression's aggregate calls (findAggregateSpans: a qualified function
// such as apoc.coll.sum is not an aggregate, and a subquery's aggregates are
// its own).
func aggregateContains(view braceView, predicate func(braceView) bool) bool {
	expression := view.text()
	for _, span := range findAggregateSpans(view) {
		call := expression[span.start:span.end]
		open := strings.IndexByte(call, '(')
		if predicate(view.window(span.start+open+1, span.end-1)) {
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

func directSemanticReference(expression string) string {
	expression = strings.TrimSpace(expression)
	if identifier := simpleSemanticIdentifier(expression); identifier != "" {
		return identifier
	}
	if variable, property, ok := parseVarPropertyRef(expression); ok {
		return normalizeProjectionColumnName(variable) + "." + normalizePropertyKey(property)
	}
	return ""
}

func semanticExpressionReferences(expression braceView) []string {
	references := make([]string, 0, 2)
	forEachSemanticExpressionReference(expression, func(reference string, _ int) {
		references = append(references, reference)
	})
	return references
}

// forEachSemanticExpressionReference calls visit with each reference
// semanticExpressionReferences finds and the offset of its variable.
func forEachSemanticExpressionReference(view braceView, visit func(reference string, at int)) {
	expression := view.text()
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
		if view.syntax.product.MapKeyAt(view.source, cyphertext.Span{Start: start, End: next}) {
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
		if propertyAccess {
			property, propertyEnd, propertyOK := scanSymbolicName(expression, cursor+1)
			if propertyOK {
				normalized += "." + normalizePropertyKey(property)
				index = propertyEnd
			}
		}
		visit(normalized, start)
	}
}

// qualifiedFunctionNameEnd reports whether the name before at continues as
// a namespace path (.name …) that a call's '(' follows, and where the path
// ends.
func qualifiedFunctionNameEnd(expression string, at int) (int, bool) {
	parts, end := dottedNameParts(expression, at, true)
	cursor := queryGapEnd(expression, end)
	if parts == "" || cursor >= len(expression) || expression[cursor] != '(' {
		return at, false
	}
	return cursor, true
}

// semanticFreeReferences returns the variables and variable.property
// references an expression reads from its scope: the names
// semanticExpressionReferences finds, without the occurrences that read one
// of the expression's own bindings where it is bound (list comprehension,
// quantifier, reduce and named path variables; localReader) and without anything
// inside braces (a COUNT / EXISTS / COLLECT subquery binds its own pattern
// variables; a map projection's keys aren't variables). The base of a map
// projection (n in n {.k}) is a reference.
func semanticFreeReferences(expression braceView) []string {
	return semanticFreeReferencesIn(expression, expression)
}

// semanticFreeReferencesIn is semanticFreeReferences of text with the local
// scopes read from scopes, a text of the same length text was blanked from
// (removeAggregateCalls): blanking reduce(acc = 0, y IN collect(x) | …)'s
// list would leave no reduce to bind y.
func semanticFreeReferencesIn(scopes, text braceView) []string {
	locals := newLocalReader(scopes.text())
	free := make([]string, 0, 2)
	// maskSemanticBraceBodies keeps offsets: each one is the scopes text's.
	forEachSemanticExpressionReference(maskSemanticBraceBodies(text), func(reference string, at int) {
		if !locals.shadows(strings.SplitN(reference, ".", 2)[0], at) {
			free = append(free, reference)
		}
	})
	return free
}

// maskSemanticBraceBodies blanks every top-level {...} of expression, and the
// COUNT / EXISTS / COLLECT keyword of a subquery before one.
func maskSemanticBraceBodies(view braceView) braceView {
	if _, _, _, found := view.firstBrace(); !found {
		return view
	}
	// Classify operands before blanking their bodies, using the same lexical
	// reader as evaluation so a comment cannot expose COUNT as a variable.
	view = view.mask(maskSubqueryBodies(view))
	expression := view.text()
	masked := []byte(expression)
	for index := 0; index < len(expression); index++ {
		switch {
		case (cypherStringLexicalByte(expression[index])) == cypherStringQuoteClass:
			index = skipQuotedSemanticText(expression, index) - 1
			continue
		case cyphertext.IsOpenBrace(cypherStringLexicalByte(expression[index])):
		default:
			continue
		}
		close, found := view.close(index)
		if !found {
			break
		}
		for blank := index; blank <= close; blank++ {
			masked[blank] = ' '
		}
		index = close
	}
	return view.mask(string(masked))
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
func removeAggregateCalls(view braceView) string {
	result := []byte(view.text())
	for _, span := range findAggregateSpans(view) {
		for blank := span.start; blank < span.end; blank++ {
			result[blank] = ' '
		}
	}
	return string(result)
}
