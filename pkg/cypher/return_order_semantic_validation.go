package cypher

import "strings"

func validateReturnOrderBySemanticScope(clause string) error {
	body := strings.TrimSpace(clause[len("RETURN"):])
	orderIndex := topLevelKeywordIndex(body, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	projectionBody := strings.TrimSpace(body[:orderIndex])
	orderBody := strings.TrimSpace(body[orderIndex+len("ORDER BY"):])
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
			orderBody = strings.TrimSpace(orderBody[:index])
		}
	}
	projectionBody, distinct := cutDistinct(projectionBody)

	aliases := make(map[string]struct{})
	directExpressions := make(map[string]struct{})
	complexReferences := make(map[string]struct{})
	projectedAggregates := make(map[string]struct{})
	hasProjectionAggregate := false
	for _, item := range splitTopLevelComma(projectionBody) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(item))
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
		if reference := directSemanticReference(expression); reference != "" {
			directExpressions[reference] = struct{}{}
			continue
		}
		for _, reference := range semanticFreeReferences(expression) {
			complexReferences[reference] = struct{}{}
		}
	}

	for _, term := range parseOrderByClause(orderBody) {
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
				references := semanticFreeReferences(call)
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
		scalarExpression := expression
		if hasOrderAggregate {
			scalarExpression = removeAggregateCalls(expression)
		}
		for _, reference := range semanticFreeReferences(scalarExpression) {
			if _, projectedAlias := aliases[reference]; projectedAlias {
				continue
			}
			if _, projectedDirectly := directExpressions[reference]; projectedDirectly {
				continue
			}
			baseReference := strings.SplitN(reference, ".", 2)[0]
			if _, projectedEntity := directExpressions[baseReference]; projectedEntity {
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

func semanticAggregateCalls(expression string) []string {
	calls := make([]string, 0, 1)
	for index := 0; index < len(expression); {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' || !isAggregateFunctionName(name) {
			index = next
			continue
		}
		close := matchingExpressionParenthesis(expression, cursor)
		if close < 0 {
			return calls
		}
		calls = append(calls, strings.TrimSpace(expression[index:close+1]))
		index = close + 1
	}
	return calls
}

func canonicalSemanticExpression(expression string) string {
	var normalized strings.Builder
	normalized.Grow(len(expression))
	var quote byte
	for index := 0; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			normalized.WriteByte(current)
			if current == '\\' && quote != '`' && index+1 < len(expression) {
				index++
				normalized.WriteByte(expression[index])
				continue
			}
			if current == quote {
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' || current == '`' {
			quote = current
			normalized.WriteByte(current)
			continue
		}
		if isASCIIWhitespace(current) {
			continue
		}
		if current >= 'a' && current <= 'z' {
			current -= 'a' - 'A'
		}
		normalized.WriteByte(current)
	}
	return normalized.String()
}

func validateReturnAggregationSemantics(body string) error {
	end := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	body, _ = cutDistinct(body)
	directExpressions := make(map[string]struct{})
	aliases := make(map[string]struct{})
	expressions := make([]string, 0)
	for _, item := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		expressions = append(expressions, expression)
		if alias != "" && alias != expression {
			aliases[normalizeProjectionColumnName(alias)] = struct{}{}
		}
		if !containsAggregateFunc(expression) {
			if reference := directSemanticReference(expression); reference != "" {
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
		// The comprehension / quantifier variables come from the whole
		// expression: removing the aggregate calls can remove their list.
		localBindings := quantifiedExpressionBindings(expression)
		for _, reference := range semanticFreeReferences(removeAggregateCalls(expression)) {
			if _, local := localBindings[strings.SplitN(reference, ".", 2)[0]]; local {
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

func quantifiedExpressionBindings(expression string) map[string]struct{} {
	bindings := make(map[string]struct{})
	// reduce(accumulator = initial, variable IN list | expression)
	for from := 0; from < len(expression); {
		index := keywordIndexFrom(expression, "reduce", from, defaultKeywordScanOpts())
		if index < 0 {
			break
		}
		from = index + len("reduce")
		open := skipSpaces(expression, from)
		if open >= len(expression) || expression[open] != '(' {
			continue
		}
		close := findMatchingParen(expression, open)
		if close < 0 {
			continue
		}
		parts := splitTopLevelComma(expression[open+1 : close])
		if len(parts) < 2 {
			continue
		}
		if equals := strings.IndexByte(parts[0], '='); equals > 0 {
			if accumulator := simpleSemanticIdentifier(parts[0][:equals]); accumulator != "" {
				bindings[accumulator] = struct{}{}
			}
		}
		rest := strings.Join(parts[1:], ",")
		if inIndex := findKeywordIndexInContext(rest, "IN"); inIndex > 0 {
			if variable := simpleSemanticIdentifier(rest[:inIndex]); variable != "" {
				bindings[variable] = struct{}{}
			}
		}
	}
	for _, function := range []string{"all", "any", "none", "single", "filter"} {
		for from := 0; from < len(expression); {
			index := keywordIndexFrom(expression, function, from, defaultKeywordScanOpts())
			if index < 0 {
				break
			}
			from = index + len(function)
			open := skipSpaces(expression, from)
			if open >= len(expression) || expression[open] != '(' {
				continue
			}
			close := findMatchingParen(expression, open)
			if close < 0 {
				continue
			}
			inner := expression[open+1 : close]
			inIndex := findKeywordIndexInContext(inner, "IN")
			if inIndex <= 0 {
				continue
			}
			if variable := simpleSemanticIdentifier(strings.TrimSpace(inner[:inIndex])); variable != "" {
				bindings[variable] = struct{}{}
			}
		}
	}
	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' {
			continue
		}
		close := matchingExpressionBracket(expression, index)
		if close < 0 {
			continue
		}
		variable, _, _, _, comprehension := parseListComprehension(expression[index+1 : close])
		if comprehension {
			bindings[normalizeProjectionColumnName(variable)] = struct{}{}
		}
		index = close
	}
	return bindings
}

func matchingExpressionBracket(expression string, open int) int {
	depth := 0
	var quote byte
	for index := open; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			if current == quote && !isBackslashEscaped(expression, index) {
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' || current == '`' {
			quote = current
			continue
		}
		switch current {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func aggregateContains(expression string, predicate func(string) bool) bool {
	for index := 0; index < len(expression); {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' || !isAggregateFunctionName(name) {
			index = next
			continue
		}
		close := matchingExpressionParenthesis(expression, cursor)
		if close < 0 {
			return false
		}
		if predicate(expression[cursor+1 : close]) {
			return true
		}
		index = close + 1
	}
	return false
}

func containsFunctionCallNamed(expression, expected string) bool {
	for index := 0; index < len(expression); {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if strings.EqualFold(normalizeProjectionColumnName(name), expected) && cursor < len(expression) && expression[cursor] == '(' {
			return true
		}
		index = next
	}
	return false
}

func pipelineReturnSourceColumns(clause string) []string {
	body := strings.TrimSpace(clause[len("RETURN"):])
	end := len(body)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	body, _ = cutDistinct(body)
	if body == "*" || body == "" {
		return nil
	}
	columns := make([]string, 0)
	for _, item := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		if alias == "" {
			alias = expression
		}
		columns = append(columns, alias)
	}
	return columns
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

func semanticExpressionReferences(expression string) []string {
	references := make([]string, 0, 2)
	for index := 0; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		if expression[index] == '$' {
			_, next, ok := scanIdentifierToken(expression, index+1)
			if ok {
				index = next
				continue
			}
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		index = next
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor < len(expression) && expression[cursor] == '(' {
			continue
		}
		if cursor < len(expression) && expression[cursor] == ':' {
			continue
		}
		normalized := normalizeProjectionColumnName(name)
		if isSemanticLiteralWord(normalized) {
			continue
		}
		if cursor < len(expression) && expression[cursor] == '.' {
			property, propertyEnd, propertyOK := scanIdentifierToken(expression, cursor+1)
			if propertyOK {
				normalized += "." + normalizePropertyKey(property)
				index = propertyEnd
			}
		}
		references = append(references, normalized)
	}
	return references
}

// semanticFreeReferences returns the variables and variable.property
// references an expression reads from its scope: the names
// semanticExpressionReferences finds, without the expression's own bindings
// (list comprehension, quantifier and reduce variables) and without anything
// inside braces (a COUNT / EXISTS / COLLECT subquery binds its own pattern
// variables; a map projection's keys aren't variables). The base of a map
// projection (n in n {.k}) is a reference.
func semanticFreeReferences(expression string) []string {
	locals := quantifiedExpressionBindings(expression)
	references := semanticExpressionReferences(maskSemanticBraceBodies(expression))
	free := references[:0]
	for _, reference := range references {
		if _, local := locals[strings.SplitN(reference, ".", 2)[0]]; local {
			continue
		}
		free = append(free, reference)
	}
	return free
}

// maskSemanticBraceBodies blanks every top-level {...} of expression, and the
// COUNT / EXISTS / COLLECT keyword of a subquery before one.
func maskSemanticBraceBodies(expression string) string {
	if strings.IndexByte(expression, '{') < 0 {
		return expression
	}
	masked := []byte(expression)
	for index := 0; index < len(expression); index++ {
		switch expression[index] {
		case '\'', '"':
			index = skipQuotedSemanticText(expression, index) - 1
			continue
		case '{':
		default:
			continue
		}
		close := findMatchingDelimiter(expression, index, '{', '}')
		if close < 0 {
			break
		}
		for blank := index; blank <= close; blank++ {
			masked[blank] = ' '
		}
		end := index
		for end > 0 && isASCIIWhitespace(expression[end-1]) {
			end--
		}
		start := end
		for start > 0 && isAlphaNumericByte(expression[start-1]) {
			start--
		}
		switch strings.ToUpper(expression[start:end]) {
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
	switch strings.ToUpper(value) {
	case "TRUE", "FALSE", "NULL", "NAN", "ASC", "ASCENDING", "DESC", "DESCENDING",
		"AND", "IN", "NOT", "OR", "WHERE", "XOR", "IS", "STARTS", "ENDS", "WITH", "CONTAINS",
		"CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT":
		return true
	default:
		return false
	}
}

func removeAggregateCalls(expression string) string {
	result := []byte(expression)
	for index := 0; index < len(expression); {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		cursor := next
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' || !isAggregateFunctionName(name) {
			index = next
			continue
		}
		close := matchingExpressionParenthesis(expression, cursor)
		if close < 0 {
			return expression
		}
		for blank := index; blank <= close; blank++ {
			result[blank] = ' '
		}
		index = close + 1
	}
	return string(result)
}

func isAggregateFunctionName(name string) bool {
	switch strings.ToUpper(normalizeProjectionColumnName(name)) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT", "STDEV", "STDEVP", "PERCENTILECONT", "PERCENTILEDISC":
		return true
	default:
		return false
	}
}

func matchingExpressionParenthesis(expression string, open int) int {
	depth := 0
	var quote byte
	for index := open; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			if current == quote && !isBackslashEscaped(expression, index) {
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' || current == '`' {
			quote = current
			continue
		}
		switch current {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}
