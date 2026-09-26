package cypher

import (
	"fmt"
	"strings"
)

type matchBindingKind uint8

const (
	matchBindingUnknown matchBindingKind = iota
	matchBindingNode
	matchBindingRelationship
	matchBindingRelationshipList
	matchBindingNodeList
	matchBindingPath
	matchBindingValue
)

type matchSemanticScope map[string]matchBindingKind

// validateMatchSemanticScopes applies entity-type rules before physical query
// routing. MATCH variables may be reused only when their binding kind remains
// stable; a node name cannot already denote a relationship, path, or scalar.
func (e *StorageExecutor) validateMatchSemanticScopes(cypher string) error {
	if e.matchSemanticValidationCache.contains(cypher) {
		return nil
	}
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		for _, branch := range branches {
			if err := e.validateMatchSemanticScopes(branch); err != nil {
				return err
			}
		}
		e.matchSemanticValidationCache.add(cypher)
		return nil
	}

	clauses, ok := splitPipelineClausesAllowingProcedureCalls(cypher)
	if !ok {
		return nil
	}
	scope := make(matchSemanticScope)
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseCall:
			// CALL proc() YIELD x: a yielded name must be new (Neo4j:
			// VariableAlreadyBound). YIELD * names nothing statically, so
			// the rest of the statement isn't checked.
			yield := parseYieldClause(clause.text)
			if yield == nil || yield.yieldAll {
				e.matchSemanticValidationCache.add(cypher)
				return nil
			}
			for _, item := range yield.items {
				name := item.name
				if item.alias != "" {
					name = item.alias
				}
				if _, bound := scope[name]; bound {
					return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
						fmt.Sprintf("procedure output %s shadows an existing variable", name))
				}
				scope[name] = matchBindingUnknown
			}
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			if err := e.validateMatchClauseBindings(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseWith:
			if containsMalformedCreateClauseToken(clause.text) {
				return nil
			}
			if err := validateWithOrderBySemanticScope(scope, clause.text); err != nil {
				return err
			}
			scope = projectMatchSemanticScope(scope, clause.text)
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope[alias] = unwindMatchSemanticKind(clause.text, scope)
			}
		case pipelineClauseReturn:
			if err := validateReturnSemanticScope(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseCreate, pipelineClauseMerge:
			addMatchPatternBindingKinds(scope, clause.text)
		}
	}
	e.matchSemanticValidationCache.add(cypher)
	return nil
}

func validateWithOrderBySemanticScope(input matchSemanticScope, clause string) error {
	body := strings.TrimSpace(clause[len("WITH"):])
	orderIndex := topLevelKeywordIndex(body, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	orderBody := strings.TrimSpace(body[orderIndex+len("ORDER BY"):])
	projectionBody := strings.TrimSpace(body[:orderIndex])
	hasProjectionAggregate := false
	for _, item := range splitTopLevelComma(projectionBody) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		if containsAggregateFunc(expression) {
			hasProjectionAggregate = true
			break
		}
	}
	if err := validateReturnOrderBySemanticScope("RETURN " + body); err != nil {
		return err
	}
	output := projectMatchSemanticScope(input, clause)
	for _, term := range parseOrderByClause(orderBody) {
		if containsAggregateFunc(term.column) && !hasProjectionAggregate {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidAggregation",
				"ORDER BY cannot introduce an aggregate after a non-aggregating WITH",
			)
		}
		for _, reference := range semanticExpressionReferences(term.column) {
			base := strings.SplitN(reference, ".", 2)[0]
			if _, available := input[base]; available {
				continue
			}
			if _, projected := output[base]; projected {
				continue
			}
			return createUndefinedVariableError(base)
		}
	}
	return nil
}

func validateReturnSemanticScope(scope matchSemanticScope, clause string) error {
	if err := validateReturnOrderBySemanticScope(clause); err != nil {
		return err
	}
	body := strings.TrimSpace(clause[len("RETURN"):])
	if err := validateReturnAggregationSemantics(body); err != nil {
		return err
	}
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	body, _ = cutDistinct(body)
	for _, raw := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
		if err := validateGraphFunctionSemanticTypes(expression, scope); err != nil {
			return err
		}
		if err := validateKnownFunctionsInExpression(expression); err != nil {
			return err
		}
		if expression == "*" {
			if len(scope) == 0 {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"NoVariablesInScope",
					"RETURN * requires at least one variable in scope",
				)
			}
			continue
		}
		if _, literal := parseLiteralValueFromComputedRow(expression); literal {
			continue
		}
		if variable := simpleSemanticIdentifier(expression); variable != "" {
			if _, found := scope[variable]; !found {
				return createUndefinedVariableError(variable)
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateMatchClauseBindings(scope matchSemanticScope, clause string) error {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	whereClause := ""
	if where := findKeywordIndexInContext(pattern, "WHERE"); where >= 0 {
		whereClause = strings.TrimSpace(pattern[where+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:where])
	}
	if mergePatternUsesParameterPredicate(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidParameterUse",
			"MATCH pattern predicates must use a property map, not a parameter map",
		)
	}
	if invalidRelationshipPattern(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidRelationshipPattern",
			"invalid variable-length relationship pattern",
		)
	}

	variableLengthRelationships := variableLengthRelationshipVariableSet(pattern)
	seenRelationships := make(map[string]struct{})
	for _, patternPart := range splitTopLevelComma(pattern) {
		patternPart = strings.TrimSpace(patternPart)
		pathVariable := extractPathAssignmentVariable(patternPart)
		entityPattern := patternPart
		if pathVariable != "" {
			if equals := strings.Index(patternPart, "="); equals >= 0 {
				entityPattern = strings.TrimSpace(patternPart[equals+1:])
			}
			if _, alreadyBound := scope[pathVariable]; alreadyBound {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			scope[pathVariable] = matchBindingPath
		}

		for _, variable := range extractNodeVariables(entityPattern) {
			if variable == pathVariable {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			if err := bindMatchSemanticKind(scope, variable, matchBindingNode); err != nil {
				return err
			}
		}
		for _, variable := range extractRelationshipVariables(entityPattern) {
			if variable == pathVariable {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			kind := matchBindingRelationship
			if _, variableLength := variableLengthRelationships[variable]; variableLength {
				kind = matchBindingRelationshipList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
				return err
			}
			if _, exists := seenRelationships[variable]; exists {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"RelationshipUniquenessViolation",
					fmt.Sprintf("relationship variable %s is used more than once in the same pattern", variable),
				)
			}
			seenRelationships[variable] = struct{}{}
		}

	}
	return e.validateMatchWhereSimpleOperands(scope, whereClause)
}

func (e *StorageExecutor) validateMatchWhereSimpleOperands(scope matchSemanticScope, whereClause string) error {
	whereClause = strings.TrimSpace(maskSubqueryBodies(whereClause))
	if containsAggregateFunc(whereClause) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed in WHERE",
		)
	}
	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		if left, right, found := splitByOperatorWithOptions(whereClause, operator, true, true); found {
			if err := e.validateMatchWhereSimpleOperands(scope, left); err != nil {
				return err
			}
			return e.validateMatchWhereSimpleOperands(scope, right)
		}
	}
	if err := e.validateWherePatternExpressionScope(scope, whereClause); err != nil {
		return err
	}
	if operands, _, comparison := splitComparisonChain(whereClause); comparison {
		for _, operand := range operands {
			operand = strings.TrimSpace(operand)
			if variable, _, propertyAccess := parseVarPropertyRef(operand); propertyAccess {
				switch scope[normalizeProjectionColumnName(variable)] {
				case matchBindingPath, matchBindingRelationshipList, matchBindingNodeList:
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"InvalidArgumentType",
						fmt.Sprintf("property access is not supported on %s", variable),
					)
				}
			}
			if _, literal := parseLiteralValueFromComputedRow(operand); literal {
				continue
			}
			identifier := simpleSemanticIdentifier(operand)
			if identifier == "" {
				continue
			}
			_, inScope := scope[identifier]
			_, externallyBound := e.fabricRecordBindings[identifier]
			if !inScope && !externallyBound {
				return createUndefinedVariableError(identifier)
			}
		}
	}
	for _, operator := range []string{" STARTS WITH ", " ENDS WITH ", " CONTAINS ", " NOT IN ", " IN ", "=~"} {
		left, _, found := splitByOperatorWithOptions(whereClause, operator, true, true)
		if !found {
			continue
		}
		left = strings.TrimSpace(left)
		if simpleSemanticIdentifier(left) == left {
			if _, exists := scope[left]; !exists {
				return createUndefinedVariableError(left)
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateWherePatternExpressionScope(scope matchSemanticScope, expression string) error {
	expression = strings.TrimSpace(expression)
	if inner, enclosed := stripEnclosingExpressionParentheses(expression); enclosed {
		if variable := simpleSemanticIdentifier(inner); variable != "" {
			if _, inScope := scope[variable]; !inScope {
				if _, externallyBound := e.fabricRecordBindings[variable]; !externallyBound {
					return createUndefinedVariableError(variable)
				}
			}
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				"a node pattern is not a boolean predicate",
			)
		}
	}
	if !containsRelExistencePattern(expression) {
		return nil
	}
	for _, variable := range append(extractNodeVariables(expression), extractRelationshipVariables(expression)...) {
		if _, inScope := scope[variable]; inScope {
			continue
		}
		if _, externallyBound := e.fabricRecordBindings[variable]; externallyBound {
			continue
		}
		return createUndefinedVariableError(variable)
	}
	return nil
}

func maskSubqueryBodies(expression string) string {
	masked := []byte(expression)
	for index := 0; index < len(expression); index++ {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok || (!strings.EqualFold(name, "exists") && !strings.EqualFold(name, "count") && !strings.EqualFold(name, "collect")) {
			continue
		}
		open := skipSpaces(expression, next)
		if open >= len(expression) || expression[open] != '{' {
			continue
		}
		close := matchingSemanticBrace(expression, open)
		if close < 0 {
			continue
		}
		for position := open + 1; position < close; position++ {
			masked[position] = ' '
		}
		index = close
	}
	return string(masked)
}

func matchingSemanticBrace(expression string, open int) int {
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
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func bindMatchSemanticKind(scope matchSemanticScope, variable string, kind matchBindingKind) error {
	variable = normalizeProjectionColumnName(strings.TrimSpace(variable))
	if variable == "" {
		return nil
	}
	if existing, found := scope[variable]; found && existing != matchBindingUnknown && existing != kind {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"VariableTypeConflict",
			fmt.Sprintf("variable %s is already bound to a different entity type", variable),
		)
	}
	scope[variable] = kind
	return nil
}

func projectMatchSemanticScope(input matchSemanticScope, clause string) matchSemanticScope {
	body := strings.TrimSpace(clause[len("WITH"):])
	for _, keyword := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	body, _ = cutDistinct(body)

	output := make(matchSemanticScope)
	for _, raw := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expression == "*" {
			for variable, kind := range input {
				output[variable] = kind
			}
			continue
		}
		name := alias
		if name == "" {
			name = simpleSemanticIdentifier(expression)
		}
		if name == "" {
			continue
		}
		kind := matchBindingValue
		if strings.EqualFold(strings.TrimSpace(expression), "null") {
			// NULL is compatible with every nullable Cypher binding kind. Keep
			// it unknown until a later pattern or expression supplies context.
			kind = matchBindingUnknown
		} else if source := simpleSemanticIdentifier(expression); source != "" {
			if sourceKind, found := input[source]; found {
				kind = sourceKind
			}
		} else if inferred, ok := coalesceSemanticKind(expression, input); ok {
			kind = inferred
		} else if relationshipListLiteral(expression, input) {
			kind = matchBindingRelationshipList
		} else if aggregateName, aggregateExpression, _, aggregate := parsePipelineAggregate(expression); aggregate && aggregateName == "collect" {
			if source := simpleSemanticIdentifier(aggregateExpression); source != "" {
				switch input[source] {
				case matchBindingNode:
					kind = matchBindingNodeList
				case matchBindingRelationship:
					kind = matchBindingRelationshipList
				}
			}
		}
		output[normalizeProjectionColumnName(name)] = kind
	}
	return output
}

func unwindMatchSemanticKind(clause string, scope matchSemanticScope) matchBindingKind {
	body := strings.TrimSpace(clause[len("UNWIND"):])
	if asIndex := findKeywordIndexInContext(body, "AS"); asIndex >= 0 {
		body = strings.TrimSpace(body[:asIndex])
	}
	if source := simpleSemanticIdentifier(body); source != "" {
		switch scope[source] {
		case matchBindingNodeList:
			return matchBindingNode
		case matchBindingRelationshipList:
			return matchBindingRelationship
		case matchBindingValue:
			return matchBindingUnknown
		}
	}
	if strings.HasPrefix(strings.TrimSpace(body), "[") || matchFuncStartAndSuffix(body, "range") {
		return matchBindingValue
	}
	return matchBindingUnknown
}

func coalesceSemanticKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	name, inner, ok := parseFunctionCallWS(expression)
	if !ok || !strings.EqualFold(name, "coalesce") {
		return matchBindingUnknown, false
	}
	kind := matchBindingUnknown
	for _, argument := range splitTopLevelComma(inner) {
		variable := simpleSemanticIdentifier(argument)
		argumentKind, found := scope[variable]
		if variable == "" || !found || argumentKind == matchBindingUnknown {
			return matchBindingUnknown, false
		}
		if kind != matchBindingUnknown && kind != argumentKind {
			return matchBindingUnknown, false
		}
		kind = argumentKind
	}
	return kind, kind != matchBindingUnknown
}

func variableLengthRelationshipVariableSet(pattern string) map[string]struct{} {
	result := make(map[string]struct{})
	for index := 0; index < len(pattern); index++ {
		if pattern[index] != '[' {
			continue
		}
		end := strings.IndexByte(pattern[index+1:], ']')
		if end < 0 {
			break
		}
		end += index + 1
		inner := strings.TrimSpace(pattern[index+1 : end])
		name, next, ok := scanIdentifierToken(inner, 0)
		if ok && strings.Contains(inner[next:], "*") {
			result[name] = struct{}{}
		}
		index = end
	}
	return result
}

func relationshipListLiteral(expression string, scope matchSemanticScope) bool {
	expression = strings.TrimSpace(expression)
	if len(expression) < 2 || expression[0] != '[' || expression[len(expression)-1] != ']' {
		return false
	}
	items := splitTopLevelComma(expression[1 : len(expression)-1])
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		name := simpleSemanticIdentifier(item)
		if name == "" || scope[name] != matchBindingRelationship {
			return false
		}
	}
	return true
}

func invalidRelationshipPattern(pattern string) bool {
	for index := 0; index < len(pattern); index++ {
		if pattern[index] != '[' {
			continue
		}
		end := strings.IndexByte(pattern[index+1:], ']')
		if end < 0 {
			return true
		}
		end += index + 1
		inner := strings.TrimSpace(pattern[index+1 : end])
		star := strings.IndexByte(inner, '*')
		if strings.Contains(inner, "..") && star < 0 {
			return true
		}
		if star >= 0 {
			spec := strings.TrimSpace(inner[star+1:])
			if property := strings.IndexByte(spec, '{'); property >= 0 {
				spec = strings.TrimSpace(spec[:property])
			}
			for _, character := range spec {
				if (character < '0' || character > '9') && character != '.' {
					return true
				}
			}
			if strings.Count(spec, "..") > 1 || (strings.Contains(spec, ".") && !strings.Contains(spec, "..")) {
				return true
			}
		}
		index = end
	}
	return false
}

func simpleSemanticIdentifier(expression string) string {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok || strings.TrimSpace(expression[next:]) != "" {
		return ""
	}
	return normalizeProjectionColumnName(name)
}

func addMatchPatternBindingKinds(scope matchSemanticScope, clause string) {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"CREATE", "MERGE"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	for _, variable := range extractNodeVariables(pattern) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingNode
		}
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingRelationship
		}
	}
	for _, patternPart := range splitTopLevelComma(pattern) {
		if variable := extractPathAssignmentVariable(strings.TrimSpace(patternPart)); variable != "" {
			if _, found := scope[variable]; !found {
				scope[variable] = matchBindingPath
			}
		}
	}
}
