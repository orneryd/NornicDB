package cypher

import (
	"fmt"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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
// A statement that passes is cached by its text, unless a Fabric APPLY binds
// variables for it: then the result depends on the bound record.
func (e *StorageExecutor) validateMatchSemanticScopes(cypher string) error {
	cacheable := len(e.fabricRecordBindings) == 0
	if cacheable && e.matchSemanticValidationCache.contains(cypher) {
		return nil
	}
	if err := e.validateMatchSemanticScopesUncached(cypher); err != nil {
		return err
	}
	if cacheable {
		e.matchSemanticValidationCache.add(cypher)
	}
	return nil
}

func (e *StorageExecutor) validateMatchSemanticScopesUncached(cypher string) error {
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		for _, branch := range branches {
			if err := e.validateMatchSemanticScopes(branch); err != nil {
				return err
			}
		}
		return nil
	}

	if isSchemaCommandStatement(cypher) {
		return nil
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(cypher)
	if !ok {
		return nil
	}
	scope := make(matchSemanticScope, len(e.fabricRecordBindings))
	// A Fabric APPLY binds its input record's variables for the statement.
	for name := range e.fabricRecordBindings {
		scope[name] = matchBindingUnknown
	}
	// valueTypes holds the static types of variables bound to a literal by
	// WITH … AS or UNWIND, for the function argument checks.
	var valueTypes map[string]string
	returnSeen := false
	for _, clause := range clauses {
		if returnSeen {
			// RETURN is the terminal clause: a clause after it is never a
			// legal statement (RETURN 1 AS x RETURN 2 AS y is Neo4j's
			// "Invalid input 'RETURN'").
			token := strings.Fields(clause.text)[0]
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInput(token))
		}
		if clause.kind == pipelineClauseReturn {
			returnSeen = true
		}
		if clause.kind == pipelineClauseWith {
			// The projection reads the incoming variables; WITH … WHERE
			// reads the projected ones.
			projection := clause.text
			if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
				projection = clause.text[:where]
				if err := graphListOperandTypeError(clause.text[where:], projectMatchSemanticScope(scope, clause.text)); err != nil {
					return err
				}
			}
			if err := graphListOperandTypeError(projection, scope); err != nil {
				return err
			}
		}
		switch clause.kind {
		case pipelineClauseLet, pipelineClauseFilter:
			if err := e.validateSharedClause(scope, valueTypes, clause); err != nil {
				return err
			}
			if clause.kind == pipelineClauseLet {
				projections, err := parsePipelineLet(clause.text)
				if err != nil {
					return err
				}
				for _, projection := range projections {
					if _, exists := scope[projection.alias]; exists {
						return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							fmt.Sprintf("variable %s is already declared", projection.alias))
					}
					kind := projectedExpressionSemanticKind(projection.expression, scope)
					if bound, exists := scope[simpleSemanticIdentifier(projection.expression)]; exists {
						kind = bound
					}
					scope[projection.alias] = kind
					delete(valueTypes, projection.alias)
				}
			}
		case pipelineClauseCallSubquery:
			body, _, _, _ := e.parseCallSubquery(clause.text)
			branches := []string{body}
			if unionBranches, _, _, union := parseTopLevelUnionBranches(body); union && len(unionBranches) > 0 {
				branches = unionBranches
			}
			if returnIndex := topLevelKeywordIndex(branches[0], "RETURN"); returnIndex >= 0 {
				columns := pipelineReturnSourceColumns(branches[0][returnIndex:])
				if columns == nil || slices.Contains(columns, "*") {
					// RETURN * returns every variable of the body, which
					// names nothing statically: as after YIELD *, the rest
					// of the statement isn't checked (#907; checking it
					// rejected CALL { UNWIND [2] AS b RETURN * } RETURN b).
					return nil
				}
				for _, name := range columns {
					// A returned column is a new variable of the enclosing
					// query: one it already binds is declared twice
					// (Neo4j 5.26.30's VariableAlreadyBound), unless every
					// branch returns that outer variable itself, unchanged
					// (callSubqueryReturnsOuterUnchanged).
					if _, bound := scope[name]; bound && !callSubqueryReturnsOuterUnchanged(branches, name) {
						return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							localization.CypherCoreVariableDeclaredInOuterScope(name))
					}
					scope[name] = matchBindingUnknown
				}
			}
		case pipelineClauseCall:
			// CALL proc() YIELD x: a yielded name must be new (Neo4j:
			// VariableAlreadyBound). YIELD * names nothing statically, so
			// the rest of the statement isn't checked.
			yield := parseYieldClause(clause.text)
			if yield == nil || yield.yieldAll {
				return nil
			}
			outputTypes := procedureOutputTypes(clause.text)
			for _, item := range yield.items {
				name := item.name
				if item.alias != "" {
					name = item.alias
				}
				if _, bound := scope[name]; bound {
					return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
						localization.CypherCoreProcedureOutputShadowsVariable(name))
				}
				scope[name] = matchBindingUnknown
				if typeName := outputTypes[item.name]; typeName != "" {
					if valueTypes == nil {
						valueTypes = make(map[string]string)
					}
					valueTypes[name] = typeName
				}
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			if err := e.validateMatchClauseBindings(scope, clause.text); err != nil {
				return err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		case pipelineClauseWith:
			if containsMalformedCreateClauseToken(clause.text) {
				return nil
			}
			if err := validateWithOrderBySemanticScope(scope, clause.text); err != nil {
				return err
			}
			// The WHERE after WITH reads the projected scope merged over the
			// incoming one, as at runtime (aliases shadow same-named inputs).
			if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
				projected := projectMatchSemanticScope(scope, clause.text)
				whereScope := make(matchSemanticScope, len(scope)+len(projected))
				for name, kind := range scope {
					whereScope[name] = kind
				}
				for name, kind := range projected {
					whereScope[name] = kind
				}
				if err := e.validateMatchWhereSimpleOperands(whereScope, clause.text[where+len("WHERE"):]); err != nil {
					return err
				}
			}
			// The projection sees the incoming variables; WHERE / ORDER BY
			// after it see the projected ones.
			projection, rest := splitWithProjection(clause.text)
			projectionBody, _ := cutDistinct(strings.TrimSpace(projection[len("WITH"):]))
			for _, raw := range splitTopLevelComma(projectionBody) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
				if err := projectionItemTermError(expression); err != nil {
					return err
				}
				if err := projectionAliasError(raw); err != nil {
					return err
				}
				if expression != "*" {
					if err := undefinedExpressionVariable(scope, expression); err != nil {
						return err
					}
				}
			}
			input := staticTypeScope{kinds: scope, values: valueTypes, complete: true}
			if err := validateStaticFunctionVariables(projection, input); err != nil {
				return err
			}
			if err := e.validateStaticOperatorTypes(clause, input, func() staticTypeScope { return projectionAliasScope(input, clause.text) }, nil); err != nil {
				return err
			}
			scope = projectMatchSemanticScope(scope, clause.text)
			valueTypes = projectStaticValueTypes(input, clause.text)
			projected := staticTypeScope{kinds: scope, values: valueTypes, complete: true}
			if err := forEachProjectedTailExpression(projection, rest, func(expression string) error {
				return validateStaticFunctionVariables(expression, projected)
			}); err != nil {
				return err
			}
		case pipelineClauseUnwind:
			if err := validateUnwindAlias(clause.text); err != nil {
				return err
			}
			if err := undefinedExpressionVariable(scope, unwindSourceExpression(clause.text)); err != nil {
				return err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
			if alias := unwindBindingName(clause.text); alias != "" {
				// FOR is new syntax: unlike the pre-existing permissive UNWIND,
				// it must not redeclare an already-bound variable, as in Neo4j.
				if startsWithKeywordFold(clause.text, "FOR") {
					if _, bound := scope[alias]; bound {
						return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							fmt.Sprintf("variable %s is already declared", alias))
					}
				}
				scope[alias] = unwindMatchSemanticKind(clause.text, scope)
				delete(valueTypes, alias)
				if typeName := unwindStaticValueType(clause.text); typeName != "" {
					if valueTypes == nil {
						valueTypes = make(map[string]string)
					}
					valueTypes[alias] = typeName
				}
			}
		case pipelineClauseReturn:
			if err := validateReturnSemanticScope(scope, clause.text); err != nil {
				return err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		case pipelineClauseDelete:
			if err := deleteTargetTypeError(clause.text, scope); err != nil {
				return err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		case pipelineClauseCreate, pipelineClauseMerge:
			addMatchPatternBindingKinds(scope, clause.text)
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		default:
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, complete: true}); err != nil {
				return err
			}
		}
		if clause.kind != pipelineClauseWith {
			// Every other clause reads its own and the incoming bindings.
			if err := graphListOperandTypeError(clause.text, scope); err != nil {
				return err
			}
		}
	}
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
	for _, term := range parseOrderByClause(orderBody) {
		if containsAggregateFunc(term.column) && !hasProjectionAggregate {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidAggregation",
				"ORDER BY cannot introduce an aggregate after a non-aggregating WITH",
			)
		}
	}
	return validateOrderByReferences(input, projectMatchSemanticScope(input, clause), orderBody)
}

// validateOrderByReferences checks that every variable an ORDER BY of a WITH
// or RETURN reads is in the clause's incoming scope or one it projects, as
// Neo4j does ("Variable `y` not defined"). orderBody is the text after ORDER
// BY; its SKIP / LIMIT are ignored.
func validateOrderByReferences(input, output matchSemanticScope, orderBody string) error {
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
			orderBody = strings.TrimSpace(orderBody[:index])
		}
	}
	for _, term := range parseOrderByClause(orderBody) {
		for _, reference := range semanticFreeReferences(term.column) {
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
	// A dangling modifier keyword (RETURN 1 ORDER BY / SKIP / LIMIT) and an
	// empty projection (RETURN, WITH 1 AS x RETURN) are Neo4j syntax errors.
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (RETURN skip[0], #894).
	clauseText := strings.TrimSpace(clause)
	projectionEnd := len(clauseText)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		index := topLevelKeywordIndex(clauseText, keyword)
		if index < len("RETURN") || index >= projectionEnd {
			continue
		}
		if strings.TrimSpace(clauseText[index+len(keyword):]) == "" {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(""))
		}
		projectionEnd = index
	}
	projectionText := strings.TrimSpace(clauseText[len("RETURN"):projectionEnd])
	body, _ = cutDistinct(projectionText)
	if body == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherMatchingReturnExpressionRequired())
	}
	if orderIndex := topLevelKeywordIndex(clauseText, "ORDER BY"); orderIndex >= len("RETURN") {
		projection := strings.TrimSpace(clauseText[len("RETURN"):orderIndex])
		if err := validateOrderByReferences(scope, projectMatchSemanticScope(scope, "WITH "+projection), strings.TrimSpace(clauseText[orderIndex+len("ORDER BY"):])); err != nil {
			return err
		}
	}
	for _, raw := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
		if err := projectionItemTermError(expression); err != nil {
			return err
		}
		if err := validateKnownFunctionsInExpression(expression); err != nil {
			return err
		}
		if err := undefinedExpressionVariable(scope, expression); err != nil {
			return err
		}
		if expression == "*" {
			if len(scope) == 0 {
				return &classifiedCypherError{
					cause:  localizedError(localization.CypherMatchingReturnStarNoVariables(), nil),
					code:   "Neo.ClientError.Statement.SyntaxError",
					detail: "NoVariablesInScope",
				}
			}
			continue
		}
		if _, literal := parseLiteralValueFromComputedRow(expression); literal {
			continue
		}
		variable := simpleSemanticIdentifier(expression)
		if variable == "" {
			// m.val: the property chain's variable must be bound.
			if base, _, chain := rowPropertyChainShape(expression); chain {
				variable = base
			}
		}
		// null.a is a property of null (null), not of a variable.
		if variable != "" && !isLiteralKeyword(variable) {
			if _, found := scope[variable]; !found {
				return createUndefinedVariableError(variable)
			}
		}
	}
	return nil
}

// undefinedExpressionVariable is "Variable `x` not defined" for the first
// variable expression reads that scope doesn't bind: anywhere in it,
// inside a CASE, a list, a map projection or a comprehension's WHERE and
// projection included, as in Neo4j (#907). A comprehension's or a
// reduce's own variable is bound inside it (expressionFreeVariables).
func undefinedExpressionVariable(scope matchSemanticScope, expression string) error {
	// A literal, a variable or a property chain reads at most its base
	// variable: checked without the scanner.
	if _, literal := parseLiteralValueFromComputedRow(expression); literal {
		return nil
	}
	variable := simpleSemanticIdentifier(expression)
	if variable == "" {
		if base, _, chain := rowPropertyChainShape(expression); chain {
			variable = base
		}
	}
	if variable != "" {
		if _, found := scope[variable]; !found && !isLiteralKeyword(variable) {
			return createUndefinedVariableError(variable)
		}
		return nil
	}
	// expressionFreeVariables leaves out true, false, null and the other
	// literal words.
	for _, name := range expressionFreeVariables(expression) {
		if _, found := scope[name]; !found {
			return createUndefinedVariableError(name)
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
	// A WHERE inside parentheses is a quantified path pattern's own.
	if where := topLevelKeywordIndex(pattern, "WHERE"); where >= 0 {
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
	groupNodes, groupRelationships := quantifiedGroupVariables(pattern)
	for variable := range groupNodes {
		if _, bound := scope[variable]; bound {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherMatchingQuantifiedPathVariableBound(variable))
		}
	}
	for variable := range groupRelationships {
		if _, bound := scope[variable]; bound {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherMatchingQuantifiedPathVariableBound(variable))
		}
	}
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
			kind := matchBindingNode
			if _, grouped := groupNodes[variable]; grouped {
				kind = matchBindingNodeList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
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
			if _, grouped := groupRelationships[variable]; grouped {
				kind = matchBindingRelationshipList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
				return err
			}
		}

	}
	return e.validateMatchWhereSimpleOperands(scope, whereClause)
}

func (e *StorageExecutor) validateMatchWhereSimpleOperands(scope matchSemanticScope, whereClause string) error {
	whereClause = strings.TrimSpace(maskSubqueryBodies(whereClause))
	if whereClause == "" {
		return nil
	}
	switch whereClause[0] {
	case '=', '<', '>', '~':
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression(string(whereClause[0])))
	case '!':
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression("!"))
	}
	// A clause keyword read as a variable followed by another term with no
	// operator between them is Neo4j's Invalid input error (WHERE RETURN n:
	// return is the variable, n is invalid — the #740 reading).
	if err := projectionItemTermError(whereClause); err != nil {
		return err
	}
	if containsAggregateFunc(whereClause) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed in WHERE",
		)
	}
	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		if left, right, found := splitByOperatorOutsideCase(whereClause, operator, true, true); found {
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
			// A clause keyword read as a variable followed by another term with
			// no operator between them is Neo4j's Invalid input error
			// (WHERE n.x = RETURN n: return is the variable, n is invalid).
			if err := projectionItemTermError(operand); err != nil {
				return err
			}
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
		left, right, found := splitByOperatorOutsideCase(whereClause, operator, true, true)
		if !found {
			continue
		}
		left = strings.TrimSpace(left)
		// A clause keyword read as a variable on the operator's right followed
		// by another term is Invalid input (WHERE n.x IN RETURN n).
		if err := projectionItemTermError(strings.TrimSpace(right)); err != nil {
			return err
		}
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
		// (true), (false) and (null) are parenthesised literals, not node
		// patterns (#878 writes an element's own predicate in parentheses).
		if variable := simpleSemanticIdentifier(inner); variable != "" && !isLiteralKeyword(variable) {
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
		close := findMatchingDelimiter(expression, open, '{', '}')
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

// schemaCommandKeywords follow CREATE or DROP in a schema command.
var schemaCommandKeywords = []string{"CONSTRAINT", "INDEX", "FULLTEXT", "VECTOR", "RANGE", "TEXT", "POINT", "LOOKUP", "BTREE", "OR REPLACE"}

// isSchemaCommandStatement reports whether cypher is a schema command
// (CREATE / DROP CONSTRAINT, INDEX, …): its patterns and expressions declare a
// schema rule over a pattern (FOR ()-[r:T]-() REQUIRE …) and bind no clause
// variables.
func isSchemaCommandStatement(cypher string) bool {
	trimmed := strings.TrimSpace(cypher)
	for _, command := range []string{"CREATE", "DROP"} {
		if !startsWithKeywordFold(trimmed, command) {
			continue
		}
		rest := strings.TrimSpace(trimmed[len(command):])
		for _, keyword := range schemaCommandKeywords {
			if startsWithKeywordFold(rest, keyword) {
				return true
			}
		}
	}
	return false
}

// validateStaticClauseTypes applies the compile-time type checks of one
// clause: function arguments (validateStaticFunctionVariables) and operators
// (validateStaticOperatorTypes).
func (e *StorageExecutor) validateStaticClauseTypes(clause pipelineClause, scope staticTypeScope) error {
	if err := validateStaticPropertySubscripts(clause.text, scope); err != nil {
		return err
	}
	if err := staticWriteTokenError(clause, scope); err != nil {
		return err
	}
	if len(scope.values) > 0 {
		if err := staticListOperandTypeError(clause.text, scope); err != nil {
			return err
		}
	}
	var projectedScope func() staticTypeScope
	if clause.kind == pipelineClauseReturn {
		projection, rest := splitWithProjection(clause.text)
		var projected *staticTypeScope
		projectedScope = func() staticTypeScope {
			if projected == nil {
				built := projectionAliasScope(scope, clause.text)
				projected = &built
			}
			return *projected
		}
		if err := validateStaticFunctionVariables(projection, scope); err != nil {
			return err
		}
		if err := forEachProjectedTailExpression(projection, rest, func(expression string) error {
			return validateStaticFunctionVariablesIn(expression, projectedScope)
		}); err != nil {
			return err
		}
	} else if err := validateStaticFunctionVariables(clause.text, scope); err != nil {
		return err
	}
	return e.validateStaticOperatorTypes(clause, scope, projectedScope, nil)
}

// projectionAliasScope is the scope of the WHERE / ORDER BY after a RETURN or
// WITH projection: the incoming variables, with each projection alias
// replacing the variable of the same name (RETURN n.num AS n ORDER BY n + 2
// sees n as a number, not the node).
func projectionAliasScope(input staticTypeScope, clause string) staticTypeScope {
	body := strings.TrimSpace(clause)
	for _, keyword := range []string{"RETURN", "WITH"} {
		if startsWithKeywordFold(body, keyword) {
			body = strings.TrimSpace(body[len(keyword):])
			break
		}
	}
	projected := projectMatchSemanticScope(input.kinds, "WITH "+body)
	projectedValues := projectStaticValueTypes(input, "WITH "+body)
	kinds := make(matchSemanticScope, len(input.kinds)+len(projected))
	for name, kind := range input.kinds {
		kinds[name] = kind
	}
	values := make(map[string]string, len(input.values)+len(projectedValues))
	for name, typeName := range input.values {
		values[name] = typeName
	}
	for name, kind := range projected {
		kinds[name] = kind
		delete(values, name)
	}
	for name, typeName := range projectedValues {
		values[name] = typeName
	}
	return staticTypeScope{kinds: kinds, values: values, complete: input.complete}
}

// projectionItemTermError is Neo4j's SyntaxError for a projection item that
// is a clause-keyword name followed by another term with no operator between
// them (WITH RETURN n: return is a variable, as Neo4j reads it (#740), and n
// is "Invalid input"). Before, the keyword was taken for a clause.
func projectionItemTermError(expression string) error {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok || !isNameableClauseKeyword(name) {
		return nil
	}
	rest := strings.TrimSpace(expression[next:])
	if rest == "" || rest == expression[next:] {
		return nil // the name alone, or followed directly by an operator
	}
	if c := rest[0]; isIdentByte(c) || c == '\'' || c == '"' || c == '`' || c == '$' {
		token, _, _ := scanIdentifierToken(rest, 0)
		if token == "" {
			token = rest[:1]
		}
		if !isExpressionContinuationWord(token) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(token))
		}
	}
	return nil
}

// projectionAliasError is Neo4j's SyntaxError for a projection item whose
// alias, as written, is not one name (WITH n AS return n: the n after the
// alias is "Invalid input"). A backtick-quoted alias is one name.
func projectionAliasError(item string) error {
	// The alias follows the item's own AS (projectionAliasIndex), so an alias
	// named as is a name (WITH n AS as, #894).
	as := projectionAliasIndex(item)
	if as < 0 {
		return nil
	}
	alias := strings.TrimSpace(item[as+len("AS"):])
	if alias == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInput(""))
	}
	_, next, ok := scanSymbolicName(alias, 0)
	if !ok || next == len(alias) {
		return nil
	}
	rest := strings.TrimSpace(alias[next:])
	token, _, _ := scanIdentifierToken(rest, 0)
	if token == "" {
		token = rest[:1]
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCoreInvalidInputExpectedExpression(token))
}

// isExpressionContinuationWord reports whether word, after an operand,
// continues the expression (an operator word: AND, IS NULL, STARTS WITH, …).
func isExpressionContinuationWord(word string) bool {
	for _, operator := range [...]string{"AND", "OR", "XOR", "IS", "IN", "STARTS", "ENDS", "CONTAINS", "NOT"} {
		if strings.EqualFold(word, operator) {
			return true
		}
	}
	return false
}

// splitWithProjection splits a WITH clause into its projection and the WHERE /
// ORDER BY / SKIP / LIMIT that follow it.
func splitWithProjection(clause string) (projection, rest string) {
	end := len(clause)
	for _, keyword := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	return clause[:end], clause[end:]
}

func projectMatchSemanticScope(input matchSemanticScope, clause string) matchSemanticScope {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")

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
		} else if listKind, entities := entityListLiteralKind(expression, input); entities {
			kind = listKind
		} else if aggregateName, aggregateExpression, _, aggregate := parsePipelineAggregate(expression); !aggregate {
			kind = projectedExpressionSemanticKind(expression, input)
		} else if aggregateName == "collect" {
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

// unwindSourceExpression is the list expression of an UNWIND clause.
func unwindSourceExpression(clause string) string {
	expression, _, _ := parsePipelineIteration(clause)
	return expression
}

// projectedExpressionSemanticKind is the binding kind of a projected
// expression that isn't a bare variable, as Neo4j types it before the
// statement runs: a value (never a node or relationship) when its type is
// known (a literal, arithmetic over known types, a node's or relationship's
// property, a call with a known result type); the entity an element of a node
// or relationship list is; and unknown otherwise (a map's member x.node, an
// element of a list of unknown elements), which Neo4j accepts in a pattern
// and checks when the row runs.
func projectedExpressionSemanticKind(expression string, input matchSemanticScope) matchBindingKind {
	expression = strings.TrimSpace(expression)
	if name, end, ok := scanIdentifierToken(expression, 0); ok && end < len(expression) && expression[end] == '[' &&
		findMatchingDelimiter(expression, end, '[', ']') == len(expression)-1 && !strings.Contains(expression[end:], "..") {
		switch input[name] {
		case matchBindingNodeList:
			return matchBindingNode
		case matchBindingRelationshipList:
			return matchBindingRelationship
		}
		return matchBindingUnknown
	}
	if variable, _, property := parseVarPropertyRef(expression); property && simpleSemanticIdentifier(variable) == variable {
		switch input[variable] {
		case matchBindingNode, matchBindingRelationship:
			return matchBindingValue
		}
		return matchBindingUnknown
	}
	typeName := (staticTypeScope{kinds: input}).staticExpressionType(expression)
	if function, arguments, call := parseFunctionCallWS(expression); typeName == "" && call {
		typeName = staticFunctionResultType(function, arguments)
	}
	switch typeName {
	case "":
		return matchBindingUnknown
	case "Node":
		return matchBindingNode
	case "Relationship":
		return matchBindingRelationship
	case "List<Node>":
		return matchBindingNodeList
	case "List<Relationship>":
		return matchBindingRelationshipList
	case "Path":
		return matchBindingPath
	}
	return matchBindingValue
}

func unwindMatchSemanticKind(clause string, scope matchSemanticScope) matchBindingKind {
	body := unwindSourceExpression(clause)
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
	switch listKind, _ := entityListLiteralKind(body, scope); listKind {
	case matchBindingNodeList:
		return matchBindingNode
	case matchBindingRelationshipList:
		return matchBindingRelationship
	}
	if typeName := staticLiteralTypeName(body); (typeName != "" && typeName != "List<T>") || matchFuncStartAndSuffix(body, "range") {
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

// entityListLiteralKind is the kind of a list literal of node variables
// ([a, b] is a List<Node>) or of relationship variables; ok is false for any
// other expression.
func entityListLiteralKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	expression = strings.TrimSpace(expression)
	if len(expression) < 2 || expression[0] != '[' || expression[len(expression)-1] != ']' {
		return matchBindingUnknown, false
	}
	items := splitTopLevelComma(expression[1 : len(expression)-1])
	if len(items) == 0 {
		return matchBindingUnknown, false
	}
	element := matchBindingUnknown
	for _, item := range items {
		name := simpleSemanticIdentifier(item)
		kind := scope[name]
		if name == "" || (kind != matchBindingNode && kind != matchBindingRelationship) || (element != matchBindingUnknown && kind != element) {
			return matchBindingUnknown, false
		}
		element = kind
	}
	if element == matchBindingNode {
		return matchBindingNodeList, true
	}
	return matchBindingRelationshipList, true
}

func invalidRelationshipPattern(pattern string) bool {
	for index := 0; index < len(pattern); index++ {
		if pattern[index] != '[' {
			continue
		}
		end := findMatchingDelimiter(pattern, index, '[', ']')
		if end < 0 {
			return true
		}
		// Quoted names and values can hold * and .. (#879).
		inner := strings.TrimSpace(blankQuotedText(pattern[index+1 : end]))
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

// simpleSemanticIdentifier returns the variable an expression consists of,
// plain or backtick-quoted (`x y`), unquoted; "" for any other expression.
func simpleSemanticIdentifier(expression string) string {
	expression = strings.TrimSpace(expression)
	// A backtick-quoted variable (`my x`, `$p`) is a variable whatever its
	// characters: it isn't a parameter, and scope checks must see it.
	if isBacktickQuotedName(expression) {
		return normalizeProjectionColumnName(expression)
	}
	name, next, ok := scanSymbolicName(expression, 0)
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

// deleteTargetTypeError rejects a DELETE target whose type, known before the
// statement runs, is a property of a node or relationship (n.p, a stored
// value): Neo4j 5.26's "Type mismatch: expected Node, Path or Relationship"
// (#907). DELETE removes entities, and a property is removed with REMOVE;
// NornicDB used to accept the statement and delete nothing. A map's key (m.k)
// has no static type. null is a valid target that deletes nothing.
//
// A list of nodes or relationships (the variable of a variable-length
// relationship, MATCH ()-[x*]->() DELETE x) is a NornicDB extension: Neo4j
// 5.26 rejects it, NornicDB deletes each entity in it, as before (at the
// project owner's direction).
func deleteTargetTypeError(clause string, scope matchSemanticScope) error {
	targets, _, _ := deleteClauseTargets(clause)
	for _, expression := range targets {
		expression = strings.TrimSpace(expression)
		if variable, _, property := parseVarPropertyRef(expression); property && isEntityPropertyAccess(expression, variable) {
			switch scope[normalizeProjectionColumnName(variable)] {
			case matchBindingNode, matchBindingRelationship:
				return typeNameMismatchError("Node, Path or Relationship", "a property value")
			}
		}
	}
	return nil
}

// isEntityPropertyAccess reports whether expression, which parseVarPropertyRef
// read as variable.key, is exactly that, with nothing after the key (no
// subscript or further access).
func isEntityPropertyAccess(expression, variable string) bool {
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(expression), variable))
	key := strings.TrimSpace(strings.TrimPrefix(rest, "."))
	return isValidIdentifier(key) || isBacktickQuotedName(key)
}

// callSubqueryReturnsOuterUnchanged reports whether every branch of a CALL
// subquery returns the enclosing query's variable name as itself (RETURN
// name, or name AS name) without declaring name again in its body. Neo4j
// rejects even that as VariableAlreadyBound; NornicDB accepts it as an
// extension, since the column is the outer value and nothing is ambiguous
// (#907; the implicit import is the same kind of extension). A branch that
// rebinds name (WITH … AS name, UNWIND … AS name, YIELD name, a nested CALL
// returning it) or returns another value under it is rejected.
func callSubqueryReturnsOuterUnchanged(branches []string, name string) bool {
	for _, branch := range branches {
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
		if !ok || len(clauses) == 0 || clauses[len(clauses)-1].kind != pipelineClauseReturn {
			return false
		}
		for _, clause := range clauses {
			switch clause.kind {
			case pipelineClauseWith, pipelineClauseReturn:
				keyword := "WITH"
				if clause.kind == pipelineClauseReturn {
					keyword = "RETURN"
				}
				items, _ := projectionSemanticBodyAndTail(clause.text, keyword)
				items, _ = cutDistinct(strings.TrimSpace(items))
				for _, raw := range splitTopLevelComma(items) {
					expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
					if normalizeProjectionColumnName(alias) == name && normalizeProjectionColumnName(expression) != name {
						return false
					}
				}
			case pipelineClauseUnwind:
				if unwindBindingName(clause.text) == name {
					return false
				}
			case pipelineClauseCallSubquery:
				inner, _, _, _ := (&StorageExecutor{}).parseCallSubquery(clause.text)
				if returnIndex := topLevelKeywordIndex(inner, "RETURN"); returnIndex >= 0 {
					for _, column := range pipelineReturnSourceColumns(inner[returnIndex:]) {
						if column == name {
							return false
						}
					}
				}
			case pipelineClauseCall:
				if yield := parseYieldClause(clause.text); yield != nil {
					if yield.yieldAll {
						return false
					}
					for _, item := range yield.items {
						if item.name == name || item.alias == name {
							return false
						}
					}
				}
			}
		}
	}
	return true
}
