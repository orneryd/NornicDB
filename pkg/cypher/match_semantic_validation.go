package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"maps"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
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
// A statement that passes is cached by its text and language version,
// unless a Fabric APPLY binds variables for it: then the result depends on
// the bound record. cypher25 holds a Cypher 25 statement to Neo4j 2026.09's
// compile-time type rules (staticTypeScope.cypher25).
func (e *StorageExecutor) validateMatchSemanticScopes(ctx context.Context, cypher string, cypher25 bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cacheable := len(e.fabricRecordBindings) == 0
	key := semanticCacheKey(cypher, cypher25)
	if cacheable && e.matchSemanticValidationCache.contains(key) {
		return nil
	}
	if err := checkedWithoutUnaryPlus(ctx, cypher, cypher25, func(text string) error {
		return e.validateMatchSemanticScopesUncached(ctx, text, cypher25)
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if cacheable {
		e.matchSemanticValidationCache.add(key)
	}
	return nil
}

// semanticCacheKey is a validation cache's key for a statement: its text,
// marked for a Cypher 25 statement, whose checks differ.
func semanticCacheKey(cypher string, cypher25 bool) string {
	if cypher25 {
		return "cypher25:" + cypher
	}
	return cypher
}

func (e *StorageExecutor) validateMatchSemanticScopesUncached(ctx context.Context, source string, cypher25 bool) error {
	if isSchemaCommandStatement(source) || isNornicExtensionStatement(source) || isMetadataCommand(source, metadataCatalogExecution) {
		return nil
	}
	initial := make(typeScope, len(e.fabricRecordBindings))
	for name := range e.fabricRecordBindings {
		initial[name] = staticOperand{}
	}
	for name := range valueBindingsFromContext(ctx) {
		initial[name] = staticOperand{}
	}
	_, _, err := e.enrichBoundTypes(ctx, source, initial, semanticBodyFromContext(ctx), "", cypher25)
	return err
}
func validateQueryBodyComposition(ctx context.Context, source string) error {
	admission, err := (&StorageExecutor{}).admitBoundStatement(ctx, source)
	if err != nil {
		return err
	}
	if admission.command {
		return validateCommandBodyComposition(ctx, source)
	}
	return nil
}

// expressionBodyUpdatesMessages is the error for an update in each
// expression container's body.
var expressionBodyUpdatesMessages = map[string]func() localization.Message{
	"EXISTS":  localization.CypherMatchingExistsExpressionUpdates,
	"COUNT":   localization.CypherMatchingCountExpressionUpdates,
	"COLLECT": localization.CypherMatchingCollectExpressionUpdates,
}

// queryBodyEndings is how each clause kind ends a query body: the keyword
// Neo4j names when it can't ("Query cannot conclude with MATCH"), or "" when
// it concludes the body (RETURN, an update clause, a procedure call). A CALL
// subquery concludes only as a unit subquery (a direct RETURN in its body).
// Every clause kind is listed (TestQueryBodyEndingsCoverEveryClauseKind).
var queryBodyEndings = map[pipelineClauseKind]string{
	pipelineClauseMatch: "MATCH", pipelineClauseOptionalMatch: "MATCH", pipelineClauseWith: "WITH",
	pipelineClauseUnwind: "UNWIND", pipelineClauseLet: "LET", pipelineClauseFilter: "FILTER",
	pipelineClauseCallSubquery: "CALL",
	pipelineClauseReturn:       "", pipelineClauseCreate: "", pipelineClauseMerge: "", pipelineClauseDelete: "",
	pipelineClauseSet: "", pipelineClauseRemove: "", pipelineClauseForeach: "", pipelineClauseCall: "",
}

// undefinedExpressionVariable is "Variable `x` not defined" for the first
// variable expression reads that scope doesn't bind: anywhere in it,
// inside a CASE, a list, a map projection or a comprehension's WHERE and
// projection included, as in Neo4j (#907). A comprehension's or a
// reduce's own variable is bound inside it (expressionFreeVariables).
func undefinedExpressionVariable(ctx context.Context, scope matchSemanticScope, expression string) error {
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
	for _, name := range expressionFreeVariables(ctx, expression) {
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
	}
	// A quantified group's WHERE is an expression, not pattern structure.
	pattern = strings.TrimSpace(matchPatternStructure(pattern))

	variableLengthRelationships := variableLengthRelationshipVariableSet(pattern)
	groupNodes, groupRelationships := quantifiedGroupVariables(pattern)
	for _, patternPart := range splitTopLevelComma(pattern) {
		patternPart = strings.TrimSpace(patternPart)
		pathVariable := extractPathAssignmentVariable(patternPart)
		entityPattern := patternPart
		if pathVariable != "" {
			if equals := strings.Index(patternPart, "="); equals >= 0 {
				entityPattern = strings.TrimSpace(patternPart[equals+1:])
			}

			scope[pathVariable] = matchBindingPath
		}

		if _, _, _, selected := splitPathSelectorTerm(whereClause); !selected {
			if err := validateShortestCallBindings(entityPattern, scope); err != nil {
				return err
			}
		}
		for _, variable := range extractNodeVariables(entityPattern) {
			kind := matchBindingNode
			if _, grouped := groupNodes[variable]; grouped {
				kind = matchBindingNodeList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
				return err
			}
		}
		for _, variable := range extractRelationshipVariables(entityPattern) {
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
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				"a node pattern is not a boolean predicate",
			)
		}
	}

	return nil
}

// maskPatternComprehensions blanks, keeping byte offsets, the inside of every
// pattern comprehension in expression ([(a)-->(b) WHERE â€¦ | b.x]), whose
// pattern binds variables of its own. Quoted text and relationship brackets
// (-[r]->) are not comprehensions.
func maskPatternComprehensions(expression string) string {
	if strings.IndexByte(expression, '[') < 0 {
		return expression
	}
	var masked []byte
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
			continue
		case '[':
		default:
			continue
		}
		if index > 0 && expression[index-1] == '-' {
			continue
		}
		close := findMatchingBracket(expression, index)
		if close < 0 {
			return expression
		}
		if _, _, comprehension := splitPatternComprehension(expression[index : close+1]); comprehension {
			if masked == nil {
				masked = []byte(expression)
			}
			for position := index + 1; position < close; position++ {
				masked[position] = ' '
			}
			index = close
		}
	}
	if masked == nil {
		return expression
	}
	return string(masked)
}

// maskSubqueryBodies replaces each classified subquery operand with a literal
// of its result kind, preserving byte offsets. Leaving EXISTS/COUNT/COLLECT
// before an erased body makes later readers mistake it for a pattern variable
// or keyword-named map projection. Classify the original body before masking;
// quoted text and actual map projections keep their meaning.
func maskSubqueryBodies(expression string) string {
	subqueries := findSubqueryExpressions(expression)
	if len(subqueries) == 0 {
		return expression
	}
	masked := []byte(expression)
	for _, subquery := range subqueries {
		for position := subquery.start; position < subquery.end; position++ {
			masked[position] = ' '
		}
		literal := "true"
		switch subquery.kind {
		case "COUNT":
			literal = "0"
		case "COLLECT":
			literal = "[]"
		}
		copy(masked[subquery.start:], literal)
	}
	return string(masked)
}

// validateSubqueryPatternBindings checks every EXISTS / COUNT / COLLECT
// subquery in text as a query of its own, walked by the one clause walk
// (semanticScopeWalk) from scope, the variables the subquery sees at its
// position (a local of an enclosing expression typed as that local;
// withLocalTypes). The walk binds the body's MATCH and WITH variables and
// checks its nested subqueries from them in turn: a pattern can't use an
// outer variable as another kind (COUNT { (n)-[s]->() } with s a string is
// Neo4j's "s defined with conflicting type", #1055). A local the body's own
// clauses declare (a comprehension's, a quantifier's, reduce's, a named
// path; not a nested subquery's or CALL's) may not redeclare a variable it
// sees: Neo4j's "is shadowing a variable with the same name from the outer
// scope". At the top level and in CALL { } such a local shadows instead.
func (e *StorageExecutor) validateSubqueryPatternBindings(ctx context.Context, scope staticTypeScope, source string) error {
	if scope.admitted != nil {
		return nil
	}
	if !mayContainSubqueryExpression(source) && !containsRelationshipPattern(source) {
		return nil
	}
	_, _, err := e.enrichBoundTypes(ctx, typeExpressionSource(source), typeScopeOf(scope), scope.bindings, "", scope.cypher25)
	return err
}
func validateExpressionPatternBindings(ctx context.Context, scope staticTypeScope, text string) error {
	if !containsRelationshipPattern(text) {
		return nil
	}
	start := 0
	// MATCH's own pattern was already checked and bound; its predicate
	// contains the expression patterns checked here.
	if startsWithKeywordFold(text, "MATCH") || startsWithKeywordFold(text, "OPTIONAL MATCH") {
		start = topLevelKeywordIndex(text, "WHERE")
		if start < 0 {
			return nil
		}
	}
	structure := maskNestedScopeBodies(text)
	local := func(at int) staticTypeScope {
		return withPatternComprehensionBindings(ctx, withLocalTypes(ctx, scope, text, at), text, at)
	}
	for index := start; index < len(structure); index++ {
		if isCypherQuoteByte(structure[index]) {
			index = skipCypherQuotedText(structure, index, structure[index]) - 1
			continue
		}
		if structure[index] == '[' {
			close := findMatchingBracket(structure, index)
			if close < 0 {
				break
			}
			if pattern, _, comprehension := splitPatternComprehension(structure[index : close+1]); comprehension {
				// The head is checked before its new locals are introduced;
				// enclosing comprehensions still supply their own local types.
				if err := validateBoundPatternKinds(local(index-1).kinds, pattern); err != nil {
					return err
				}
			}
		}
		if structure[index] != '(' {
			continue
		}
		end, chain := relationshipChainEnd(structure, index, len(structure))
		if !chain {
			continue
		}
		if err := validateBoundPatternKinds(local(index).kinds, structure[index:end]); err != nil {
			return err
		}
		index = end - 1
	}
	return nil
}

func validateBoundPatternKinds(scope matchSemanticScope, pattern string) error {
	pattern = matchPatternStructure(pattern)
	kinds := maps.Clone(scope)
	if kinds == nil {
		kinds = make(matchSemanticScope)
	}
	for _, variable := range extractNodeVariables(pattern) {
		variable = normalizeProjectionColumnName(variable)
		if err := bindMatchSemanticKind(kinds, variable, matchBindingNode); err != nil {
			return err
		}
	}
	lists := variableLengthRelationshipVariableSet(pattern)
	for _, variable := range extractRelationshipVariables(pattern) {
		variable = normalizeProjectionColumnName(variable)
		kind := matchBindingRelationship
		if _, list := lists[variable]; list {
			kind = matchBindingRelationshipList
		}
		if err := bindMatchSemanticKind(kinds, variable, kind); err != nil {
			return err
		}
	}
	return nil
}

// maskNestedScopeBodies blanks the bodies of the EXISTS / COUNT / COLLECT
// and CALL subqueries in text, which start scopes of their own.
func maskNestedScopeBodies(text string) string {
	masked := []byte(maskSubqueryBodies(text))
	for index := 0; index < len(text); index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		}
		if !matchKeywordAt(text, index, "CALL") {
			continue
		}
		open := skipSpaces(text, index+len("CALL"))
		if open < len(text) && text[open] == '(' {
			if close := findMatchingParen(text, open); close >= 0 {
				open = skipSpaces(text, close+1)
			}
		}
		if open >= len(text) || text[open] != '{' {
			continue
		}
		close := cyphertext.FindMatchingDelimiter(text, open, '{', '}', cyphertext.OpeningDelimiter)
		if close < 0 {
			break
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
	if err := patternBindingKindError(variable, scope[variable], kind); err != nil {
		return err
	}
	scope[variable] = kind
	return nil
}

// patternBindingKindError owns the existing operand compatibility diagnostic.
// Both retained-ID typing and legacy pattern consumers use the same predicate.
func patternBindingKindError(variable string, existing, kind matchBindingKind) error {
	if existing != matchBindingUnknown && existing != kind {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableTypeConflict", fmt.Sprintf("variable %s is already bound to a different entity type", variable))
	}
	return nil
}

// schemaCommandKeywords follow CREATE or DROP in a schema command.
var schemaCommandKeywords = []string{"CONSTRAINT", "INDEX", "FULLTEXT", "VECTOR", "RANGE", "TEXT", "POINT", "LOOKUP", "BTREE", "OR REPLACE"}

// isSchemaCommandStatement reports whether cypher is a schema command
// (CREATE / DROP CONSTRAINT, INDEX, â€¦): its patterns and expressions declare a
// schema rule over a pattern (FOR ()-[r:T]-() REQUIRE â€¦) and bind no clause
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
func (e *StorageExecutor) validateStaticClauseTypes(ctx context.Context, clause pipelineClause, scope staticTypeScope) error {
	if scope.admitted != nil {
		return scope.admitted.checkClauseOperandHandles(ctx, clause, scope)
	}
	if err := validateStaticPropertySubscripts(ctx, clause.text, scope); err != nil {
		return err
	}
	if err := staticWriteTokenError(ctx, clause, scope); err != nil {
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
				built := projectionAliasScope(ctx, scope, clause.text)
				projected = &built
			}
			return *projected
		}
		if err := validateStaticFunctionVariables(ctx, projection, scope); err != nil {
			return err
		}
		if err := forEachProjectedTailExpression(ctx, projection, rest, func(expression string) error {
			return validateStaticFunctionVariablesIn(ctx, expression, projectedScope)
		}); err != nil {
			return err
		}
	} else if err := validateStaticFunctionVariables(ctx, clause.text, scope); err != nil {
		return err
	}
	return e.validateStaticOperatorTypes(ctx, clause, scope, projectedScope, nil)
}

// projectionAliasScope is the scope of the WHERE / ORDER BY after a RETURN or
// WITH projection: the incoming variables, with each projection alias
// replacing the variable of the same name (RETURN n.num AS n ORDER BY n + 2
// sees n as a number, not the node).
func projectionAliasScope(ctx context.Context, input staticTypeScope, clause string) staticTypeScope {
	if input.projected != nil {
		return *input.projected
	}

	values, _, err := (&StorageExecutor{}).enrichBoundTypes(ctx, clause, typeScopeOf(input), input.bindings, "", input.cypher25)
	if err != nil || values == nil {
		input.typeFailure = err
		return input
	}
	branch := values.program.root.branches[0]
	last := values.program.clauses[branch.shape.clauses[len(branch.shape.clauses)-1].syntax.ID()]
	names, ids := last.combinedBindings()
	return values.view(boundBindingScope(projectedMapNames(names), ids), input.bindings)
}

// projectionItemTermError is Neo4j's SyntaxError for a projection item that
// is a clause-keyword name followed by another term with no operator between
// them (WITH RETURN n: return is a variable, as Neo4j reads it (#740), and n
// is "Invalid input"). Before, the keyword was taken for a clause.
func projectionItemTermError(expression string) error {
	expression = strings.TrimSpace(expression)
	name, ok := cyphertext.ScanIdentifier(expression, 0)
	if !ok || !isNameableClauseKeyword(name.Value) {
		return nil
	}
	rest := strings.TrimSpace(expression[name.End:])
	if rest == "" || rest == expression[name.End:] {
		return nil // the name alone, or followed directly by an operator
	}
	if c := rest[0]; cyphertext.NamePartAt(rest, 0) || isCypherQuoteByte(c) || c == '$' {
		token, _ := cyphertext.ScanIdentifier(rest, 0)
		if token.Value == "" {
			token.Value = rest[:1]
		}
		if !isExpressionContinuationWord(token.Value) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(token.Value))
		}
	}
	return nil
}

// projectionAliasError is Neo4j's SyntaxError for a projection item whose
// alias, as written, is not one name (WITH n AS return n: the n after the
// alias is "Invalid input"). A backtick-quoted alias is one name.
func projectionAliasError(ctx context.Context, item string) error {
	// The alias follows the item's retained AS, so an alias
	// named as is a name (WITH n AS as, #894).
	itemSource := readProjectionItemSource(ctx, item, projectionAdmitted)
	if itemSource.as < 0 {
		return nil
	}
	alias := itemSource.rawAlias
	if alias == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInput(""))
	}
	_, next, ok := scanSymbolicName(alias, 0)
	if !ok || next == len(alias) {
		return nil
	}
	rest := strings.TrimSpace(alias[next:])
	token, _ := cyphertext.ScanIdentifier(rest, 0)
	if token.Value == "" {
		token.Value = rest[:1]
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCoreInvalidInputExpectedExpression(token.Value))
}

// isExpressionContinuationWord reports whether word, after an operand,
// continues the expression (an operator word: AND, IS NULL, STARTS WITH, â€¦).
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

// unwindSourceExpression is the list expression of an UNWIND clause.
func unwindSourceExpression(clause string) string {
	expression, _, _ := parsePipelineIteration(clause)
	return expression
}

func coalesceSemanticKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	var callFacts1 functionCallFacts
	ok := inspectFunctionCall(expression, qualifiedFunctionCall, "", &callFacts1)
	name, inner := callFacts1.name, callFacts1.inner
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
	for open, end := readRelationshipBracket(pattern, 0, requireClosedRelationship); open >= 0; open, end = readRelationshipBracket(pattern, end+1, requireClosedRelationship) {
		declaration := relationshipDeclarationHead(pattern[open+1 : end])
		if !declaration.hasLength {
			continue
		}
		if name, ok := cyphertext.ScanIdentifier(declaration.variable, 0); ok {
			result[name.Value] = struct{}{}
		}
	}
	return result
}

// entityListLiteralKind is the kind of a list literal of node variables
// ([a, b] is a List<Node>) or of relationship variables; ok is false for any
// other expression.
func entityListLiteralKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	expression = strings.TrimSpace(expression)
	if !isListLiteral(expression) {
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

// invalidRelationshipPattern reports a relationship bracket that is not closed
// or whose range is malformed. Only relationship brackets are read: a '[' in
// a property map ($ids[i * 20 + 1], $ws[0..2]) is a subscript or a slice.
func invalidRelationshipPattern(pattern string) bool {
	for open, end := readRelationshipBracket(pattern, 0, reportUnclosedRelationship); open >= 0; open, end = readRelationshipBracket(pattern, end+1, reportUnclosedRelationship) {
		if end < 0 {
			return true
		}
		// Quoted names and values can hold * and .. (#879), and so can
		// property values and dynamic types: the declaration reader skips
		// them.
		if relationshipDeclarationHead(pattern[open+1 : end]).lengthInvalid {
			return true
		}
	}
	return false
}

// simpleSemanticIdentifier returns the variable an expression consists of,
// plain or backtick-quoted (`x y`), unquoted; "" for any other expression.
func simpleSemanticIdentifier(expression string) string {
	expression = lexicalExpressionText(expression)
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

func addMatchPatternBindingKinds(ctx context.Context, scope matchSemanticScope, clause string) {
	forEachMatchPatternBinding(ctx, clause, func(variable string, kind matchBindingKind) {
		if _, found := scope[variable]; !found {
			scope[variable] = kind
		}
	})
}

// forEachMatchPatternBinding is the declaration reader for semantic scope and
// runtime schemas. Property expressions and predicate-only names never declare
// nodes; callers must not recover declarations from expression parentheses.
func forEachMatchPatternBinding(ctx context.Context, clause string, visit func(string, matchBindingKind)) {
	node, ok := canonicalQueryClause(ctx, clause)
	if !ok {
		clause = clause[:patternPartEnd(pipelineClause{text: clause})]
		syntax, err := cypherantlr.ParseSyntax(ctx, clause, cypherantlr.PatternEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
		if err != nil {
			return
		}
		node = syntax.Root()
	}
	syntaxPatternDefinitions(node, func(rule int, symbol cypherantlr.SyntaxNode) {
		kind := matchBindingNode
		switch rule {
		case cypherantlr.CypherParserRULE_relationDetail:
			kind = matchBindingRelationship
		case cypherantlr.CypherParserRULE_patternPart:
			kind = matchBindingPath
		}
		visit(syntaxSymbolName(symbol), kind)
	})
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
	return cyphertext.IsIdentifier(key) || isBacktickQuotedName(key)
}

// callSubqueryReturnsOuterUnchanged reports whether every branch of a CALL
// subquery returns the enclosing query's variable name as itself (RETURN
// name, or name AS name) without declaring name again in its body. Neo4j
// rejects even that as VariableAlreadyBound; NornicDB accepts it as an
// extension, since the column is the outer value and nothing is ambiguous
// (#907; the implicit import is the same kind of extension). A branch that
// rebinds name (WITH â€¦ AS name, UNWIND â€¦ AS name, YIELD name, a nested CALL
// returning it) or returns another value under it is rejected.
func callSubqueryReturnsOuterUnchanged(ctx context.Context, branches []string, name string) bool {
	source := "CALL (" + cyphertext.PrintIdentifier(name, cyphertext.IsIdentifier, cyphertext.AcceptedSource) + ") { " + strings.Join(branches, " UNION ALL ") + " }"
	facts := (&StorageExecutor{}).statementProjectionProgram(ctx, source, resultmetadata.KnownScope([]string{name}), false, nil)
	if facts.err != nil || facts.statement == nil || len(facts.statement.Clauses) != 1 {
		return false
	}
	original, known := facts.statement.Incoming.Binding(name)
	call := facts.statement.Clauses[0].Call
	if !known || call == nil {
		return false
	}
	output, exists := call.After.Binding(name)
	return exists && original == output
}
func withPatternComprehensionBindings(ctx context.Context, scope staticTypeScope, source string, at int) staticTypeScope {
	if scope.admitted != nil {
		return scope
	}
	return boundExpressionTypeView(ctx, scope, source, at)
}
func (e *StorageExecutor) validateClauseSubqueries(ctx context.Context, clause pipelineClause, _ typeScope, scope staticTypeScope, _ typeScope, cypher25 bool) error {
	if scope.admitted != nil {
		return nil
	}
	_, _, err := e.enrichBoundTypes(ctx, clause.text, typeScopeOf(scope), scope.bindings, "", cypher25)
	return err
}
