package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
//
// A statement whose backtick-quoted variables were canonicalized is cached
// under the text the client wrote: statements that differ only in quoting
// canonicalize alike but can validate differently (their column names
// differ).
func (e *StorageExecutor) validateSemanticScopes(ctx context.Context, cypher string) (failure error) {
	// Interrupted structural readers may decline their view; preserve the
	// caller cancellation rather than synthesizing a semantic query error.
	defer func() {
		if err := ctx.Err(); err != nil {
			failure = err
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateTransactionalCalls(ctx, cypher, nil); err != nil {
		return err
	}
	names := quotedVariableNamesFor(ctx, cypher)
	cypher25 := cypherVersionFromContext(ctx) == "25"
	cacheKey := cypher
	if names != nil {
		cacheKey = names.Original
	}
	cacheKey = semanticCacheKey(cacheKey, cypher25)
	// A text cached under its own key passed every check here, the lexical
	// one included, so a repeated query skips them all (#823). A rewritten
	// text cached under its original still gets its own lexical check.
	// Literal-bearing texts share one compile verdict through the canonical
	// form (PR #1043 #6), looked up only when the exact text misses: a
	// canonical hit still runs the lexical check on its own text, then
	// returns.
	cached := e.semanticValidationCache.contains(cacheKey)
	var canonical string
	var normalized bool
	if !cached {
		canonical, normalized = normalizeLiteralText(cacheKey)
		cached = normalized && e.semanticValidationCache.contains(canonical)
	}
	if cached && names == nil && !normalized {
		return nil
	}
	if err := validateExpressionLexicalTokens(ctx, cypher); err != nil {
		return err
	}
	if cached {
		// A canonical hit shared the compile verdict with a sibling that
		// differs only in literal values, but SKIP / LIMIT bounds are
		// value-dependent: LIMIT 2-3 and LIMIT 5-1 share one canonical form,
		// yet only the latter is a non-negative integer. Re-evaluate them on
		// this text before returning (monster retest part 3).
		return e.validateStaticPaginationExpressions(ctx, extractPaginationExpressions(ctx, cypher)...)
	}
	if err := e.validateCallSubqueryScopes(ctx, cypher, cypher25); err != nil {
		return err
	}
	if err := validateListComprehensionVariables(cypher); err != nil {
		return err
	}
	if err := validateStaticQuantifierTypes(ctx, cypher); err != nil {
		return err
	}
	if err := checkedWithoutUnaryPlus(cypher, cypher25, func(text string) error {
		return validateStaticFunctionArguments(ctx, text, cypher25)
	}); err != nil {
		return err
	}
	if err := e.validateStaticPaginationExpressions(ctx, extractPaginationExpressions(ctx, cypher)...); err != nil {
		return err
	}
	if err := validateProjectionSemantics(ctx, cypher); err != nil {
		return err
	}
	if err := validatePatternExpressionPlacement(ctx, cypher); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";"))
	if strings.HasSuffix(trimmed, ",") {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidProjection",
			"RETURN projection cannot end with an empty item",
		)
	}
	if invalidAggregationInListComprehension(cypher) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed inside list-comprehension predicates or projections",
		)
	}
	if err := e.validateDuplicateReturnColumnName(ctx, cypher, names); err != nil {
		return err
	}
	if clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, cypher); ok {
		pendingUpdate := false
		for _, clause := range clauses {
			if pendingUpdate && clause.kind == pipelineClauseCall {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidClauseComposition", "WITH is required between an updating clause and CALL")
			}
			switch clause.kind {
			case pipelineClauseWith:
				pendingUpdate = false
			case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
				pendingUpdate = true
			}
			// A WITH or RETURN projection body reads its DISTINCT modifier
			// through projectionBodyDistinct, the one owner of that decision:
			// where the canonical classifier reads the leading word as the
			// keyword but the retained items kept it as a name, the statement
			// has no projection and is Neo4j's SyntaxError.
			if clause.kind == pipelineClauseWith || clause.kind == pipelineClauseReturn {
				if _, err := projectionBodyDistinct(clause.syntax.ChildRule(cypherantlr.CypherParserRULE_projectionBody)); err != nil {
					return err
				}
			}
			// Subquery bodies are checked as their own statements.
			if whereIndex := topLevelKeywordIndex(ctx, clause.text, "WHERE"); whereIndex >= 0 &&
				hasUnexpectedIdentifierAfterNumber(maskSubqueryBodies(clause.text[whereIndex+len("WHERE"):])) {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unexpected identifier in WHERE")
			}
			if clause.kind == pipelineClauseWith && withProjectionHasEmptyItem(ctx, clause.text) {
				return emptyProjectionItemError("WITH")
			}
			if clause.kind == pipelineClauseReturn {
				body := strings.TrimSpace(clause.text[len("RETURN"):])
				if err := validateReturnAggregationSemantics(ctx, body); err != nil {
					return err
				}
				// The clause is scanned with its keyword, which tells a
				// keyword-named first item from a clause (RETURN union[0], #894).
				clauseText := strings.TrimSpace(clause.text)
				end := len(clauseText)
				for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT", "UNION"} {
					if index := topLevelKeywordIndex(ctx, clauseText, keyword); index >= len("RETURN") && index < end {
						end = index
					}
				}
				items := clauseText[len("RETURN"):end]
				if projectionHasEmptyItem(items) {
					return emptyProjectionItemError("RETURN")
				}
				for _, itemSource := range projectionItemsFromSyntax(clause.syntax) {
					expression := itemSource.expression
					if err := validateExpressionOperandCompleteness(expression); err != nil {
						return err
					}
					if itemSource.as >= 0 {
						alias := itemSource.rawAlias
						if simpleSemanticIdentifier(alias) == "" && !(len(alias) >= 2 && isCypherBacktickByte(alias[0]) && isCypherBacktickByte(alias[len(alias)-1])) {
							return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: invalid RETURN alias")
						}
					}
				}
			}
		}
	}
	if err := e.validateCreateSemanticScopes(ctx, cypher, cypher25); err != nil {
		return err
	}
	if err := e.validateMergeSemanticScopes(ctx, cypher, cypher25); err != nil {
		return err
	}
	if err := e.validateMatchSemanticScopes(ctx, cypher, cypher25); err != nil {
		return err
	}
	if err := e.validateSetSemanticScopes(ctx, cypher); err != nil {
		return err
	}
	// Every function a statement calls, in any clause, is known (#1166).
	// Neo4j reports the statement's other semantic errors first.
	if err := validateKnownFunctions(ctx, cypher); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if normalized {
		e.semanticValidationCache.add(canonical)
		e.semanticValidationCache.add(cacheKey)
	} else {
		e.semanticValidationCache.add(cacheKey)
	}
	return nil
}

// emptyProjectionItemError is Neo4j's SyntaxError for a RETURN or WITH item
// list with an empty item (RETURN 1,,2 / RETURN , 1 / WITH a,, b).
func emptyProjectionItemError(clause string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidProjection",
		clause+" projection cannot contain an empty item",
	)
}

// withProjectionHasEmptyItem reports whether a WITH clause's item list,
// before its WHERE / ORDER BY / SKIP / LIMIT, has an empty item.
func withProjectionHasEmptyItem(ctx context.Context, clause string) bool {
	body, _ := projectionSemanticBodyAndTail(ctx, clause, "WITH")
	return projectionHasEmptyItem(body)
}

// projectionHasEmptyItem reports whether a projection item list has an
// empty item: a leading, trailing or doubled top-level comma.
func projectionHasEmptyItem(items string) bool {
	items = strings.TrimSpace(items)
	if items == "" || strings.HasPrefix(items, ",") || strings.HasSuffix(items, ",") {
		return true
	}
	for _, item := range splitTopLevelComma(items) {
		if strings.TrimSpace(item) == "" {
			return true
		}
	}
	return false
}

// validateCallSubqueryScopes applies the update-clause scope check
// (validateSetSemanticScopes: SET, REMOVE, DELETE) to the body of every
// top-level CALL { … } and CALL (vars) { … } subquery, recursively for nested
// subqueries. A body sees only what it imports, as in Neo4j: an importing WITH
// at its start, or the variables of CALL (vars), which are checked as a
// leading WITH vars. An undefined variable in a subquery update clause is
// therefore rejected before any route runs the subquery. CALL (*) and a body
// starting with WITH * import the whole outer scope and are not checked here.
func (e *StorageExecutor) validateCallSubqueryScopes(ctx context.Context, cypher string, cypher25 bool) error {
	if !containsFold(cypher, "CALL") {
		return nil
	}
	positions := findAllTopLevelPipelineKeywordPositions(ctx, cypher, "CALL")
	if len(positions) == 0 {
		return ctx.Err()
	}
	// Valid statements can select CALL nodes from their retained product rather
	// than parsing a suffix for every CALL. Rejected input still goes through
	// the diagnostic reader, which refines malformed scope errors.
	syntax, parseErr := cypherantlr.ParseSyntax(ctx, cypher, cypherantlr.StatementEntry, cypher25, cypherantlr.AcceptedSource)
	var callNodes map[int]cypherantlr.SyntaxNode
	if parseErr == nil {
		var err error
		callNodes, err = callSubqueryNodes(ctx, syntax)
		if err != nil {
			return err
		}
	} else if err := ctx.Err(); err != nil {
		return err
	}
	for _, position := range positions {
		var scope callSubqueryScopeClause
		var body queryBody
		refused := ""
		if parseErr == nil {
			node, ok := callNodes[position]
			if !ok {
				continue // a procedure call
			}
			scope = scopeClauseFromSyntax(node)
			retained, parsed := callBodyFromSyntax(pipelineClause{kind: pipelineClauseCallSubquery, syntax: node})
			if !parsed {
				continue
			}
			body = retained
		} else {
			// Rejected input has no retained product: the diagnostic scope
			// reader and the body it names are all that is left.
			scope = readCallSubqueryScope(ctx, cypher, position)
			index := scope.bodyOffset
			if !scope.invalid {
				if scope.bodyOffset == 0 || index >= len(cypher) || cypher[index] != '{' {
					continue // a procedure call
				}
				closeBrace := e.findMatchingBrace(cypher, index)
				if closeBrace < 0 {
					continue
				}
				refused = strings.TrimSpace(cypher[index+1 : closeBrace])
				if stripped, finishes := stripUnionBranchFinishes(ctx, refused); finishes {
					refused = stripped
				}
			}
		}
		if scope.invalid {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
				"CALL imports must contain only variable names")
		}
		imports, scoped := scope.text, scope.scoped
		// A RETURN inside the body projects its columns too: two items of one
		// name are as malformed there as at the top level.
		if parseErr != nil {
			if err := e.validateDuplicateReturnColumnName(ctx, refused, nil); err != nil {
				return err
			}
		} else {
			for _, branch := range body.branches {
				for _, clause := range branch.clauses {
					if clause.kind != pipelineClauseReturn {
						continue
					}
					if err := validateDuplicateProjectionNames(projectionPlanFromSyntax(clause.syntax)); err != nil {
						return err
					}
				}
			}
		}
		bodyText := refused
		if parseErr == nil {
			bodyText = queryBodyText(body)
		}
		// An unscoped body's branches (the body, or each UNION branch)
		// import with their own leading WITH (#907). A branch that imports
		// nothing still sees the outer variables it reads, a NornicDB
		// extension over Neo4j 5.26 (which rejects them): only a leading
		// WITH that reads no outer variable at all is checked here.
		if !scoped {
			outer := func(variable string) bool { return isIdentifierReferenced(cypher[:position], variable) }
			if err := e.validateUnscopedImportForms(ctx, body, parseErr == nil, refused, outer); err != nil {
				return err
			}
		}
		if scoped && scope.names == nil || startsWithKeywordFold(bodyText, "WITH *") {
			continue
		}
		var retained []string
		if scoped && len(scope.names) > 0 {
			retained = scope.names
			bodyText = "WITH " + imports + " " + bodyText
		}
		if bodyText == "" {
			continue
		}
		// A body that imports nothing (CALL () { … }) sees no outer
		// variable: it is checked as a statement of its own.
		if scoped && len(scope.names) == 0 {
			if err := e.validateMatchSemanticScopes(ctx, bodyText, cypher25); err != nil {
				return err
			}
		}
		if err := e.validateSetSemanticScopes(ctx, bodyText, retained...); err != nil {
			return err
		}
		if err := e.validateCallSubqueryScopes(ctx, bodyText, cypher25); err != nil {
			return err
		}
	}
	return nil
}

// validateUnscopedImportForms checks the import form of every branch of an
// unscoped CALL body: a branch that imports with a leading WITH may list only
// plain references to outside variables, and a leading WITH that reads no outer
// variable is a branch's first clause, so every name it reads is undefined
// there. A valid statement's branches are its retained body's own. Rejected
// input has no retained body yet: its refused text is admitted once, here, and
// the branches that admission retains are read the same way; a refused body the
// admission declines keeps its raw text, which is the only product left for the
// diagnostic checks.
func (e *StorageExecutor) validateUnscopedImportForms(ctx context.Context, body queryBody, retained bool, refused string, outer func(string) bool) error {
	branches := body.branches
	declined := ""
	if !retained {
		refusedBody := readQueryBody(ctx, refused, 0, len(refused))
		branches = refusedBody.branches
		if refusedBody.status == queryBodyDeclined || len(branches) == 0 {
			branches, declined = nil, strings.TrimSpace(refused)
		}
	}
	for index, branch := range branches {
		// A malformed import is reported where the CALL runs.
		_, importing, _ := branchSyntaxImports(ctx, branch, outer, false)
		if !importing {
			// A leading WITH that imports nothing is the branch's first
			// clause: any variable it reads is undefined there.
			if len(branch.clauses) == 0 || branch.clauses[0].kind != pipelineClauseWith {
				continue
			}
			if variable := firstUndefinedImportedName(ctx, projectionItemsFromSyntax(branch.clauses[0].syntax)); variable != "" {
				return createUndefinedVariableError(variable)
			}
			continue
		}
		// The import form is checked on the first branch only, as before: a
		// later branch's importing WITH may carry WHERE or an alias (WITH x
		// WHERE x IS NOT NULL), which NornicDB runs as a projection over its
		// implicit imports (an extension; Neo4j 5.26 rejects it).
		if index > 0 {
			continue
		}
		if err := importingWithFormError(ctx, branch); err != nil {
			return err
		}
	}
	if declined == "" {
		return nil
	}
	// The admission declined the refused body: its raw text is the branch the
	// diagnostic reader sees, and it is not admitted a second time.
	if startsWithKeywordFold(declined, "WITH") {
		clause, _ := leadingWithClause(declined)
		projection, _ := projectionSemanticBodyAndTail(ctx, clause, "WITH")
		rawItems := splitTopLevelComma(projection)
		items := make([]projectionItemSource, len(rawItems))
		for index, item := range rawItems {
			items[index] = readProjectionItemSource(ctx, strings.TrimSpace(item), projectionAdmitted)
		}
		if variable := firstUndefinedImportedName(ctx, items); variable != "" {
			return createUndefinedVariableError(variable)
		}
	}
	return nil
}

// firstUndefinedImportedName is the first name an importing WITH that imports
// nothing reads: the clause is the branch's first, so no variable is bound
// there yet.
func firstUndefinedImportedName(ctx context.Context, items []projectionItemSource) string {
	for _, item := range items {
		if item.star {
			continue
		}
		for _, variable := range expressionFreeVariables(ctx, item.expression) {
			if !isSemanticLiteralWord(variable) {
				return variable
			}
		}
	}
	return ""
}

// importingWithFormError rejects an importing WITH that is not a list of plain
// outside references: an AS alias or a WHERE / ORDER BY / SKIP / LIMIT tail
// makes it a projection (Neo4j 5.26, #907). A branch the pipeline cannot split
// (one with a procedure call, USE or FINISH in it) keeps NornicDB's implicit
// imports, so its WITH is not checked here.
func importingWithFormError(ctx context.Context, branch queryBodyBranch) error {
	split := cachedPipelineClausesBranch(ctx, branch, false)
	if !split.ok || len(split.clauses) == 0 {
		return nil
	}
	first := split.clauses[0]
	projection := first.syntax.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	if first.syntax.ChildRule(cypherantlr.CypherParserRULE_where).Valid() ||
		projection.ChildRule(cypherantlr.CypherParserRULE_orderSt).Valid() ||
		projection.ChildRule(cypherantlr.CypherParserRULE_skipSt).Valid() ||
		projection.ChildRule(cypherantlr.CypherParserRULE_limitSt).Valid() {
		return invalidImportingWithError()
	}
	// The alias is the item's own AS token, never a word in the clause text.
	for _, item := range projectionItemsFromSyntax(first.syntax) {
		if item.as >= 0 {
			return invalidImportingWithError()
		}
	}
	return nil
}

func invalidImportingWithError() error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
		"Importing WITH must contain only simple references to outside variables")
}

// callSubqueryNodes maps the actual CALL token to its retained occurrence.
// The bounded checks keep a large admitted tree responsive to cancellation.
func callSubqueryNodes(ctx context.Context, syntax cypherantlr.StatementSyntax) (map[int]cypherantlr.SyntaxNode, error) {
	nodes := make(map[int]cypherantlr.SyntaxNode)
	visited := 0
	var visit func(cypherantlr.SyntaxNode) error
	visit = func(node cypherantlr.SyntaxNode) error {
		visited++
		if visited&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if node.Rule() == cypherantlr.CypherParserRULE_callSubquery {
			if token, ok := node.DirectToken(cypherantlr.CypherLexerCALL); ok {
				start, _ := token.Span()
				nodes[start] = node
			}
		}
		for i := 0; i < node.ChildCount(); i++ {
			if err := visit(node.Child(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := visit(syntax.Root()); err != nil {
		return nil, err
	}
	return nodes, ctx.Err()
}

func hasUnexpectedIdentifierAfterNumber(expression string) bool {
	for index := 0; index < len(expression); index++ {
		if end, skipped := skipCypherLexicalToken(expression, index); skipped {
			index = end - 1
			continue
		}
		if !cyphertext.NumberStartsAt(expression, index) {
			continue
		}
		token, _ := cyphertext.ScanNumber(expression, index)
		end := token.TailEnd
		next := skipSpaces(expression, end)
		if next > end {
			if name, ok := cyphertext.ScanIdentifier(expression, next); ok {
				switch upperASCII(name.Value) {
				case "AND", "OR", "XOR", "IN", "IS", "THEN", "ELSE", "END", "AS", "STARTS", "ENDS", "CONTAINS", "ORDER", "SKIP", "LIMIT", "UNION":
				default:
					return true
				}
			}
		}
		index = end - 1
	}
	return false
}

func containsMalformedCreateClauseToken(expression string) bool {
	for index := 0; index < len(expression); {
		name, ok := cyphertext.ScanIdentifier(expression, index)
		if !ok {
			index++
			continue
		}
		if strings.EqualFold(name.Value, "CREAT") {
			cursor := skipSpaces(expression, name.End)
			return cursor < len(expression) && expression[cursor] == '('
		}
		index = name.End
	}
	return false
}

// validatePatternExpressionPlacement enforces the rule that a pattern
// expression only tests whether its pattern exists: it is valid wherever a
// boolean is expected (maskPredicatePatterns), and never as a value of its
// own in a projection or an updating expression (Neo4j 5.26.30, #907).
func validatePatternExpressionPlacement(ctx context.Context, cypher string) error {
	clauses, ok := splitPipelineClauses(ctx, cypher)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseReturn, pipelineClauseWith:
			keyword := "RETURN"
			if clause.kind == pipelineClauseWith {
				keyword = "WITH"
			}
			for _, item := range splitTopLevelComma(projectionSemanticBody(ctx, clause.text, keyword)) {
				expression := readProjectionItemSource(ctx, strings.TrimSpace(item), projectionAdmitted).expression
				if containsIllegalProjectedPatternExpression(maskPredicatePatterns(expression)) {
					return invalidPatternExpressionPlacementError()
				}
			}
		case pipelineClauseSet:
			if containsIllegalProjectedPatternExpression(maskPredicatePatterns(clause.text)) {
				return invalidPatternExpressionPlacementError()
			}
		}
	}
	return nil
}

// containsIllegalProjectedPatternExpression distinguishes a raw pattern value
// from a legal pattern comprehension. It recursively examines ordinary list
// expressions while treating `[pattern | projection]` as one valid scalar
// expression. Relationship brackets (`-[r]->`) are not list delimiters.
func containsIllegalProjectedPatternExpression(expression string) bool {
	expression = maskSubqueryBodies(expression)
	// shortestPath(...) / allShortestPaths(...) are path functions, not
	// legacy pattern expressions: their pattern argument stays valid in value
	// position (RETURN / WITH / SET projections).
	expression = maskPathFunctionCalls(expression)
	segmentStart := 0
	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' || (index > 0 && expression[index-1] == '-') {
			continue
		}
		close := findMatchingBracket(expression, index)
		if close < 0 {
			continue
		}
		if containsRelationshipPattern(expression[segmentStart:index]) {
			return true
		}
		candidate := expression[index : close+1]
		if _, _, patternComprehension := splitPatternComprehension(candidate); !patternComprehension {
			if containsIllegalProjectedPatternExpression(expression[index+1 : close]) {
				return true
			}
		}
		index = close
		segmentStart = close + 1
	}
	return containsRelationshipPattern(expression[segmentStart:])
}

// maskPathFunctionCalls blanks the argument spans of shortestPath(...) and
// allShortestPaths(...) so pattern scanners do not mistake their relationship
// syntax for a legacy pattern expression. The returned string keeps its byte
// length so every caller index stays valid.
func maskPathFunctionCalls(expression string) string {
	// Zero-allocation fast path: ordinary projections contain neither call.
	if indexASCIIFold(expression, "shortestpath") < 0 && indexASCIIFold(expression, "allshortestpaths") < 0 {
		return expression
	}
	masked := []byte(expression)
	for _, name := range []string{"shortestpath", "allshortestpaths"} {
		searchStart := 0
		for searchStart < len(expression) {
			idx := indexASCIIFold(expression[searchStart:], name)
			if idx < 0 {
				break
			}
			idx += searchStart
			if !functionNameStartsAt(expression, idx) {
				searchStart = idx + 1
				continue
			}
			openParen := skipSpaces(expression, idx+len(name))
			if openParen >= len(expression) || expression[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(expression, openParen)
			if closeParen < 0 {
				break
			}
			for position := idx; position <= closeParen; position++ {
				if masked[position] != '\n' && masked[position] != '\r' {
					masked[position] = ' '
				}
			}
			searchStart = closeParen + 1
		}
	}
	return string(masked)
}

// maskPredicatePatterns blanks, keeping byte offsets, every pattern in
// expression that stands where a boolean is expected, which Neo4j allows
// anywhere: the argument of exists(), an operand of NOT, AND, OR or XOR, a
// CASE WHEN condition, or the WHERE of a list comprehension or quantifier.
// A pattern left is a value (RETURN (n)-->() AS x).
func maskPredicatePatterns(expression string) string {
	var masked []byte
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
			continue
		case '(':
		default:
			continue
		}
		if index > 0 && expression[index-1] == '[' { // a pattern comprehension
			continue
		}
		chainEnd, chain := relationshipChainEnd(expression, index, len(expression))
		if !chain {
			continue
		}
		if patternInBooleanPosition(expression, index, chainEnd) {
			if masked == nil {
				masked = []byte(expression)
			}
			for position := index; position < chainEnd; position++ {
				masked[position] = ' '
			}
		}
		index = chainEnd - 1
	}
	if masked == nil {
		return expression
	}
	return string(masked)
}

// patternInBooleanPosition reports whether the pattern expression[start:end]
// is a boolean operand: inside exists(...), or between a boolean keyword
// (NOT, AND, OR, XOR, WHEN, WHERE, or the start of the expression) and a
// boolean boundary (AND, OR, XOR, THEN, |, a closing parenthesis, or the
// end), with grouping parentheses around it allowed. A whole projection that
// is only the pattern is a value.
func patternInBooleanPosition(expression string, start, end int) bool {
	before := strings.TrimRightFunc(expression[:start], cyphertext.IsWhitespace)
	after := strings.TrimLeftFunc(expression[end:], cyphertext.IsWhitespace)
	for strings.HasSuffix(before, "(") && strings.HasPrefix(after, ")") {
		call := strings.TrimRightFunc(before[:len(before)-1], cyphertext.IsWhitespace)
		if name := trailingIdentifier(call); name != "" && !isBooleanKeyword(name) {
			return strings.EqualFold(name, "exists")
		}
		before = call
		after = strings.TrimLeftFunc(after[1:], cyphertext.IsWhitespace)
	}
	leftKeyword := ""
	switch name := trailingIdentifier(before); {
	case before == "":
	case isBooleanKeyword(name):
		leftKeyword = name
	default:
		return false
	}
	rightBoundary := after == "" || strings.HasPrefix(after, ")") || strings.HasPrefix(after, "|")
	for _, keyword := range []string{"AND", "OR", "XOR", "THEN"} {
		if matchKeywordAt(after, 0, keyword) {
			rightBoundary = true
		}
	}
	if !rightBoundary {
		return false
	}
	// The pattern alone, with nothing before it, is a projected value unless
	// a boolean operator follows it.
	return leftKeyword != "" || (after != "" && !strings.HasPrefix(after, ")"))
}

// isBooleanKeyword reports whether name introduces a boolean operand: NOT,
// AND, OR, XOR, WHEN or WHERE.
func isBooleanKeyword(name string) bool {
	switch upperASCII(name) {
	case "NOT", "AND", "OR", "XOR", "WHEN", "WHERE":
		return true
	}
	return false
}

// trailingIdentifier is the identifier text ends with, or "".
func trailingIdentifier(text string) string {
	end := len(text)
	start := end
	for start > 0 && cyphertext.NamePartAt(text, start-1) {
		start--
	}
	return text[start:end]
}

func invalidPatternExpressionPlacementError() error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnexpectedSyntax",
		"pattern expressions are only valid as predicates in WHERE",
	)
}

func (e *StorageExecutor) validateDuplicateReturnColumnName(ctx context.Context, cypher string, names *quotedVariableNames) error {
	returnPositions := findAllTopLevelPipelineKeywordPositions(ctx, cypher, "RETURN")
	unionPositions := append(
		findAllTopLevelPipelineKeywordPositions(ctx, cypher, "UNION"),
		findAllTopLevelPipelineKeywordPositions(ctx, cypher, "UNION ALL")...,
	)
	for returnIndex, position := range returnPositions {
		end := len(cypher)
		if returnIndex+1 < len(returnPositions) && returnPositions[returnIndex+1] < end {
			end = returnPositions[returnIndex+1]
		}
		for _, unionPosition := range unionPositions {
			if unionPosition > position && unionPosition < end {
				end = unionPosition
			}
		}
		plan := returnProjectionPlanFor(ctx, cypher[position:end])
		if names != nil {
			originalText, _ := names.OriginalText(cyphertext.Span{Start: position + len("RETURN"), End: end}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
			plan = returnProjectionPlanFor(ctx, "RETURN "+strings.TrimSpace(originalText))
		}
		if err := validateDuplicateProjectionNames(plan); err != nil {
			return err
		}
	}
	return nil
}

// validateDuplicateProjectionNames is the column-conflict decision shared by
// source admission and retained RETURN readers. The caller supplies the plan
// from its actual input stage; an existing clause is never reparsed here.
func validateDuplicateProjectionNames(plan *returnProjectionPlan) error {
	projections := plan.projections
	if plan.star {
		projections = plan.starProjections
	}
	seen := make(map[string]struct{}, len(projections))
	for _, item := range projections {
		name := item.alias
		if name == "" {
			name = strings.TrimSpace(item.expr)
		}
		if name == "" || name == "*" {
			continue
		}
		// Identifiers and projected column names are case-sensitive: n.Name
		// and n.name remain distinct result columns.
		if _, exists := seen[name]; exists {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "ColumnNameConflict", "Multiple RETURN items project the same column name: "+name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func invalidAggregationInListComprehension(cypher string) bool {
	for start := 0; start < len(cypher); start++ {
		if cypher[start] != '[' {
			continue
		}
		end := findMatchingBracket(cypher, start)
		if end < 0 {
			continue
		}
		_, _, predicate, projection, comprehension := parseListComprehension(cypher[start+1 : end])
		if comprehension && (containsAggregateFunc(predicate) || containsAggregateFunc(projection)) {
			return true
		}
	}
	return false
}
