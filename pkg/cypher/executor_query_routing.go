package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func isShowConstraintContractsCommand(cypher string) bool {
	return startsWithKeywords(cypher, "SHOW", "CONSTRAINT CONTRACTS")
}

// topLevelUnion reports whether cypher composes complete single queries with
// UNION at its top level, and whether it is UNION ALL. Such a statement is a
// UNION before it is anything else: every route sends it to executeUnion
// before a handler for its first clause can take the leading branch for the
// whole statement, including the auto-commit async CREATE fast paths (#781).
// The substring guard keeps other queries off the structural scanner, which
// tells top-level separators from nested subqueries.
func topLevelUnion(ctx context.Context, cypher, upperQuery string) (unionAll, union bool) {
	if !strings.Contains(upperQuery, "UNION") {
		return false, false
	}
	branches, unionAll, mixed, ok := parseTopLevelUnionBranches(ctx, cypher)
	if !ok {
		return false, false
	}
	// Mixed UNION modes return no branch list because they are invalid, but
	// they still need to reach executeUnion so it can return the canonical
	// mixed-operator error.
	if mixed {
		return false, true
	}
	return unionAll, len(branches) > 1
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (result *ExecuteResult, err error) {
	// The owner answers first; every route below reads its answer.
	class := classifyStatement(cypher)
	reveal := hasRevealCall(cypher)
	if scope := e.executionReadScopeOf(); scope != nil {
		if reveal && !scope.reveal {
			return nil, newSemanticError("Neo.ClientError.Statement.AccessMode", "AccessMode", "Nested reveal requires reveal in the enclosing statement")
		}
		ctx = storage.InheritQueryRevealScopes(ctx, scope.context)
		reveal = reveal || scope.reveal
	}
	ctx, cleanupReveal, readScopeEngine := setRevealOnEngine(ctx, e.storage, reveal)
	defer cleanupReveal()
	if readScopeEngine != nil {
		defer clearRevealScope(ctx, readScopeEngine)
	}
	if scoped := e.withStatementPropertyViews(ctx, cypher); scoped != nil {
		return scoped.executeWithoutTransaction(ctx, cypher, upperQuery)
	}
	if mayContainSubqueryExpression(cypher) && len(findSubqueryExpressions(cypher)) > 0 {
		scope := &expressionStatsContext{Context: ctx}
		ctx = scope
		defer func() {
			if err == nil && result != nil {
				stats := scope.snapshot()
				result.Stats = mergeQueryStats(result.Stats, &stats)
			}
		}()
	}
	defer func() { e.projectQueryResultProperties(result) }()
	defer func() {
		if recorded := getExpressionFailure(ctx); recorded != nil && err == nil {
			result, err = nil, recorded
		}
	}()
	// A command the owner names runs on its command owner. The clause words
	// in its own text (a procedure body's WITH, SET, DELETE, CALL { } or
	// UNION) never route it as a graph query.
	if class.command != commandNone {
		if result, handled, err := e.executeNamedCommand(ctx, cypher, class.command); handled {
			return result, err
		}
	}
	if class.command == commandAlterDatabase || class.command == commandAlterDatabaseLimit || class.command == commandAlterDatabaseOption {
		return e.executeAlterDatabase(ctx, cypher)
	}
	// A top-level UNION composes complete single queries. Route it before any
	// handler can consume the leading MATCH, RETURN, or UNWIND branch.
	if unionAll, union := topLevelUnion(ctx, cypher, upperQuery); union {
		return e.executeUnion(ctx, cypher, unionAll, nil)
	}
	if result, handled, err := e.executeShowStatement(ctx, cypher); handled {
		return result, err
	}
	if strings.Contains(upperQuery, "CALL") {
		if firstTopLevelCallSubquery(ctx, cypher) >= 0 {
			return e.executeRequiredPipeline(ctx, cypher)
		}
	}

	if result, handled := e.tryFastPathSimpleMatchReturnLimit(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathMatchVectorCosine(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathMatchWithVectorCosineProjection(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathMatchRelationshipVectorCosine(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathMatchWithRelationshipVectorCosineProjection(ctx, cypher, upperQuery); handled {
		return result, nil
	}

	startsWithMatch := strings.HasPrefix(upperQuery, "MATCH")
	startsWithCreate := strings.HasPrefix(upperQuery, "CREATE")
	startsWithMerge := strings.HasPrefix(upperQuery, "MERGE")

	if startsWithMatch && topLevelKeywordIndex(cypher, "CALL") > 0 {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	if startsWithMerge {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	var mergeIdx, createIdx, withIdx, optionalMatchIdx int = -1, -1, -1, -1

	if startsWithMatch {
		mergeIdx = findKeywordIndex(cypher, "MERGE")
		createIdx = findKeywordIndex(cypher, "CREATE")
		optionalMatchIdx = findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	} else if startsWithCreate {
		if _, ok := canExecuteAsPipeline(ctx, cypher); ok {
			return e.executeRequiredPipeline(ctx, cypher)
		}
		withIdx = findKeywordIndex(cypher, "WITH")
	}

	if startsWithMatch && mergeIdx > 0 {
		return e.executeRequiredPipeline(ctx, cypher)
	}
	if startsWithMatch && createIdx > 0 {
		return e.executeRequiredPipeline(ctx, cypher)
	}
	if startsWithCreate && withIdx > 0 {
		return e.executeRequiredPipeline(ctx, cypher)
	}
	if startsWithKeywordFold(cypher, "UNWIND") || startsWithKeywordFold(cypher, "FOR") ||
		startsWithKeywordFold(cypher, "LET") || startsWithKeywordFold(cypher, "FILTER") {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	hasDelete := findKeywordIndex(cypher, "DELETE") >= 0
	hasDetachDelete := containsKeywordOutsideStrings(cypher, "DETACH DELETE")
	if hasDelete || hasDetachDelete {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	hasSet := containsKeywordOutsideStrings(cypher, "SET")
	hasOnCreateSet := containsKeywordOutsideStrings(cypher, "ON CREATE SET")
	hasOnMatchSet := containsKeywordOutsideStrings(cypher, "ON MATCH SET")
	if startsWithMatch && hasSet && containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	if startsWithCreate && hasSet && containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRequiredPipeline(ctx, cypher)
	}
	if startsWithCreate && hasSet && !hasOnCreateSet && !hasOnMatchSet {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	if hasSet && !hasOnCreateSet && !hasOnMatchSet {
		if startsWithMatch || findKeywordIndex(cypher, "SET") == 0 {
			return e.executeRequiredPipeline(ctx, cypher)
		}
	}

	if containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	if startsWithMatch && optionalMatchIdx > 0 {
		if outcome := e.executePipeline(ctx, cypher); outcome.terminal() {
			return outcome.result, outcome.err
		}
		// The pipeline is the one executor for MATCH … OPTIONAL MATCH: a
		// form it declines is rejected, not run by another handler (#898).
		return nil, unsupportedOptionalMatchShapeError(cypher)
	}

	switch {
	case startsWithKeywords(cypher, "OPTIONAL", "MATCH"),
		startsWithKeywords(cypher, "OPTIONAL", "CALL"):
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithMatch:
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithCreate:
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithKeywordFold(cypher, "CALL"):
		return e.executeCall(ctx, cypher)
	case startsWithKeywordFold(cypher, "RETURN"):
		return e.executeReturn(ctx, cypher)
	case startsWithKeywordFold(cypher, "WITH"):
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithKeywordFold(cypher, "UNWIND"):
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "LOAD CSV") == 0:
		return e.executeLoadCSV(ctx, cypher)
	default:
		// Terminal chokepoint of the converged router: a statement that passed
		// syntax validation but matches no handler is rejected here — never a
		// silent success, alternate text executor, or re-dispatch. Neo4j
		// reports unrecognized statements as syntax errors, so the localized
		// message keeps its text while Bolt carries the proper status code.
		firstWord, _, _ := cyphertext.CutWhitespace(upperQuery)
		err := localizedError(localization.CypherTransactionsQueryTypeUnsupported(firstWord), nil)
		return nil, &classifiedCypherError{
			cause:  err,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
}

// executeNamedCommand runs a command statement in the owner its command
// names (statementClass.command). SHOW commands run in executeShowStatement.
func (e *StorageExecutor) executeNamedCommand(ctx context.Context, cypher string, command statementCommand) (*ExecuteResult, bool, error) {
	var result *ExecuteResult
	var err error
	switch command {
	case commandProcedurePrivilege:
		result, err = e.executeProcedurePrivilegeCommand(ctx, cypher)
	case commandUnsupportedPrivilege, commandMalformedPrivilege:
		// Only procedure execution privileges are supported.
		err = unsupportedAdministrationCommandError(cypher)
	case commandCreateProcedure:
		result, err = e.executeCreateProcedure(ctx, cypher)
	case commandCreateKnowledgePolicy, commandAlterKnowledgePolicy, commandDropKnowledgePolicy:
		result, err = e.executeKnowledgePolicyDDL(ctx, cypher)
	case commandSchema:
		result, err = e.executeSchemaCommand(ctx, cypher)
	case commandCreateCompositeDatabase:
		result, err = e.executeCompositeCommand(ctx, cypher, compositeCreateCommand)
	case commandCreateOrReplaceDatabase:
		result, err = e.executeDatabaseCreate(ctx, cypher, replaceExistingDatabase)
	case commandCreateDatabase, commandCreateDatabaseOptions:
		result, err = e.executeDatabaseCreate(ctx, cypher, createNewDatabase)
	case commandCreateAlias:
		result, err = e.executeCreateAlias(ctx, cypher)
	case commandDropCompositeDatabase:
		result, err = e.executeDatabaseDrop(ctx, cypher, dropCompositeDatabase)
	case commandDropDatabase:
		result, err = e.executeDatabaseDrop(ctx, cypher, dropStandardDatabase)
	case commandDropAlias:
		result, err = e.executeDropAlias(ctx, cypher)
	case commandDropProcedure:
		result, err = e.executeDropProcedure(ctx, cypher)
	case commandDropRole:
		err = unsupportedAdministrationCommandError(cypher)
	case commandInvalidDrop, commandDropSchemaOther, commandDropAdministration, commandDropRetentionPolicy:
		err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid DROP clause: "+truncateQuery(cypher, 80))
	case commandAlterCompositeDatabase:
		result, err = e.executeCompositeCommand(ctx, cypher, compositeAlterCommand)
	default:
		return nil, false, nil
	}
	return result, true, err
}

func (e *StorageExecutor) executeRequiredPipeline(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if outcome := e.executePipeline(ctx, cypher); outcome.terminal() {
		return outcome.result, outcome.err
	}
	return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "query could not be planned as a clause pipeline")
}

// executeReturn runs a statement that is only a RETURN (RETURN 1, RETURN
// DISTINCT $p AS x, RETURN count(*) AS c SKIP 1). The projection is the
// shared RETURN projection (pipelineApplyReturn) over one row holding the
// parameters and bound values, so DISTINCT, aggregation, ORDER BY, SKIP and
// LIMIT behave as after any other clause (#713).
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parameters are row values ($name), not text: a substituted value would
	// read as a literal (RETURN $a / $b with a = 1.0, b = 0 would fold like
	// RETURN 1.0 / 0).
	// RETURN consumes its input row without modifying it. Reuse the query's
	// immutable parameter projection unless bound variable scopes need overlaying.
	parameters := parameterRowValues(ctx)
	bindings := valueBindingsFromContext(ctx)
	row := pipelineRow(parameters)
	if len(e.fabricRecordBindings) > 0 || len(bindings) > 0 {
		row = make(pipelineRow, len(e.fabricRecordBindings)+len(parameters)+len(bindings))
		for name, value := range e.fabricRecordBindings {
			row[name] = value
		}
		for name, value := range parameters {
			row[name] = value
		}
		// Innermost value bindings retain their precedence over parameters.
		for name, value := range bindings {
			row[name] = value
		}
	}

	returnIdx := queryGapEnd(cypher, 0)
	if !startsWithKeywordFold(cypher[returnIdx:], "RETURN") {
		return nil, localizedError(localization.CypherTransactionsReturnClauseNotFound(truncateQuery(cypher, 80)), nil)
	}

	body := strings.TrimSpace(cypher[returnIdx+len("RETURN"):])
	// Validation and execution consume the same immutable clause plan. Parsing
	// each item again duplicates grammar work and discards its retained products.
	plan := returnProjectionPlanFor(ctx, cypher[returnIdx:])
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	projections := plan.projections
	if plan.star {
		projections = plan.starProjections
	}
	for _, projection := range projections {
		part := projection.expr
		if err := e.validateStaticBooleanOperands(ctx, part); err != nil {
			return nil, err
		}
		if err := e.validateRangeCalls(part, nil); err != nil {
			return nil, err
		}
		if err := e.validateRowSubscriptTypes(part, nil); err != nil {
			return nil, err
		}

		if variable := undefinedStandaloneMapValue(part); variable != "" {
			return nil, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UndefinedVariable",
				"variable is not defined: "+variable,
			)
		}
	}

	rows := []pipelineRow{row}
	if result, ok := e.pipelineApplyReturnPlan(ctx, rows, plan, pipelineRowsSource(rows), false); ok {
		return result, nil
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, failure
	}
	unparsed := body
	for _, projection := range projections {
		if _, defined := e.evaluateRowExpressionWithContext(ctx, projection.expr, row); !defined {
			unparsed = projection.expr
			break
		}
	}
	return nil, unresolvedReturnItemError(ctx, unparsed)
}

// firstTopLevelModifierIndex is the position of a projection's first
// top-level ORDER BY, SKIP or LIMIT, or -1. The shared RETURN projection and
// standalone RETURN both use it.
func firstTopLevelModifierIndex(clause string) int {
	cut := -1
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		// The keyword scan is costly; most projections have no modifier.
		if !containsFold(clause, kw[:4]) {
			continue
		}
		if idx := topLevelKeywordIndex(clause, kw); idx >= 0 && (cut == -1 || idx < cut) {
			cut = idx
		}
	}
	return cut
}

// splitReturnExpressions splits RETURN expressions by comma while preserving
// nested parentheses, lists, and map literals.
func splitReturnExpressions(clause string) []string {
	return splitTopLevelComma(clause)
}

// validateStatementSyntax is the one statement admission owner: public and
// internal execution, Fabric validation, EXPLAIN/PROFILE and the Bolt/HTTP
// entries all call it. It classifies the statement once
// (classifyStatement). NornicDB's own checks answer first, with the same codes,
// messages and classes on either parser; the shared grammar then rejects what
// only it rejects, after the semantic checks that explain such a rejection.
// Neither a parser configuration nor an isolated validator verdict can choose
// another terminal or bypass these checks.
func (e *StorageExecutor) validateStatementSyntax(ctx context.Context, cypher string, cypher25 bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.hasCachedValidSyntax(cypher, syntaxStatementAccepted) {
		return nil
	}
	class := classifyStatement(cypher)
	checks := []func(string) error{validateUnicodeOperators, validateStaticMapKeys, validateNumericLiterals, validateArrowOperators, validatePlaceholderOperands}
	if class.optionsDefinition {
		checks[1] = nil
	}
	if class.limitsAlteration {
		checks[2] = nil
	}
	for _, check := range checks {
		if check == nil {
			continue
		}
		if err := check(cypher); err != nil {
			return err
		}
	}
	if err := validateNativeSurfaceSyntax(cypher, class); err != nil {
		return err
	}
	verdict := syntaxStatementAccepted
	if !class.commandOwned {
		_, err := antlr.ParseSyntax(ctx, cypher, antlr.StatementEntry, cypher25, cyphertext.AcceptedSource)
		if interrupted := ctx.Err(); interrupted != nil {
			return interrupted
		}
		if err != nil {
			if err := e.validateMatchSemanticScopes(ctx, cypher, cypher25); err != nil {
				return err
			}
			if err := validateKnownFunctions(cypher); err != nil {
				return err
			}
			return observability.RedactError(syntaxAdmissionError(ctx, cypher, AcceptedSource, err), observability.ContextCredentials(cypher)...)
		}
		verdict |= syntaxGrammarAccepted
	}
	e.markCachedValidSyntax(cypher, verdict)
	return nil
}

// validateNativeSurfaceSyntax holds NornicDB's own lexical and statement-shape
// checks, which statement admission runs first.
func validateNativeSurfaceSyntax(cypher string, class statementClass) error {
	cypher = lexicalExpressionText(cypher)
	if containsNotInOperator(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input 'NOT': NOT IN is not a Cypher operator; write NOT x IN [list]")
	}
	if containsTrailingListComma(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input ']': expected an expression")
	}
	if singleNodePatternComprehension(cypher) {
		// [(n) | …]: a pattern comprehension needs a relationship, as the
		// ANTLR parser and Neo4j require ("Invalid input '|'").
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression("|"))
	}
	if hasAdjacentStringLiterals(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input: adjacent string literals require an operator between them")
	}
	if explainProfileConflict(cypher) {
		return executionModeConflictError()
	}
	if _, ok := readTrailingFinish(cypher, detectFinishSuffix); ok {
		// FINISH can't follow RETURN, as in Neo4j; every valid trailing
		// FINISH was stripped before validation.
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "RETURN can only be used at the end of the query")
	}
	if !hasValidStartKeyword(class) {
		// Neo4j reports an unrecognized statement as a syntax error; classify
		// the localized terminal so Bolt carries the proper status code.
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherTransactionsSyntaxStartInvalid(), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
	if err := validateLeadingNodePatternTransition(cypher); err != nil {
		return err
	}

	if class.graphQuery && hasAdjacentOperands(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: an expression is followed by another expression without an operator")
	}

	parenCount := 0
	bracketCount := 0
	braceCount := 0

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]
		if isCypherQuoteByte(c) {
			end, closed := scanCypherQuotedText(cypher, i, c)
			if !closed {
				return &classifiedCypherError{cause: localizedError(localization.CypherTransactionsSyntaxUnclosedQuote(), nil), code: "Neo.ClientError.Statement.SyntaxError", detail: "UnexpectedSyntax"}
			}
			i = end - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(cypher, i); end >= 0 {
				i = end - 1
				continue
			}
		}
		if c == '.' && i+1 < len(cypher) && cypher[i+1] == '.' && bracketCount == 0 {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
		}
		if c == '.' && (i == 0 || cypher[i-1] != '.') && (i+1 == len(cypher) || cypher[i+1] != '.') {
			// A dot other than a range's (1..2) starts a property key (n.k,
			// n.`k`), a map projection's all-properties (.*) or a number's
			// digits: RETURN n. is Neo4j's "expected an identifier".
			if next := queryGapEnd(cypher, i+1); next == len(cypher) || (!cyphertext.NamePartAt(cypher, next) || cypher[next] == '$') && !isCypherBacktickByte(cypher[next]) && cypher[next] != '*' {
				token := ""
				if next < len(cypher) {
					token = cypher[next : next+1]
				}
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
					localization.CypherCoreInvalidInputExpectedIdentifier(token))
			}
		}
		if c == '+' {
			next := skipSpaces(cypher, i+1)
			if next < len(cypher) && cypher[next] == '*' {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
			}
		}

		switch c {
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '[':
			bracketCount++
		case ']':
			bracketCount--
		case '{':
			braceCount++
		case '}':
			braceCount--
		}

		if parenCount < 0 || bracketCount < 0 || braceCount < 0 {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UnexpectedSyntax",
				fmt.Sprintf("syntax error: unbalanced delimiter at position %d", i),
			)
		}
	}

	if parenCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced parentheses")
	}
	if bracketCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced square brackets")
	}
	if braceCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced curly braces")
	}
	return nil
}

// hasAdjacentOperands reports two operands with no operator between them,
// such as n.name 'x', (a =~ 'T.*')'T.*', 5 'x', [1] 'x' or n.a n.b, which
// Neo4j rejects as a syntax error in every expression (RETURN, WITH, WHERE,
// ORDER BY, SET, property maps). An operand ends with a string or number
// literal, ')' , ']' or a property access (x.name); the next token may not
// start another literal, $parameter or property access. A bare word followed
// by a literal is a variable next to an operand (n 'x') unless it is one of
// the keywords a literal may follow (literalLeadingKeywords); otherwise a bare
// word resets the check.
// It applies to graph queries only (statementClass.graphQuery): schema,
// administration and knowledge-policy statements have their own grammars,
// e.g. constraint contract blocks separate predicates by line breaks.
func hasAdjacentOperands(cypher string) bool {
	operandEnded := false
	for index := 0; index < len(cypher); {
		c := cypher[index]
		switch {
		case cyphertext.IsWhitespaceAt(cypher, index):
			index++
		case isCypherStringQuoteByte(c):
			if operandEnded {
				return true
			}
			end, _ := scanCypherQuotedText(cypher, index, c)
			index = end
			operandEnded = true
		case isCypherBacktickByte(c):
			end, closed := scanCypherQuotedText(cypher, index, c)
			if !closed {
				return false
			}
			property := index > 0 && cypher[index-1] == '.'
			index = end
			operandEnded = property
		case cyphertext.NumberStartsAt(cypher, index):
			if operandEnded {
				return true
			}
			token, _ := cyphertext.ScanNumber(cypher, index)
			index = token.TailEnd
			operandEnded = true
		case c == '$':
			if operandEnded {
				return true
			}
			index++
			index = cyphertext.NamePartEnd(cypher, index)
			operandEnded = true
		case cyphertext.NamePartAt(cypher, index):
			start := index
			index = cyphertext.NamePartEnd(cypher, index)
			startsProperty := index < len(cypher) && cypher[index] == '.' &&
				(index+1 >= len(cypher) || cypher[index+1] != '.')
			if operandEnded && startsProperty {
				return true
			}
			property := start > 0 && cypher[start-1] == '.' && (start < 2 || cypher[start-2] != '.')
			if operandEnded && !property && !startsProperty && !isSyntaxBoundaryKeyword(cypher[start:index]) {
				return true
			}
			if !property && !startsProperty && bareWordBeforeLiteral(cypher, start, index) {
				return true
			}
			operandEnded = property
		case c == ')' || c == ']':
			index++
			operandEnded = true
		default:
			index++
			operandEnded = false
		}
	}
	return false
}

func isSyntaxBoundaryKeyword(word string) bool {
	if _, allowed := literalLeadingKeywords[upperASCII(word)]; allowed {
		return true
	}
	switch upperASCII(word) {
	case "AS", "END", "STARTS", "ENDS", "MATCH", "OPTIONAL", "CREATE", "MERGE", "SET", "REMOVE", "DETACH", "FOREACH", "CALL", "ON", "ORDER", "ASC", "DESC", "ASCENDING", "DESCENDING", "NULL", "ROWS", "ROW", "TRANSACTIONS", "TRANSACTION", "REPORT", "STATUS", "BREAK", "CONTINUE", "FAIL", "ERROR", "UNION", "FINISH", "USING", "INDEX", "JOIN", "SCAN", "LET", "FILTER", "FOR":
		return true
	}
	return false
}

// literalLeadingKeywords are the words a string / number literal or a
// $parameter may directly follow in a graph query (RETURN 'x', LIMIT 5,
// n.name CONTAINS 'x', STARTS WITH $p, CASE 'a' WHEN 'a' THEN 1 ELSE 2,
// ORDER BY 1, LOAD CSV FROM 'url' ... FIELDTERMINATOR ';', IN TRANSACTIONS
// OF 10 ROWS, USING PERIODIC COMMIT 500, SHORTEST 2, ...).
var literalLeadingKeywords = map[string]struct{}{
	"RETURN": {}, "WITH": {}, "WHERE": {}, "AND": {}, "OR": {}, "XOR": {}, "NOT": {},
	"IN": {}, "IS": {}, "CASE": {}, "WHEN": {}, "THEN": {}, "ELSE": {}, "CONTAINS": {},
	"SKIP": {}, "LIMIT": {}, "UNWIND": {}, "FROM": {}, "FIELDTERMINATOR": {}, "OF": {},
	"DISTINCT": {}, "BY": {}, "YIELD": {}, "SHORTEST": {}, "ANY": {}, "ALL": {},
	"COMMIT": {}, "USE": {}, "OFFSET": {}, "DELETE": {},
	// trim([LEADING | TRAILING | BOTH] 'x' FROM s)
	"BOTH": {}, "LEADING": {}, "TRAILING": {},
}

// bareWordBeforeLiteral reports whether the word cypher[start:end] (not a
// property name) is followed by a string or number literal and
// is not one of literalLeadingKeywords, i.e. a variable directly followed by
// another operand. A word followed by '(' (a function call) or ':' (a map key
// or label) is never such a variable.
func bareWordBeforeLiteral(cypher string, start, end int) bool {
	next := skipSpaces(cypher, end)
	if next == end || next >= len(cypher) {
		return false
	}
	switch c := cypher[next]; {
	case isCypherStringQuoteByte(c) || cyphertext.IsDigit(c):
	case c == '$':
		// A $parameter after a node or relationship pattern's variable or
		// label is its property map ((n:Label $props), [r $props]);
		// elsewhere it is an operand after an operand (RETURN a $b) or a
		// parameter where a name must be (UNWIND [1] AS $x), Neo4j's
		// "Invalid input '$'".
		before := start
		for before > 0 && cyphertext.IsWhitespaceAt(cypher, before-1) {
			before--
		}
		if before > 0 && strings.IndexByte("([:|&!", cypher[before-1]) >= 0 {
			return false
		}
	default:
		return false
	}
	if cyphertext.IsDigit(cypher[start]) {
		return false
	}
	_, keyword := literalLeadingKeywords[upperASCII(cypher[start:end])]
	return !keyword
}

func validateLeadingNodePatternTransition(cypher string) error {
	query := strings.TrimSpace(cypher)
	keyword := ""
	if matchKeywordAt(query, 0, "MATCH") {
		keyword = "MATCH"
	} else if matchKeywordAt(query, 0, "CREATE") {
		keyword = "CREATE"
	} else {
		return nil
	}
	open := skipSpaces(query, len(keyword))
	if open >= len(query) || query[open] != '(' {
		return nil
	}
	if groupOpen, _, _, grouped := nextQuantifiedGroup(query, open); grouped && groupOpen == open {
		// A pattern may start with a quantified path pattern: ((x)-->(y))+.
		return nil
	}
	close := findMatchingParen(query, open)
	if close < 0 {
		return nil
	}
	remaining := strings.TrimSpace(query[close+1:])
	if remaining == "" || remaining[0] == ',' || remaining[0] == '-' || remaining[0] == '<' || remaining[0] == ';' {
		return nil
	}
	// A quantified path pattern follows its node directly: (a)((x)-->(y))+.
	if open, _, _, ok := nextQuantifiedGroup(remaining, 0); ok && open == 0 && keyword == "MATCH" {
		return nil
	}
	for _, allowed := range []string{"WHERE", "USING", "RETURN", "WITH", "MATCH", "OPTIONAL", "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "UNWIND", "CALL", "FOREACH", "ORDER", "SKIP", "LIMIT", "UNION", "LET", "FILTER", "FOR"} {
		if matchKeywordAt(remaining, 0, allowed) {
			return nil
		}
	}
	if matchKeywordAt(remaining, 0, "DETACH") && matchKeywordAt(strings.TrimSpace(remaining[len("DETACH"):]), 0, "DELETE") {
		return nil
	}
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unexpected text after node pattern")
}

// hasValidStartKeyword reports whether the statement opens with a word
// NornicDB accepts at its start (statementClass.opening), or is a privilege
// command.
func hasValidStartKeyword(class statementClass) bool {
	return class.opening != openingNone || class.privilegeShaped
}

// ensureSyntaxValidationCache lazily installs the syntax-validation cache
// pointer using sync.Once so concurrent CALL { ... } subqueries (which fan
// out via executeCallTailParallel) cannot race on the pointer write. The
// underlying cache itself is already mutex-guarded; the race was on the
// initial pointer assignment.
func (e *StorageExecutor) ensureSyntaxValidationCache() *syntaxValidationCache {
	e.syntaxValidationOnce.Do(func() {
		if e.syntaxValidationCache == nil {
			e.syntaxValidationCache = newSyntaxValidationCache()
		}
	})
	return e.syntaxValidationCache
}

// ShareStatementCachesFrom gives a fresh executor the source executor's
// caches that depend only on the statement text: the syntax verdicts and
// the upper-cased text. Configure it before executing queries. Protocol
// transactions (Bolt and HTTP explicit transactions) build a new executor
// per transaction; without the shared verdicts every statement in such a
// transaction was validated again from scratch, which is a full grammar
// parse of the statement on either parser.
//
//	transactionExecutor := NewStorageExecutor(databaseStorage)
//	transactionExecutor.ShareStatementCachesFrom(databaseExecutor)
func (e *StorageExecutor) ShareStatementCachesFrom(source *StorageExecutor) {
	if source == nil || source == e {
		return
	}
	e.UseStatementCaches(source.StatementCaches())
	e.statementCacheScope = source.statementCacheScope
}

// The acceptance facts a statement text is cached with. Each owner stamps only
// its own fact; none depends on the configured parser.
const (
	// syntaxGrammarAccepted marks a text the shared grammar itself accepted.
	// Statement admission also accepts NornicDB's extension statements, which
	// the grammar rejects. The log redactor reads it (redactForLog).
	syntaxGrammarAccepted uint8 = 1 << iota
	// A full execution admission, independent of the configured parser.
	syntaxStatementAccepted
)

func (e *StorageExecutor) hasCachedValidSyntax(cypher string, validator uint8) bool {
	if cypher == "" {
		return false
	}
	accepted := e.cachedSyntaxVerdict(cypher)
	if accepted&validator != 0 {
		return true
	}
	// Redaction reuses only an exact-text grammar verdict. Canonical
	// literal variants remain valid only for the execution validators.
	validator &^= syntaxGrammarAccepted
	if validator == 0 {
		return false
	}
	// Parser acceptance is structural: literal values never change it, so
	// statements that differ only in literals share one cached verdict
	// through the canonical form (PR #1043 #6). The canonical form is only
	// computed on an exact-text miss.
	if canonical, normalized := normalizeLiteralText(cypher); normalized {
		accepted = e.cachedSyntaxVerdict(canonical)
	}
	return accepted&validator != 0
}

func (e *StorageExecutor) markCachedValidSyntax(cypher string, validator uint8) {
	if cypher == "" {
		return
	}
	scopes, cacheable := e.cacheDependencyScopes(cypher)
	if !cacheable {
		return
	}
	e.addSyntaxCacheFamily(cypher, validator, scopes)
}

// unresolvedReturnItemError is the error of a RETURN item the row evaluator
// could not resolve: the failure it recorded (a division by zero, a runtime
// TypeError, ...), or a SyntaxError for the item. It records the error.
func unresolvedReturnItemError(ctx context.Context, item string) error {
	if failure := getExpressionFailure(ctx); failure != nil {
		return failure
	}
	err := newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnexpectedSyntax",
		"could not parse RETURN expression: "+item,
	)
	recordExpressionFailure(ctx, err)
	return err
}

// unsupportedOptionalMatchShapeError is the error for a MATCH … OPTIONAL MATCH
// statement the clause pipeline declines. It carries the router terminal's
// SyntaxError classification: an unhandled shape fails, it never falls back to
// another executor.
func unsupportedOptionalMatchShapeError(cypher string) error {
	return &classifiedCypherError{
		cause:  localizedError(localization.CypherMatchingOptionalMatchShapeUnsupported(truncateQuery(cypher, 80)), nil),
		code:   "Neo.ClientError.Statement.SyntaxError",
		detail: "UnexpectedSyntax",
	}
}

// executeShowStatement runs a SHOW (or TERMINATE TRANSACTIONS) command,
// handled false when cypher is none. It runs before the routes that send a
// statement to the clause pipeline by the clauses it contains: since Cypher 25
// a SHOW command's YIELD may be followed by any clause (CALL { … }, DELETE,
// SET, …), and the command still runs as a SHOW (#907).
func (e *StorageExecutor) executeShowStatement(ctx context.Context, cypher string) (*ExecuteResult, bool, error) {
	command := classifyStatement(cypher).command
	var run func(context.Context, string) (*ExecuteResult, error)
	switch command {
	case commandShowConstraints:
		// SHOW [kind] CONSTRAINTS (showConstraintKindOf).
		if _, _, err := showConstraintKindOf(cypher, cypherVersionFromContext(ctx) == "25"); err != nil {
			return nil, true, err
		}
		run = e.executeShowConstraints
	case commandShowIndexes:
		run = e.executeShowIndexes
	case commandShowKnowledgePolicies:
		run = e.executeKnowledgePolicyDDL
	case commandShowProcedures:
		run = e.executeShowProcedures
	case commandShowFunctions:
		run = e.executeShowFunctions
	case commandShowCompositeDatabases:
		run = e.executeShowCompositeDatabases
	case commandShowConstituents:
		run = e.executeShowConstituents
	case commandShowDefaultDatabase:
		run = e.executeShowDefaultDatabase
	case commandShowUsers, commandShowCurrentUser:
		run = e.executeShowUsers
	case commandShowTransactions:
		run = e.executeShowTransactions
	case commandTerminateTransactions:
		run = e.executeTerminateTransactions
	case commandShowPrivileges:
		run = e.executeShowPrivileges
	case commandShowUnsupportedAdministration, commandShowUserDefined, commandShowServers:
		return nil, true, unsupportedAdministrationCommandError(cypher)
	case commandShowDatabases:
		run = e.executeShowDatabases
	case commandShowDatabase:
		run = e.executeShowDatabase
	case commandShowAliases:
		run = e.executeShowAliases
	case commandShowSettings:
		run = e.executeShowSettings
	case commandShowLimits:
		run = e.executeShowLimits
	default:
		return nil, false, nil
	}
	result, err := e.executeShowWithTail(ctx, cypher, run)
	return result, true, err
}
