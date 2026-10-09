package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// isSystemCommandNoGraph returns true for statements that operate on database metadata
// (CREATE/DROP DATABASE, SHOW DATABASES, etc.) and must not use the async engine or
// implicit transactions. These are routed to executeWithoutTransaction directly.
func isSystemCommandNoGraph(cypher string) bool {
	return startsWithKeywords(cypher, "CREATE", "COMPOSITE DATABASE") ||
		isCreateOrReplaceDatabaseQuery(cypher) ||
		startsWithKeywords(cypher, "CREATE", "DATABASE") ||
		startsWithKeywords(cypher, "CREATE", "ALIAS") ||
		startsWithKeywords(cypher, "DROP", "COMPOSITE DATABASE") ||
		startsWithKeywords(cypher, "DROP", "DATABASE") ||
		startsWithKeywords(cypher, "DROP", "ALIAS") ||
		startsWithKeywords(cypher, "SHOW", "DATABASES") ||
		startsWithKeywords(cypher, "ALTER", "DATABASE")
}

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
func topLevelUnion(cypher, upperQuery string) (unionAll, union bool) {
	if !strings.Contains(upperQuery, "UNION") {
		return false, false
	}
	branches, unionAll, _, ok := parseTopLevelUnionBranches(cypher)
	return unionAll, ok && len(branches) > 1
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (result *ExecuteResult, err error) {
	ctx, cleanupReveal, readScopeEngine := setRevealOnEngine(ctx, e.storage, hasRevealCall(cypher))
	defer cleanupReveal()
	if readScopeEngine != nil {
		defer clearRevealScope(ctx, readScopeEngine)
	}
	defer func() {
		if recorded := getExpressionFailure(ctx); recorded != nil && err == nil {
			result, err = nil, recorded
		}
	}()
	// A top-level UNION composes complete single queries. Route it before any
	// handler can consume the leading MATCH, RETURN, or UNWIND branch.
	if unionAll, union := topLevelUnion(cypher, upperQuery); union {
		return e.executeUnion(ctx, cypher, unionAll)
	}
	if strings.Contains(upperQuery, "CALL") {
		if firstTopLevelCallSubquery(cypher) >= 0 {
			return e.executeRequiredPipeline(ctx, cypher)
		}
	}

	if result, handled := e.tryFastPathSimpleMatchReturnLimit(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathAnyMatchVectorCosine(ctx, cypher, upperQuery); handled {
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
		if !isCreateProcedureCommand(cypher) &&
			!startsWithKeywords(cypher, "CREATE", "DECAY PROFILE") &&
			!startsWithKeywords(cypher, "CREATE", "PROMOTION PROFILE") &&
			!startsWithKeywords(cypher, "CREATE", "PROMOTION POLICY") {
			if _, ok := canExecuteAsPipeline(cypher); ok {
				return e.executeRequiredPipeline(ctx, cypher)
			}
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
	if findKeywordIndex(cypher, "UNWIND") == 0 || startsWithKeywordFold(cypher, "FOR") ||
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
	if startsWithCreate && !isCreateProcedureCommand(cypher) && hasSet && !hasOnCreateSet && !hasOnMatchSet &&
		!startsWithKeywords(cypher, "CREATE", "DECAY PROFILE") &&
		!startsWithKeywords(cypher, "CREATE", "PROMOTION PROFILE") &&
		!startsWithKeywords(cypher, "CREATE", "PROMOTION POLICY") {
		return e.executeRequiredPipeline(ctx, cypher)
	}

	if startsWithKeywords(cypher, "ALTER", "DATABASE") {
		return e.executeAlterDatabase(ctx, cypher)
	}

	if hasSet && !isCreateProcedureCommand(cypher) && !hasOnCreateSet && !hasOnMatchSet &&
		!startsWithKeywords(cypher, "CREATE", "DECAY PROFILE") &&
		!startsWithKeywords(cypher, "CREATE", "PROMOTION PROFILE") &&
		!startsWithKeywords(cypher, "CREATE", "PROMOTION POLICY") &&
		!startsWithKeywords(cypher, "ALTER", "DECAY PROFILE") &&
		!startsWithKeywords(cypher, "ALTER", "PROMOTION PROFILE") &&
		!startsWithKeywords(cypher, "ALTER", "PROMOTION POLICY") {
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
	case isCreateProcedureCommand(cypher):
		return e.executeCreateProcedure(ctx, cypher)
	case startsWithKeywords(cypher, "CREATE", "DECAY PROFILE"),
		startsWithKeywords(cypher, "CREATE", "PROMOTION PROFILE"),
		startsWithKeywords(cypher, "CREATE", "PROMOTION POLICY"):
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case startsWithKeywords(cypher, "OPTIONAL", "MATCH"),
		startsWithKeywords(cypher, "OPTIONAL", "CALL"):
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithMatch:
		return e.executeRequiredPipeline(ctx, cypher)
	case startsWithKeywords(cypher, "CREATE", "CONSTRAINT"),
		startsWithKeywords(cypher, "CREATE", "RANGE INDEX"),
		startsWithKeywords(cypher, "CREATE", "FULLTEXT INDEX"),
		startsWithKeywords(cypher, "CREATE", "VECTOR INDEX"),
		startsWithKeywords(cypher, "CREATE", "TEXT INDEX"),
		startsWithKeywords(cypher, "CREATE", "POINT INDEX"),
		startsWithKeywords(cypher, "CREATE", "LOOKUP INDEX"),
		findKeywordIndex(cypher, "CREATE INDEX") == 0:
		return e.executeSchemaCommand(ctx, cypher)
	case startsWithKeywords(cypher, "CREATE", "COMPOSITE DATABASE"):
		return e.executeCreateCompositeDatabase(ctx, cypher)
	case isCreateOrReplaceDatabaseQuery(cypher):
		return e.executeCreateOrReplaceDatabase(ctx, cypher)
	case startsWithKeywords(cypher, "CREATE", "DATABASE"):
		return e.executeCreateDatabase(ctx, cypher)
	case startsWithKeywords(cypher, "CREATE", "ALIAS"):
		return e.executeCreateAlias(ctx, cypher)
	case startsWithCreate:
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "CALL") == 0:
		return e.executeCall(ctx, cypher)
	case findKeywordIndex(cypher, "RETURN") == 0:
		return e.executeReturn(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "COMPOSITE DATABASE"):
		return e.executeDropCompositeDatabase(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "DATABASE"):
		return e.executeDropDatabase(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "ALIAS"):
		return e.executeDropAlias(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "CONSTRAINT"):
		return e.executeSchemaCommand(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "DECAY PROFILE"),
		startsWithKeywords(cypher, "DROP", "PROMOTION PROFILE"),
		startsWithKeywords(cypher, "DROP", "PROMOTION POLICY"):
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case isDropProcedureCommand(cypher):
		return e.executeDropProcedure(ctx, cypher)
	case startsWithKeywords(cypher, "DROP", "INDEX"):
		return e.executeSchemaCommand(ctx, cypher)
	case findKeywordIndex(cypher, "DROP") == 0:
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid DROP clause: "+truncateQuery(cypher, 80))
	case findKeywordIndex(cypher, "WITH") == 0:
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "UNWIND") == 0:
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		return e.executeRequiredPipeline(ctx, cypher)
	case findKeywordIndex(cypher, "LOAD CSV") == 0:
		return e.executeLoadCSV(ctx, cypher)
	case startsWithKeywords(cypher, "SHOW", "FULLTEXT INDEXES"),
		startsWithKeywords(cypher, "SHOW", "FULLTEXT INDEX"),
		startsWithKeywords(cypher, "SHOW", "RANGE INDEXES"),
		startsWithKeywords(cypher, "SHOW", "RANGE INDEX"),
		startsWithKeywords(cypher, "SHOW", "VECTOR INDEXES"),
		startsWithKeywords(cypher, "SHOW", "VECTOR INDEX"),
		startsWithKeywords(cypher, "SHOW", "LOOKUP INDEXES"),
		startsWithKeywords(cypher, "SHOW", "LOOKUP INDEX"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowIndexes)
	case startsWithKeywords(cypher, "SHOW", "INDEXES"),
		startsWithKeywords(cypher, "SHOW", "INDEX"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowIndexes)
	case startsWithKeywords(cypher, "SHOW", "DECAY PROFILES"),
		startsWithKeywords(cypher, "SHOW", "PROMOTION PROFILES"),
		startsWithKeywords(cypher, "SHOW", "PROMOTION POLICIES"):
		return e.executeShowWithTail(ctx, cypher, e.executeKnowledgePolicyDDL)
	case startsWithKeywords(cypher, "SHOW", "CONSTRAINTS"),
		startsWithKeywords(cypher, "SHOW", "CONSTRAINT"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowConstraints)
	case startsWithKeywords(cypher, "SHOW", "PROCEDURES"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowProcedures)
	case findKeywordIndex(cypher, "SHOW FUNCTIONS") == 0:
		return e.executeShowWithTail(ctx, cypher, e.executeShowFunctions)
	case startsWithKeywords(cypher, "SHOW", "COMPOSITE DATABASES"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowCompositeDatabases)
	case startsWithKeywords(cypher, "SHOW", "CONSTITUENTS"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowConstituents)
	case startsWithKeywords(cypher, "SHOW", "DEFAULT DATABASE"),
		startsWithKeywords(cypher, "SHOW", "HOME DATABASE"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowDefaultDatabase)
	case startsWithKeywords(cypher, "SHOW", "USERS"),
		startsWithKeywords(cypher, "SHOW", "CURRENT USER"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowUsers)
	case startsWithKeywords(cypher, "SHOW", "TRANSACTIONS"),
		startsWithKeywords(cypher, "SHOW", "TRANSACTION"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowTransactions)
	case startsWithKeywords(cypher, "TERMINATE", "TRANSACTIONS"),
		startsWithKeywords(cypher, "TERMINATE", "TRANSACTION"):
		return e.executeShowWithTail(ctx, cypher, e.executeTerminateTransactions)
	case startsWithKeywords(cypher, "SHOW", "ROLES"),
		startsWithKeywords(cypher, "SHOW", "ROLE"),
		startsWithKeywords(cypher, "SHOW", "PRIVILEGES"),
		startsWithKeywords(cypher, "SHOW", "USER"),
		startsWithKeywords(cypher, "SHOW", "SERVERS"),
		startsWithKeywords(cypher, "SHOW", "SERVER"):
		return nil, unsupportedAdministrationCommandError(cypher)
	case startsWithKeywords(cypher, "SHOW", "DATABASES"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowDatabases)
	case startsWithKeywords(cypher, "SHOW", "DATABASE"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowDatabase)
	case startsWithKeywords(cypher, "SHOW", "ALIASES"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowAliases)
	case startsWithKeywords(cypher, "SHOW", "SETTINGS"),
		startsWithKeywords(cypher, "SHOW", "SETTING"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowSettings)
	case startsWithKeywords(cypher, "ALTER", "COMPOSITE DATABASE"):
		return e.executeAlterCompositeDatabase(ctx, cypher)
	case startsWithKeywords(cypher, "ALTER", "DECAY PROFILE"),
		startsWithKeywords(cypher, "ALTER", "PROMOTION PROFILE"),
		startsWithKeywords(cypher, "ALTER", "PROMOTION POLICY"):
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case startsWithKeywords(cypher, "SHOW", "LIMITS"):
		return e.executeShowWithTail(ctx, cypher, e.executeShowLimits)
	default:
		// Terminal chokepoint of the converged router: a statement that passed
		// syntax validation but matches no handler is rejected here — never a
		// silent success, alternate text executor, or re-dispatch. Neo4j
		// reports unrecognized statements as syntax errors, so the localized
		// message keeps its text while Bolt carries the proper status code.
		firstWord := strings.Split(upperQuery, " ")[0]
		err := localizedError(localization.CypherTransactionsQueryTypeUnsupported(firstWord), nil)
		return nil, &classifiedCypherError{
			cause:  err,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
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
	params := getParamsFromContext(ctx)
	row := make(pipelineRow, len(e.fabricRecordBindings)+len(params))
	for name, value := range e.fabricRecordBindings {
		row[name] = value
	}
	bindParameterRow(ctx, row)
	// Bound child contexts (§6.2): UNION/CALL branches may reference values
	// that travel in the value scope; the innermost bindings shadow params.
	if bindings := valueBindingsFromContext(ctx); bindings != nil {
		for name, value := range bindings {
			row[name] = value
		}
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, localizedError(localization.CypherTransactionsReturnClauseNotFound(truncateQuery(cypher, 80)), nil)
	}

	body := strings.TrimSpace(cypher[returnIdx+len("RETURN"):])
	items := body
	if cut := firstTopLevelModifierIndex(items); cut >= 0 {
		items = strings.TrimSpace(items[:cut])
	}
	items, _ = cutDistinct(items)
	parts := splitTopLevelComma(items)
	for index, part := range parts {
		// Same alias parsing as every projection (parseProjectionExprAlias).
		part, _ = parseProjectionExprAlias(strings.TrimSpace(part))
		parts[index] = part
		if err := e.validateStaticBooleanOperands(ctx, part); err != nil {
			return nil, err
		}
		if err := e.validateRangeCalls(part, pipelineRow{}); err != nil {
			return nil, err
		}
		if err := e.validateRowSubscriptTypes(part, pipelineRow{}); err != nil {
			return nil, err
		}
		if err := e.validateRowConversionArguments(part, row); err != nil {
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

	if result, ok := e.pipelineApplyReturn(ctx, []pipelineRow{row}, "RETURN "+body); ok {
		return result, nil
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, failure
	}
	unparsed := body
	for _, part := range parts {
		if _, defined := e.evaluateRowExpressionWithContext(ctx, part, row); !defined {
			unparsed = part
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

// validateSyntax performs syntax validation.
// When NORNICDB_PARSER=antlr, uses ANTLR for strict OpenCypher grammar validation.
// When NORNICDB_PARSER=nornic (default), uses fast inline validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	// A text the Nornic validator accepted passed every check below (it is
	// marked valid only then), so a repeated query skips them all (#823).
	if !config.IsANTLRParser() && e.hasCachedValidSyntax(cypher) {
		return nil
	}
	if err := validateUnicodeOperators(cypher); err != nil {
		return err
	}
	if err := validateUnicodeStringLiterals(cypher); err != nil {
		return err
	}
	if err := validateStaticMapKeys(cypher); err != nil {
		return err
	}
	if err := validateNumericLiterals(cypher); err != nil {
		return err
	}
	if config.IsANTLRParser() {
		return e.validateSyntaxANTLR(cypher)
	}
	return e.validateSyntaxNornic(cypher)
}

// validateSyntaxANTLR uses ANTLR for strict OpenCypher grammar validation.
// Provides detailed error messages with line/column information.
func (e *StorageExecutor) validateSyntaxANTLR(cypher string) error {
	if isNornicExtensionStatement(cypher) {
		return e.validateSyntaxNornic(cypher)
	}
	parserError := antlr.Validate(cypher)
	if parserError == nil {
		return nil
	}
	if err := e.validateSyntaxNornic(cypher); err != nil {
		return err
	}
	if err := e.validateMatchSemanticScopes(cypher); err != nil {
		return err
	}
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", parserError.Error())
}

// isNornicExtensionStatement reports whether cypher is a NornicDB-only
// statement, outside Neo4j's Cypher and so outside the ANTLR grammar, which
// the Nornic validator checks instead under NORNICDB_PARSER=antlr (#957):
// knowledge-policy DDL, procedure DDL (CREATE [OR REPLACE] PROCEDURE, DROP
// PROCEDURE) and database limits (ALTER DATABASE … SET LIMIT, SHOW LIMITS).
func isNornicExtensionStatement(cypher string) bool {
	return isKnowledgePolicyDDLStatement(cypher) || isCreateProcedureCommand(cypher) || isDropProcedureCommand(cypher) ||
		startsWithKeywords(cypher, "SHOW", "LIMITS") ||
		(startsWithKeywords(cypher, "ALTER", "DATABASE") && findKeywordIndex(cypher, "SET LIMIT") >= 0)
}

// validateSyntaxNornic performs fast inline syntax validation.
func (e *StorageExecutor) validateSyntaxNornic(cypher string) error {
	if e.hasCachedValidSyntax(cypher) {
		return nil
	}
	if containsNotInOperator(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input 'NOT': NOT IN is not a Cypher operator; write NOT x IN [list]")
	}
	if containsTrailingListComma(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input ']': expected an expression")
	}
	if hasAdjacentStringLiterals(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input: adjacent string literals require an operator between them")
	}
	if explainProfileConflict(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "EXPLAIN cannot be combined with PROFILE")
	}
	if _, ok := trailingBareFinish(cypher); ok {
		// FINISH can't follow RETURN, as in Neo4j; every valid trailing
		// FINISH was stripped before validation.
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "RETURN can only be used at the end of the query")
	}
	if !hasValidStartKeyword(cypher) {
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

	if isGraphQueryStatement(cypher) && hasAdjacentOperands(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: an expression is followed by another expression without an operator")
	}

	parenCount := 0
	bracketCount := 0
	braceCount := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]

		if inString {
			// A backtick-quoted name has no backslash escapes (a doubled
			// backtick closes and reopens it); string literals do.
			if c == stringChar && (c == '`' || !isBackslashEscaped(cypher, i)) {
				inString = false
			}
			continue
		}
		if c == '.' && i+1 < len(cypher) && cypher[i+1] == '.' && bracketCount == 0 {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
		}
		if c == '+' {
			next := skipSpaces(cypher, i+1)
			if next < len(cypher) && cypher[next] == '*' {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
			}
		}

		switch c {
		case '"', '\'', '`':
			inString = true
			stringChar = c
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
	if inString {
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherTransactionsSyntaxUnclosedQuote(), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}

	e.markCachedValidSyntax(cypher)
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
// It applies to graph queries only (isGraphQueryStatement): schema,
// administration and knowledge-policy statements have their own grammars,
// e.g. constraint contract blocks separate predicates by line breaks.
func hasAdjacentOperands(cypher string) bool {
	operandEnded := false
	for index := 0; index < len(cypher); {
		c := cypher[index]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			index++
		case c == '\'' || c == '"':
			if operandEnded {
				return true
			}
			end := index + 1
			for end < len(cypher) {
				if cypher[end] == c && !isBackslashEscaped(cypher, end) {
					if end+1 < len(cypher) && cypher[end+1] == c {
						end += 2 // a doubled quote is an escaped quote
						continue
					}
					break
				}
				end++
			}
			index = end + 1
			operandEnded = true
		case c == '`':
			end := strings.IndexByte(cypher[index+1:], '`')
			if end < 0 {
				return false
			}
			property := index > 0 && cypher[index-1] == '.'
			index += end + 2
			operandEnded = property
		case c >= '0' && c <= '9':
			if operandEnded {
				return true
			}
			index++
			for index < len(cypher) && (isIdentByte(cypher[index]) ||
				(cypher[index] == '.' && index+1 < len(cypher) && cypher[index+1] >= '0' && cypher[index+1] <= '9')) {
				index++
			}
			operandEnded = true
		case c == '$':
			if operandEnded {
				return true
			}
			index++
			for index < len(cypher) && isIdentByte(cypher[index]) {
				index++
			}
			operandEnded = true
		case isIdentByte(c):
			start := index
			for index < len(cypher) && isIdentByte(cypher[index]) {
				index++
			}
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
	// A $parameter after a word can be a node pattern's property map
	// parameter ((n:Label $props)), so only string and number literals count.
	switch c := cypher[next]; {
	case c == '\'' || c == '"' || (c >= '0' && c <= '9'):
	default:
		return false
	}
	if c := cypher[start]; c >= '0' && c <= '9' {
		return false
	}
	_, keyword := literalLeadingKeywords[upperASCII(cypher[start:end])]
	return !keyword
}

// isGraphQueryStatement reports whether a statement is a Cypher graph query:
// it starts (after EXPLAIN / PROFILE) with MATCH, OPTIONAL MATCH, WITH,
// RETURN, UNWIND, MERGE, CALL, FOREACH, LOAD CSV, UNION or USE, or with
// CREATE followed by a pattern ("CREATE (" or "CREATE p = "). CREATE INDEX /
// CONSTRAINT / DATABASE / USER and the other CREATE ... definitions are not.
func isGraphQueryStatement(cypher string) bool {
	query := strings.TrimSpace(cypher)
	for _, prefix := range []string{"EXPLAIN", "PROFILE"} {
		if matchKeywordAt(query, 0, prefix) {
			query = strings.TrimSpace(query[len(prefix):])
		}
	}
	for _, keyword := range []string{"MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "MERGE", "CALL", "FOREACH", "LOAD", "UNION", "USE"} {
		if matchKeywordAt(query, 0, keyword) {
			return true
		}
	}
	if !matchKeywordAt(query, 0, "CREATE") {
		return false
	}
	rest := strings.TrimSpace(query[len("CREATE"):])
	if strings.HasPrefix(rest, "(") {
		return true
	}
	name, next, ok := scanIdentifierToken(rest, 0)
	return ok && name != "" && strings.HasPrefix(strings.TrimSpace(rest[next:]), "=")
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
	close := findMatchingParen(query, open)
	if close < 0 {
		return nil
	}
	remaining := strings.TrimSpace(query[close+1:])
	if remaining == "" || remaining[0] == ',' || remaining[0] == '-' || remaining[0] == '<' || remaining[0] == ';' {
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

var validSyntaxStarts = [...]string{
	"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH",
	"UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN",
	"PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK", "TERMINATE", "LET", "FILTER", "FOR",
}

func hasValidStartKeyword(cypher string) bool {
	for _, start := range validSyntaxStarts {
		if startsWithKeywordFold(cypher, start) {
			return true
		}
	}
	return false
}

// ensureSyntaxValidationCache lazily installs the syntax-validation cache
// pointer using sync.Once so concurrent CALL { ... } subqueries (which fan
// out via executeCallTailParallel) cannot race on the pointer write. The
// underlying cache itself is already mutex-guarded; the race was on the
// initial pointer assignment.
func (e *StorageExecutor) ensureSyntaxValidationCache() *syntaxValidationCache {
	e.syntaxValidationOnce.Do(func() {
		if e.syntaxValidationCache == nil {
			e.syntaxValidationCache = &syntaxValidationCache{
				cache: make(map[string]struct{}, 1024),
				max:   4096,
			}
		}
	})
	return e.syntaxValidationCache
}

func (e *StorageExecutor) hasCachedValidSyntax(cypher string) bool {
	if cypher == "" {
		return false
	}
	c := e.ensureSyntaxValidationCache()
	c.mu.RLock()
	_, ok := c.cache[cypher]
	c.mu.RUnlock()
	return ok
}

func (e *StorageExecutor) markCachedValidSyntax(cypher string) {
	if cypher == "" {
		return
	}
	c := e.ensureSyntaxValidationCache()
	c.mu.Lock()
	if len(c.cache) >= c.max {
		for k := range c.cache {
			delete(c.cache, k)
			break
		}
	}
	c.cache[cypher] = struct{}{}
	c.mu.Unlock()
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
