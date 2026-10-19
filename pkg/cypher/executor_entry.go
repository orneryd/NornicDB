package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
)

// Execute parses and executes a Cypher query with optional parameters.
//
// This is the main entry point for Cypher query execution. The method handles
// the complete query lifecycle: parsing, validation, parameter substitution,
// execution planning, and result formatting.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - cypher: Cypher query string
//   - params: Optional parameters for $param substitution
//
// Returns:
//   - ExecuteResult with columns and rows
//   - Error if query parsing or execution fails
//
// Example:
//
//	// Simple query without parameters
//	result, err := executor.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Parameterized query
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx, `
//		MATCH (n:Person {name: $name})
//		WHERE n.age >= $minAge
//		RETURN n.name, n.age
//	`, params)
//
//	// Process results
//	// emit "Columns: %v" via the configured logger
//	for _, row := range result.Rows {
//		// process row (e.g. emit "Row: %v" via the configured logger)
//	}
//
// Supported Query Types:
//
//	Core Clauses:
//	- MATCH: Pattern matching and traversal
//	- OPTIONAL MATCH: Left outer joins (returns nulls for no matches)
//	- CREATE: Node and relationship creation
//	- MERGE: Upsert operations with ON CREATE SET / ON MATCH SET
//	- DELETE / DETACH DELETE: Node and relationship deletion
//	- SET: Property updates
//	- REMOVE: Property and label removal
//
//	Projection & Chaining:
//	- RETURN: Result projection with expressions, aliases, aggregations
//	- WITH: Query chaining and intermediate aggregation
//	- UNWIND: List expansion into rows
//
//	Filtering & Ordering:
//	- WHERE: Filtering conditions (=, <>, <, >, <=, >=, IS NULL, IS NOT NULL, IN, CONTAINS, STARTS WITH, ENDS WITH, AND, OR, NOT)
//	- ORDER BY: Result sorting (ASC/DESC)
//	- SKIP / LIMIT: Pagination
//
//	Aggregation Functions:
//	- COUNT, SUM, AVG, MIN, MAX, COLLECT
//
//	Procedures & Functions:
//	- CALL: Procedure invocation (db.labels, db.propertyKeys, db.index.vector.*, etc.)
//	- CALL {}: Subquery execution with UNION support
//
//	Advanced:
//	- UNION / UNION ALL: Query composition
//	- FOREACH: Iterative updates
//	- LOAD CSV: Data import
//	- EXPLAIN / PROFILE: Query analysis
//	- SHOW: Schema introspection
//
//	Path Functions:
//	- shortestPath / allShortestPaths
//
// Error Handling:
//
//	Returns detailed error messages for syntax errors, type mismatches,
//	and execution failures with Neo4j-compatible error codes.
func (e *StorageExecutor) Execute(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	return e.execute(ctx, cypher, params, nil, true)
}

// executeRouted continues the same semantic statement through a transparent
// dispatcher. Public Execute, including independent reentrant calls made by
// consumers, always starts a fresh outcome instead.
func (e *StorageExecutor) executeRouted(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	var outcome *statementOutcome
	if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok {
		outcome = statement.outcome
	}
	return e.execute(ctx, cypher, params, outcome, false)
}

func (e *StorageExecutor) execute(ctx context.Context, cypher string, params map[string]interface{}, outcome *statementOutcome, clientText bool) (result *ExecuteResult, retErr error) {
	// The statement's context (deadlineCtx, once the database's query limits
	// apply) ends at the first of its deadlines, and its cause names that
	// deadline's owner: a Bolt client's tx_timeout, the server's statement
	// timeout, the database's query time limit. A deadline error the
	// statement returns is that owner's transaction timeout (#1339); a
	// deadline no owner installed (a caller's own budget, a dependency's
	// timeout) keeps its error as it is.
	deadlineCtx := ctx
	defer func() {
		if retErr != nil {
			retErr = nornicerrors.MarkStatementTimeout(retErr, context.Cause(deadlineCtx))
		}
	}()
	// Every rejection of the caller's statement fails its explicit transaction,
	// including errors before parsing, routing or the system statement policy.
	if e.txContext != nil && e.txContext.active && ctx.Value(ctxKeyTxStorage) == nil {
		defer func() { e.failTransaction(ctx, retErr) }()
	}
	defer func() {
		if result != nil && retErr == nil && result.MapKeyOrders == nil {
			if orders := snapshotMapKeyOrders(ctx); len(orders) > 0 {
				result.MapKeyOrders = orders
			}
		}
	}()
	// A result stream the caller passed is for this statement only, not for
	// the statements it runs on its behalf (#939).
	stream, ctx := takeResultStream(ctx)
	e.resetHotPathTrace()
	// A result served from the result cache that returns no node or
	// relationship has no access to record (resultHasMaterializedEntities).
	recordAccess := true
	defer func() {
		if recordAccess && retErr == nil && result != nil {
			e.recordMaterializedResultAccess(result)
		}
	}()

	// TRC-15: top-level cypher execute span. Started before timing so the
	// span duration matches the metric observation window exactly. The span
	// is ended in the same defer that emits the slow-query log.
	ctx, execSpan := startExecuteSpan(ctx, "", cypher)

	// TRC-17: propagate span context to the storage layer so storage spans
	// nest as children of the cypher execute span.
	if te, ok := e.storage.(*storage.TracedEngine); ok {
		te.SetContext(ctx)
	}

	// D-04c slow-query log timing. Captured at the top so the threshold check
	// covers every Execute return path (early-out, fabric, normal). Pre-bind
	// the original query text — by the time the deferred emission fires,
	// `cypher` has been normalized; we want to log what the client submitted.
	slowStart := time.Now()
	statement := e.newStatementExecutionContext(ctx, slowStart.UTC(), outcome)
	embeddingState := statement.state
	var embeddingGeneration uint64
	if embeddingState != nil {
		embeddingGeneration = embeddingState.generation
	}
	ctx = statement
	// Logging is part of the statement's deadline. Release registry and query
	// limit contexts after its deferred emission, so normal completion does
	// not look like cancellation to the redactor.
	originalCypher := cypher
	collectionGeneration := e.queryStatistics.start(originalCypher, slowStart)
	var executionDuration time.Duration
	var running runningStatement
	var queryLimitCancel context.CancelFunc
	defer func() {
		statementErr := ctx.Err()
		terminated := running.done()
		if !statement.outcome.committed.Load() {
			if terminated {
				result, retErr = nil, transactionTerminatedError()
			} else if retErr == nil && statementErr != nil {
				result, retErr = nil, statementErr
			}
		}
		e.queryStatistics.record(collectionGeneration, originalCypher, slowStart, executionDuration, retErr == nil)
		e.observeSlowQueryIfThresholded(executionDuration)
		recordSpanError(execSpan, retErr)
		execSpan.End()
		if queryLimitCancel != nil {
			queryLimitCancel()
		}
	}()
	defer func() {
		executionDuration = time.Since(slowStart)
		dur := executionDuration
		e.emitRejectionReport(ctx, originalCypher, retErr)
		// Plan is unavailable for non-EXPLAIN/PROFILE queries; pass nil and
		// rely on PlanHash's zero-placeholder behavior. Phase 6 (TRC-04) will
		// thread the planned tree here once cypher EXPLAIN refactoring is in.
		e.emitSlowQueryLog(ctx, originalCypher, nil, dur)

	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Normalize query: trim BOM (some clients send it) then whitespace
	cypher = trimBOM(cypher)
	cypher = normalizeCypherSyntaxConfusables(cypher)
	// The statement as the client sent it may not call NornicDB's internal
	// functions; the rewrites below produce them (#1105).
	// Transparent routing continues already-admitted text, including generated
	// functions. A public reentrant call still admits its own client text.
	if clientText {
		if err := internalFunctionCallError(cypher); err != nil {
			return nil, err
		}
	}
	// Comments and keyword spacing are canonical from here on; what the
	// client sees (column names, messages, plans) is the text it sent (#740).
	// NORNICDB_CYPHER_QUERY_NORMALIZATION=false skips the pass: the
	// statement runs as sent.
	canonical, rewrite := cypher, (*queryRewrite)(nil)
	// Establish Fabric's grammar roles before shared normalization, framing
	// and extension rewrites can mistake a keyword-spelled name for a clause.
	fabricQuery := e.shouldUseFabricPlanner(cypher)
	if fabricQuery {
		cypher = fabric.LocalQuery(cypher)
	}
	if config.IsCypherQueryNormalizationEnabled() {
		canonical, rewrite = canonicalizeQueryText(cypher)
	}
	if rewrite != nil {
		cypher = canonical
		defer func() { result, retErr = rewrite.restore(result, retErr) }()
	}
	// Digit-grouping underscores (1_000) go here, on every route; columns
	// and messages keep the client's text (#907).
	if grouped, numericRewrite := canonicalizeNumericLiterals(cypher); numericRewrite != nil {
		cypher = grouped
		defer func() { result, retErr = numericRewrite.restore(result, retErr) }()
	}
	// GQL function aliases (collect_list, ln, …) call the function they
	// name; columns and messages keep the client's text.
	if !rewriteFree(rewriteFreeFunctionAliases, false, cypher) {
		if aliased, aliasRewrite := canonicalizeFunctionAliases(cypher); aliasRewrite != nil {
			cypher = aliased
			defer func() { result, retErr = aliasRewrite.restore(result, retErr) }()
		} else {
			markRewriteFree(rewriteFreeFunctionAliases, false, cypher)
		}
	}
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	ctx = withCypherVersion(ctx, cypher)
	if err := validateCypherPreamble(cypher); err != nil {
		return nil, err
	}
	if config.IsANTLRParser() {
		body, _ := stripCypherPreamble(cypher)
		if cypherGrammarVersion(cypher) != "25" && usesSharedGrammarClauses(body) {
			return nil, sharedClauseSyntaxError("LET, FILTER and FOR require CYPHER 25 with the ANTLR parser")
		}
	}
	// Neo4j 5 statement framing: leading CYPHER [version] [option=value …]
	// groups run the statement they precede, and a trailing FINISH (on every
	// UNION branch) runs it and returns no rows.
	cypher, _ = stripCypherPreamble(cypher)
	cypher = strings.TrimSpace(cypher)
	// Cypher 25 expression forms (RETURN ALL, s"…{x}…", map comprehensions)
	// become the expressions they stand for, once, here (#907).
	cypher25 := cypherVersionFromContext(ctx) == "25"
	// In Cypher 25 a '$' after a name starts a parameter (L$props is
	// L $props); Cypher 5 reads a$b as one name.
	if cypher25 {
		if separated, separateRewrite := separateCypher25Parameters(cypher); separateRewrite != nil {
			cypher = separated
			defer func() { result, retErr = separateRewrite.restore(result, retErr) }()
		}
	}
	if !rewriteFree(rewriteFreeCypher25Expressions, cypher25, cypher) {
		expanded, expressionRewrites, err := desugarCypher25Expressions(cypher, cypher25)
		if err != nil {
			return nil, err
		}
		if len(expressionRewrites) > 0 {
			cypher = expanded
			// One defer, not one per pass: a defer in a loop would make every
			// statement pay for heap-allocated defers. Restored last pass first.
			defer func() {
				for index := len(expressionRewrites) - 1; index >= 0; index-- {
					result, retErr = expressionRewrites[index].restore(withoutGeneratedColumns(result), retErr)
				}
			}()
		} else {
			markRewriteFree(rewriteFreeCypher25Expressions, cypher25, cypher)
		}
	}
	// Cypher 25 composition (NEXT, WHEN, braced query parts) becomes the
	// CALL subqueries and UNIONs it stands for, once, here; columns and
	// errors are mapped back (#907).
	if !rewriteFree(rewriteFreeQueryStructure, false, cypher) {
		if composed, structureRewrite, err := desugarQueryStructure(cypher, e); err != nil {
			return nil, err
		} else if structureRewrite != nil {
			cypher = composed
			defer func() { result, retErr = structureRewrite.restore(withoutGeneratedColumns(result), retErr) }()
		} else if !mayUseQueryStructure(cypher) {
			// Only the quick check's verdict is the text's alone.
			markRewriteFree(rewriteFreeQueryStructure, false, cypher)
		}
	}
	if err := e.validateStatementFraming(cypher); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishes(cypher); ok {
		cypher = strings.TrimSpace(stripped)
		finishTerminated = true
		ctx = withFinishedStatement(ctx, cypher)
	}
	if cypher == "" {
		if finishTerminated {
			return &ExecuteResult{}, nil
		}
		return nil, localizedError(localization.CypherCoreEmptyQuery(), nil)
	}
	// Knowledge-policy DDL that cannot be parsed (a missing profile or policy
	// name, a missing OPTIONS block) is a syntax error on every route, the
	// auto-commit and HTTP routes included (#514).
	if isKnowledgePolicyDDLStatement(cypher) {
		if _, _, err := ParseKnowledgePolicyDDL(cypher); err != nil {
			return nil, &classifiedCypherError{
				cause:  err,
				code:   "Neo.ClientError.Statement.SyntaxError",
				detail: "UnexpectedSyntax",
			}
		}
	}
	if finishTerminated {
		defer func() {
			if result != nil {
				result.Columns = nil
				result.Rows = nil
			}
		}()
	}

	// Typed Go maps and slices become Cypher maps and lists here, once, for
	// every route (#712).
	params = normalizeQueryParameters(params)

	// Handle Neo4j shell/browser commands like :USE and :param before validation.
	useBefore := GetUseDatabaseFromContext(ctx)
	processedQuery, processedCtx, shellResult, err := e.preprocessShellCommands(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	ctx = processedCtx
	cypher = processedQuery
	if cypher == "" {
		return shellResult, nil
	}
	if err := clientTransactionCommand(ctx, cypher); err != nil {
		return nil, err
	}
	// Only BEGIN can start a script; avoid its allocating classifier for
	// ordinary statements while admitting them through the same owner.
	if bareTransactionCommand(cypher) == "" && (!startsWithKeywordFold(cypher, "BEGIN") || !transactionScriptShape(cypher)) {
		// Neo4j parses a statement before it checks it against its
		// transaction, so a rejected USE clause (USE before an
		// administration command among them) is the SyntaxError in an
		// explicit transaction too.
		if startsWithKeywordFold(cypher, "USE") {
			if _, _, _, err := parseUseClause(cypher, false); err != nil {
				return nil, err
			}
		}
		if err := e.admitTransactionStatement(cypher); err != nil {
			return nil, err
		}
		routed, err := e.routeAdministrationStatement(ctx, cypher)
		if err != nil {
			return nil, err
		}
		if routed != e {
			return routed.executeRouted(ctx, cypher, params)
		}
	}
	// A cached per-database executor is shared by every auto-commit client of
	// its database. A bare transaction command must not open a transaction on
	// it: any later statement (or one-statement script) would then run inside
	// another caller's transaction, and COMMIT/ROLLBACK would end writes the
	// caller never issued. Neo4j clients cannot send Cypher transaction
	// statements either; embedded callers that want explicit transactions
	// create their own session executor.
	if e.sharedExecutor && (e.txContext == nil || !e.txContext.active) {
		if word := bareTransactionCommand(cypher); word != "" {
			message := localization.CypherTransactionsCommandNotStatement(word)
			return nil, localizedError(message, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", message.Fallback))
		}
	}
	// A query after :USE <db> runs on that database, like one after a USE
	// clause (#738).
	if useDB := GetUseDatabaseFromContext(ctx); useDB != "" && useDB != useBefore {
		return e.executeOnDatabase(ctx, useDB, cypher, params, false)
	}

	// The quoted-variable and label-expression rewrites map the finished
	// result's rows back, so a statement with either doesn't stream.
	rowsRewritten := false
	// Backtick-quoted variables become plain identifiers here, once, for
	// every route; the result's columns and errors are mapped back (#734).
	if canonical, names := canonicalizeQuotedVariables(cypher); names != nil {
		cypher = canonical
		params = names.parameterValues(params)
		ctx = withQuotedVariableNames(ctx, names)
		rowsRewritten = true
		defer func() { result, retErr = names.restore(result, retErr, e.parseReturnItems) }()
	}

	// ORDER BY, SKIP / OFFSET and LIMIT as clauses of their own become the
	// WITH * they stand for, once, here; errors are mapped back (#907).
	if desugared, orderRewrite := desugarStandaloneOrderClauses(cypher); orderRewrite != nil {
		cypher = desugared
		defer func() { result, retErr = orderRewrite.restore(result, retErr) }()
	}

	// Route multi-graph CALL { USE ... } queries through the Fabric planner/executor
	// so subquery decomposition and cross-graph routing use a single deterministic path.
	if fabricQuery {
		if err := e.statementParametersError(ctx, cypher, params); err != nil {
			return nil, err
		}
		mergedParams := e.mergeShellParams(ctx, mergeInheritedQueryParams(ctx, params))
		ctx = withQueryParams(ctx, mergedParams)
		mode, modeQuery := parseExecutionMode(cypher)
		if mode != ModeNormal {
			if err := e.validateSyntax(ctx, modeQuery, cypherVersionFromContext(ctx) == "25"); err != nil {
				return nil, err
			}
			if err := e.validateSemanticScopes(ctx, modeQuery); err != nil {
				return nil, err
			}
			if mode == ModeExplain {
				return e.executeExplain(ctx, modeQuery)
			}
			return e.executeProfile(ctx, modeQuery)
		}
		info := e.analyzer.Analyze(cypher)
		// Plan 04-03 Site 3 (fabric branch): isFabric=true → op_type="fabric"
		// per RISK-1 corrected classifier. Observation pre-execute so the
		// counter reflects intent regardless of execution outcome (errors
		// still bucket by intended op_type, matching how queries_total works
		// in the reference catalogs).
		e.observeQuery(classifyOpType(info, true /* isFabric */, false), true, slowStart)
		execSpan.SetAttributes(attribute.String("cypher.op_type", "fabric"))
		inExplicitTx := e.txContext != nil && e.txContext.active
		preparedFabric, err := e.prepareFabricExecution(ctx, cypher)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, fabricPreparedExecKey{}, preparedFabric)
		allowResultCache := !preparedFabric.hasRemote
		fabricResultCacheKey := ""

		// Mirror normal query-cache policy for Fabric reads (autocommit only).
		// A cached result is served only to a caller the statement is
		// authorized for (fabricResultCacheKey).
		if allowResultCache && e.canUseResultCache(ctx, info, cypher) {
			if key, cacheable := e.fabricResultCacheKey(ctx, preparedFabric, cypher, mergedParams); cacheable {
				fabricResultCacheKey = key
				if cached, _, _, found := e.cache.getWithTraceForEmbedding(fabricResultCacheKey, embeddingGeneration); found {
					return cached, nil
				}
			}
		}

		var result *ExecuteResult
		var execErr error
		// When an explicit transaction is active on a composite route, execute through
		// the same FabricTransaction so many-read/one-write constraints are enforced
		// across all statements in the session.
		if inExplicitTx {
			if ftx, ok := e.txContext.tx.(*fabric.FabricTransaction); ok {
				result, execErr = e.executeViaPreparedFabricWithTx(ctx, cypher, mergedParams, ftx, false, preparedFabric)
			} else {
				result, execErr = e.executeViaFabric(ctx, cypher, mergedParams)
			}
		} else {
			result, execErr = e.executeViaFabric(ctx, cypher, mergedParams)
		}
		if execErr != nil {
			return nil, execErr
		}

		if fabricResultCacheKey != "" {
			e.cache.putWithLabelsAndTraceForEmbedding(fabricResultCacheKey, result, e.queryCacheTTL, extractLabelsFromQuery(cypher), HotPathTrace{}, embeddingGeneration)
		}

		if info.IsWriteQuery && e.cache != nil {
			if len(info.Labels) > 0 {
				e.cache.InvalidateLabels(info.Labels)
			} else {
				e.cache.Invalidate()
			}
		}

		return result, nil
	}

	// EXPLAIN / PROFILE USE <graph> …: the graph is resolved as for the
	// statement itself (Neo4j's DatabaseNotFound for a missing one, #744),
	// and the plan is the target database's.
	if mode, inner := parseExecutionMode(cypher); mode != ModeNormal {
		if use, remaining, hasUse, err := parseUseClause(inner, false); hasUse || err != nil {
			if err != nil {
				return nil, err
			}
			if err := e.dynamicUseError(use); err != nil {
				return nil, err
			}
			keyword := "EXPLAIN "
			if mode == ModeProfile {
				keyword = "PROFILE "
			}
			return e.executeOnDatabase(ctx, use.Name, keyword+remaining, params, false)
		}
	}
	// Handle leading Cypher USE clause (openCypher multi-graph syntax).
	if use, remaining, hasUse, err := parseUseClause(cypher, false); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		if err := e.dynamicUseError(use); err != nil {
			return nil, err
		}
		return e.executeOnDatabase(ctx, use.Name, remaining, params, false)
	}
	// Label expressions in patterns become the label forms and WHERE
	// predicates the local evaluator reads. Route source Cypher first: remote
	// evaluators do not implement our internal functions. The result's columns and
	// errors are mapped back (#860).
	desugared, labelRewrite, err := desugarLabelExpressions(cypher, params, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil, err
	}
	if labelRewrite != nil {
		rowsRewritten = true
		cypher = desugared
		defer func() { result, retErr = labelRewrite.restore(withoutGeneratedColumns(result), retErr) }()
	}
	if err := e.authorizeSelectedDatabase(ctx, "", cypher); err != nil {
		return nil, err
	}
	e.statementAdmitted(ctx)
	if err := e.systemDatabaseWriteError(cypher); err != nil {
		return nil, err
	}

	// Reject data queries on composite root — callers must USE a constituent.
	// System/admin commands (SHOW DATABASES, CREATE/DROP DATABASE, ALTER, SHOW COMPOSITE,
	// SHOW CONSTITUENTS, SHOW ALIASES, SHOW LIMITS, BEGIN, COMMIT, ROLLBACK) are allowed,
	// and so is a statement that reads or writes no graph (RETURN 1,
	// UNWIND graph.names() AS g RETURN g), which runs on the composite
	// itself, as in Neo4j.
	if isCompositeRoot(e.storage) && !isCompositeAllowedCommand(cypher) && statementAccessesGraphText(cypher) {
		return nil, localizedError(localization.CypherCoreCompositeTargetRequired(), nil)
	}

	// Merge session-scoped shell parameters with per-call parameters.
	// Explicit params win over shell params to preserve HTTP/Bolt semantics.
	params = mergeInheritedQueryParams(ctx, params)
	params = e.mergeShellParams(ctx, params)

	// Check for transaction control statements and transaction scripts FIRST.
	// These are Nornic extensions and must bypass strict ANTLR validation.
	// A one-statement script opens and closes its own transaction; it runs on
	// a private executor so a client statement can never leave the shared
	// per-database executor inside a transaction — concurrent auto-commit
	// statements would otherwise observe and collide with the script's
	// transaction state (acknowledged-write loss, mid-run panics). An
	// embedded caller that already owns an active transaction keeps running
	// the script on itself, where handleBegin fails as before.
	if transactionScriptShape(cypher) {
		if e.txContext != nil && e.txContext.active {
			if result, err := e.executeTransactionScript(ctx, cypher); result != nil || err != nil {
				return result, err
			}
		} else {
			scriptExec := e.cloneForStorage(e.storage)
			scriptExec.txContext = nil
			if result, err := scriptExec.executeTransactionScript(ctx, cypher); result != nil || err != nil {
				return result, err
			}
		}
	} else if result, err := e.executeTransactionScript(ctx, cypher); result != nil || err != nil {
		return result, err
	}
	if result, err := e.parseTransactionStatement(ctx, cypher); result != nil || err != nil {
		if err == nil && e.txContext != nil && e.txContext.active {
			if tx, ok := e.txContext.tx.(*fabric.FabricTransaction); ok {
				tx.BindContext(ctx)
			}
			// BEGIN: the client's transaction metadata (WithTransactionMetadata).
			if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok {
				if metadataErr := applyTransactionMetadata(ctx, tx); metadataErr != nil {
					_, _ = e.handleRollback()
					return nil, metadataErr
				}
			}
		}
		if err == nil && e.txContext != nil && e.txContext.active && e.txContext.running == nil {
			e.txContext.running = runningTransactions.begin(ctx, e.currentDatabaseName())
		}
		return result, err
	}
	// A statement that fails in an explicit transaction marks it failed, as in
	// Neo4j (#683): later statements are refused, COMMIT rolls it back and
	// ROLLBACK discards everything it wrote. Only the caller's own statement
	// counts: executions nested inside a statement (they carry the
	// transaction's storage wrapper) handle their own errors.
	if tx := e.txContext; tx != nil && tx.active && ctx.Value(ctxKeyTxStorage) == nil {
		if tx.failed != nil {
			return nil, queryOnFailedTransactionError(tx.failed)
		}
	}
	// SHOW TRANSACTIONS lists the statement while it runs; TERMINATE
	// TRANSACTIONS cancels it (#718). A statement of an explicit
	// transaction is registered here, so a terminated transaction refuses
	// it before anything else; an auto-commit statement after the result
	// cache, since a statement served from the cache runs nothing that
	// could be listed or terminated.
	// A statement whose transaction TERMINATE TRANSACTIONS terminated while
	// it ran fails with Neo4j's Neo.ClientError.Transaction.Terminated,
	// whichever layer noticed the cancellation (a storage call, the
	// checked write path, the row loop) and whether it read or wrote;
	// only an auto-commit statement that had already committed keeps its
	// result (#751).

	registerStatement := func() error {
		statementCtx, statement, err := e.withRunningStatement(ctx, originalCypher)
		if err != nil {
			return err
		}
		running, ctx = statement, statementCtx
		return nil
	}
	if e.txContext != nil && e.txContext.active {
		if runErr := registerStatement(); runErr != nil {
			_, _ = e.handleRollback()
			return nil, runErr
		}
	}

	// Validate basic syntax
	if err := e.validateSyntax(ctx, cypher, cypherVersionFromContext(ctx) == "25"); err != nil {
		// Plan 04-03 Site 2 (parse-error chokepoint): emit op_type="parse_error"
		// per D-04b sixth enum value. No duration observation — parse cost is
		// sub-microsecond and not meaningful to bucket. The queries_total
		// counter still increments so the SRE can alert on parse-error rate.
		e.observeQuery("parse_error", false /* observeDuration */, slowStart)
		execSpan.SetAttributes(attribute.String("cypher.op_type", "parse_error"))
		return nil, err
	}
	// A statement whose last clause is a reading clause (UNWIND, MATCH,
	// OPTIONAL MATCH, WITH) has nothing after it — Neo4j rejects it. This is
	// a whole-statement rule that runs after syntax validation, so malformed
	// text keeps the parser's own syntax error; fabric fragments legitimately
	// end in UNWIND when the surrounding statement continues in another
	// fragment, and a FINISH terminator is itself the clause that follows, so
	// both are exempt.
	if !finishTerminated {
		last := lastTopLevelClauseWord(cypher)
		// `WITH EMBEDDING` is a NornicDB execution option, not a WITH
		// projection: a statement that ends with it is complete. Only exempt
		// it when the text before the trailer is itself a complete statement.
		if last == "WITH" {
			if scope, ok := stripWithEmbeddingSuffix(cypher); ok {
				switch lastTopLevelClauseWord(scope) {
				case "", "UNWIND", "MATCH", "OPTIONAL", "WITH", "CALL", "LET", "FILTER", "FOR":
				default:
					last = ""
				}
			}
		}
		switch last {
		case "UNWIND", "MATCH", "OPTIONAL", "WITH", "LET", "FILTER", "FOR":
			return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				fmt.Sprintf("Invalid input: %s must be followed by a clause", last))
		}
		// A bare CALL has no procedure or subquery after it; the statement is
		// incomplete, not a lookup of a procedure named 'CALL' (#514).
		if strings.EqualFold(cypher, "CALL") {
			return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				"Invalid input: CALL must be followed by a procedure name or subquery")
		}
	}
	// WITH EMBEDDING is an execution option, not a WITH projection: the
	// scopes are those of the statement without it.
	scopeText, _ := stripWithEmbeddingSuffix(cypher)
	if err := e.validateSemanticScopes(ctx, scopeText); err != nil {
		return nil, err
	}
	if err := e.statementParametersError(ctx, cypher, params); err != nil {
		return nil, err
	}

	// IMPORTANT: Do NOT substitute parameters before routing!
	// We need to route the query based on the ORIGINAL query structure,
	// not the substituted one. Otherwise, keywords inside parameter values
	// (like 'MATCH (n) SET n.x = 1' stored as content) will be incorrectly
	// detected as Cypher clauses.
	//
	// Parameter substitution happens AFTER routing, inside each handler.
	// This matches Neo4j's architecture where params are kept separate.

	// Store params in context for handlers to use
	ctx = withQueryParams(ctx, params)
	if err := e.validateBoundParameterExpressions(ctx, cypher, params); err != nil {
		return nil, err
	}

	// Check query limits if storage engine supports it
	// Uses interface{} to avoid importing multidb package (prevents circular dependencies)
	if namespacedEngine, ok := e.storage.(interface {
		GetQueryLimitChecker() interface {
			CheckQueryRate() error
			CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
			GetQueryLimits() interface{}
		}
	}); ok {
		if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
			// Check query rate limit
			if err := qlc.CheckQueryRate(); err != nil {
				return nil, err
			}

			// Check write rate limit for write queries
			// We need to check this early, but we don't know if it's a write query yet
			// So we'll check it in the write handlers too

			// Apply query timeout and concurrent query limits
			var err error
			ctx, queryLimitCancel, err = qlc.CheckQueryLimits(ctx)
			if err != nil {
				return nil, err
			}
			deadlineCtx = ctx
		}
	}

	// TRC-15: plan span wraps the analysis/classification phase.
	_, planSpan := startPlanSpan(ctx)
	// Analyze query - uses cached analysis if available
	// This extracts query metadata (HasMatch, IsReadOnly, Labels, etc.) once
	// and caches it for repeated queries, avoiding redundant string parsing
	info := e.analyzer.Analyze(cypher)

	// Plan 04-03 Sites 1 + 3 (RISK-1 corrected): classify ONCE post-Analyze.
	// isFabric=false here because the fabric branch returns at line ~963
	// before reaching this code path. isAdmin=true overrides the QueryInfo-
	// derived classification (e.g., SHOW DATABASES would otherwise classify
	// as "schema" via HasShow → IsSchemaQuery; D-04a says system/admin
	// commands bucket as "admin" instead). Single observation point covers
	// cache-hit early-return AND every downstream execution path so the
	// counter reflects intent regardless of how the query resolves.
	opType := classifyOpType(info, false /* isFabric */, isSystemCommandNoGraph(cypher))
	e.observeQuery(opType, true /* observeDuration */, slowStart)
	planSpan.SetAttributes(attribute.String("cypher.op_type", opType))
	planSpan.End()
	// Update the execute span with the resolved op_type.
	execSpan.SetAttributes(attribute.String("cypher.op_type", opType))

	// For routing, we still need upperQuery for some handlers
	// TODO: Migrate handlers to use QueryInfo directly
	upperQuery := e.cachedUpperQuery(cypher)

	// Capture the storage and search-index revisions before execution so
	// mutations performed outside this executor cannot leave a stale cached
	// result behind. The search index is updated after the graph (a node's
	// vector is indexed after its embedding is saved), so a search procedure
	// that ran in between would otherwise keep its result until the TTL
	// (#945). The key carries a language version other than the default 5:
	// the same text can mean different things in Cypher 5 and Cypher 25
	// (#907); a Cypher 5 key pays for no version text.
	resultCacheKey := ""
	if e.canUseResultCache(ctx, info, cypher) {
		resultCacheKey = resultCacheEntryKey(cypher, params)
		if version := cypherVersionFromContext(ctx); version != "5" {
			resultCacheKey += ":cypher:" + version
		}
		if provider, ok := e.storage.(storage.GraphMutationVersionProvider); ok {
			if version, supported := provider.GraphMutationVersion(); supported {
				resultCacheKey += ":graph:" + strconv.FormatUint(version, 10)
			}
		}
		if svc := e.searchService; svc != nil {
			resultCacheKey += ":search:" + strconv.FormatUint(svc.IndexRevision(), 10)
		}
		if cached, trace, entities, found := e.cache.getWithTraceForEmbedding(resultCacheKey, embeddingGeneration); found {
			e.restoreHotPathTrace(trace)
			recordAccess = entities
			return cached, nil
		}
	}
	if running.tx == nil {
		// Registering an auto-commit statement can't fail: only a
		// terminated explicit transaction refuses a statement, and a
		// statement of one is registered above.
		_ = registerStatement()
	}

	// Check for EXPLAIN/PROFILE execution modes (using cached analysis)
	if info.HasExplain {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeExplain(ctx, innerQuery)
	}
	if info.HasProfile {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeProfile(ctx, innerQuery)
	}

	// If in explicit transaction, execute within it
	if e.txContext != nil && e.txContext.active {
		ctx = withExpressionFailureSlot(ctx)
		result, err := e.executeInTransaction(ctx, cypher, upperQuery)
		if failure := getExpressionFailure(ctx); failure != nil {
			return result, failure
		}
		return result, err
	}

	// System commands (CREATE/DROP DATABASE, SHOW DATABASES, etc.) must not use
	// implicit transactions: they operate on dbManager/metadata, not graph
	// storage. Routing them through executeWithoutTransaction directly ensures
	// correct handling and avoids the write path.
	if isSystemCommandNoGraph(cypher) || isOrdinarySchemaDDL(cypher) {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		// This owner executes metadata/schema effects directly, with no
		// implicit transaction to publish their completed outcome.
		if !info.IsReadOnly && !e.inTransaction(ctx) {
			publishStatementCommit(ctx)
		}
		return result, nil
	}

	// An auto-commit read hands its rows to the caller's result stream as it
	// produces them (#939). The result-limit truncation and the restores of
	// rewritten rows below need the whole result, so a statement with
	// either keeps it. A streamed result is not cached.
	if stream != nil && info.IsReadOnly && !rowsRewritten && !finishTerminated && e.maxResults() == 0 {
		ctx = armResultStream(ctx, stream, cypher, func(columns []string) []string {
			restored, _ := rewrite.restore(&ExecuteResult{Columns: columns}, nil)
			return restored.Columns
		}, func(rows [][]interface{}) {
			e.recordMaterializedResultAccess(&ExecuteResult{Rows: rows})
		})
	}

	// Auto-commit single query through the implicit-transaction write path.
	ctx = withExpressionFailureSlot(ctx)
	result, err = e.executeImplicitAsync(ctx, cypher, upperQuery)
	if result != nil && err == nil {
		if orders := snapshotMapKeyOrders(ctx); len(orders) > 0 {
			result.MapKeyOrders = orders
		}
	}
	// An expression error recorded while the statement ran is its error,
	// whichever route ran it: no route's result stands in for it.
	if err == nil {
		if failure := getExpressionFailure(ctx); failure != nil {
			return nil, failure
		}
	}

	// Apply result limit if set
	if err == nil && result != nil {
		if maxResults := e.maxResults(); maxResults > 0 && int64(len(result.Rows)) > maxResults {
			result.Rows = result.Rows[:maxResults]
		}
	}

	// Retain the revision captured before execution. A read overlapping a
	// mutation must not publish its stale result under the newer revision.
	if err == nil && resultCacheKey != "" && !stream.streamed() {
		e.cache.putWithLabelsAndTraceForEmbedding(resultCacheKey, result, e.queryCacheTTL, extractLabelsFromQuery(cypher), e.LastHotPathTrace(), embeddingGeneration)
	}

	// Invalidate caches on write operations (using cached analysis)
	if info.IsWriteQuery {
		// Only invalidate node lookup cache when NODES are deleted
		// Relationship-only deletes (like benchmark CREATE rel DELETE rel) don't affect node cache
		if info.HasDelete && queryDeletesNodes(cypher) {
			e.invalidateNodeLookupCache()
		}

		// Invalidate query result cache using cached labels
		if e.cache != nil {
			if len(info.Labels) > 0 {
				e.cache.InvalidateLabels(info.Labels)
			} else {
				e.cache.Invalidate()
			}
		}
	}

	return result, err
}

// maxResults is the database's result-row limit (0 for none): the result of
// a statement is cut to it.
func (e *StorageExecutor) maxResults() int64 {
	namespacedEngine, ok := e.storage.(interface {
		GetQueryLimitChecker() interface {
			CheckQueryRate() error
			CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
			GetQueryLimits() interface{}
		}
	})
	if !ok {
		return 0
	}
	qlc := namespacedEngine.GetQueryLimitChecker()
	if qlc == nil {
		return 0
	}
	queryLimits := qlc.GetQueryLimits()
	if queryLimits == nil {
		return 0
	}
	limits, ok := queryLimits.(interface{ GetMaxResults() int64 })
	if !ok {
		return 0
	}
	return limits.GetMaxResults()
}
