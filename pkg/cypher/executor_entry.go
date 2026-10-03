package cypher

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
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
	defer func() {
		if result != nil && retErr == nil && result.MapKeyOrders == nil {
			if orders := snapshotMapKeyOrders(ctx); len(orders) > 0 {
				result.MapKeyOrders = orders
			}
		}
	}()
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
	ctx = context.WithValue(ctx, temporalStatementTimeKey{}, slowStart.UTC())
	originalCypher := cypher
	collectionGeneration := e.queryStatistics.start(originalCypher, slowStart)
	defer func() {
		dur := time.Since(slowStart)
		e.queryStatistics.record(collectionGeneration, originalCypher, slowStart, dur, retErr == nil)
		e.emitRejectionReport(originalCypher, retErr)
		// Plan is unavailable for non-EXPLAIN/PROFILE queries; pass nil and
		// rely on PlanHash's zero-placeholder behavior. Phase 6 (TRC-04) will
		// thread the planned tree here once cypher EXPLAIN refactoring is in.
		e.emitSlowQueryLog(originalCypher, nil, dur)
		// Plan 04-03 / MET-08 slow_queries_total: increments only when
		// duration meets the configured threshold (matches the D-04c
		// emitSlowQueryLog gate semantics — single threshold, single
		// emission point per Execute return).
		e.observeSlowQueryIfThresholded(dur)
		recordSpanError(execSpan, retErr)
		execSpan.End()
	}()
	if err := ctx.Err(); err != nil {
		if ctx.Value(ctxKeyTxStorage) == nil {
			e.failTransaction(err)
		}
		return nil, err
	}
	// Normalize query: trim BOM (some clients send it) then whitespace
	cypher = trimBOM(cypher)
	cypher = normalizeCypherSyntaxConfusables(cypher)
	// Comments and keyword spacing are canonical from here on; what the
	// client sees (column names, messages, plans) is the text it sent (#740).
	// NORNICDB_CYPHER_QUERY_NORMALIZATION=false skips the pass: the
	// statement runs as sent.
	canonical, rewrite := cypher, (*queryRewrite)(nil)
	if config.IsCypherQueryNormalizationEnabled() {
		canonical, rewrite = canonicalizeQueryText(cypher)
	}
	if rewrite != nil {
		cypher = canonical
		defer func() { result, retErr = rewrite.restore(result, retErr) }()
	}
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	if err := validateCypherPreamble(cypher); err != nil {
		return nil, err
	}
	// Neo4j 5 statement framing: leading CYPHER [version] [option=value …]
	// groups run the statement they precede, and a trailing FINISH (on every
	// UNION branch) runs it and returns no rows.
	cypher, _ = stripCypherPreamble(cypher)
	cypher = strings.TrimSpace(cypher)
	if err := e.validateStatementFraming(cypher); err != nil {
		return nil, err
	}
	finishTerminated := false
	if stripped, ok := stripUnionBranchFinishes(cypher); ok {
		cypher = strings.TrimSpace(stripped)
		finishTerminated = true
	}
	if cypher == "" {
		if finishTerminated {
			return &ExecuteResult{}, nil
		}
		return nil, localizedError(localization.CypherCoreEmptyQuery(), nil)
	}
	// A statement whose last clause is UNWIND has nothing after it — Neo4j
	// rejects it. This is a whole-statement rule, not part of
	// validateSyntaxNornic: fabric fragments legitimately end in UNWIND when
	// the surrounding statement continues in another fragment. A FINISH
	// terminator is itself the clause that follows UNWIND, so a
	// FINISH-terminated statement is exempt.
	if !finishTerminated && lastTopLevelClauseWord(cypher) == "UNWIND" {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid input: UNWIND must be followed by a clause")
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
		return e.executeOnDatabase(ctx, useDB, cypher, params)
	}

	// Backtick-quoted variables become plain identifiers here, once, for
	// every route; the result's columns and errors are mapped back (#734).
	if canonical, names := canonicalizeQuotedVariables(cypher); names != nil {
		cypher = canonical
		params = names.parameterValues(params)
		ctx = withQuotedVariableNames(ctx, names)
		defer func() { result, retErr = names.restore(result, retErr, e.parseReturnItems) }()
	}

	// Route multi-graph CALL { USE ... } queries through the Fabric planner/executor
	// so subquery decomposition and cross-graph routing use a single deterministic path.
	if e.shouldUseFabricPlanner(cypher) {
		if err := e.statementParametersError(ctx, cypher, params); err != nil {
			return nil, err
		}
		mergedParams := e.mergeShellParams(ctx, params)
		ctx = context.WithValue(ctx, paramsKey, mergedParams)
		mode, modeQuery := parseExecutionMode(cypher)
		if mode != ModeNormal {
			if err := e.validateSyntax(modeQuery); err != nil {
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
		if allowResultCache && !inExplicitTx && info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) && !profileExecutionBypassesCache(ctx) {
			if key, cacheable := e.fabricResultCacheKey(ctx, preparedFabric, cypher, mergedParams); cacheable {
				fabricResultCacheKey = key
				if cached, found := e.cache.get(fabricResultCacheKey); found {
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
			e.cache.putWithLabels(fabricResultCacheKey, result, e.queryCacheTTL, extractLabelsFromQuery(cypher))
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

	// Handle leading Cypher USE clause (openCypher multi-graph syntax).
	if use, remaining, hasUse, err := parseUseClause(cypher, false); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		if err := e.dynamicUseError(use); err != nil {
			return nil, err
		}
		return e.executeOnDatabase(ctx, use.Name, remaining, params)
	}
	if err := AuthorizeQuery(ctx, cypher); err != nil {
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
	if result, err := e.parseTransactionStatement(cypher); result != nil || err != nil {
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
		defer func() {
			e.failTransaction(retErr)
		}()
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
	var running runningStatement
	defer func() {
		if running.tx != nil && running.tx.terminated.Load() && !running.tx.committed.Load() {
			result, retErr = nil, transactionTerminatedError()
		}
		running.done()
	}()
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
	if err := e.validateSyntax(cypher); err != nil {
		// Plan 04-03 Site 2 (parse-error chokepoint): emit op_type="parse_error"
		// per D-04b sixth enum value. No duration observation — parse cost is
		// sub-microsecond and not meaningful to bucket. The queries_total
		// counter still increments so the SRE can alert on parse-error rate.
		e.observeQuery("parse_error", false /* observeDuration */, slowStart)
		execSpan.SetAttributes(attribute.String("cypher.op_type", "parse_error"))
		return nil, err
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
	ctx = context.WithValue(ctx, paramsKey, params)
	if err := e.validateBoundParameterExpressions(ctx, cypher, params); err != nil {
		return nil, err
	}

	// Check query limits if storage engine supports it
	// Uses interface{} to avoid importing multidb package (prevents circular dependencies)
	var queryLimitCancel context.CancelFunc
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
			// Ensure cancel is called when done
			defer func() {
				if queryLimitCancel != nil {
					queryLimitCancel()
				}
			}()
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

	// Capture the storage revision before execution so mutations performed
	// outside this executor cannot leave a stale cached result behind.
	resultCacheKey := ""
	if info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) && !profileExecutionBypassesCache(ctx) {
		resultCacheKey = resultCacheEntryKey(cypher, params)
		if provider, ok := e.storage.(storage.GraphMutationVersionProvider); ok {
			if version, supported := provider.GraphMutationVersion(); supported {
				resultCacheKey += ":graph:" + strconv.FormatUint(version, 10)
			}
		}
		if cached, trace, entities, found := e.cache.getWithTrace(resultCacheKey); found {
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

	// System commands (CREATE/DROP DATABASE, SHOW DATABASES, etc.) must not use the async engine
	// or implicit transactions: they operate on dbManager/metadata, not graph storage.
	// Routing them through executeWithoutTransaction directly ensures correct handling and
	// avoids the write path (tryAsyncCreateNodeBatch / executeWithImplicitTransaction).
	if isSystemCommandNoGraph(cypher) {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Auto-commit single query - use async path for performance
	// This uses AsyncEngine's write-behind cache instead of synchronous disk I/O
	// For strict ACID, users should use explicit BEGIN/COMMIT transactions
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
		if namespacedEngine, ok := e.storage.(interface {
			GetQueryLimitChecker() interface {
				CheckQueryRate() error
				CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
				GetQueryLimits() interface{}
			}
		}); ok {
			if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
				if queryLimits := qlc.GetQueryLimits(); queryLimits != nil {
					// Type assert to check if it has MaxResults field
					// We use reflection-like approach: check if it's a struct with MaxResults
					if limits, ok := queryLimits.(interface {
						GetMaxResults() int64
					}); ok {
						if maxResults := limits.GetMaxResults(); maxResults > 0 && int64(len(result.Rows)) > maxResults {
							// Truncate results to limit
							result.Rows = result.Rows[:maxResults]
						}
					}
				}
			}
		}
	}

	// Retain the revision captured before execution. A read overlapping a
	// mutation must not publish its stale result under the newer revision.
	if err == nil && resultCacheKey != "" {
		e.cache.putWithLabelsAndTrace(resultCacheKey, result, e.queryCacheTTL, extractLabelsFromQuery(cypher), e.LastHotPathTrace())
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
