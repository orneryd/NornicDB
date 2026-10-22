package cypher

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// parseLeadingUseClause extracts a leading `USE <database>` clause from a
// top-level query: the graph's name (empty for a dynamic reference), the
// remaining query, and whether a USE clause was found. See parseUseClause.
func parseLeadingUseClause(cypher string) (database, remaining string, hasUse bool, err error) {
	clause, remaining, hasUse, err := parseUseClause(cypher, false)
	return clause.Name, remaining, hasUse, err
}

// parseUseClause reads a leading USE clause with the one USE grammar
// (fabric.ParseUseClause, Neo4j 5.26's) and reports a rejected clause as
// Neo4j's SyntaxError. inSubquery is true for a CALL { } body.
func parseUseClause(cypher string, inSubquery bool) (fabric.UseClause, string, bool, error) {
	clause, remaining, hasUse, err := fabric.ParseUseClause(cypher, inSubquery)
	if err == nil {
		return clause, remaining, hasUse, nil
	}
	var syntaxErr *fabric.UseSyntaxError
	if errors.As(err, &syntaxErr) {
		err = localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", syntaxErr.Message)
	}
	return clause, remaining, hasUse, err
}

// dynamicUseError is Neo4j's SyntaxError for a dynamic graph reference
// (graph.byName(…), graph.byElementId(…)) outside a composite database:
// only a composite database's queries look graphs up dynamically.
func (e *StorageExecutor) dynamicUseError(clause fabric.UseClause) error {
	if !clause.IsDynamic() || e.sessionIsComposite() {
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCommandRoutingUseDynamicLookupNotAllowed(clause.Text()))
}

// CompositeGraphs lists the graphs of the composite database this executor
// runs on, as qualified names (composite.alias, sorted), for graph.names();
// composite is false for any other database. The executor is the function
// contexts' GraphCatalog (cypherfn.Context.Graphs).
func (e *StorageExecutor) CompositeGraphs() (graphs []string, composite bool) {
	if !e.sessionIsComposite() {
		return nil, false
	}
	name := e.currentDatabaseName()
	if e.dbManager != nil {
		if constituents, err := e.dbManager.GetCompositeConstituents(name); err == nil {
			for _, raw := range constituents {
				if ref, ok := toConstituentRef(raw); ok && strings.TrimSpace(ref.Alias) != "" {
					graphs = append(graphs, name+"."+ref.Alias)
				}
			}
		}
	}
	sort.Strings(graphs)
	return graphs, true
}

// sessionIsComposite reports whether this executor's database is a
// composite database.
func (e *StorageExecutor) sessionIsComposite() bool {
	return isCompositeRoot(e.storage)
}

func (e *StorageExecutor) cloneForStorage(store storage.Engine) *StorageExecutor {
	cloned := NewStorageExecutor(store)
	cloned.deferFlush = e.deferFlush
	cloned.ShareEmbedderFrom(e)
	cloned.sharedExecutor = e.sharedExecutor
	// Do not propagate the parent's search service to composite engines —
	// composite search must come from constituent-scoped executors, not
	// from a parent-namespace-scoped service.
	if !isCompositeRoot(store) {
		cloned.searchService = e.searchService
	}
	cloned.inferenceManager = e.inferenceManager
	cloned.onNodeMutated = e.onNodeMutated
	cloned.inlineEmbeddingTextOptions = e.inlineEmbeddingTextOptions
	cloned.inlineEmbeddingChunkSize = e.inlineEmbeddingChunkSize
	cloned.inlineEmbeddingChunkOverlap = e.inlineEmbeddingChunkOverlap
	cloned.allowLocalAPOCImportFileAccess = e.allowLocalAPOCImportFileAccess
	cloned.allowLocalAPOCExportFileAccess = e.allowLocalAPOCExportFileAccess
	cloned.allowRemoteAPOCURLAccess = e.allowRemoteAPOCURLAccess
	cloned.apocRemoteURLAllowlist = append([]string(nil), e.apocRemoteURLAllowlist...)
	cloned.apocRemoteHTTPClient = e.apocRemoteHTTPClient
	cloned.apocRemoteHostResolver = e.apocRemoteHostResolver
	cloned.apocLocalFileAccessRoot = e.apocLocalFileAccessRoot
	cloned.defaultEmbeddingDimensions = e.defaultEmbeddingDimensions
	cloned.dbManager = e.dbManager
	cloned.vectorRegistry = e.vectorRegistry
	cloned.vectorIndexSpaces = e.vectorIndexSpaces
	cloned.txContext = e.txContext
	cloned.fabricPlanCache = e.fabricPlanCache
	cloned.hotPathTraceState = e.hotPathTraceState

	e.shellParamsMu.RLock()
	if len(e.shellParams) > 0 {
		cloned.shellParams = make(map[string]interface{}, len(e.shellParams))
		for k, v := range e.shellParams {
			cloned.shellParams[k] = v
		}
	}
	e.shellParamsMu.RUnlock()
	cloned.shellParamsByToken = cloneShellParamsByToken(e)

	return cloned
}

func (e *StorageExecutor) scopedExecutorForUse(db string, authToken string) (*StorageExecutor, string, error) {
	targetDB := strings.TrimSpace(db)
	if targetDB == "" {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseRequired(), nil)
	}

	if e.dbManager != nil {
		// Handle dotted composite.constituent references (e.g. "nornic.tr").
		// Split at first dot: composite name + constituent alias.
		if dotIdx := strings.IndexByte(targetDB, '.'); dotIdx > 0 {
			compositeName := targetDB[:dotIdx]
			if e.dbManager.IsCompositeDatabase(compositeName) {
				currentDB := strings.TrimSpace(e.currentDatabaseName())
				if currentDB != "" && e.dbManager.IsCompositeDatabase(currentDB) && !strings.EqualFold(currentDB, compositeName) {
					return nil, "", localizedError(localization.CypherCommandRoutingUseConstituentOutsideComposite(targetDB, compositeName, currentDB), nil)
				}
				// Resolve the full composite.constituent via GetStorageForUse.
				// The composite engine's getConstituent will resolve the alias.
				return e.resolveCompositeConstituent(targetDB, compositeName, targetDB[dotIdx+1:], authToken)
			}
		}

		// Check if the target is itself a composite database.
		if e.dbManager.IsCompositeDatabase(targetDB) {
			return e.resolveCompositeStorage(targetDB, authToken)
		}

		// Standard database: resolve alias.
		resolved, err := e.dbManager.ResolveDatabase(targetDB)
		if err != nil {
			return nil, "", localizedStatusError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", localization.CypherCommandRoutingGraphNotFound(targetDB))
		}
		targetDB = resolved
	}

	// The executor's own database runs here.
	if strings.EqualFold(e.currentDatabaseName(), targetDB) {
		return e, targetDB, nil
	}
	if e.dbManager != nil {
		engine, err := e.dbManager.GetStorageForUse(targetDB, authToken)
		if err != nil {
			return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(targetDB, err), err)
		}
		store, ok := engine.(storage.Engine)
		if !ok {
			return nil, "", localizedError(localization.CypherCommandRoutingUseStorageTypeInvalid(targetDB), nil)
		}
		return e.cloneForStorage(store), targetDB, nil
	}

	// Without a database manager (embedded), a namespaced store switches
	// namespace on its inner engine.
	ns, ok := e.storage.(*storage.NamespacedEngine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseBackendUnsupported(targetDB), nil)
	}
	return e.cloneForStorage(storage.NewNamespacedEngine(ns.GetInnerEngine(), targetDB)), targetDB, nil
}

// TransactionDatabases resolves database names for TransactionDatabaseError:
// aliases, and whether a name is a composite database.
type TransactionDatabases interface {
	ResolveDatabase(name string) (string, error)
	IsCompositeDatabase(name string) bool
}

// TransactionDatabaseError is the one check that a transaction stays on one
// database: the error for a statement of a transaction on txDB that targets
// database target (with USE or :USE), or nil when target is txDB (by name or
// alias, through databases) or a constituent of composite txDB. The executor
// and the HTTP transaction boundary both use it. A transaction cannot span
// databases (Neo4j Operations Manual: "a transaction cannot span across
// multiple databases"); composite databases read several constituents
// through Fabric instead. A write gets Neo4j's
// Neo.ClientError.Statement.AccessMode "Writing to more than one database
// per transaction is not allowed" (verified against Neo4j 5.26). Neo4j
// Community has one user database, so its error for a read of a second one
// could not be observed; a read gets the same code with the message worded
// for a read.
func TransactionDatabaseError(databases TransactionDatabases, txDB, target, query string) error {
	txDB = StatementDatabase(txDB, query)
	target = StatementDatabase(target, query)
	if sameTransactionDatabase(databases, txDB, target) {
		return nil
	}
	requirements := QueryPermissionRequirements(query)
	if requirements.Write || requirements.Schema {
		return localizedStatusError("Neo.ClientError.Statement.AccessMode", "AccessMode",
			localization.CypherCommandRoutingTransactionSecondDatabaseWrite(target, txDB))
	}
	return localizedStatusError("Neo.ClientError.Statement.AccessMode", "AccessMode",
		localization.CypherCommandRoutingTransactionSecondDatabaseAccess(target, txDB))
}

// sameTransactionDatabase reports whether target is txDB, by name or alias,
// or a constituent of composite txDB.
func sameTransactionDatabase(databases TransactionDatabases, txDB, target string) bool {
	if strings.EqualFold(txDB, target) {
		return true
	}
	if databases == nil {
		return false
	}
	if databases.IsCompositeDatabase(txDB) && strings.HasPrefix(target, txDB+".") {
		return true
	}
	resolvedTx, txErr := databases.ResolveDatabase(txDB)
	resolvedTarget, targetErr := databases.ResolveDatabase(target)
	return txErr == nil && targetErr == nil && resolvedTx == resolvedTarget
}

// inTransaction reports whether a statement run with ctx belongs to a
// transaction: this executor's explicit one, or the transaction of the
// statement it is nested in (an implicit transaction's storage wrapper
// carried on ctx), or the transaction pinned to this executor's storage view.
func (e *StorageExecutor) inTransaction(ctx context.Context) bool {
	if e.txContext != nil && e.txContext.active {
		return true
	}
	if _, nested := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); nested {
		return true
	}
	_, scoped := e.storage.(*transactionStorageWrapper)
	return scoped
}

// executeOnDatabase is the one database router: it runs query on database
// db, the target a leading USE clause or :USE command names, on this
// executor for its own database, else on db's executor. Every route that
// selects a database with USE goes through useDatabase: a top-level
// statement, a statement a procedure runs (nested, run with executeInternal
// so it stays in the caller's execution) and a CALL { USE … } body.
func (e *StorageExecutor) executeOnDatabase(ctx context.Context, db, query string, params map[string]interface{}, nested bool) (*ExecuteResult, error) {
	scopedExec, ctx, err := e.useDatabase(ctx, db, query)
	if err != nil {
		return nil, err
	}
	var result *ExecuteResult
	if nested {
		result, err = scopedExec.executeInternal(ctx, query, params)
	} else {
		result, err = scopedExec.executeRouted(ctx, query, params)
	}
	if result != nil {
		// A routed result can come from a shared cache. Attach the selected
		// database to a private copy for transport metadata and entity ids.
		routed := *result
		routed.Metadata = make(map[string]interface{}, len(result.Metadata)+1)
		for key, value := range result.Metadata {
			routed.Metadata[key] = value
		}
		routed.Metadata["db"] = GetUseDatabaseFromContext(ctx)
		result = &routed
	}
	return result, err
}

// useDatabase resolves the database db a USE selects for query and applies
// the rules every such statement follows: the caller may use db, the system
// database runs only what its statement policy allows, and a statement of a
// transaction (explicit, or the one the statement is nested in) stays on
// that transaction's database. It returns db's executor and ctx scoped to db.
func (e *StorageExecutor) useDatabase(ctx context.Context, db, query string) (*StorageExecutor, context.Context, error) {
	if err := e.authorizeSelectedDatabase(ctx, db, query); err != nil {
		return nil, ctx, err
	}
	scopedExec, resolvedDB, err := e.scopedExecutorForUse(db, GetAuthTokenFromContext(ctx))
	if err != nil {
		return nil, ctx, err
	}
	if strings.EqualFold(resolvedDB, "system") {
		if err := systemDatabaseStatementError(query); err != nil {
			return nil, ctx, err
		}
	}
	if scopedExec != e && e.inTransaction(ctx) {
		if err := TransactionDatabaseError(e.dbManager, e.currentDatabaseName(), resolvedDB, query); err != nil {
			return nil, ctx, err
		}
	}
	return scopedExec, withExecutionDatabase(ctx, resolvedDB), nil
}

// systemDatabaseStatementError is the error for a statement the system
// database doesn't run, or nil. The system database holds NornicDB's own
// data (users, roles, privileges, procedure grants), which only its
// administration commands write, so a statement that writes graph data
// there (CREATE, MERGE, SET, DELETE, REMOVE, a write procedure) is refused
// on every route, as Neo4j refuses it (Neo.ClientError.Statement.SemanticError);
// this keeps procedure grants coming only from GRANT and REVOKE. Schema
// commands and graph reads are refused too. The system database runs what
// Neo4j's runs: administration commands, SHOW commands whose tail reads no
// graph (systemShowTailError), and calls of system
// procedures (ProcedureSpec.WorksOnSystem) whose results read no graph. A
// call of any other registered procedure, wherever it stands in the
// statement, and any other statement (RETURN 1, RETURN COUNT { … },
// UNWIND …) fail with Neo4j's SemanticError "Not a recognised system
// command or procedure". A system procedure that reads the graph sees none
// of the system database's records (systemProcedureScope). This is the one
// policy, applied on every route: the request's or session's database, USE,
// a statement a procedure runs, and CALL { USE … }.
func systemDatabaseStatementError(query string) error {
	_, query = parseExecutionMode(query)
	query = strings.TrimSpace(query)
	// A SHOW command runs, and its tail is held to Neo4j's rules for it
	// (systemShowTailError).
	if startsWithKeywordFold(query, "SHOW") {
		return systemShowTailError(query)
	}
	// Transaction control (BEGIN, COMMIT, ROLLBACK) is no statement on a
	// graph; a BEGIN … COMMIT script is its statement.
	if isSystemCommandNoGraph(query) || bareTransactionCommand(query) != "" {
		return nil
	}
	if startsWithKeywordFold(query, "BEGIN") && transactionScriptShape(query) {
		body, _ := stripBeginTransactionPrefix(query)
		if statement, _, ok := splitTransactionScriptTailAction(body); ok {
			body = statement
		}
		return systemDatabaseStatementError(body)
	}
	notRecognised := localizedStatusError("Neo.ClientError.Statement.SemanticError", "SemanticError",
		localization.CypherAdminSystemProcedureUnsupported(query))
	for _, procedure := range registeredProceduresForCalls(query) {
		if !procedure.WorksOnSystem {
			return notRecognised
		}
	}
	requirements := QueryPermissionRequirements(query)
	graphAccess := localizedStatusError("Neo.ClientError.Statement.SemanticError", "SemanticError", localization.CypherAdminSystemGraphAccessUnsupported())
	if requirements.Write || requirements.Schema || queryKeywords(query)["MATCH"] {
		return graphAccess
	}
	if StatementDatabase("", query) == "system" || startsWithKeywordFold(query, "TERMINATE") || isNornicExtensionStatement(query) {
		return nil
	}
	if startsWithKeywordFold(query, "CALL") {
		if _, procedureCall := procedureNameAt(query, len("CALL")); procedureCall {
			if expressionReadsGraphText(query) {
				return graphAccess
			}
			return nil
		}
	}
	return notRecognised
}

// systemShowTailError is the system database's rule for the tail of a SHOW
// command (YIELD, WHERE, ORDER BY, SKIP, LIMIT, RETURN), as Neo4j 5.26.30
// applies it there: the tail filters, orders, pages and projects the
// command's own columns, and reads no graph. Another clause after YIELD
// (WITH, UNWIND, CALL { }, MATCH, a write, …) is a SyntaxError "Invalid
// input …"; a COUNT, EXISTS or COLLECT subquery ("The COUNT expression is
// not valid on SHOW commands."), a pattern comprehension or a pattern
// expression (graphReadingExpression) is Neo4j's SyntaxError for it. On a
// user database a SHOW tail is not limited this way (the Cypher 25 clauses
// after YIELD and graph-reading expressions run there, as Neo4j runs the
// expressions).
func systemShowTailError(query string) error {
	query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	tail := strings.TrimSpace(query[len(showCommandHead(query)):])
	if tail == "" {
		return nil
	}
	body := tail
	if startsWithKeywordFold(body, "YIELD") {
		body = body[len("YIELD"):]
	}
	if start := showContinuationStart(body); start >= 0 {
		clause := body[start:]
		end := 0
		for end < len(clause) && isIdentByte(clause[end]) {
			end++
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherAdminShowClauseUnexpected(clause[:end]))
	}
	switch kind := graphReadingExpression(tail); kind {
	case "":
		return nil
	case "COUNT", "EXISTS", "COLLECT":
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherAdminShowExpressionInvalid(kind))
	case graphReadingPatternComprehension:
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherAdminShowPatternComprehensionInvalid())
	default:
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherAdminShowPatternExpressionInvalid())
	}
}

// systemDatabaseWriteError is systemDatabaseStatementError for a statement
// run on this executor's own database (a request's or session's database,
// not one a USE selects): nil unless that is the system database.
func (e *StorageExecutor) systemDatabaseWriteError(query string) error {
	database := e.currentDatabaseName()
	if e.txContext != nil && e.txContext.active && e.txContext.statementKind == "Administration" {
		database = "system"
	}
	if !strings.EqualFold(database, "system") {
		return nil
	}
	return systemDatabaseStatementError(query)
}

// resolveCompositeStorage resolves USE <composite> to a CompositeEngine-backed executor.
func (e *StorageExecutor) resolveCompositeStorage(compositeName string, authToken string) (*StorageExecutor, string, error) {
	if e.dbManager == nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseManagerUnavailable(compositeName), nil)
	}

	engineIface, err := e.dbManager.GetStorageForUse(compositeName, authToken)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(compositeName, err), err)
	}

	engine, ok := engineIface.(storage.Engine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseStorageTypeInvalid(compositeName), nil)
	}

	return e.cloneForStorage(engine), compositeName, nil
}

// resolveCompositeConstituent resolves USE <composite.alias> to a specific
// constituent engine within a composite database.
func (e *StorageExecutor) resolveCompositeConstituent(fullName, compositeName, alias string, authToken string) (*StorageExecutor, string, error) {
	if e.dbManager == nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseManagerUnavailable(fullName), nil)
	}

	// Get the composite engine first.
	engineIface, err := e.dbManager.GetStorageForUse(compositeName, authToken)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(fullName, err), err)
	}

	compositeEngine, ok := engineIface.(*storage.CompositeEngine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseNotComposite(fullName, compositeName), nil)
	}

	// Resolve the specific constituent by alias.
	constituentEngine, err := compositeEngine.GetConstituentByAlias(alias)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(fullName, err), err)
	}

	return e.cloneForStorage(constituentEngine), fullName, nil
}
