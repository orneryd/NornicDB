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
	cloned.embedder = e.embedder
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

// otherDatabaseInTransactionError is the error for a statement of an
// explicit transaction that targets database target (with USE or :USE)
// other than the transaction's own, or nil. A transaction cannot span
// databases (Neo4j Operations Manual: "a transaction cannot span across
// multiple databases"); composite databases read several constituents
// through Fabric instead. A write gets Neo4j's
// Neo.ClientError.Statement.AccessMode "Writing to more than one database
// per transaction is not allowed" (verified against Neo4j 5.26). Neo4j
// Community has one user database, so its error for a read of a second one
// could not be observed; a read gets the same code with the message worded
// for a read.
func (e *StorageExecutor) otherDatabaseInTransactionError(target, query string) error {
	if e.txContext == nil || !e.txContext.active {
		return nil
	}
	current := e.currentDatabaseName()
	if strings.EqualFold(current, target) {
		return nil
	}
	requirements := QueryPermissionRequirements(query)
	if requirements.Write || requirements.Schema {
		return localizedStatusError("Neo.ClientError.Statement.AccessMode", "AccessMode",
			localization.CypherCommandRoutingTransactionSecondDatabaseWrite(target, current))
	}
	return localizedStatusError("Neo.ClientError.Statement.AccessMode", "AccessMode",
		localization.CypherCommandRoutingTransactionSecondDatabaseAccess(target, current))
}

// executeOnDatabase runs query on database db, the target a leading USE
// clause or :USE command names: on this executor for its own database,
// else on db's executor. In an explicit transaction the target must be the
// transaction's database.
func (e *StorageExecutor) executeOnDatabase(ctx context.Context, db, query string, params map[string]interface{}) (*ExecuteResult, error) {
	if err := e.authorizeSelectedDatabase(ctx, db); err != nil {
		return nil, err
	}
	scopedExec, resolvedDB, err := e.scopedExecutorForUse(db, GetAuthTokenFromContext(ctx))
	if err != nil {
		return nil, err
	}
	if scopedExec != e {
		if err := e.otherDatabaseInTransactionError(resolvedDB, query); err != nil {
			return nil, err
		}
	}
	ctx = withExecutionDatabase(ctx, resolvedDB)
	return scopedExec.Execute(ctx, query, params)
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
