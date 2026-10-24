package cypher

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type fabricPreparedExecKey struct{}

// participantSchemaForStatement is the participant schema boundary shared by
// statement metadata consumers and Fabric planning. The Fabric integration
// supplies the resolver; without it, participant-owned implicit signatures are
// deliberately unresolved before execution.
func (e *StorageExecutor) participantSchemaForStatement(ctx context.Context, query string) resultmetadata.ParticipantSchema {
	return nil
}

func (e *StorageExecutor) shouldUseFabricPlanner(cypher string) bool {
	if e.dbManager == nil {
		return false
	}
	// Engage Fabric only when a parsed USE target references a composite scope:
	//   - USE <composite>
	//   - USE <composite>.<constituent>
	// Word presence alone is insufficient (e.g., "USE" inside string payloads).
	opts := defaultKeywordScanOpts()
	searchFrom := 0
	for {
		useIdx := keywordIndexFrom(cypher, "USE", searchFrom, opts)
		if useIdx < 0 {
			break
		}
		use, _, hasUse, err := fabric.ParseUseClause(cypher[useIdx:], true)
		if hasUse && err == nil && (e.useTargetRequiresFabric(use.Name) || (use.IsDynamic() && e.sessionIsComposite())) {
			return true
		}
		searchFrom = useIdx + len("USE")
	}
	return false
}

func (e *StorageExecutor) useTargetRequiresFabric(target string) bool {
	target = strings.TrimSpace(target)
	if target == "" || e.dbManager == nil {
		return false
	}
	if e.dbManager.IsCompositeDatabase(target) {
		return true
	}
	if dot := strings.IndexByte(target, '.'); dot > 0 {
		base := strings.TrimSpace(target[:dot])
		if base != "" && e.dbManager.IsCompositeDatabase(base) {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) executeViaFabric(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	if composite := compositeStatementFrom(ctx); composite != nil {
		return e.executeNestedFabric(ctx, cypher, params, composite)
	}
	tx := fabric.NewFabricTransaction(fmt.Sprintf("fab-%d", time.Now().UnixNano()))
	return e.executeViaFabricWithTx(ctx, cypher, params, tx, true)
}

// executeNestedFabric runs a composite statement that a statement of
// composite runs (a statement a procedure of a fragment was given): it is
// part of composite. Its fragments run on the same transaction and
// participants, and commit or roll back with the whole statement, which ends
// the transaction. They run one at a time, in the fragment that runs them.
func (e *StorageExecutor) executeNestedFabric(ctx context.Context, cypher string, params map[string]interface{}, composite *compositeStatement) (*ExecuteResult, error) {
	prepared, err := e.preparedFabricFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if prepared == nil || prepared.query != cypher {
		if prepared, err = e.prepareFabricExecution(ctx, cypher); err != nil {
			return nil, err
		}
	}
	return e.runFabricStatement(ctx, cypher, params, composite.tx, prepared, composite.executor, false)
}

func (e *StorageExecutor) executeViaFabricWithTx(ctx context.Context, cypher string, params map[string]interface{}, tx *fabric.FabricTransaction, autoCommit bool) (*ExecuteResult, error) {
	prepared, err := e.preparedFabricFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		prepared, err = e.prepareFabricExecution(ctx, cypher)
		if err != nil {
			return nil, err
		}
	}
	return e.executeViaPreparedFabricWithTx(ctx, cypher, params, tx, autoCommit, prepared)
}

func (e *StorageExecutor) executeViaPreparedFabricWithTx(ctx context.Context, cypher string, params map[string]interface{}, tx *fabric.FabricTransaction, autoCommit bool, prepared *fabricPreparedExec) (*ExecuteResult, error) {
	if prepared == nil {
		return nil, localizedError(localization.CypherCommandRoutingFabricNotPrepared(), nil)
	}
	if hasCallInTransactions(cypher) {
		return nil, transactionalCallContextError("transactional CALL on composite targets is not supported")
	}
	participants := &cypherFabricExecutor{
		base:       e,
		authToken:  GetAuthTokenFromContext(ctx),
		autoCommit: autoCommit,
		catalog:    prepared.catalog,
	}
	if !autoCommit && e.txContext != nil && e.txContext.active {
		if cached := e.txContext.fabricRemoteExe; cached != nil {
			participants.remote = cached
		} else {
			participants.remote = fabric.NewRemoteFragmentExecutor()
			e.txContext.fabricRemoteExe = participants.remote
		}
	} else {
		participants.remote = fabric.NewRemoteFragmentExecutor()
		defer func() { _ = participants.remote.Close() }()
	}

	// An autocommit statement owns its transaction: every way it ends other
	// than its commit (an error, a cancellation, a panic) rolls back every
	// participant. An explicit transaction's statement leaves it to the
	// client's COMMIT or ROLLBACK; Execute fails the transaction when the
	// statement fails or panics, so COMMIT can't keep a failed statement's
	// writes.
	if autoCommit {
		ended := false
		defer func() {
			if !ended {
				_ = tx.Rollback(nil)
			}
		}()
		result, err := e.runFabricStatement(ctx, cypher, params, tx, prepared, participants, autoCommit)
		if err != nil {
			return nil, err
		}
		ended = true
		if err := tx.Commit(nil, nil); err != nil {
			return nil, err
		}
		if tx.WriteShard() != "" {
			publishStatementCommit(ctx)
		}
		return result, nil
	}
	return e.runFabricStatement(ctx, cypher, params, tx, prepared, participants, autoCommit)
}

// runFabricStatement runs a prepared composite statement on tx, its
// fragments on participants; concurrent lets independent fragments run at
// once. The caller ends tx.
func (e *StorageExecutor) runFabricStatement(ctx context.Context, cypher string, params map[string]interface{}, tx *fabric.FabricTransaction, prepared *fabricPreparedExec, participants *cypherFabricExecutor, concurrent bool) (*ExecuteResult, error) {
	catalog := prepared.catalog
	authToken := GetAuthTokenFromContext(ctx)
	localExec := fabric.NewLocalFragmentExecutor(participants, func(dbName string) (storage.Engine, error) {
		if e.dbManager != nil {
			engineIface, err := e.dbManager.GetStorageForUse(dbName, authToken)
			if err == nil {
				if engine, ok := engineIface.(storage.Engine); ok {
					return engine, nil
				}
				return nil, localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(dbName), nil)
			}
		}
		scoped, _, err := e.scopedExecutorForUse(dbName, authToken)
		if err != nil {
			return nil, err
		}
		return scoped.storage, nil
	})

	statsAcc := &queryStatsAccumulator{}
	ctx = context.WithValue(ctx, queryStatsAccumulatorKey{}, statsAcc)
	fabricTrace := &fabric.HotPathTrace{}
	ctx = fabric.WithHotPathTrace(ctx, fabricTrace)
	fabricExecutor := fabric.NewFabricExecutor(catalog, localExec, participants.remote)
	fabricExecutor.SetUnionContract(fabricUnionContract)
	fabricExecutor.SetConcurrentExecution(concurrent)
	fabricExecutor.SetGraphArgumentEvaluator(e.evaluateGraphArguments)
	fabricExecutor.SetGraphAuthorizer(func(ctx context.Context, database, query string) error {
		if err := e.authorizeSelectedDatabase(ctx, database, query); err != nil {
			return err
		}
		return e.recordStatementReference(cypher, database)
	})
	stream, err := fabricExecutor.Execute(ctx, tx, prepared.fragment, params, authToken)
	if err != nil {
		return nil, fabricGraphReferenceError(err)
	}
	e.setFabricBatchedApplyRowsUsed(fabricTrace.ApplyBatchedLookupRows.Load())
	if stream == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	// Schema commands have a void result established by their command executor.
	// USE selects the participant; it does not turn DDL into a graph projection.
	_, command, _, useErr := parseUseClause(cypher, false)
	if useErr != nil {
		return nil, useErr
	}
	if len(stream.Columns) == 0 && !isSchemaCommandStatement(command) {
		columns, err := e.ResultColumns(cypher, e.resultIncomingScope(), cypherVersionFromContext(ctx) == "25")
		if err != nil {
			return nil, err
		}
		stream.Columns = columns
	}
	if err := e.normalizeFabricRowWrapper(cypher, stream); err != nil {
		return nil, err
	}
	s := statsAcc.snapshot()
	return &ExecuteResult{
		Columns: stream.Columns,
		Rows:    stream.Rows,
		Stats:   &s,
	}, nil
}

type fabricPreparedExec struct {
	query     string
	catalog   *fabric.Catalog
	sessionDB string
	fragment  fabric.Fragment
	hasRemote bool
}

// fabricResultCacheKey binds each local target's identity, catalog generation
// and graph revision in sorted order. Equal numeric revisions on different
// targets, or on a recreated selection, must never reuse the same result.
func (e *StorageExecutor) fabricResultCacheKey(ctx context.Context, prepared *fabricPreparedExec, cypher string, params map[string]interface{}) (string, bool) {
	selections, local := e.fabricLocalSelections(ctx, prepared)
	if !local || !e.StatementCacheSelectionValid() {
		return "", false
	}
	if len(selections) == 0 && e.authorizeSelectedDatabase(ctx, "", cypher) != nil {
		return "", false
	}
	resolver, ok := e.dbManager.(statementCacheScopeResolver)
	if !ok {
		return "", false
	}
	key := cacheKeyBuilder{}
	var scratch [binary.MaxVarintLen64 + 1]byte
	writeCacheKeyString(&key, &scratch, resultCacheEntryKey(cypher, params))
	key.buf = binary.AppendUvarint(key.buf, e.statementCacheScope.Generation())
	scopes := make([]*multidb.StatementCacheScope, 0, len(selections))
	for _, selection := range selections {
		scope, err := resolver.StatementCacheScope(selection.name)
		if err != nil || !scope.Valid() {
			return "", false
		}
		engine, err := e.dbManager.GetStorageForUse(selection.database, GetAuthTokenFromContext(ctx))
		if err != nil {
			return "", false
		}
		provider, ok := engine.(storage.GraphMutationVersionProvider)
		if !ok {
			return "", false
		}
		revision, supported := provider.GraphMutationVersion()
		if !supported {
			return "", false
		}
		writeCacheKeyString(&key, &scratch, selection.name)
		writeCacheKeyString(&key, &scratch, selection.database)
		key.buf = binary.AppendUvarint(key.buf, scope.Generation())
		key.buf = binary.AppendUvarint(key.buf, revision)
		scopes = append(scopes, scope)
	}
	if !e.StatementCacheSelectionValid() || !statementScopesValid(scopes) {
		return "", false
	}
	return string(key.buf), true
}

type fabricLocalSelection struct{ name, database string }

// Authorization and current catalog resolution precede every result lookup.
// Dynamic and remote targets keep executing through dispatch without caching.
func (e *StorageExecutor) fabricLocalSelections(ctx context.Context, prepared *fabricPreparedExec) (selections []fabricLocalSelection, local bool) {
	if prepared == nil || prepared.catalog == nil || prepared.fragment == nil {
		return nil, false
	}
	seen := make(map[fabricLocalSelection]struct{})
	var collect func(fabric.Fragment) bool
	collect = func(fragment fabric.Fragment) bool {
		switch current := fragment.(type) {
		case *fabric.FragmentExec:
			if current.Graph != nil && current.Graph.IsDynamic() || e.authorizeSelectedDatabase(ctx, current.GraphName, current.Query) != nil {
				return false
			}
			location, err := prepared.catalog.Resolve(current.GraphName)
			if err != nil {
				return false
			}
			target, ok := location.(*fabric.LocationLocal)
			if !ok {
				return false
			}
			name := current.GraphName
			if name == "" {
				name = target.DBName
			}
			selection := fabricLocalSelection{name: name, database: target.DBName}
			if _, listed := seen[selection]; !listed {
				seen[selection] = struct{}{}
				selections = append(selections, selection)
			}
			return collect(current.Input)
		case *fabric.FragmentApply:
			return collect(current.Input) && collect(current.Inner)
		case *fabric.FragmentUnion:
			return collect(current.LHS) && collect(current.RHS)
		case *fabric.FragmentLeaf:
			return collect(current.Input)
		case *fabric.FragmentInit:
			return true
		default:
			return false
		}
	}
	if !collect(prepared.fragment) {
		return nil, false
	}
	sort.Slice(selections, func(i, j int) bool {
		if selections[i].name != selections[j].name {
			return selections[i].name < selections[j].name
		}
		return selections[i].database < selections[j].database
	})
	return selections, true
}
func (e *StorageExecutor) prepareFabricExecution(ctx context.Context, cypher string) (*fabricPreparedExec, error) {
	catalog, err := e.buildFabricCatalog()
	if err != nil {
		return nil, err
	}
	sessionDB := e.currentDatabaseName()
	if dbFromCtx := GetUseDatabaseFromContext(ctx); strings.TrimSpace(dbFromCtx) != "" {
		sessionDB = dbFromCtx
	}
	fragment, err := e.planFabricQuery(catalog, cypher, sessionDB)
	if err != nil {
		return nil, fabricGraphReferenceError(err)
	}
	return &fabricPreparedExec{
		query:     cypher,
		catalog:   catalog,
		sessionDB: sessionDB,
		fragment:  fragment,
		hasRemote: fabricFragmentHasRemoteTarget(catalog, fragment),
	}, nil
}

func (e *StorageExecutor) preparedFabricFromContext(ctx context.Context) (*fabricPreparedExec, error) {
	if ctx == nil {
		return nil, nil
	}
	v := ctx.Value(fabricPreparedExecKey{})
	if v == nil {
		return nil, nil
	}
	prepared, ok := v.(*fabricPreparedExec)
	if !ok {
		return nil, localizedError(localization.CypherCommandRoutingFabricPreparedContextInvalid(), nil)
	}
	return prepared, nil
}

func fabricFragmentHasRemoteTarget(catalog *fabric.Catalog, f fabric.Fragment) bool {
	if catalog == nil || f == nil {
		return false
	}
	switch frag := f.(type) {
	case *fabric.FragmentExec:
		loc, err := catalog.Resolve(frag.GraphName)
		if err != nil {
			return false
		}
		_, isRemote := loc.(*fabric.LocationRemote)
		return isRemote
	case *fabric.FragmentApply:
		return fabricFragmentHasRemoteTarget(catalog, frag.Input) || fabricFragmentHasRemoteTarget(catalog, frag.Inner)
	case *fabric.FragmentUnion:
		return fabricFragmentHasRemoteTarget(catalog, frag.LHS) || fabricFragmentHasRemoteTarget(catalog, frag.RHS)
	case *fabric.FragmentLeaf:
		return fabricFragmentHasRemoteTarget(catalog, frag.Input)
	case *fabric.FragmentInit:
		return false
	default:
		return false
	}
}

// fabricUnionContract gives the UNION branches Fabric combines the semantics
// executeUnionBranches gives a UNION on one graph.
var fabricUnionContract = fabric.UnionContract{
	ColumnOrder:  unionColumnOrder,
	ColumnsError: unionColumnsError,
	NewDistinct: func() func(row []interface{}) bool {
		seen := make(map[string]bool)
		return func(row []interface{}) bool {
			key := cypherValueKey(row, cypherEquivalenceContract)
			if seen[key] {
				return true
			}
			seen[key] = true
			return false
		}
	},
}

func (e *StorageExecutor) planFabricQuery(catalog *fabric.Catalog, cypher, sessionDB string) (fabric.Fragment, error) {
	if e.fabricPlanCache != nil {
		if cached, found := e.fabricPlanCache.Get(cypher, sessionDB); found && cached != nil {
			return cached, nil
		}
	}

	planner := fabric.NewFabricPlanner(catalog)
	// A name spelled UNION, OPTIONAL or RETURN is read as ordinary statements read it.
	planner.SetClauseKeywordNameReader(clauseKeywordUsedAsName)
	fragment, err := planner.Plan(cypher, sessionDB)
	if err != nil {
		return nil, err
	}
	if e.fabricPlanCache != nil {
		e.fabricPlanCache.Put(cypher, sessionDB, fragment)
	}
	return fragment, nil
}

func (e *StorageExecutor) normalizeFabricRowWrapper(cypher string, stream *fabric.ResultStream) error {
	if stream == nil || len(stream.Columns) != 1 || stream.Columns[0] != "__fabric_row" || len(stream.Rows) == 0 {
		return nil
	}
	inferred, err := e.ResultColumns(cypher, e.resultIncomingScope(), cypherGrammarVersion(cypher) == "25")
	if err != nil {
		return err
	}
	if len(inferred) == 0 {
		return nil
	}
	projectedRows := make([][]interface{}, 0, len(stream.Rows))
	for _, row := range stream.Rows {
		if len(row) == 0 {
			continue
		}
		m, ok := normalizeFabricRowMap(row[0])
		if !ok {
			continue
		}
		out := make([]interface{}, len(inferred))
		sourceID, _ := m["sourceId"]
		for i, col := range inferred {
			if v, ok := m[col]; ok {
				out[i] = v
				continue
			}
			if sourceID != nil {
				if v, ok := lookupColumnBySourceIDInRows(m["rows"], sourceID, col); ok {
					out[i] = v
				}
			}
		}
		projectedRows = append(projectedRows, out)
	}
	if len(projectedRows) == 0 {
		return nil
	}
	stream.Columns = inferred
	stream.Rows = projectedRows
	return nil
}

func normalizeFabricRowMap(v interface{}) (map[string]interface{}, bool) {
	switch m := v.(type) {
	case map[string]interface{}:
		return m, true
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = val
		}
		return out, true
	default:
		return nil, false
	}
}

func lookupColumnBySourceIDInRows(rowsAny interface{}, sourceID interface{}, col string) (interface{}, bool) {
	switch rows := rowsAny.(type) {
	case []interface{}:
		for _, item := range rows {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if sameSourceID(m["sourceId"], sourceID) {
				v, exists := m[col]
				return v, exists
			}
		}
	case []map[string]interface{}:
		for _, m := range rows {
			if sameSourceID(m["sourceId"], sourceID) {
				v, exists := m[col]
				return v, exists
			}
		}
	}
	return nil, false
}

func sameSourceID(a interface{}, b interface{}) bool {
	normalize := func(v interface{}) string {
		s := strings.TrimSpace(fmt.Sprint(v))
		return strings.Trim(s, `"`)
	}
	return normalize(a) == normalize(b)
}

func (e *StorageExecutor) currentDatabaseName() string {
	if composite, ok := e.storage.(interface{ CompositeName() string }); ok {
		if name := strings.TrimSpace(composite.CompositeName()); name != "" {
			return name
		}
	}
	if ns, ok := e.storage.(interface{ Namespace() string }); ok {
		if name := strings.TrimSpace(ns.Namespace()); name != "" {
			return name
		}
	}
	return "nornic"
}

func (e *StorageExecutor) buildFabricCatalog() (*fabric.Catalog, error) {
	catalog := fabric.NewCatalog()
	for _, db := range e.dbManager.ListDatabases() {
		dbName := strings.TrimSpace(db.Name())
		if dbName == "" {
			continue
		}
		catalog.Register(dbName, &fabric.LocationLocal{DBName: dbName})
		for alias := range e.dbManager.ListAliases(dbName) {
			alias = strings.TrimSpace(alias)
			if alias != "" {
				catalog.Register(alias, &fabric.LocationLocal{DBName: dbName})
			}
		}

		if db.Type() != "composite" {
			continue
		}
		constituents, err := e.dbManager.GetCompositeConstituents(dbName)
		if err != nil {
			return nil, localizedError(localization.CypherCommandRoutingFabricConstituentsFailed(dbName, err), err)
		}
		for _, raw := range constituents {
			ref, ok := toConstituentRef(raw)
			if !ok || strings.TrimSpace(ref.Alias) == "" {
				continue
			}
			qualified := dbName + "." + ref.Alias
			if strings.EqualFold(strings.TrimSpace(ref.Type), "remote") {
				catalog.Register(qualified, &fabric.LocationRemote{
					DBName:   ref.DatabaseName,
					URI:      ref.URI,
					AuthMode: strings.TrimSpace(ref.AuthMode),
					User:     ref.User,
					Password: ref.Password,
				})
				continue
			}
			catalog.Register(qualified, &fabric.LocationLocal{DBName: ref.DatabaseName})
		}
	}
	return catalog, nil
}

func toConstituentRef(raw interface{}) (multidb.ConstituentRef, bool) {
	if ref, ok := raw.(multidb.ConstituentRef); ok {
		return ref, true
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return multidb.ConstituentRef{}, false
	}
	return multidb.ConstituentRef{
		Alias:        mapString(m, "alias"),
		DatabaseName: mapString(m, "database_name"),
		Type:         mapString(m, "type"),
		AccessMode:   mapString(m, "access_mode"),
		URI:          mapString(m, "uri"),
		SecretRef:    mapString(m, "secret_ref"),
		AuthMode:     mapString(m, "auth_mode"),
		User:         mapString(m, "user"),
		Password:     mapString(m, "password"),
	}, true
}

func mapString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type cypherFabricExecutor struct {
	base       *StorageExecutor
	authToken  string
	autoCommit bool
	// catalog names the statement's participants; remote runs its remote
	// fragments, and those of a composite statement nested in it.
	catalog *fabric.Catalog
	remote  *fabric.RemoteFragmentExecutor

	mu        sync.Mutex
	statement *compositeStatement // the statement's fragments run in (statementFor)
	// ownParticipants holds the local participants of a statement run
	// outside an explicit transaction; an explicit transaction keeps them
	// in its TransactionContext (participants).
	ownParticipants map[string]*participantHandle
}

func (c *cypherFabricExecutor) ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	return c.ExecuteQueryWithRecord(ctx, dbName, engine, query, params, nil)
}

func (c *cypherFabricExecutor) ExecuteRecordQuery(ctx context.Context, dbName, query string, params, recordBindings map[string]interface{}) ([]string, [][]interface{}, bool, error) {
	clauses, supported := canExecuteAsPipeline(query)
	if !supported || containsRelationshipPattern(query) {
		return nil, nil, false, nil
	}
	for _, clause := range clauses {
		if clause.kind != pipelineClauseWith && clause.kind != pipelineClauseUnwind && clause.kind != pipelineClauseReturn {
			return nil, nil, false, nil
		}
	}
	keywords := queryKeywords(query)
	if keywords["MATCH"] || keywords["STARTNODE"] || keywords["ENDNODE"] || keywords["SHORTESTPATH"] {
		return nil, nil, false, nil
	}
	executor := c.base.cloneForStorage(c.base.storage)
	if err := executor.bindFabricStatementCacheScope(ctx, dbName); err != nil {
		return nil, nil, true, err
	}
	executor.fabricRecordBindings = recordBindings
	ctx = withExecutionDatabase(WithAuthToken(ctx, c.authToken), dbName)
	columns, rows, err := c.executeFragmentQuery(ctx, executor, query, params)
	return columns, rows, true, err
}

func (c *cypherFabricExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, [][]interface{}, error) {
	ctx = WithAuthToken(ctx, c.authToken)
	var exec *StorageExecutor
	if tx, ok := fabric.FabricTransactionFromContext(ctx); ok {
		// The fragment, and each statement it runs, runs on its database's
		// participant storage; the composite database routes to its
		// constituents'.
		composite := c.statementFor(ctx, tx)
		if root, isRoot := engine.(*storage.CompositeEngine); isRoot {
			// The executor reads the stored procedure catalog from the
			// databases themselves, as it is built, not through the
			// participants: that read is not the statement's, so it begins
			// no participant transaction.
			exec = c.base.cloneForStorage(root).cloneWithStorage(composite.router(root))
			ctx = &compositeContext{Context: ctx, statement: composite}
		} else {
			participant, err := composite.participant(dbName, engine)
			if err != nil {
				return nil, nil, err
			}
			exec, ctx = participant.run(ctx, composite)
		}
	} else {
		// A fragment view owns its removable catalog selection.
		exec = c.base.cloneForStorage(engine)
	}
	if err := exec.bindFabricStatementCacheScope(ctx, dbName); err != nil {
		return nil, nil, err
	}
	if len(recordBindings) > 0 {
		exec.fabricRecordBindings = recordBindings
		query = stripLeadingWithImportsForFabricRecord(query, recordBindings)
	}
	ctx = withExecutionDatabase(ctx, dbName)
	return c.executeFragmentQuery(ctx, exec, query, params)
}

func (c *cypherFabricExecutor) executeFragmentQuery(ctx context.Context, exec *StorageExecutor, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	// Local fragments bypass implicit execution's decision. Classify this
	// fragment itself: writes and procedures must retain full node reads.
	exec.readOnlyQueryNodes = exec.readsQueryNodesWithoutVectors(exec.analyzer.Analyze(query))
	result, err := exec.executeInternal(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return []string{}, [][]interface{}{}, nil
	}
	if acc := queryStatsAccumulatorFromContext(ctx); acc != nil {
		acc.add(result.Stats)
	}
	return result.Columns, result.Rows, nil
}

func stripLeadingWithImportsForFabricRecord(query string, recordBindings map[string]interface{}) string {
	if len(recordBindings) == 0 {
		return query
	}
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(upperASCII(trimmed), "WITH ") {
		return query
	}
	withEnd := findLeadingWithEndLocal(trimmed)
	if withEnd <= 0 || withEnd >= len(trimmed) {
		return query
	}
	withClause := strings.TrimSpace(trimmed[len("WITH "):withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	if rest == "" {
		return query
	}
	// Stripping is safe for simple imported bindings when the next clause is a
	// top-level read/projection/pipeline clause that can resolve identifiers from
	// Fabric record bindings.
	if !(startsWithKeywordFold(rest, "MATCH") ||
		startsWithKeywordFold(rest, "OPTIONAL MATCH") ||
		startsWithKeywordFold(rest, "RETURN") ||
		startsWithKeywordFold(rest, "UNWIND") ||
		startsWithKeywordFold(rest, "USE")) {
		return query
	}
	imports := splitTopLevelComma(withClause)
	if len(imports) == 0 {
		return query
	}
	for _, item := range imports {
		name := strings.TrimSpace(item)
		if name == "" || strings.Contains(name, " ") {
			return query
		}
		if _, ok := recordBindings[name]; !ok {
			return query
		}
	}
	rewritten := rest
	local := localOccurrences{text: rest}
	for _, item := range imports {
		name := strings.TrimSpace(item)
		rewritten = replaceIdentifierOutsideQuotesUnless(rewritten, name, "$"+name, &local)
	}
	return rewritten
}

func findLeadingWithEndLocal(query string) int {
	const prefix = "WITH "
	if len(query) <= len(prefix) {
		return -1
	}
	body := query[len(prefix):]
	end := -1
	for _, keyword := range [...]string{
		"MATCH", "OPTIONAL MATCH", "RETURN", "WHERE", "USE", "WITH", "CALL",
		"CREATE", "MERGE", "UNWIND", "SET", "DELETE", "DETACH DELETE",
	} {
		if index := topLevelKeywordIndex(body, keyword); index >= 0 && (end < 0 || index < end) {
			end = index
		}
	}
	if end < 0 {
		return -1
	}
	return len(prefix) + end
}

// evaluateGraphArguments evaluates a dynamic graph reference's arguments
// (USE graph.byName(g)) for Fabric: values holds the statement's
// parameters and the current row's variables.
func (e *StorageExecutor) evaluateGraphArguments(ctx context.Context, expressions []string, values map[string]interface{}) ([]string, error) {
	ctx = withQueryParams(ctx, values)
	out := make([]string, len(expressions))
	for i, expression := range expressions {
		value, ok := e.evaluateRowExpressionWithContext(ctx, expression, pipelineRow(values))
		if !ok {
			if err, undefined := undefinedVariableError(expression); undefined {
				return nil, err
			}
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCommandRoutingGraphFunctionArgumentInvalid(expression))
		}
		text, isString := value.(string)
		if !isString {
			// Neo4j: "Expected <argument> to be a STRING, but it was an INTEGER".
			return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "TypeError",
				localization.CypherCommandRoutingGraphFunctionArgumentType(expression, cypherTypeSystemNameWithArticle(value)))
		}
		out[i] = text
	}
	return out, nil
}

// fabricGraphReferenceError reports a dynamic graph reference Fabric could
// not resolve with its Neo4j status: a graph that doesn't exist is
// DatabaseNotFound "Graph not found: <name>".
func fabricGraphReferenceError(err error) error {
	if errors.Is(err, fabric.ErrMixedUnion) {
		return unionMixedError()
	}
	var notFound *fabric.GraphNotFoundError
	if errors.As(err, &notFound) {
		return localizedStatusError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", localization.CypherCommandRoutingGraphNotFound(notFound.Name))
	}
	var reference *fabric.GraphReferenceError
	if errors.As(err, &reference) {
		return localizedStatusError(reference.Code, "GraphReference", reference.Message)
	}
	var syntax *fabric.UseSyntaxError
	if errors.As(err, &syntax) {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", syntax.Message)
	}
	return err
}

func (e *cypherFabricExecutor) CachedResultColumns(ctx context.Context, query string, incoming []string) resultmetadata.Names {
	return e.base.CachedResultColumns(ctx, query, incoming)
}

func (e *cypherFabricExecutor) ResultProjection(query string, incoming []string) resultmetadata.Projection {
	return e.base.ResultProjection(query, incoming)
}
