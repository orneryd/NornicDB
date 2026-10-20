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

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
)

type fabricPreparedExecKey struct{}

func (e *StorageExecutor) shouldUseFabricPlanner(cypher string) bool {
	if e.dbManager == nil {
		return false
	}
	// Engage Fabric only when a parsed USE target references a composite scope:
	//   - USE <composite>
	//   - USE <composite>.<constituent>
	// Word presence alone is insufficient (e.g., "USE" inside string payloads).
	return fabric.ReadQueryStructure(cypher).AnyUse(func(use fabric.UseClause) bool {
		return e.useTargetRequiresFabric(use.Name) || (use.IsDynamic() && e.sessionIsComposite())
	})
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
	tx := fabric.NewFabricTransaction(fmt.Sprintf("fab-%d", time.Now().UnixNano()))
	return e.executeViaFabricWithTx(ctx, cypher, params, tx, true)
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
	contract, err := readStatementExecutionContract(cypher)
	if err != nil {
		return nil, err
	}
	fragment, err := fabricProcedureWritePlan(prepared.fragment)
	if err != nil {
		return nil, err
	}
	catalog := prepared.catalog
	if err := catalog.BindTransactionScopes(tx); err != nil {
		return nil, err
	}

	authToken := GetAuthTokenFromContext(ctx)
	adapter := &cypherFabricExecutor{
		base:        e,
		authToken:   authToken,
		autoCommit:  autoCommit,
		transaction: tx,
		catalog:     catalog,
	}
	localExec := fabric.NewLocalFragmentExecutor(adapter, func(dbName string) (storage.Engine, error) {
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
	var remoteExec *fabric.RemoteFragmentExecutor
	if !autoCommit && e.txContext != nil && e.txContext.active {
		if cached := e.txContext.fabricRemoteExe; cached != nil {
			remoteExec = cached
		} else {
			remoteExec = fabric.NewRemoteFragmentExecutor()
			e.txContext.fabricRemoteExe = remoteExec
		}
	} else {
		remoteExec = fabric.NewRemoteFragmentExecutor()
		defer func() { _ = remoteExec.Close() }()
	}
	adapter.remote = remoteExec

	statsAcc := &queryStatsAccumulator{}
	ctx = context.WithValue(ctx, queryStatsAccumulatorKey{}, statsAcc)
	fabricTrace := &fabric.HotPathTrace{}
	ctx = fabric.WithHotPathTrace(ctx, fabricTrace)
	fabricExecutor := fabric.NewFabricExecutor(catalog, localExec, remoteExec, newFabricRowEquivalence)
	// Conditional statements may promote while running. Serialize their
	// dispatch without voting WRITE before their actual body executes.
	fabricExecutor.SetConcurrentExecution(autoCommit && !contract.dynamic)
	fabricExecutor.SetGraphArgumentEvaluator(e.evaluateGraphArguments)
	fabricExecutor.SetRemoteModeUncertainty(func(query string) bool {
		contract, err := readStatementExecutionContract(query)
		return err != nil || contract.calls
	})
	fabricExecutor.SetGraphAuthorizer(func(ctx context.Context, database, query string) error {
		if err := e.authorizeSelectedDatabase(ctx, database, query); err != nil {
			return err
		}
		return e.recordStatementReference(cypher, database)
	})
	stream, err := fabricExecutor.Execute(ctx, tx, fragment, params, authToken)
	if err != nil {
		// In explicit transactions (autoCommit=false), preserve transaction lifecycle
		// for client-issued COMMIT/ROLLBACK. In autocommit mode, rollback immediately.
		if autoCommit {
			_ = tx.Rollback(nil)
		}
		return nil, fabricGraphReferenceError(err)
	}
	e.setFabricBatchedApplyRowsUsed(fabricTrace.ApplyBatchedLookupRows.Load())
	if autoCommit {
		if _, err := txphase.CommitContext(ctx); err != nil {
			_ = tx.Rollback(nil)
			return nil, err
		}
		if err := tx.Commit(nil, nil); err != nil {
			return nil, err
		}
		if tx.WriteShard() != "" {
			publishStatementCommit(ctx)
		}
	}
	if stream == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	if len(stream.Columns) == 0 {
		if inferred := e.inferTopLevelReturnColumns(cypher); len(inferred) > 0 {
			stream.Columns = inferred
		}
	}
	e.normalizeFabricRowWrapper(cypher, stream)
	s := statsAcc.snapshot()
	return resolvedStatementModeResult(&ExecuteResult{
		Columns: stream.Columns,
		Rows:    stream.Rows,
		Stats:   &s,
	}, tx.WriteShard() != ""), nil
}

type fabricPreparedExec struct {
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
	fragment, err := e.planFabricQuery(ctx, catalog, cypher, sessionDB)
	if err != nil {
		return nil, fabricGraphReferenceError(err)
	}
	return &fabricPreparedExec{
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

func (e *StorageExecutor) planFabricQuery(ctx context.Context, catalog *fabric.Catalog, cypher, sessionDB string) (fabric.Fragment, error) {
	planCacheKey := semanticCacheKey(cypher, cypherVersionFromContext(ctx) == "25")
	if e.fabricPlanCache != nil {
		if cached, found := e.fabricPlanCache.Get(planCacheKey, sessionDB); found && cached != nil {
			// The immutable plan carries the logical compiler's admission
			// verdict and frames, independently of physical result caching.
			return cached, nil
		}
	}

	plannedQuery := fabric.PreserveNameRoles(cypher)
	admittedQuery, names := canonicalizeQuotedVariables(fabric.LocalQuery(plannedQuery))
	logicalQuery := fabric.SemanticQuery(admittedQuery)
	planner := fabric.NewFabricPlanner(catalog)
	recording := newScopeRecording(admittedQuery, cypherVersionFromContext(ctx) == "25")
	recording.names = names
	admissionCtx := withQueryParams(ctx, names.parameterValues(getParamsFromContext(ctx)))
	if err := e.validateSemanticScopes(admissionCtx, logicalQuery, recording); err != nil {
		_, err = names.restore(nil, err, e.parseReturnItems)
		return nil, err
	}
	planner.SetColumnResolver(recording.resolve)
	fragment, err := planner.Plan(plannedQuery, sessionDB)
	if err != nil {
		return nil, err
	}
	if e.fabricPlanCache != nil {
		e.fabricPlanCache.Put(planCacheKey, sessionDB, fragment)
	}
	return fragment, nil
}

// fabricProcedureWritePlan resolves the registered CALL contract at statement
// admission, just as the native transaction route does. Cached structural plans
// stay immutable and do not retain an obsolete procedure mode after registration.
// The existing Fabric participant owner then handles writes and their rollback.
func fabricProcedureWritePlan(fragment fabric.Fragment) (fabric.Fragment, error) {
	switch current := fragment.(type) {
	case *fabric.FragmentExec:
		input, err := fabricProcedureWritePlan(current.Input)
		if err != nil {
			return nil, err
		}
		contract, err := readStatementExecutionContract(current.Query)
		if err != nil {
			return nil, err
		}
		write := contract.writes
		if input == current.Input && write == current.IsWrite {
			return current, nil
		}
		copy := *current
		copy.Input, copy.IsWrite = input, write
		return &copy, nil
	case *fabric.FragmentApply:
		input, err := fabricProcedureWritePlan(current.Input)
		if err != nil {
			return nil, err
		}
		inner, err := fabricProcedureWritePlan(current.Inner)
		if err != nil {
			return nil, err
		}
		if input == current.Input && inner == current.Inner {
			return current, nil
		}
		copy := *current
		copy.Input, copy.Inner = input, inner
		return &copy, nil
	case *fabric.FragmentUnion:
		left, err := fabricProcedureWritePlan(current.LHS)
		if err != nil {
			return nil, err
		}
		right, err := fabricProcedureWritePlan(current.RHS)
		if err != nil {
			return nil, err
		}
		if left == current.LHS && right == current.RHS {
			return current, nil
		}
		copy := *current
		copy.LHS, copy.RHS = left, right
		return &copy, nil
	case *fabric.FragmentLeaf:
		input, err := fabricProcedureWritePlan(current.Input)
		if err != nil {
			return nil, err
		}
		if input == current.Input {
			return current, nil
		}
		copy := *current
		copy.Input = input
		return &copy, nil
	default:
		return fragment, nil
	}
}

func (e *StorageExecutor) normalizeFabricRowWrapper(cypher string, stream *fabric.ResultStream) {
	if stream == nil || len(stream.Columns) != 1 || stream.Columns[0] != "__fabric_row" || len(stream.Rows) == 0 {
		return
	}
	inferred := e.inferTopLevelReturnColumns(cypher)
	if len(inferred) == 0 {
		return
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
		return
	}
	stream.Columns = inferred
	stream.Rows = projectedRows
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

// inferTopLevelReturnColumns best-effort derives outer RETURN columns for Fabric queries.
// This is used when a distributed execution path returns zero rows and no columns.
func (e *StorageExecutor) inferTopLevelReturnColumns(query string) []string {
	return fabric.ReturnColumns(query)
}

func (e *StorageExecutor) buildFabricCatalog() (*fabric.Catalog, error) {
	catalog := fabric.NewCatalog()
	for _, db := range e.dbManager.ListDatabases() {
		dbName := strings.TrimSpace(db.Name())
		if dbName == "" {
			continue
		}
		register := func(name string) {
			location := &fabric.LocationLocal{DBName: dbName}
			if db.Type() == "composite" {
				catalog.RegisterComposite(name, location)
			} else {
				catalog.Register(name, location)
			}
		}
		register(dbName)
		for alias := range e.dbManager.ListAliases(dbName) {
			alias = strings.TrimSpace(alias)
			if alias != "" {
				register(alias)
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
			actual, err := e.dbManager.ResolveDatabase(ref.DatabaseName)
			if err != nil {
				return nil, err
			}
			catalog.Register(qualified, &fabric.LocationLocal{DBName: actual})
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
	catalog         *fabric.Catalog
	remote          *fabric.RemoteFragmentExecutor
	base            *StorageExecutor
	authToken       string
	autoCommit      bool
	transaction     *fabric.FabricTransaction
	localDispatchMu sync.Mutex

	mu sync.Mutex
	// localTxExecBySub holds the sub-transaction executors of a statement
	// run outside an explicit transaction; an explicit transaction keeps
	// them in its TransactionContext (localShardTxExecutors).
	localTxExecBySub map[string]*StorageExecutor
}

// localShardTxExecutors is the map of sub-transaction executors by shard
// name: the explicit transaction's, so its statements share one
// sub-transaction per constituent, or else this statement's own.
func (c *cypherFabricExecutor) localShardTxExecutors() map[string]*StorageExecutor {
	if c.base != nil && c.base.txContext != nil && c.base.txContext.active {
		if c.base.txContext.fabricLocalTxExec == nil {
			c.base.txContext.fabricLocalTxExec = make(map[string]*StorageExecutor)
		}
		return c.base.txContext.fabricLocalTxExec
	}
	if c.localTxExecBySub == nil {
		c.localTxExecBySub = make(map[string]*StorageExecutor)
	}
	return c.localTxExecBySub
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
	engine := c.base.storage
	executor := c.base.cloneForStorage(engine)
	if err := executor.bindFabricStatementCacheScope(ctx, dbName); err != nil {
		return nil, nil, true, err
	}
	ctx = withExecutionDatabase(WithAuthToken(ctx, c.authToken), dbName)
	columns, rows, err := c.executeFragmentQuery(ctx, executor, query, params, recordBindings)
	return columns, rows, true, err
}

func (c *cypherFabricExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, [][]interface{}, error) {
	ctx = WithAuthToken(ctx, c.authToken)
	ctx = context.WithValue(ctx, dynamicWriteAdmissionKey{}, dynamicWriteAdmission(func(callCtx context.Context, caller *StorageExecutor, statement string) error {
		return c.promoteResolvedStatement(callCtx, caller, dbName, engine, statement)
	}))
	var exec *StorageExecutor
	if sub, ok := fabric.SubTransactionFromContext(ctx); ok {
		logical := c.fabricTransaction() != nil && c.fabricTransaction().IsCoordinatorScope(sub.ShardName)
		if logical {
			ctx = compositeStatementContext{Context: ctx}
		}
		if logical || !c.autoCommit || c.transaction != nil && c.transaction.WriteShard() == sub.ShardName {
			// Fabric has one write participant. Its local fragments share one
			// transactional executor until the coordinator admits COMMIT.
			// Concurrent read-only participants keep their independent path.
			if c.autoCommit {
				c.localDispatchMu.Lock()
				defer c.localDispatchMu.Unlock()
			}
			txExec, err := c.ensureLocalShardTxExecutor(ctx, sub, dbName, engine)
			if err != nil {
				return nil, nil, err
			}
			exec = txExec
		}
	}
	if exec == nil {
		exec = c.base.cloneForStorage(engine)
	}
	// Explicit participant dispatch is serialized. Scope the removable catalog
	// selection to this fragment without replacing the participant's owner.
	previousScope := exec.statementCacheScope
	defer func() { exec.statementCacheScope = previousScope }()
	if err := exec.bindFabricStatementCacheScope(ctx, dbName); err != nil {
		return nil, nil, err
	}
	ctx = withExecutionDatabase(ctx, dbName)
	return c.executeFragmentQuery(ctx, exec, query, params, recordBindings)
}

func (c *cypherFabricExecutor) executeFragmentQuery(ctx context.Context, exec *StorageExecutor, query string, params, recordBindings map[string]interface{}) ([]string, [][]interface{}, error) {
	previousIdentityScope := exec.graphIdentityScope
	var catalog *fabric.Catalog
	if prepared, err := exec.preparedFabricFromContext(ctx); err == nil && prepared != nil {
		catalog = prepared.catalog
	}
	exec.graphIdentityScope = newGraphIdentityScope(catalog, exec.executionDatabaseName(ctx), exec.dbManager, c.authToken)
	defer func() { exec.graphIdentityScope = previousIdentityScope }()
	query = fabric.LocalQuery(query)
	query, names := canonicalizeQuotedVariablesWithBindings(query, recordBindings)
	if names != nil {
		params = names.parameterValues(params)
		recordBindings = names.bindingValues(recordBindings)
		ctx = withQuotedVariableNames(ctx, names)
	}
	if fabric.LogicalFrameFromContext(ctx) != nil {
		globals := fabric.CallScopeFromContext(ctx).Globals
		if names != nil {
			globals = names.bindingValues(globals)
		}
		ctx = withPipelineImports(ctx, pipelineRow(globals))
	}
	previousBindings := exec.fabricRecordBindings
	exec.fabricRecordBindings = recordBindings
	defer func() { exec.fabricRecordBindings = previousBindings }()
	// The complete logical statement passed semantic admission before planning.
	// A physical fragment no longer owns or reinterprets that compile verdict.
	var result *ExecuteResult
	var err error
	if stripped, embedding := stripWithEmbeddingSuffix(query); c.autoCommit && (exec.txContext == nil || !exec.txContext.active) && (embedding || fabric.ReadQueryStructure(query).HasEmbeddingCandidate()) {
		// Inline embeddings use the normal mutation/transaction owner. Internal
		// fragment execution alone deliberately does not start an implicit tx.
		result, err = exec.executeWithImplicitTransactionCallback(ctx, query, upperASCII(query), func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
			return txExec.executeInternal(txCtx, stripped, params)
		})
	} else {
		result, err = exec.executeInternal(ctx, query, params)
	}
	result, err = names.restore(result, err, exec.parseReturnItems)
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
	return fabric.StripLeadingImportsForRecord(query, recordBindings)
}

func findLeadingWithEndLocal(query string) int {
	end, ok := fabric.LeadingWithClauseEnd(query)
	if !ok || end == len(query) {
		return -1
	}
	return end
}

func startsWithKeywordAtLocal(s string, i int, kw string) bool {
	return fabric.ClauseKeywordAt(s, i, kw)
}

func (c *cypherFabricExecutor) ensureLocalShardTxExecutor(ctx context.Context, sub *fabric.SubTransaction, dbName string, engine storage.Engine) (*StorageExecutor, error) {
	if tx := c.fabricTransaction(); tx != nil && tx.IsCoordinatorScope(sub.ShardName) {
		return c.logicalCompositeExecutor(ctx, dbName, engine)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	executors := c.localShardTxExecutors()
	if existing := executors[sub.ShardName]; existing != nil {
		return existing, nil
	}

	txExec := NewStorageExecutor(engine)
	txExec.ShareStatementCachesFrom(c.base)
	txExec.deferFlush = c.base.deferFlush
	txExec.ShareEmbedderFrom(c.base)
	txExec.searchService = c.base.searchService
	txExec.inferenceManager = c.base.inferenceManager
	txExec.onNodeMutated = c.base.onNodeMutated
	txExec.defaultEmbeddingDimensions = c.base.defaultEmbeddingDimensions
	txExec.dbManager = c.base.dbManager
	if err := txExec.bindStatementCacheScope(dbName); err != nil {
		return nil, err
	}
	txExec.vectorRegistry = c.base.vectorRegistry
	txExec.vectorIndexSpaces = c.base.vectorIndexSpaces

	beginCtx := WithTransactionControl(WithAuthToken(ctx, c.authToken))
	if _, err := txExec.Execute(beginCtx, "BEGIN", nil); err != nil {
		return nil, localizedError(localization.CypherCommandRoutingFabricShardTransactionFailed(dbName, err), err)
	}

	terminalCtx := context.WithoutCancel(beginCtx)
	if tx := c.fabricTransaction(); tx != nil {
		terminalCtx = WithTransactionControl(WithAuthToken(tx.BindContext(beginCtx), c.authToken))
	}
	commitFn := func(_ *fabric.SubTransaction) error {
		_, err := txExec.Execute(terminalCtx, "COMMIT", nil)
		return err
	}
	rollbackFn := func(_ *fabric.SubTransaction) error {
		// Local storage rollback has no context API. Always release its state;
		// the protocol terminal owner bounds its wait independently.
		_, err := txExec.Execute(context.WithoutCancel(terminalCtx), "ROLLBACK", nil)
		return err
	}
	if err := c.bindCallbacksOnce(sub, commitFn, rollbackFn); err != nil {
		return nil, err
	}

	executors[sub.ShardName] = txExec
	return txExec, nil
}

func (c *cypherFabricExecutor) bindCallbacksOnce(sub *fabric.SubTransaction, commitFn fabric.CommitCallback, rollbackFn fabric.RollbackCallback) error {
	tx := c.fabricTransaction()
	if tx == nil {
		return nil
	}
	return tx.BindParticipantCallbacks(sub.ShardName, commitFn, rollbackFn)
}

// evaluateGraphArguments evaluates a dynamic graph reference's arguments
// (USE graph.byName(g)) with the same separate row and parameter environments
// used by native execution and physical fragment adapters.
func (e *StorageExecutor) evaluateGraphArguments(ctx context.Context, expressions []string, parameters map[string]interface{}) ([]string, error) {
	records, _ := fabric.RecordBindingsFromContext(ctx)
	out := make([]string, len(expressions))
	for i, expression := range expressions {
		query, names := canonicalizeQuotedVariablesWithBindings("RETURN "+expression, records)
		bindings, params := records, parameters
		if names != nil {
			bindings, params = names.bindingValues(records), names.parameterValues(parameters)
		}
		projection := fabric.ReadQueryStructure(query).ResultClause()
		if projection == nil || projection.Projection == nil || len(projection.Projection.Items) != 1 {
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCommandRoutingGraphFunctionArgumentInvalid(expression))
		}
		value, ok := e.evaluateRowExpressionWithContext(withQueryParams(ctx, params), projection.Projection.Items[0].Expression, pipelineRow(bindings))
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
	var notFound *fabric.GraphNotFoundError
	if errors.As(err, &notFound) {
		return localizedStatusError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", localization.CypherCommandRoutingGraphNotFound(notFound.Name))
	}
	var reference *fabric.GraphReferenceError
	if errors.As(err, &reference) {
		return localizedStatusError(reference.Code, "GraphReference", reference.Message)
	}
	var querySyntax *fabric.QuerySyntaxError
	if errors.As(err, &querySyntax) {
		// Native embedded callers retain a status-bearing Error string as well
		// as the localized/typed protocol error below it.
		return fmt.Errorf("%s: %w", querySyntax.BoltErrorCode(), localizedStatusError(querySyntax.BoltErrorCode(), "UnexpectedSyntax", querySyntax.Message))
	}
	var syntax *fabric.UseSyntaxError
	if errors.As(err, &syntax) {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", syntax.Message)
	}
	return err
}

// fabricTransaction is the coordinator's participant/lifetime owner on both
// implicit and explicit dispatch; direct helper callers keep the session owner.
func (c *cypherFabricExecutor) fabricTransaction() *fabric.FabricTransaction {
	if c.transaction != nil {
		return c.transaction
	}
	if c.base != nil && c.base.txContext != nil {
		tx, _ := c.base.txContext.tx.(*fabric.FabricTransaction)
		return tx
	}
	return nil
}
