package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type fabricStatsAccumulatorKey struct{}
type fabricPreparedExecKey struct{}

type fabricStatsAccumulator struct {
	mu    sync.Mutex
	stats QueryStats
}

func (a *fabricStatsAccumulator) add(in *QueryStats) {
	if a == nil || in == nil {
		return
	}
	a.mu.Lock()
	a.stats.NodesCreated += in.NodesCreated
	a.stats.NodesDeleted += in.NodesDeleted
	a.stats.RelationshipsCreated += in.RelationshipsCreated
	a.stats.RelationshipsDeleted += in.RelationshipsDeleted
	a.stats.PropertiesSet += in.PropertiesSet
	a.stats.LabelsAdded += in.LabelsAdded
	a.mu.Unlock()
}

func (a *fabricStatsAccumulator) snapshot() QueryStats {
	if a == nil {
		return QueryStats{}
	}
	a.mu.Lock()
	out := a.stats
	a.mu.Unlock()
	return out
}

func fabricStatsAccumulatorFromContext(ctx context.Context) *fabricStatsAccumulator {
	v := ctx.Value(fabricStatsAccumulatorKey{})
	if v == nil {
		return nil
	}
	acc, _ := v.(*fabricStatsAccumulator)
	return acc
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
		target, _, hasUse, err := parseLeadingUseClause(cypher[useIdx:])
		if hasUse && err == nil && e.useTargetRequiresFabric(target) {
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
	catalog := prepared.catalog

	authToken := GetAuthTokenFromContext(ctx)
	localExec := fabric.NewLocalFragmentExecutor(&cypherFabricExecutor{
		base:       e,
		authToken:  authToken,
		autoCommit: autoCommit,
	}, func(dbName string) (storage.Engine, error) {
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

	statsAcc := &fabricStatsAccumulator{}
	ctx = context.WithValue(ctx, fabricStatsAccumulatorKey{}, statsAcc)
	fabricTrace := &fabric.HotPathTrace{}
	ctx = fabric.WithHotPathTrace(ctx, fabricTrace)
	fabricExecutor := fabric.NewFabricExecutor(catalog, localExec, remoteExec)
	stream, err := fabricExecutor.Execute(ctx, tx, prepared.fragment, params, authToken)
	if err != nil {
		// In explicit transactions (autoCommit=false), preserve transaction lifecycle
		// for client-issued COMMIT/ROLLBACK. In autocommit mode, rollback immediately.
		if autoCommit {
			_ = tx.Rollback(nil)
		}
		return nil, err
	}
	e.setFabricBatchedApplyRowsUsed(fabricTrace.ApplyBatchedLookupRows)
	if autoCommit {
		if err := tx.Commit(nil, nil); err != nil {
			return nil, err
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
	return &ExecuteResult{
		Columns: stream.Columns,
		Rows:    stream.Rows,
		Stats:   &s,
	}, nil
}

type fabricPreparedExec struct {
	catalog   *fabric.Catalog
	sessionDB string
	fragment  fabric.Fragment
	hasRemote bool
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
		return nil, err
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

func (e *StorageExecutor) planFabricQuery(catalog *fabric.Catalog, cypher, sessionDB string) (fabric.Fragment, error) {
	if e.fabricPlanCache != nil {
		if cached, found := e.fabricPlanCache.Get(cypher, sessionDB); found && cached != nil {
			return cached, nil
		}
	}

	planner := fabric.NewFabricPlanner(catalog)
	fragment, err := planner.Plan(cypher, sessionDB)
	if err != nil {
		return nil, err
	}
	if e.fabricPlanCache != nil {
		e.fabricPlanCache.Put(cypher, sessionDB, fragment)
	}
	return fragment, nil
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
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true

	lastReturn := -1
	searchFrom := 0
	for {
		idx := keywordIndexFrom(query, "RETURN", searchFrom, opts)
		if idx < 0 {
			break
		}
		lastReturn = idx
		searchFrom = idx + len("RETURN")
	}
	if lastReturn < 0 {
		return nil
	}

	clause := strings.TrimSpace(query[lastReturn+len("RETURN"):])
	if clause == "" {
		return nil
	}

	// Strip trailing ORDER BY / SKIP / LIMIT at top-level.
	end := len(clause)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(clause, kw); idx >= 0 && idx < end {
			end = idx
		}
	}
	clause = strings.TrimSpace(clause[:end])
	clause = strings.TrimSuffix(clause, ";")
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return nil
	}

	items := e.parseReturnItems(clause)
	if len(items) == 0 {
		return nil
	}
	return e.buildColumnsFromReturnItems(items)
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

	mu               sync.Mutex
	localTxExecBySub map[string]*StorageExecutor
}

func (c *cypherFabricExecutor) ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	return c.ExecuteQueryWithRecord(ctx, dbName, engine, query, params, nil)
}

func (c *cypherFabricExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, [][]interface{}, error) {
	ctx = WithAuthToken(ctx, c.authToken)

	exec := c.base.cloneForStorage(engine)
	if len(recordBindings) > 0 {
		exec.fabricRecordBindings = recordBindings
		query = stripLeadingWithImportsForFabricRecord(query, recordBindings)
	}
	if !c.autoCommit {
		if sub, ok := fabric.SubTransactionFromContext(ctx); ok {
			txExec, err := c.ensureLocalShardTxExecutor(ctx, sub, dbName, engine)
			if err != nil {
				return nil, nil, err
			}
			exec = txExec
			if len(recordBindings) > 0 {
				exec.fabricRecordBindings = recordBindings
			}
		}
	}

	result, err := exec.executeInternal(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return []string{}, [][]interface{}{}, nil
	}
	if acc := fabricStatsAccumulatorFromContext(ctx); acc != nil {
		acc.add(result.Stats)
	}
	return result.Columns, result.Rows, nil
}

func stripLeadingWithImportsForFabricRecord(query string, recordBindings map[string]interface{}) string {
	if len(recordBindings) == 0 {
		return query
	}
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "WITH ") {
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
	imports := splitCommaTopLevelLocal(withClause)
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
	for _, item := range imports {
		name := strings.TrimSpace(item)
		rewritten = replaceIdentifierOutsideQuotes(rewritten, name, "$"+name)
	}
	return rewritten
}

func findLeadingWithEndLocal(query string) int {
	inSingle, inDouble, inBacktick := false, false, false
	depth := 0
	for i := len("WITH "); i < len(query); i++ {
		ch := query[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if startsWithKeywordAtLocal(query, i, "MATCH") ||
			startsWithKeywordAtLocal(query, i, "OPTIONAL MATCH") ||
			startsWithKeywordAtLocal(query, i, "RETURN") ||
			startsWithKeywordAtLocal(query, i, "WHERE") ||
			startsWithKeywordAtLocal(query, i, "USE") ||
			startsWithKeywordAtLocal(query, i, "WITH") ||
			startsWithKeywordAtLocal(query, i, "CALL") ||
			startsWithKeywordAtLocal(query, i, "CREATE") ||
			startsWithKeywordAtLocal(query, i, "MERGE") ||
			startsWithKeywordAtLocal(query, i, "UNWIND") ||
			startsWithKeywordAtLocal(query, i, "SET") ||
			startsWithKeywordAtLocal(query, i, "DELETE") ||
			startsWithKeywordAtLocal(query, i, "DETACH DELETE") {
			return i
		}
	}
	return -1
}

func startsWithKeywordAtLocal(s string, i int, kw string) bool {
	if i < 0 || i+len(kw) > len(s) {
		return false
	}
	if i > 0 {
		prev := s[i-1]
		if (prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z') || (prev >= '0' && prev <= '9') || prev == '_' {
			return false
		}
	}
	if !strings.EqualFold(s[i:i+len(kw)], kw) {
		return false
	}
	j := i + len(kw)
	if j < len(s) {
		next := s[j]
		if (next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') || next == '_' {
			return false
		}
	}
	return true
}

func splitCommaTopLevelLocal(s string) []string {
	var parts []string
	start := 0
	depth := 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func (c *cypherFabricExecutor) ensureLocalShardTxExecutor(ctx context.Context, sub *fabric.SubTransaction, dbName string, engine storage.Engine) (*StorageExecutor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.localTxExecBySub == nil {
		c.localTxExecBySub = make(map[string]*StorageExecutor)
	}
	if existing := c.localTxExecBySub[sub.ShardName]; existing != nil {
		return existing, nil
	}

	txExec := NewStorageExecutor(engine)
	txExec.deferFlush = c.base.deferFlush
	txExec.embedder = c.base.embedder
	txExec.searchService = c.base.searchService
	txExec.inferenceManager = c.base.inferenceManager
	txExec.onNodeMutated = c.base.onNodeMutated
	txExec.defaultEmbeddingDimensions = c.base.defaultEmbeddingDimensions
	txExec.dbManager = c.base.dbManager
	txExec.vectorRegistry = c.base.vectorRegistry
	txExec.vectorIndexSpaces = c.base.vectorIndexSpaces

	beginCtx := WithAuthToken(ctx, c.authToken)
	if _, err := txExec.Execute(beginCtx, "BEGIN", nil); err != nil {
		return nil, localizedError(localization.CypherCommandRoutingFabricShardTransactionFailed(dbName, err), err)
	}

	commitFn := func(_ *fabric.SubTransaction) error {
		_, err := txExec.Execute(beginCtx, "COMMIT", nil)
		return err
	}
	rollbackFn := func(_ *fabric.SubTransaction) error {
		_, err := txExec.Execute(beginCtx, "ROLLBACK", nil)
		return err
	}
	if err := c.bindCallbacksOnce(sub, commitFn, rollbackFn); err != nil {
		return nil, err
	}

	c.localTxExecBySub[sub.ShardName] = txExec
	return txExec, nil
}

func (c *cypherFabricExecutor) bindCallbacksOnce(sub *fabric.SubTransaction, commitFn fabric.CommitCallback, rollbackFn fabric.RollbackCallback) error {
	if c.base == nil || c.base.txContext == nil {
		return nil
	}
	tx, ok := c.base.txContext.tx.(*fabric.FabricTransaction)
	if !ok || tx == nil {
		return nil
	}
	return tx.BindParticipantCallbacks(sub.ShardName, commitFn, rollbackFn)
}
