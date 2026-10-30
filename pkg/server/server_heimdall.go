package server

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ==========================================================================
// Heimdall Database/Metrics Wrappers
// ==========================================================================

// heimdallDBRouter wraps NornicDB + DatabaseManager for Heimdall's DatabaseRouter interface.
// This is multi-database aware and routes all operations through multidb namespaces.
type heimdallDBRouter struct {
	db        *nornicdb.DB
	dbManager *multidb.DatabaseManager
	features  *nornicConfig.FeatureFlagsConfig

	statementCacheIndex multidb.StatementCacheIndex

	executorsMu sync.RWMutex
	executors   map[string]*cypher.StorageExecutor
}

func newHeimdallDBRouter(db *nornicdb.DB, dbManager *multidb.DatabaseManager, features *nornicConfig.FeatureFlagsConfig) *heimdallDBRouter {
	return &heimdallDBRouter{
		db:        db,
		dbManager: dbManager,
		features:  features,
		executors: make(map[string]*cypher.StorageExecutor),
	}
}

func (r *heimdallDBRouter) DefaultDatabaseName() string {
	if r.dbManager != nil {
		return r.dbManager.DefaultDatabaseName()
	}
	// Fallback for edge-cases (should not happen in server mode)
	return "nornic"
}

func (r *heimdallDBRouter) ResolveDatabase(nameOrAlias string) (string, error) {
	if r.dbManager == nil {
		if nameOrAlias == "" {
			return r.DefaultDatabaseName(), nil
		}
		return nameOrAlias, nil
	}
	if nameOrAlias == "" {
		return r.dbManager.DefaultDatabaseName(), nil
	}
	return r.dbManager.ResolveDatabase(nameOrAlias)
}

func (r *heimdallDBRouter) ListDatabases() []string {
	if r.dbManager == nil {
		return []string{r.DefaultDatabaseName()}
	}
	names := r.dbManager.ListDatabaseNames()
	sort.Strings(names)
	return names
}

func (r *heimdallDBRouter) storageForDatabase(database string) (dbName string, engine storage.Engine, err error) {
	r.bindStatementCacheIndex()
	dbName, err = r.ResolveDatabase(database)
	if err != nil {
		return "", nil, err
	}
	if r.dbManager == nil {
		// Best-effort fallback: server mode should always have dbManager.
		if r.db == nil {
			return "", nil, fmt.Errorf("database router not initialized")
		}
		return dbName, r.db.GetStorage(), nil
	}
	engine, err = r.dbManager.GetStorage(dbName)
	if err != nil {
		return "", nil, err
	}
	return dbName, engine, nil
}

func (r *heimdallDBRouter) executorForDatabase(database string) (dbName string, exec *cypher.StorageExecutor, engine storage.Engine, err error) {
	dbName, engine, err = r.storageForDatabase(database)
	if err != nil {
		return "", nil, nil, err
	}
	selection := database
	if selection == "" {
		selection = dbName
	}
	if cached := r.cachedExecutor(selection); cached != nil {
		return dbName, cached, engine, nil
	}
	exec, err = r.newExecutor(engine, selection)
	if err != nil {
		return "", nil, nil, err
	}
	r.finishExecutor(dbName, engine, exec)
	return dbName, r.publishExecutor(selection, exec), engine, nil
}

// cachedExecutor is the executor the router shares for dbName, or nil.
func (r *heimdallDBRouter) cachedExecutor(dbName string) *cypher.StorageExecutor {
	r.executorsMu.RLock()
	defer r.executorsMu.RUnlock()
	cached := r.executors[dbName]
	if cached != nil && cached.StatementCacheSelectionValid() {
		return cached
	}
	return nil
}

// newExecutor is an executor scoped to engine with the base executor's
// embedder, localization and logging, and multi-database commands, but not
// yet the database's search service or embed queue (finishExecutor).
func (r *heimdallDBRouter) newExecutor(engine storage.Engine, database string) (*cypher.StorageExecutor, error) {
	exec := cypher.NewStorageExecutor(engine)
	if r.dbManager != nil {
		if err := exec.UseDatabaseStatementCaches(r.dbManager, database); err != nil {
			return nil, err
		}
	} else if r.db != nil {
		exec.ShareStatementCachesFrom(r.db.GetCypherExecutor())
	}
	if r.db != nil {
		if baseExecutor := r.db.GetCypherExecutor(); baseExecutor != nil {
			exec.ShareEmbedderFrom(baseExecutor)
			exec.ShareInferenceManagerFrom(baseExecutor)
			exec.SetLocalizationRenderer(baseExecutor.GetLocalizationRenderer())
			// NornicDB issue #563 defect 1: without this, Heimdall-routed
			// queries against a non-default database log to io.Discard and
			// never emit a slow_query record.
			exec.SetLogger(baseExecutor.Logger())
			exec.SetSlowQueryThreshold(baseExecutor.SlowQueryThreshold())
		}
	}

	// Enable multi-db commands (SHOW/CREATE/DROP DATABASE...) when supported.
	// The adapter lives in server_db.go (same package), so we can reuse it here.
	if r.dbManager != nil {
		exec.SetDatabaseManager(&databaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(r.dbManager), db: r.db, server: nil})
	}
	return exec, nil
}

// finishExecutor gives exec dbName's search service and the embed queue.
func (r *heimdallDBRouter) finishExecutor(dbName string, engine storage.Engine, exec *cypher.StorageExecutor) {
	// Reuse DB's cached search service instead of creating a new one.
	if r.db != nil {
		if searchSvc, err := r.db.GetOrCreateSearchService(dbName, engine); err == nil && searchSvc != nil {
			exec.SetSearchServiceResolver(dbName, r.db.SearchServiceResolver())
		}
	}

	// Wire embed queue so nodes created/mutated via Heimdall (e.g. OrderStatus CREATE) get embeddings.
	if r.db != nil {
		if q := r.db.GetEmbedQueue(); q != nil {
			exec.SetNodeMutatedCallback(func(nodeID string) {
				q.Enqueue(nodeID)
			})
		}
	}
}

// publishExecutor shares exec as dbName's executor, or returns the one
// another call shared first.
func (r *heimdallDBRouter) publishExecutor(dbName string, exec *cypher.StorageExecutor) *cypher.StorageExecutor {
	r.executorsMu.Lock()
	defer r.executorsMu.Unlock()
	if r.executors == nil {
		r.executors = make(map[string]*cypher.StorageExecutor)
	}
	if existing := r.executors[dbName]; existing != nil && existing.StatementCacheSelectionValid() {
		return existing
	}
	if !exec.StatementCacheSelectionValid() {
		return exec
	}
	r.executors[dbName] = exec
	return exec
}

// statementExecutor is the executor a statement runs on in dbName. The
// shared one when there is one; otherwise a new executor, which gets the
// database's search service and embed queue and is shared only once it has
// admitted the statement (cypher.WithStatementAdmitted), so a statement the
// principal may not run there opens no search service and leaves nothing
// cached. It returns ctx with that hook.
func (r *heimdallDBRouter) statementExecutor(ctx context.Context, database string) (context.Context, *cypher.StorageExecutor, error) {
	dbName, engine, err := r.storageForDatabase(database)
	if err != nil {
		return nil, nil, err
	}
	if cached := r.cachedExecutor(dbName); cached != nil {
		return ctx, cached, nil
	}
	exec, err := r.newExecutor(engine, dbName)
	if err != nil {
		return nil, nil, err
	}
	var admit util.OnceCompletion
	ctx = cypher.WithStatementAdmitted(ctx, exec, func() {
		admit.Run(func() {
			r.finishExecutor(dbName, engine, exec)
			r.publishExecutor(dbName, exec)
		})
	})
	return ctx, exec, nil
}

// authorizeDatabase is the request principal's decision on the database a
// call selects, taken before the router resolves or opens anything. The
// server attaches the principal to the request (withBifrostRBAC): its one
// permission resolver, which HTTP, Bolt and GraphQL use, and the databases
// and aliases it may select. A selection outside that scope, or one where
// the principal lacks permission, gets the same Forbidden error naming the
// selection as given. Otherwise it returns the canonical name and ctx
// carrying the resolver for the executor, which then authorizes each
// statement as on the other transports. A context the server marked as
// served with authentication disabled resolves the name as given; a context
// with no principal and no such mark, a principal without its database scope
// (auth.WithRequestPrincipal attaches both), or no context at all is refused.
func (r *heimdallDBRouter) authorizeDatabase(ctx context.Context, database string, permission auth.Permission) (context.Context, string, error) {
	if ctx == nil {
		return nil, "", r.databaseForbidden(database)
	}
	allows := auth.RequestEntitlementResolverFromContext(ctx)
	if allows == nil {
		if !auth.RequestAuthenticationDisabledFromContext(ctx) {
			return nil, "", r.databaseForbidden(database)
		}
		dbName, err := r.ResolveDatabase(database)
		if err != nil {
			return nil, "", err
		}
		return ctx, dbName, nil
	}
	scope := auth.RequestDatabaseScopeFromContext(ctx)
	if scope == nil {
		return nil, "", r.databaseForbidden(database)
	}
	dbName, selectable := scope.Resolve(database)
	if !selectable {
		return nil, "", r.databaseForbidden(database)
	}
	if !allows(dbName, permission) {
		if permission == auth.Permission("access") {
			return nil, "", r.databaseForbidden(database)
		}
		return nil, "", &cypher.PermissionDeniedError{Permission: string(permission)}
	}
	return cypher.WithDatabasePermissionResolver(ctx, dbName, func(database, permission string) bool {
		return allows(database, auth.Permission(permission))
	}), dbName, nil
}

// databaseForbidden is Neo4j's Forbidden error for the selection as the
// caller gave it (the default database's name when it gave none).
func (r *heimdallDBRouter) databaseForbidden(database string) error {
	if database == "" {
		database = r.DefaultDatabaseName()
	}
	denied := localization.DatabaseAccessDenied(database)
	return localization.NewLocalizedError("Neo.ClientError.Security.Forbidden", denied, nil)
}

// requireNonComposite refuses a composite database for the calls that read
// one database's storage directly (Stats, Discover), as the REST search and
// similarity routes do: they would read every constituent under the one
// decision on the composite. Cypher (Query) authorizes each constituent.
func (r *heimdallDBRouter) requireNonComposite(dbName string) error {
	if r.dbManager != nil && r.dbManager.IsCompositeDatabase(dbName) {
		return fmt.Errorf("composite database '%s' is not supported here; target a constituent database explicitly", dbName)
	}
	return nil
}

func (r *heimdallDBRouter) Query(ctx context.Context, database string, cypherQuery string, params map[string]interface{}) ([]map[string]interface{}, error) {
	ctx, dbName, err := r.authorizeDatabase(ctx, database, auth.Permission("access"))
	if err != nil {
		return nil, err
	}
	ctx, exec, err := r.statementExecutor(ctx, dbName)
	if err != nil {
		return nil, err
	}

	result, err := exec.Execute(cypher.WithClientStatement(ctx), cypherQuery, params)
	if err != nil {
		return nil, err
	}

	// Convert result to []map[string]interface{}
	var rows []map[string]interface{}
	for _, row := range result.Rows {
		rowMap := make(map[string]interface{})
		for i, col := range result.Columns {
			if i < len(row) {
				rowMap[col] = row[i]
			}
		}
		rows = append(rows, rowMap)
	}
	return rows, nil
}

func (r *heimdallDBRouter) Stats(ctx context.Context, database string) (heimdall.DatabaseStats, error) {
	_, dbName, err := r.authorizeDatabase(ctx, database, auth.PermRead)
	if err != nil {
		return heimdall.DatabaseStats{}, err
	}
	if err := r.requireNonComposite(dbName); err != nil {
		return heimdall.DatabaseStats{}, err
	}
	dbName, engine, err := r.storageForDatabase(dbName)
	if err != nil {
		return heimdall.DatabaseStats{}, err
	}

	nodeCount, _ := engine.NodeCount()
	edgeCount, _ := engine.EdgeCount()

	result := heimdall.DatabaseStats{
		NodeCount:         nodeCount,
		RelationshipCount: edgeCount,
		LabelCounts:       make(map[string]int64), // Label counts not yet implemented (future enhancement)
	}

	// Add search/cluster stats if available (not supported for system database).
	if r.db != nil && dbName != "system" {
		if svc, svcErr := r.db.GetOrCreateSearchService(dbName, engine); svcErr == nil && svc != nil {
			// ClusterStats is only available when clustering is enabled and has run.
			if cs := svc.ClusterStats(); cs != nil {
				result.ClusterStats = &heimdall.ClusterStats{
					NumClusters:    cs.NumClusters,
					EmbeddingCount: cs.EmbeddingCount,
					IsClustered:    cs.Clustered,
					AvgClusterSize: cs.AvgClusterSize,
					Iterations:     cs.Iterations,
				}
			} else {
				// Still report embedding count even if not clustered.
				result.ClusterStats = &heimdall.ClusterStats{
					NumClusters:    0,
					EmbeddingCount: svc.EmbeddingCount(),
					IsClustered:    false,
				}
			}
		}
	}

	// Add feature flags
	if r.features != nil {
		// Derive clustering enabled from cluster stats
		clusteringEnabled := false
		if result.ClusterStats != nil {
			clusteringEnabled = result.ClusterStats.NumClusters > 0 || result.ClusterStats.EmbeddingCount > 0
		}

		asyncWritesEnabled := false
		if r.db != nil {
			asyncWritesEnabled = r.db.IsAsyncWritesEnabled()
		}

		result.FeatureFlags = &heimdall.FeatureFlags{
			HeimdallEnabled:          r.features.HeimdallEnabled,
			HeimdallAnomalyDetection: r.features.HeimdallAnomalyDetection,
			HeimdallRuntimeDiagnosis: r.features.HeimdallRuntimeDiagnosis,
			HeimdallMemoryCuration:   r.features.HeimdallMemoryCuration,
			ClusteringEnabled:        clusteringEnabled,
			TopologyEnabled:          r.features.TopologyAutoIntegrationEnabled,
			KalmanEnabled:            r.features.KalmanEnabled,
			AsyncWritesEnabled:       asyncWritesEnabled,
		}
	}

	return result, nil
}

// Discover implements semantic search with graph traversal for Graph-RAG.
func (r *heimdallDBRouter) Discover(ctx context.Context, database string, query string, nodeTypes []string, limit int, depth int) (*heimdall.DiscoverResult, error) {
	ctx, dbName, err := r.authorizeDatabase(ctx, database, auth.PermRead)
	if err != nil {
		return nil, err
	}
	if err := r.requireNonComposite(dbName); err != nil {
		return nil, err
	}
	dbName, engine, err := r.storageForDatabase(dbName)
	if err != nil {
		return nil, err
	}
	if r.db == nil {
		return nil, fmt.Errorf("search not available: database not initialized")
	}
	if dbName == "system" {
		return nil, fmt.Errorf("search not available for system database")
	}
	if limit <= 0 {
		limit = 10
	}

	// Ensure indexes are built (best-effort: if build fails, we still attempt search).
	svc, buildErr := r.db.EnsureSearchIndexes(ctx, dbName, engine, nornicdb.SearchIndexBuildWait)
	if svc == nil {
		if buildErr != nil {
			return nil, buildErr
		}
		return nil, fmt.Errorf("search service not initialized")
	}

	// Keyword search (BM25 / hybrid service in keyword-only mode).
	opts := search.GetAdaptiveRRFConfig(query)
	opts.Limit = limit
	if len(nodeTypes) > 0 {
		opts.Types = nodeTypes
	}
	response, err := svc.Search(ctx, query, nil, opts)
	if err != nil {
		return nil, err
	}

	results := make([]heimdall.SearchResult, 0, len(response.Results))
	for _, r0 := range response.Results {
		sr := heimdall.SearchResult{
			ID:         r0.ID,
			Type:       getFirstLabel(r0.Labels),
			Title:      getStringProperty(r0.Properties, "title"),
			Similarity: r0.Score,
			Properties: r0.Properties,
		}

		// Content preview
		if content := getStringProperty(r0.Properties, "content"); content != "" {
			if len(content) > 200 {
				sr.ContentPreview = content[:200] + "..."
			} else {
				sr.ContentPreview = content
			}
		}

		// Get related nodes when depth >= 1 (1 = direct neighbors, 2+ = multi-hop)
		if depth >= 1 {
			sr.Related = r.getRelatedNodesContext(ctx, engine, r0.ID, depth)
		}

		results = append(results, sr)
	}

	// If index build errored, surface that as a best-effort warning by returning
	// results anyway (caller can decide how to present this).
	_ = buildErr

	return &heimdall.DiscoverResult{
		Results: results,
		Method:  "keyword",
		Total:   len(results),
	}, nil
}

// getRelatedNodes fetches connected nodes up to the specified depth (BFS).
func (r *heimdallDBRouter) getRelatedNodes(engine storage.Engine, nodeID string, depth int) []heimdall.RelatedNode {
	return r.getRelatedNodesContext(context.Background(), engine, nodeID, depth)
}

func (r *heimdallDBRouter) getRelatedNodesContext(ctx context.Context, engine storage.Engine, nodeID string, depth int) []heimdall.RelatedNode {
	if depth < 1 || engine == nil {
		return nil
	}

	_, visible, release := storage.BeginQueryResultProjectionContext(ctx, engine)
	defer release()

	var related []heimdall.RelatedNode
	visited := map[string]bool{nodeID: true}

	type queueItem struct {
		id       string
		distance int
	}
	queue := []queueItem{{id: nodeID, distance: 0}}

	for len(queue) > 0 && len(related) < 50 {
		current := queue[0]
		queue = queue[1:]

		if current.distance >= depth {
			continue
		}

		out, _ := engine.GetOutgoingEdges(storage.NodeID(current.id))
		in, _ := engine.GetIncomingEdges(storage.NodeID(current.id))

		type edgeRef struct {
			edge      *storage.Edge
			direction string
		}
		edgeRefs := make([]edgeRef, 0, len(out)+len(in))
		for _, e := range out {
			edgeRefs = append(edgeRefs, edgeRef{edge: e, direction: "outgoing"})
		}
		for _, e := range in {
			edgeRefs = append(edgeRefs, edgeRef{edge: e, direction: "incoming"})
		}

		for _, er := range edgeRefs {
			e := er.edge
			if e == nil {
				continue
			}

			var neighborID string
			if string(e.StartNode) == current.id {
				neighborID = string(e.EndNode)
			} else {
				neighborID = string(e.StartNode)
			}

			if neighborID == "" || visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			node, err := engine.GetNode(storage.NodeID(neighborID))
			if err != nil || node == nil {
				continue
			}

			rel := heimdall.RelatedNode{
				ID:           neighborID,
				Type:         getFirstLabel(node.Labels),
				Title:        getStringProperty(visible.NodeProperties(node), "title"),
				Distance:     current.distance + 1,
				Relationship: e.Type,
				Direction:    er.direction,
			}
			related = append(related, rel)

			if current.distance+1 < depth {
				queue = append(queue, queueItem{id: neighborID, distance: current.distance + 1})
			}
		}
	}

	return related
}

// getFirstLabel returns the first label or empty string.
func getFirstLabel(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}

// getStringProperty extracts a string property from a map.
func getStringProperty(props map[string]interface{}, key string) string {
	if props == nil {
		return ""
	}
	if v, ok := props[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// heimdallMetricsReader provides runtime metrics for Heimdall.
type heimdallMetricsReader struct{}

func (r *heimdallMetricsReader) Runtime() heimdall.RuntimeMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return heimdall.RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  m.Alloc / 1024 / 1024,
		NumGC:          m.NumGC,
	}
}

func (r *heimdallDBRouter) bindStatementCacheIndex() {
	r.statementCacheIndex.Bind(r.dbManager, func() {
		r.executorsMu.Lock()
		defer r.executorsMu.Unlock()
		for name, executor := range r.executors {
			if !executor.StatementCacheSelectionValid() {
				delete(r.executors, name)
			}
		}
	})
}

func (r *heimdallDBRouter) Close() {
	r.statementCacheIndex.Close()
	r.executorsMu.Lock()
	r.executors = nil
	r.executorsMu.Unlock()
}
