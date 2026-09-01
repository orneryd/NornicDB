package server

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	infos := r.dbManager.ListDatabases()
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		if info == nil || info.Name == "" {
			continue
		}
		names = append(names, info.Name)
	}
	sort.Strings(names)
	return names
}

func (r *heimdallDBRouter) storageForDatabase(database string) (dbName string, engine storage.Engine, err error) {
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

	// Fast path: cached executor
	r.executorsMu.RLock()
	if r.executors != nil {
		if cached := r.executors[dbName]; cached != nil {
			r.executorsMu.RUnlock()
			return dbName, cached, engine, nil
		}
	}
	r.executorsMu.RUnlock()

	// Create executor scoped to this database.
	exec = cypher.NewStorageExecutor(engine)
	if r.db != nil {
		if baseExecutor := r.db.GetCypherExecutor(); baseExecutor != nil {
			exec.SetLocalizationRenderer(baseExecutor.GetLocalizationRenderer())
		}
	}

	// Enable multi-db commands (SHOW/CREATE/DROP DATABASE...) when supported.
	// The adapter lives in server_db.go (same package), so we can reuse it here.
	if r.dbManager != nil {
		exec.SetDatabaseManager(&databaseManagerAdapter{manager: r.dbManager, db: r.db, server: nil})
	}

	// Reuse DB's cached search service instead of creating a new one.
	if r.db != nil {
		if searchSvc, err := r.db.GetOrCreateSearchService(dbName, engine); err == nil && searchSvc != nil {
			exec.SetSearchService(searchSvc)
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

	// Cache executor
	r.executorsMu.Lock()
	if r.executors == nil {
		r.executors = make(map[string]*cypher.StorageExecutor)
	}
	if existing := r.executors[dbName]; existing != nil {
		r.executorsMu.Unlock()
		return dbName, existing, engine, nil
	}
	r.executors[dbName] = exec
	r.executorsMu.Unlock()

	return dbName, exec, engine, nil
}

func (r *heimdallDBRouter) Query(ctx context.Context, database string, cypherQuery string, params map[string]interface{}) ([]map[string]interface{}, error) {
	_, exec, _, err := r.executorForDatabase(database)
	if err != nil {
		return nil, err
	}

	result, err := exec.Execute(ctx, cypherQuery, params)
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

func (r *heimdallDBRouter) Stats(database string) (heimdall.DatabaseStats, error) {
	dbName, engine, err := r.storageForDatabase(database)
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
	dbName, engine, err := r.storageForDatabase(database)
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
	svc, buildErr := r.db.EnsureSearchIndexesBuilt(ctx, dbName, engine)
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
			sr.Related = r.getRelatedNodes(engine, r0.ID, depth)
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
	if depth < 1 || engine == nil {
		return nil
	}

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
				Title:        getStringProperty(node.Properties, "title"),
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
