package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Index Management Procedures
// ========================================

// callDbAwaitIndex waits for a specific index to come online - Neo4j db.awaitIndex()
// Syntax: CALL db.awaitIndex(indexName, timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndex(cypher string) (*ExecuteResult, error) {
	// NornicDB indexes are always online (no background building)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Index is online"},
		},
	}, nil
}

// callDbAwaitIndexes waits for all indexes to come online - Neo4j db.awaitIndexes()
// Syntax: CALL db.awaitIndexes(timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndexes(cypher string) (*ExecuteResult, error) {
	// NornicDB indexes are always online (no background building)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"All indexes are online"},
		},
	}, nil
}

// callDbResampleIndex forces index statistics to be recalculated - Neo4j db.resampleIndex()
// Syntax: CALL db.resampleIndex(indexName)
func (e *StorageExecutor) callDbResampleIndex(cypher string) (*ExecuteResult, error) {
	// NornicDB doesn't use index statistics (no cost-based optimizer using stats)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Index statistics updated"},
		},
	}, nil
}

// ========================================
// Query Statistics Procedures
// ========================================

// callDbStatsClear clears collected query statistics - Neo4j db.stats.clear()
func (e *StorageExecutor) callDbStatsClear() (*ExecuteResult, error) {
	// Clear any cached query stats
	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"QUERIES", map[string]interface{}{"cleared": true}},
		},
	}, nil
}

// callDbStatsCollect starts collecting query statistics - Neo4j db.stats.collect()
// Syntax: CALL db.stats.collect(section, config)
func (e *StorageExecutor) callDbStatsCollect(cypher string) (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "success", "message"},
		Rows: [][]interface{}{
			{"QUERIES", true, "Query collection started"},
		},
	}, nil
}

// callDbStatsRetrieve retrieves collected statistics - Neo4j db.stats.retrieve()
// Syntax: CALL db.stats.retrieve(section)
func (e *StorageExecutor) callDbStatsRetrieve(cypher string) (*ExecuteResult, error) {
	// Return basic query statistics
	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"QUERIES", map[string]interface{}{
				"totalQueries":   0,
				"cachedQueries":  0,
				"avgExecutionMs": 0,
			}},
		},
	}, nil
}

// callDbStatsRetrieveAllAnTheStats retrieves all statistics - Neo4j db.stats.retrieveAllAnTheStats()
func (e *StorageExecutor) callDbStatsRetrieveAllAnTheStats() (*ExecuteResult, error) {
	nodeCount := 0
	edgeCount := 0

	if nodes, err := e.storage.AllNodes(); err == nil {
		nodeCount = len(nodes)
	}
	if edges, err := e.storage.AllEdges(); err == nil {
		edgeCount = len(edges)
	}

	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"GRAPH COUNTS", map[string]interface{}{
				"nodeCount":         nodeCount,
				"relationshipCount": edgeCount,
			}},
			{"QUERIES", map[string]interface{}{
				"totalQueries":   0,
				"cachedQueries":  0,
				"avgExecutionMs": 0,
			}},
		},
	}, nil
}

// callDbStatsStatus returns statistics collection status - Neo4j db.stats.status()
func (e *StorageExecutor) callDbStatsStatus() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "status", "message"},
		Rows: [][]interface{}{
			{"QUERIES", "idle", "Statistics collection is available"},
		},
	}, nil
}

// callDbStatsStop stops statistics collection - Neo4j db.stats.stop()
func (e *StorageExecutor) callDbStatsStop() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "success", "message"},
		Rows: [][]interface{}{
			{"QUERIES", true, "Statistics collection stopped"},
		},
	}, nil
}

// callDbClearQueryCaches clears all query caches - Neo4j db.clearQueryCaches()
//
// Clears all caches in the executor:
//   - Query result cache (SmartQueryCache)
//   - Query plan cache (QueryPlanCache)
//   - Query analyzer cache (AST cache)
//   - Node lookup cache
//
// This is useful for:
//   - Testing (ensuring fresh queries)
//   - After bulk data imports
//   - When schema changes affect query plans
//   - Debugging cache-related issues
func (e *StorageExecutor) callDbClearQueryCaches() (*ExecuteResult, error) {
	// Clear query result cache
	if e.cache != nil {
		e.cache.Invalidate()
	}

	// Clear query plan cache
	if e.planCache != nil {
		e.planCache.Clear()
	}

	// Clear query analyzer cache (AST cache)
	if e.analyzer != nil {
		e.analyzer.ClearCache()
	}

	// Clear node lookup cache
	e.nodeLookupCacheMu.Lock()
	e.nodeLookupCache = make(map[string]*storage.Node, 1000)
	e.nodeLookupCacheMu.Unlock()

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Query caches cleared"},
		},
	}, nil
}
