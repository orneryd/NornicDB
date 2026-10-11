package cypher

import (
	"context"
	"fmt"
)

// ========================================
// Index Management Procedures
// ========================================

// callDbAwaitIndex waits for a specific index to come online - Neo4j db.awaitIndex()
// Syntax: CALL db.awaitIndex(indexName, timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndex(cypher string) (*ExecuteResult, error) {
	arguments, err := extractCallArguments(cypher)
	if err != nil {
		return nil, err
	}
	return e.callNamedIndexManagement(arguments)
}

// callDbAwaitIndexes waits for all indexes to come online - Neo4j db.awaitIndexes()
// Syntax: CALL db.awaitIndexes(timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndexes() (*ExecuteResult, error) {
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callDbResampleIndex forces index statistics to be recalculated - Neo4j db.resampleIndex()
// Syntax: CALL db.resampleIndex(indexName)
func (e *StorageExecutor) callDbResampleIndex(cypher string) (*ExecuteResult, error) {
	arguments, err := extractCallArguments(cypher)
	if err != nil {
		return nil, err
	}
	return e.callNamedIndexManagement(arguments)
}

func (e *StorageExecutor) callNamedIndexManagement(arguments []interface{}) (*ExecuteResult, error) {
	if len(arguments) == 0 {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments", "an index name is required")
	}
	name, valid := arguments[0].(string)
	if !valid {
		return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "the index name must be a STRING")
	}
	for _, entry := range e.storage.GetSchema().GetIndexes() {
		if index, ok := entry.(map[string]interface{}); ok && index["name"] == name {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
	}
	return nil, newSemanticError("Neo.ClientError.Schema.IndexNotFound", "IndexNotFound", fmt.Sprintf("No such index '%s'", name))
}

// ========================================
// Query Statistics Procedures
// ========================================

// callDbStatsClear clears collected query statistics - Neo4j db.stats.clear()
func (e *StorageExecutor) callDbStatsClear() (*ExecuteResult, error) {
	return e.callQueryStatistics(context.Background(), "clear", nil)
}

// callDbStatsCollect starts collecting query statistics - Neo4j db.stats.collect()
// Syntax: CALL db.stats.collect(section, config)
func (e *StorageExecutor) callDbStatsCollect(cypher string) (*ExecuteResult, error) {
	arguments, err := extractCallArguments(cypher)
	if err != nil {
		return nil, err
	}
	return e.callQueryStatistics(context.Background(), "collect", arguments)
}

// callDbStatsRetrieve retrieves collected statistics - Neo4j db.stats.retrieve()
// Syntax: CALL db.stats.retrieve(section)
func (e *StorageExecutor) callDbStatsRetrieve(cypher string) (*ExecuteResult, error) {
	arguments, err := extractCallArguments(cypher)
	if err != nil {
		return nil, err
	}
	return e.callQueryStatistics(context.Background(), "retrieve", arguments)
}

// callDbStatsRetrieveAllAnTheStats retrieves all statistics - Neo4j db.stats.retrieveAllAnTheStats()
func (e *StorageExecutor) callDbStatsRetrieveAllAnTheStats() (*ExecuteResult, error) {
	return e.retrieveGraphStatistics(context.Background(), "ALL")
}

// callDbStatsStatus returns statistics collection status - Neo4j db.stats.status()
func (e *StorageExecutor) callDbStatsStatus() (*ExecuteResult, error) {
	return e.callQueryStatistics(context.Background(), "status", nil)
}

// callDbStatsStop stops statistics collection - Neo4j db.stats.stop()
func (e *StorageExecutor) callDbStatsStop() (*ExecuteResult, error) {
	return e.callQueryStatistics(context.Background(), "stop", nil)
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
	e.ClearQueryCaches()

	return &ExecuteResult{
		Columns: []string{"value"},
		Rows: [][]interface{}{
			{"Query caches cleared"},
		},
	}, nil
}
