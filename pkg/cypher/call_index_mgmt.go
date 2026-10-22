package cypher

import (
	"context"
	"fmt"
)

// ========================================
// Index Management Procedures
// ========================================

// callDbAwaitIndexes waits for all indexes to come online - Neo4j db.awaitIndexes()
// Syntax: CALL db.awaitIndexes(timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndexes(ctx context.Context) (*ExecuteResult, error) {
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callNamedIndexManagement is db.awaitIndex(indexName, timeOutSeconds) and
// db.resampleIndex(indexName) on their evaluated arguments, in Neo4j's
// order: a null timeout fails the call (ProcedureCallFailed) before the
// index is looked up, and a null name is looked up as 'null'
// (IndexNotFound), as Neo4j 5.26.30 does.
func (e *StorageExecutor) callNamedIndexManagement(ctx context.Context, procedure string, arguments []interface{}) (*ExecuteResult, error) {
	if len(arguments) == 0 {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments", "an index name is required")
	}
	if len(arguments) > 1 && arguments[1] == nil {
		return nil, procedureArgumentNullError(procedure, "timeOutSeconds")
	}
	name, valid := arguments[0].(string)
	if arguments[0] == nil {
		name, valid = "null", true
	}
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
func (e *StorageExecutor) callDbClearQueryCaches(ctx context.Context) (*ExecuteResult, error) {
	e.ClearQueryCaches()

	return &ExecuteResult{
		Columns: []string{"value"},
		Rows: [][]interface{}{
			{"Query caches cleared"},
		},
	}, nil
}
