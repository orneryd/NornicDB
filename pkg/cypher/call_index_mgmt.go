package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
)

// ========================================
// Index Management Procedures
// ========================================

// callDbAwaitIndexes waits for all indexes to come online - Neo4j db.awaitIndexes()
// Syntax: CALL db.awaitIndexes(timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndexes(ctx context.Context, arguments []interface{}) (*ExecuteResult, error) {
	deadline, err := indexWaitDeadline("db.awaitIndexes", arguments, 0)
	if err != nil {
		return nil, err
	}
	for _, entry := range e.storage.GetSchema().GetIndexes() {
		index, ok := entry.(map[string]interface{})
		if !ok || index["type"] != "VECTOR" {
			continue // Native indexes are maintained synchronously.
		}
		name, _ := index["name"].(string)
		if err := e.waitForCurrentVectorIndex(ctx, name, deadline, false); err != nil {
			return nil, err
		}
	}
	return emptyIndexProcedureResult(), nil
}

func emptyIndexProcedureResult() *ExecuteResult {
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
}

func indexWaitDeadline(procedure string, arguments []interface{}, offset int) (time.Time, error) {
	seconds := int64(300)
	if len(arguments) > offset {
		value, err := requiredProcedureInteger(procedure, arguments, offset, "timeOutSeconds")
		if err != nil {
			return time.Time{}, err
		}
		seconds = value
	}
	if seconds < 0 {
		return time.Time{}, newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", "timeOutSeconds must not be negative")
	}
	if seconds > math.MaxInt64/int64(time.Second) {
		seconds = math.MaxInt64 / int64(time.Second)
	}
	return time.Now().Add(time.Duration(seconds) * time.Second), nil
}

// waitForCurrentVectorIndex follows the state reported by SHOW INDEXES. Vector
// queries have a separate storage fallback during startup and cannot supply
// an explicit procedure's ONLINE promise. A drop or replacement during the
// wait must not acknowledge the retired definition.
func (e *StorageExecutor) waitForCurrentVectorIndex(ctx context.Context, name string, deadline time.Time, requireName bool) error {
	for {
		current, exists := e.storage.GetSchema().GetVectorIndex(name)
		if !exists {
			if requireName {
				return newSemanticError("Neo.ClientError.Schema.IndexNotFound", "IndexNotFound", fmt.Sprintf("No such index '%s'", name))
			}
			return nil
		}
		state, failure := readVectorIndexState(ctx, e.search(), name)
		switch state {
		case search.VectorIndexOnline:
			if later, exists := e.storage.GetSchema().GetVectorIndex(name); exists && later == current {
				return nil
			}
			continue
		case search.VectorIndexFailed:
			if later, exists := e.storage.GetSchema().GetVectorIndex(name); !exists || later != current {
				continue
			}
			message := fmt.Sprintf("Index `%s` entered a FAILED state. Please see database logs.", name)
			if failure != nil {
				message += " Cause of failure: " + failure.Error()
			}
			return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", message)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", "Expected index to come online within a reasonable time.")
		}
		if remaining > 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
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
	for _, entry := range readSchemaIndexCatalog(ctx, e.storage.GetSchema()) {
		if index, ok := entry.(map[string]interface{}); ok && index["name"] == name {
			if procedure == "db.awaitIndex" && index["type"] == "VECTOR" {
				deadline, err := indexWaitDeadline(procedure, arguments, 1)
				if err != nil {
					return nil, err
				}
				if err := e.waitForCurrentVectorIndex(ctx, name, deadline, true); err != nil {
					return nil, err
				}
			}
			return emptyIndexProcedureResult(), nil
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
func (e *StorageExecutor) callDbClearQueryCaches() (*ExecuteResult, error) {
	e.ClearQueryCaches()

	return &ExecuteResult{
		Columns: columnNames(dbClearQueryCachesColumns),
		Rows: [][]interface{}{
			{"Query caches cleared"},
		},
	}, nil
}
