package cypher

import (
	"context"
	"fmt"
	"strings"
)

// =============================================================================
// APOC Dynamic Cypher Execution Procedures
// =============================================================================

// callApocCypherRun executes a dynamic Cypher query string.
// CALL apoc.cypher.run(statement, params) YIELD value
// The statement and its parameters are the call's evaluated arguments, so
// either may be bound earlier in the statement (WITH 'RETURN 1' AS q CALL
// apoc.cypher.run(q, {})). procedure is the name it was called by
// (apoc.cypher.run or its alias apoc.cypher.doitall).
func (e *StorageExecutor) callApocCypherRun(ctx context.Context, procedure string, args []interface{}) (*ExecuteResult, error) {
	innerQuery, err := requiredProcedureString(procedure, args, 0, "statement")
	if err != nil {
		return nil, err
	}
	params, err := optionalProcedureMap(procedure, args, 1, "params")
	if err != nil {
		return nil, err
	}

	// Execute the inner query
	innerResult, err := e.executeInternal(ctx, innerQuery, params)
	if err != nil {
		return nil, fmt.Errorf("apoc.cypher.run inner query failed: %w", err)
	}

	// Transform result to match APOC format (YIELD value)
	// Each row becomes a map under the "value" column
	result := &ExecuteResult{
		Columns: []string{"value"},
		Rows:    make([][]interface{}, 0, len(innerResult.Rows)),
		Stats:   innerResult.Stats,
	}

	for _, row := range innerResult.Rows {
		// Convert row to a map with column names as keys
		valueMap := make(map[string]interface{})
		for i, col := range innerResult.Columns {
			if i < len(row) {
				valueMap[col] = row[i]
			}
		}
		result.Rows = append(result.Rows, []interface{}{valueMap})
	}

	return result, nil
}

// callApocCypherRunMany executes multiple Cypher statements separated by semicolons.
// CALL apoc.cypher.runMany(statements, params) YIELD row, result
// The statements and parameters are the call's evaluated arguments.
func (e *StorageExecutor) callApocCypherRunMany(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.cypher.runMany"
	statements, err := requiredProcedureString(procedure, args, 0, "statements")
	if err != nil {
		return nil, err
	}
	params, err := optionalProcedureMap(procedure, args, 1, "params")
	if err != nil {
		return nil, err
	}

	// Split by semicolons (respecting quotes)
	queries := e.splitBySemicolon(statements)

	result := &ExecuteResult{
		Columns: []string{"row", "result"},
		Rows:    make([][]interface{}, 0),
		Stats:   &QueryStats{},
	}

	for i, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		innerResult, err := e.executeInternal(ctx, query, params)
		if err != nil {
			// Include error in result instead of failing
			result.Rows = append(result.Rows, []interface{}{
				int64(i),
				map[string]interface{}{"error": err.Error()},
			})
			continue
		}

		// Add each row from the inner result
		for _, row := range innerResult.Rows {
			valueMap := make(map[string]interface{})
			for j, col := range innerResult.Columns {
				if j < len(row) {
					valueMap[col] = row[j]
				}
			}
			result.Rows = append(result.Rows, []interface{}{
				int64(i),
				valueMap,
			})
		}

		// Accumulate stats
		if innerResult.Stats != nil {
			addQueryStats(result.Stats, innerResult.Stats)
		}
	}

	return result, nil
}
