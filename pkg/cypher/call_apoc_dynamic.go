package cypher

import (
	"context"
	"fmt"
	"strings"
	"time"
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
	innerResult, err := e.executeUserStatement(ctx, innerQuery, params)
	if err != nil {
		return nil, fmt.Errorf("%s inner query failed: %w", procedure, err)
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

// runManyStatistics is apoc.cypher.runMany's statistics row for one
// statement, with APOC's keys.
func runManyStatistics(result *ExecuteResult, elapsed time.Duration) map[string]interface{} {
	stats := result.Stats
	if stats == nil {
		stats = &QueryStats{}
	}
	return map[string]interface{}{
		"rows":                 int64(len(result.Rows)),
		"time":                 elapsed.Milliseconds(),
		"nodesCreated":         int64(stats.NodesCreated),
		"nodesDeleted":         int64(stats.NodesDeleted),
		"relationshipsCreated": int64(stats.RelationshipsCreated),
		"relationshipsDeleted": int64(stats.RelationshipsDeleted),
		"propertiesSet":        int64(stats.PropertiesSet),
		"labelsAdded":          int64(stats.LabelsAdded),
		"labelsRemoved":        int64(stats.LabelsRemoved),
		"indexesAdded":         int64(stats.IndexesAdded),
		"indexesRemoved":       int64(stats.IndexesRemoved),
		"constraintsAdded":     int64(stats.ConstraintsAdded),
		"constraintsRemoved":   int64(stats.ConstraintsRemoved),
	}
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
	// APOC's config: statistics (default true) gives each statement's
	// statistics row.
	config, err := optionalProcedureMap(procedure, args, 2, "config")
	if err != nil {
		return nil, err
	}
	statistics := config["statistics"] != false

	// Split by semicolons (respecting quotes)
	queries := e.splitBySemicolon(statements)

	result := &ExecuteResult{
		Columns: []string{"row", "result"},
		Rows:    make([][]interface{}, 0),
		Stats:   &QueryStats{},
	}

	// As in APOC, each statement gives its rows, numbered from 0, then a
	// row -1 with its statistics unless config says statistics: false
	// (#1164).
	for _, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		started := time.Now()
		innerResult, err := e.executeUserStatement(ctx, query, params)
		if err != nil {
			// A failed statement fails the call, as in apoc.cypher.run, and
			// with it the caller's transaction: nothing the earlier
			// statements wrote is kept.
			return nil, err
		}

		// Add each row from the inner result
		for i, row := range innerResult.Rows {
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
		if statistics {
			result.Rows = append(result.Rows, []interface{}{int64(-1), runManyStatistics(innerResult, time.Since(started))})
		}

		// Accumulate stats
		if innerResult.Stats != nil {
			addQueryStats(result.Stats, innerResult.Stats)
		}
	}

	return result, nil
}
