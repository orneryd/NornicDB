package cypher

import (
	"context"
	"fmt"
	"strings"
)

// =============================================================================
// APOC Periodic/Batch Operations
// =============================================================================

// callApocPeriodicIterate performs batch processing with periodic commits.
// CALL apoc.periodic.iterate(cypherIterate, cypherAction, {batchSize:1000, parallel:false})
// This is used for large-scale data processing to avoid memory issues.
func (e *StorageExecutor) callApocPeriodicIterate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PERIODIC.ITERATE")
	if callIdx == -1 {
		// Try rock_n_roll alias
		callIdx = strings.Index(upper, "APOC.PERIODIC.ROCK_N_ROLL")
		if callIdx == -1 {
			return nil, fmt.Errorf("invalid apoc.periodic.iterate call")
		}
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.periodic.iterate requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.periodic.iterate")
	}

	// Parse arguments: (iterateQuery, actionQuery, config)
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])
	iterateQuery, actionQuery, config, err := e.parseApocPeriodicIterateArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.periodic.iterate arguments: %w", err)
	}

	// Extract config options
	batchSize := 1000
	if bs, ok := config["batchSize"].(float64); ok {
		batchSize = int(bs)
	} else if bs, ok := config["batchSize"].(int); ok {
		batchSize = bs
	} else if bs, ok := config["batchSize"].(int64); ok {
		batchSize = int(bs)
	}

	// Execute the iterate query to get data to process
	iterateResult, err := e.Execute(ctx, iterateQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("iterate query failed: %w", err)
	}

	// Process in batches
	totalRows := len(iterateResult.Rows)
	batches := (totalRows + batchSize - 1) / batchSize

	stats := &QueryStats{}
	errorCount := int64(0)
	successCount := int64(0)

	for batchNum := 0; batchNum < batches; batchNum++ {
		startIdx := batchNum * batchSize
		endIdx := startIdx + batchSize
		if endIdx > totalRows {
			endIdx = totalRows
		}

		// Process each row in the batch
		for i := startIdx; i < endIdx; i++ {
			row := iterateResult.Rows[i]

			// Build params map from row
			params := make(map[string]interface{})
			for j, col := range iterateResult.Columns {
				if j < len(row) {
					params[col] = row[j]
				}
			}

			// Execute action query with row data as parameters
			actionResult, err := e.Execute(ctx, actionQuery, params)
			if err != nil {
				errorCount++
				continue
			}
			successCount++

			// Accumulate stats
			if actionResult.Stats != nil {
				stats.NodesCreated += actionResult.Stats.NodesCreated
				stats.NodesDeleted += actionResult.Stats.NodesDeleted
				stats.RelationshipsCreated += actionResult.Stats.RelationshipsCreated
				stats.RelationshipsDeleted += actionResult.Stats.RelationshipsDeleted
				stats.PropertiesSet += actionResult.Stats.PropertiesSet
			}
		}
	}

	return &ExecuteResult{
		Columns: []string{"batches", "total", "timeTaken", "committedOperations", "failedOperations", "failedBatches", "retries", "errorMessages", "batch", "operations", "wasTerminated", "failedParams", "updateStatistics"},
		Rows: [][]interface{}{
			{
				int64(batches),           // batches
				int64(totalRows),         // total
				int64(0),                 // timeTaken (ms) - not measured
				successCount,             // committedOperations
				errorCount,               // failedOperations
				int64(0),                 // failedBatches
				int64(0),                 // retries
				map[string]interface{}{}, // errorMessages
				map[string]interface{}{ // batch
					"total":     int64(batches),
					"committed": int64(batches),
					"failed":    int64(0),
					"errors":    map[string]interface{}{},
				},
				map[string]interface{}{ // operations
					"total":     int64(totalRows),
					"committed": successCount,
					"failed":    errorCount,
					"errors":    map[string]interface{}{},
				},
				false,                    // wasTerminated
				map[string]interface{}{}, // failedParams
				map[string]interface{}{ // updateStatistics
					"nodesCreated":         stats.NodesCreated,
					"nodesDeleted":         stats.NodesDeleted,
					"relationshipsCreated": stats.RelationshipsCreated,
					"relationshipsDeleted": stats.RelationshipsDeleted,
					"propertiesSet":        stats.PropertiesSet,
				},
			},
		},
		Stats: stats,
	}, nil
}

// callApocPeriodicCommit performs a query with periodic commits.
// CALL apoc.periodic.commit(statement, params) YIELD updates, executions, runtime, batches
// This commits every N operations to avoid large transactions.
func (e *StorageExecutor) callApocPeriodicCommit(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PERIODIC.COMMIT")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.periodic.commit call")
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.periodic.commit requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.periodic.commit")
	}

	// Parse arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])
	statement, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.periodic.commit arguments: %w", err)
	}

	// Extract limit from params if present
	limit := 10000
	if l, ok := params["limit"].(float64); ok {
		limit = int(l)
	} else if l, ok := params["limit"].(int); ok {
		limit = l
	}

	// Execute the statement repeatedly until it affects 0 rows
	totalUpdates := int64(0)
	executions := int64(0)
	stats := &QueryStats{}

	for {
		// Add LIMIT to statement if not present
		stmtUpper := strings.ToUpper(statement)
		if !strings.Contains(stmtUpper, "LIMIT") {
			statement = statement + fmt.Sprintf(" LIMIT %d", limit)
		}

		result, err := e.Execute(ctx, statement, params)
		if err != nil {
			break
		}
		executions++

		// Check if any updates were made
		updates := int64(0)
		if result.Stats != nil {
			updates = int64(result.Stats.NodesCreated + result.Stats.NodesDeleted +
				result.Stats.RelationshipsCreated + result.Stats.RelationshipsDeleted +
				result.Stats.PropertiesSet)

			stats.NodesCreated += result.Stats.NodesCreated
			stats.NodesDeleted += result.Stats.NodesDeleted
			stats.RelationshipsCreated += result.Stats.RelationshipsCreated
			stats.RelationshipsDeleted += result.Stats.RelationshipsDeleted
			stats.PropertiesSet += result.Stats.PropertiesSet
		}

		if updates == 0 {
			break
		}
		totalUpdates += updates

		// Safety limit
		if executions > 1000 {
			break
		}
	}

	return &ExecuteResult{
		Columns: []string{"updates", "executions", "runtime", "batches"},
		Rows: [][]interface{}{
			{totalUpdates, executions, int64(0), executions},
		},
		Stats: stats,
	}, nil
}
