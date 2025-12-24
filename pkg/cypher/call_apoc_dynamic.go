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
// This allows executing Cypher queries stored in strings or variables.
func (e *StorageExecutor) callApocCypherRun(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse the CALL statement to extract the inner query and parameters
	// Format: CALL apoc.cypher.run('MATCH (n) RETURN n', {})

	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.CYPHER.RUN")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.cypher.run call")
	}

	// Find the opening parenthesis after the procedure name
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.cypher.run requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.cypher.run")
	}

	// Extract arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])

	// Parse the first argument (the query string)
	innerQuery, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.cypher.run arguments: %w", err)
	}

	// Execute the inner query
	innerResult, err := e.Execute(ctx, innerQuery, params)
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
func (e *StorageExecutor) callApocCypherRunMany(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.CYPHER.RUNMANY")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.cypher.runMany call")
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.cypher.runMany requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.cypher.runMany")
	}

	// Extract arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])

	// Parse the first argument (the multi-statement string)
	statements, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.cypher.runMany arguments: %w", err)
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

		innerResult, err := e.Execute(ctx, query, params)
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
			result.Stats.NodesCreated += innerResult.Stats.NodesCreated
			result.Stats.NodesDeleted += innerResult.Stats.NodesDeleted
			result.Stats.RelationshipsCreated += innerResult.Stats.RelationshipsCreated
			result.Stats.RelationshipsDeleted += innerResult.Stats.RelationshipsDeleted
			result.Stats.PropertiesSet += innerResult.Stats.PropertiesSet
		}
	}

	return result, nil
}
