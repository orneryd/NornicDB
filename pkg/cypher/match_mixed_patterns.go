package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func hasRelationshipPattern(patterns []string) bool {
	for _, pattern := range patterns {
		if containsOutsideStrings(pattern, "-[") || containsOutsideStrings(pattern, "]-") {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) executeMixedPatternMatch(
	ctx context.Context,
	cypher string,
	patterns []string,
	whereClause string,
	returnClause string,
	distinct bool,
) (*ExecuteResult, error) {
	rows := []pipelineRow{{}}
	for _, pattern := range patterns {
		var ok bool
		var err error
		rows, ok, err = e.pipelineApplyMatch(ctx, rows, "MATCH "+strings.TrimSpace(pattern))
		if err != nil {
			return nil, err
		}
		if !ok {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}, nil
		}
	}

	if whereClause != "" {
		filtered := make([]pipelineRow, 0, len(rows))
		for _, row := range rows {
			nodes := make(map[string]*storage.Node)
			rels := make(map[string]*storage.Edge)
			for name, value := range row {
				switch entity := value.(type) {
				case *storage.Node:
					nodes[name] = entity
				case *storage.Edge:
					rels[name] = entity
				}
			}
			if keep, ok := e.evaluateExpressionWithContext(ctx, whereClause, nodes, rels).(bool); ok && keep {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	result, ok := e.pipelineApplyReturn(ctx, rows, "RETURN "+returnClause)
	if !ok {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}, nil
	}
	result.Stats = &QueryStats{}
	if distinct {
		seen := make(map[string]struct{}, len(result.Rows))
		unique := make([][]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			keys := make([]string, len(row))
			for i, value := range row {
				keys[i] = pipelineValueKey(value)
			}
			key := strings.Join(keys, "\x1f")
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, row)
		}
		result.Rows = unique
	}
	return result, nil
}
