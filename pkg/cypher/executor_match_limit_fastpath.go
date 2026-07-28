package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/util"
)

// tryFastPathSimpleMatchReturnLimit handles the common low-latency read shape:
//
//	MATCH (n) RETURN n LIMIT <k>
//
// and label/alias variants like:
//
//	MATCH (n:Label) RETURN n LIMIT <k>
//	MATCH (n) RETURN n AS node LIMIT <k>
//
// It is intentionally strict to avoid semantic drift from full Cypher execution.
func (e *StorageExecutor) tryFastPathSimpleMatchReturnLimit(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	trimmed := strings.TrimSpace(cypher)
	if !strings.HasPrefix(strings.TrimSpace(upperQuery), "MATCH") {
		return nil, false
	}

	// Reject richer clauses: this fast path is for simple MATCH/RETURN/LIMIT only.
	for _, kw := range []string{
		"WHERE", "ORDER", "SKIP", "WITH", "UNWIND", "OPTIONAL MATCH", "CALL",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE", "UNION",
	} {
		if findKeywordIndex(trimmed, kw) > 0 {
			return nil, false
		}
	}

	returnIdx := findKeywordIndex(trimmed, "RETURN")
	limitIdx := findKeywordIndex(trimmed, "LIMIT")
	if returnIdx <= 0 || limitIdx <= returnIdx {
		return nil, false
	}

	matchPart := strings.TrimSpace(trimmed[len("MATCH"):returnIdx])
	varName, labels, ok := parseSimpleMatchSingleNodePattern(matchPart)
	if !ok {
		return nil, false
	}

	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN") : limitIdx])
	columnName, ok := parseSimpleReturnVariable(returnPart, varName)
	if !ok {
		return nil, false
	}

	limitPart := strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):])
	limitFields := strings.Fields(limitPart)
	if len(limitFields) == 0 {
		return nil, false
	}
	limit, err := strconv.Atoi(limitFields[0])
	if err != nil || limit < 0 {
		return nil, false
	}

	rows := make([][]interface{}, 0, util.SafePreallocCap(limit))
	if limit == 0 {
		e.markSimpleMatchLimitFastPathUsed()
		return &ExecuteResult{Columns: []string{columnName}, Rows: rows, Stats: &QueryStats{}}, true
	}

	nodes, err := e.collectNodesWithStreaming(ctx, labels, nil, varName, "", limit)
	if err != nil {
		return nil, false
	}

	for _, node := range nodes {
		rows = append(rows, []interface{}{node})
		if len(rows) >= limit {
			break
		}
	}

	e.markSimpleMatchLimitFastPathUsed()
	return &ExecuteResult{
		Columns: []string{columnName},
		Rows:    rows,
		Stats:   &QueryStats{},
	}, true
}

func parseSimpleMatchSingleNodePattern(pattern string) (string, []string, bool) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", nil, false
	}
	if strings.Contains(pattern, "-") || strings.Contains(pattern, "{") || strings.Contains(pattern, ",") {
		return "", nil, false
	}

	inner := strings.TrimSpace(pattern[1 : len(pattern)-1])
	if inner == "" {
		return "", nil, false
	}

	parts := strings.Split(inner, ":")
	varName := strings.TrimSpace(parts[0])
	if varName == "" {
		return "", nil, false
	}

	labels := make([]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		lbl := strings.TrimSpace(p)
		if lbl == "" {
			return "", nil, false
		}
		labels = append(labels, lbl)
	}
	return varName, labels, true
}

func parseSimpleReturnVariable(returnPart string, varName string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(returnPart))
	if len(fields) == 0 {
		return "", false
	}
	if !strings.EqualFold(fields[0], varName) {
		return "", false
	}
	if len(fields) == 1 {
		return varName, true
	}
	if len(fields) == 3 && strings.EqualFold(fields[1], "AS") {
		alias := strings.TrimSpace(fields[2])
		if alias != "" {
			return alias, true
		}
	}
	return "", false
}
