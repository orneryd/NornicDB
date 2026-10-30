// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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

	columnName, ok := parseSimpleReturnVariable(ctx, trimmed, returnIdx, varName)
	if !ok {
		return nil, false
	}

	limitExpression := StripComments(pipelinePaginationExpression(trimmed[returnIdx:], "LIMIT"))
	limit, ok := e.evaluatePipelinePagination(ctx, limitExpression, nil)
	if !ok {
		return nil, false
	}

	rows := make([][]interface{}, 0)
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

	varName, labels, ok := strictNodeHeadLabels(inner)
	if !ok || varName == "" {
		return "", nil, false
	}
	return varName, labels, true
}

// parseSimpleReturnVariable reads RETURN v [AS a] from the RETURN occurrence
// of statement at offset at; its LIMIT is the caller's, and any ORDER BY or
// SKIP declines the plan.
func parseSimpleReturnVariable(ctx context.Context, statement string, at int, varName string) (string, bool) {
	clause, found := projectionClauseAt(ctx, statement, at)
	if !found {
		return "", false
	}
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	if body.ChildRule(cypherantlr.CypherParserRULE_orderSt).Valid() || body.ChildRule(cypherantlr.CypherParserRULE_skipSt).Valid() {
		return "", false
	}
	plan := projectionPlanOf(ctx, clause)
	if !plan.valid || plan.star || plan.distinct || plan.hasAggregate || len(plan.projections) != 1 || plan.columns[0] == "" {
		return "", false
	}
	if simpleSemanticIdentifier(plan.projections[0].expr) != varName {
		return "", false
	}
	return plan.columns[0], true
}
