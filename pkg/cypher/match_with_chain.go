package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type matchWithStage struct {
	matchClause string
	withClause  string
}

// executeChainedMatchWithAggregations handles pipelines shaped like:
//
//	MATCH (...) WITH ...
//	MATCH (...) WITH ...
//	...
//	RETURN ...
//
// where each WITH projects pass-through aliases from earlier stages plus
// aggregate expressions over the current MATCH variable.
func (e *StorageExecutor) executeChainedMatchWithAggregations(ctx context.Context, cypher string) (*ExecuteResult, bool, error) {
	stages, returnClause, ok := parseMatchWithStages(cypher)
	if !ok || len(stages) < 2 {
		return nil, false, nil
	}

	scope := make(map[string]interface{})
	for _, stage := range stages {
		nodes, matchVar, err := e.evaluateMatchClauseNodes(ctx, stage.matchClause)
		if err != nil {
			return nil, false, err
		}

		withItems := e.splitWithItems(stage.withClause)
		if len(withItems) == 0 {
			return nil, false, nil
		}

		nextScope := make(map[string]interface{}, len(scope)+len(withItems))
		for k, v := range scope {
			nextScope[k] = v
		}

		for _, item := range withItems {
			expr, alias := parseProjectionExprAlias(item)
			if expr == "" || alias == "" {
				return nil, false, nil
			}

			if isAggregateFuncName(expr, "count") {
				count, supported := countForExpr(nodes, matchVar, extractFuncInner(expr))
				if !supported {
					return nil, false, nil
				}
				nextScope[alias] = count
				continue
			}

			// Pass-through of prior projected alias (e.g., WITH fact_versions, count(me) AS mutation_events)
			if v, exists := scope[expr]; exists {
				nextScope[alias] = v
				continue
			}

			return nil, false, nil
		}

		scope = nextScope
	}

	returnItems := e.parseReturnItems(strings.TrimSpace(returnClause))
	if len(returnItems) == 0 {
		return nil, false, nil
	}

	row := make([]interface{}, len(returnItems))
	cols := make([]string, len(returnItems))
	for i, item := range returnItems {
		cols[i] = item.expr
		if item.alias != "" {
			cols[i] = item.alias
		}
		if v, ok := scope[item.expr]; ok {
			row[i] = v
		}
	}

	return &ExecuteResult{
		Columns: cols,
		Rows:    [][]interface{}{row},
		Stats:   &QueryStats{},
	}, true, nil
}

func parseMatchWithStages(cypher string) ([]matchWithStage, string, bool) {
	query := strings.TrimSpace(cypher)
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "MATCH ") {
		return nil, "", false
	}

	var stages []matchWithStage
	pos := 0
	for {
		matchIdxRel := findKeywordIndexInContext(query[pos:], "MATCH")
		if matchIdxRel < 0 {
			return nil, "", false
		}
		matchIdx := pos + matchIdxRel
		if strings.TrimSpace(query[pos:matchIdx]) != "" {
			return nil, "", false
		}

		withIdxRel := findKeywordIndexInContext(query[matchIdx:], "WITH")
		if withIdxRel < 0 {
			return nil, "", false
		}
		withIdx := matchIdx + withIdxRel
		matchClause := strings.TrimSpace(query[matchIdx+5 : withIdx])
		if matchClause == "" {
			return nil, "", false
		}

		nextMatchRel := findKeywordIndexInContext(query[withIdx:], "MATCH")
		nextReturnRel := findKeywordIndexInContext(query[withIdx:], "RETURN")
		if nextReturnRel < 0 {
			return nil, "", false
		}
		nextReturn := withIdx + nextReturnRel
		nextPos := nextReturn
		if nextMatchRel >= 0 {
			nextMatch := withIdx + nextMatchRel
			if nextMatch < nextReturn {
				nextPos = nextMatch
			}
		}

		withClause := strings.TrimSpace(query[withIdx+4 : nextPos])
		if withClause == "" {
			return nil, "", false
		}
		stages = append(stages, matchWithStage{matchClause: matchClause, withClause: withClause})

		if nextPos == nextReturn {
			returnClause := strings.TrimSpace(query[nextReturn+6:])
			if returnClause == "" {
				return nil, "", false
			}
			return stages, returnClause, true
		}
		pos = nextPos
	}
}

func (e *StorageExecutor) evaluateMatchClauseNodes(ctx context.Context, clause string) ([]*storage.Node, string, error) {
	trimmed := strings.TrimSpace(clause)
	whereIdx := findKeywordIndexInContext(trimmed, "WHERE")
	patternPart := trimmed
	whereClause := ""
	if whereIdx > 0 {
		patternPart = strings.TrimSpace(trimmed[:whereIdx])
		whereClause = strings.TrimSpace(trimmed[whereIdx+5:])
	}

	pattern := e.parseNodePattern(ctx, patternPart)
	if pattern.variable == "" {
		return nil, "", localizedError(localization.CypherMatchingMatchPatternVariableMissing(clause), nil)
	}

	var nodes []*storage.Node
	var err error
	if len(pattern.labels) > 0 {
		nodes, err = e.loadNodesWithTemporalViewport(ctx, pattern.labels)
	} else {
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nil)
	}
	if err != nil {
		return nil, "", err
	}
	if len(pattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, pattern.properties)
	}
	if whereClause != "" {
		nodes = e.filterNodesByWhereClause(ctx, nodes, whereClause, pattern.variable)
	}
	return nodes, pattern.variable, nil
}

func parseProjectionExprAlias(item string) (string, string) {
	trimmed := strings.TrimSpace(item)
	if trimmed == "" {
		return "", ""
	}
	asIdx := projectionAliasIndex(trimmed)
	if asIdx < 0 {
		// A projected variable is named by the variable itself: `x` is the
		// column x, as in Neo4j. Other expressions are named by their text.
		if name := simpleSemanticIdentifier(trimmed); name != "" {
			return trimmed, name
		}
		return trimmed, trimmed
	}
	expr := strings.TrimSpace(trimmed[:asIdx])
	alias := normalizeProjectionColumnName(trimmed[asIdx+len("AS"):])
	return expr, alias
}

// projectionAliasIndex is the index of the AS that introduces item's alias:
// the first AS outside strings, parentheses, brackets and braces, with
// whitespace on both sides. An AS inside a nested expression
// (COLLECT { UNWIND l AS y RETURN y }, 'a AS b') is not the alias; -1 when
// there is none.
func projectionAliasIndex(item string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for from := 0; from < len(item); {
		index := keywordIndexFrom(item, "AS", from, opts)
		if index < 0 {
			return -1
		}
		end := index + len("AS")
		if index > 0 && isWhitespace(item[index-1]) && end < len(item) && isWhitespace(item[end]) {
			return index
		}
		from = end
	}
	return -1
}

func countForExpr(nodes []*storage.Node, matchVar, inner string) (int64, bool) {
	inner = strings.TrimSpace(inner)
	if inner == "*" || inner == matchVar {
		return int64(len(nodes)), true
	}
	if strings.HasPrefix(inner, matchVar+".") {
		prop := strings.TrimSpace(inner[len(matchVar)+1:])
		count := int64(0)
		for _, n := range nodes {
			if n != nil && n.Properties != nil && n.Properties[prop] != nil {
				count++
			}
		}
		return count, true
	}
	return 0, false
}
