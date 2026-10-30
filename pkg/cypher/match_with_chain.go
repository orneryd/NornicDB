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

func parseMatchWithStages(view braceView) ([]matchWithStage, string, bool) {
	view = view.trim()
	query := view.text()
	upper := upperASCII(query)
	if !strings.HasPrefix(upper, "MATCH ") {
		return nil, "", false
	}

	var stages []matchWithStage
	pos := 0
	for {
		matchIdxRel := findKeywordIndexInContext(view.window(pos, len(query)), "MATCH")
		if matchIdxRel < 0 {
			return nil, "", false
		}
		matchIdx := pos + matchIdxRel
		if strings.TrimSpace(query[pos:matchIdx]) != "" {
			return nil, "", false
		}

		withIdxRel := findKeywordIndexInContext(view.window(matchIdx, len(query)), "WITH")
		if withIdxRel < 0 {
			return nil, "", false
		}
		withIdx := matchIdx + withIdxRel
		matchClause := strings.TrimSpace(query[matchIdx+5 : withIdx])
		if matchClause == "" {
			return nil, "", false
		}

		nextMatchRel := findKeywordIndexInContext(view.window(withIdx, len(query)), "MATCH")
		nextReturnRel := findKeywordIndexInContext(view.window(withIdx, len(query)), "RETURN")
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

func (e *StorageExecutor) evaluateMatchClauseNodes(ctx context.Context, clauseView braceView) ([]*storage.Node, string, error) {
	clause := clauseView.text()
	trimmed := clauseView.trim()
	whereIdx := findKeywordIndexInContext(trimmed, "WHERE")
	patternPart := trimmed
	whereView := trimmed.window(0, 0)
	if whereIdx > 0 {
		patternPart = trimmed.window(0, whereIdx).trim()
		whereView = trimmed.window(whereIdx+5, len(trimmed.text())).trim()
	}
	whereClause := whereView.text()

	pattern, parseErr := e.parseNodePattern(ctx, patternPart)
	if parseErr != nil {
		return nil, "", parseErr
	}
	if pattern.variable == "" {
		return nil, "", localizedError(localization.CypherMatchingMatchPatternVariableMissing(clause), nil)
	}

	var nodes []*storage.Node
	var err error
	nodes, err = e.loadPatternNodes(ctx, pattern.labels, pattern.properties)
	if err != nil {
		return nil, "", err
	}
	if whereClause != "" {
		nodes = e.filterNodesByWhereClause(ctx, nodes, whereView, pattern.variable)
	}
	return nodes, pattern.variable, nil
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
