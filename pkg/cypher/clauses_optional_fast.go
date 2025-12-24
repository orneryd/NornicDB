package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) tryFastCompoundOptionalMatchCount(initialNodes []*storage.Node, source nodePatternInfo, rel optionalRelPattern, restOfQuery string) (*ExecuteResult, bool, error) {
	// Fast-path the common:
	//   MATCH (p:Product)
	//   OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
	//   RETURN p.productName, count(o) AS orderCount
	//   ORDER BY orderCount DESC
	//
	// This avoids a per-node GetIncomingEdges scan and avoids constructing joinedRows.
	if strings.TrimSpace(source.variable) == "" || len(initialNodes) == 0 {
		return nil, false, nil
	}
	if rel.direction != "in" || rel.relType == "" || rel.targetVar == "" {
		return nil, false, nil
	}

	upperRest := strings.ToUpper(strings.TrimSpace(restOfQuery))
	if !strings.HasPrefix(upperRest, "RETURN") {
		return nil, false, nil
	}

	// Extract RETURN clause body, stripping ORDER BY / SKIP / LIMIT.
	returnBody := strings.TrimSpace(restOfQuery[len("RETURN"):])
	endIdx := len(returnBody)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnBody, kw); idx >= 0 && idx < endIdx {
			endIdx = idx
		}
	}
	returnClause := strings.TrimSpace(returnBody[:endIdx])
	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 2 {
		return nil, false, nil
	}

	m := exprMatcher{}
	if m.key(returnItems[0].expr) != m.key(source.variable+".productName") {
		return nil, false, nil
	}
	if m.key(returnItems[1].expr) != m.key("count("+rel.targetVar+")") {
		return nil, false, nil
	}

	// Ensure the relType matches the query's semantic shape; keep this narrow.
	if strings.ToUpper(rel.relType) != "ORDERS" {
		return nil, false, nil
	}

	edgeList, idPrefix, err := e.getEdgesByTypeFast(rel.relType)
	if err != nil {
		return nil, false, err
	}

	incomingCount := make(map[storage.NodeID]int64, 1024)
	for _, edge := range edgeList {
		incomingCount[edge.EndNode]++
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    make([][]interface{}, 0, len(initialNodes)),
		Stats:   &QueryStats{},
	}
	for _, item := range returnItems {
		if item.alias != "" {
			result.Columns = append(result.Columns, item.alias)
		} else {
			result.Columns = append(result.Columns, item.expr)
		}
	}

	for _, node := range initialNodes {
		// initialNodes come from the (possibly namespaced) storage wrapper; IDs may be unprefixed.
		nodeID := e.prefixNodeIDIfNeeded(node.ID, idPrefix)
		result.Rows = append(result.Rows, []interface{}{node.Properties["productName"], incomingCount[nodeID]})
	}

	// Apply ORDER BY / SKIP / LIMIT using the same whitespace-tolerant logic as MATCH.
	orderByIdx := findKeywordIndex(restOfQuery, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5 // skip "ORDER"
		for orderStart < len(restOfQuery) && isWhitespace(restOfQuery[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(restOfQuery) && strings.ToUpper(restOfQuery[orderStart:orderStart+2]) == "BY" {
			orderStart += 2
			for orderStart < len(restOfQuery) && isWhitespace(restOfQuery[orderStart]) {
				orderStart++
			}
		}
		orderEnd := len(restOfQuery)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(restOfQuery[orderStart:], kw); idx >= 0 {
				if orderStart+idx < orderEnd {
					orderEnd = orderStart + idx
				}
			}
		}
		orderExpr := strings.TrimSpace(restOfQuery[orderStart:orderEnd])
		if orderExpr != "" {
			result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
		}
	}

	skipIdx := findKeywordIndex(restOfQuery, "SKIP")
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(restOfQuery[skipIdx+4:])
		if fields := strings.Fields(skipPart); len(fields) > 0 {
			if s, err := strconv.Atoi(fields[0]); err == nil && s > 0 {
				if s < len(result.Rows) {
					result.Rows = result.Rows[s:]
				} else {
					result.Rows = [][]interface{}{}
				}
			}
		}
	}

	limitIdx := findKeywordIndex(restOfQuery, "LIMIT")
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(restOfQuery[limitIdx+5:])
		if fields := strings.Fields(limitPart); len(fields) > 0 {
			if l, err := strconv.Atoi(fields[0]); err == nil && l >= 0 {
				if l < len(result.Rows) {
					result.Rows = result.Rows[:l]
				}
			}
		}
	}

	return result, true, nil
}
