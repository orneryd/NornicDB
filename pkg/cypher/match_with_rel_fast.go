package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) tryFastRevenueByProduct(matches *TraversalMatch, withClause string, returnClause string, orderByClause string, skipVal int, limitVal int) (*ExecuteResult, bool, error) {
	// MATCH (p:Product)<-[r:ORDERS]-(:Order)
	// WITH p, sum(p.unitPrice * r.quantity) as revenue
	// RETURN p.productName, revenue
	if matches == nil || matches.IsChained {
		return nil, false, nil
	}
	if matches.Relationship.MinHops != 1 || matches.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	if len(matches.Relationship.Types) != 1 || strings.ToUpper(matches.Relationship.Types[0]) != "ORDERS" {
		return nil, false, nil
	}
	if matches.Relationship.Direction != "incoming" {
		return nil, false, nil
	}
	if len(matches.StartNode.labels) != 1 || matches.StartNode.labels[0] != "Product" {
		return nil, false, nil
	}

	pVar := matches.StartNode.variable
	rVar := matches.Relationship.Variable
	if pVar == "" || rVar == "" {
		return nil, false, nil
	}

	withItems := e.splitWithItems(withClause)
	if len(withItems) != 2 {
		return nil, false, nil
	}
	if strings.TrimSpace(withItems[0]) != pVar {
		return nil, false, nil
	}

	// Parse "sum(p.unitPrice * r.quantity) as revenue"
	second := strings.TrimSpace(withItems[1])
	upperSecond := strings.ToUpper(second)
	asIdx := strings.Index(upperSecond, " AS ")
	if asIdx < 0 {
		return nil, false, nil
	}
	sumExpr := strings.TrimSpace(second[:asIdx])
	revenueVar := strings.TrimSpace(second[asIdx+4:])

	m := exprMatcher{}
	wantSumKey := m.key("sum(" + pVar + ".unitPrice*" + rVar + ".quantity)")
	if m.key(sumExpr) != wantSumKey {
		return nil, false, nil
	}

	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 2 {
		return nil, false, nil
	}
	if m.key(returnItems[0].expr) != m.key(pVar+".productName") {
		return nil, false, nil
	}
	if m.key(returnItems[1].expr) != m.key(revenueVar) {
		return nil, false, nil
	}

	edgeList, _, err := e.getEdgesByTypeFast(matches.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}

	// Collect product IDs first so we can batch fetch unitPrice/productName.
	productSeen := make(map[storage.NodeID]struct{}, 1024)
	productIDs := make([]storage.NodeID, 0, 1024)
	for _, edge := range edgeList {
		productID := edge.EndNode
		if _, ok := productSeen[productID]; ok {
			continue
		}
		productSeen[productID] = struct{}{}
		productIDs = append(productIDs, productID)
	}

	products, _, err := e.batchGetNodesFast(productIDs)
	if err != nil {
		return nil, false, err
	}

	revenueByProduct := make(map[storage.NodeID]float64, len(productIDs))
	for _, edge := range edgeList {
		productID := edge.EndNode
		pNode := products[productID]
		if pNode == nil {
			continue
		}
		unitPriceRaw, ok := pNode.Properties["unitPrice"]
		if !ok {
			continue
		}
		var unitPrice float64
		switch v := unitPriceRaw.(type) {
		case float64:
			unitPrice = v
		case float32:
			unitPrice = float64(v)
		case int:
			unitPrice = float64(v)
		case int64:
			unitPrice = float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				unitPrice = f
			} else {
				continue
			}
		default:
			continue
		}

		qtyRaw, ok := edge.Properties["quantity"]
		if !ok {
			continue
		}
		var qty float64
		switch v := qtyRaw.(type) {
		case float64:
			qty = v
		case float32:
			qty = float64(v)
		case int:
			qty = float64(v)
		case int64:
			qty = float64(v)
		default:
			continue
		}

		revenueByProduct[productID] += unitPrice * qty
	}

	result := &ExecuteResult{
		Columns: make([]string, 0, len(returnItems)),
		Rows:    make([][]interface{}, 0, len(revenueByProduct)),
		Stats:   &QueryStats{},
	}

	for _, it := range returnItems {
		if it.alias != "" {
			result.Columns = append(result.Columns, it.alias)
		} else {
			result.Columns = append(result.Columns, it.expr)
		}
	}

	for productID, revenue := range revenueByProduct {
		pNode := products[productID]
		if pNode == nil {
			continue
		}
		result.Rows = append(result.Rows, []interface{}{pNode.Properties["productName"], revenue})
	}

	if strings.TrimSpace(orderByClause) != "" {
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderByClause)
	}
	if skipVal > 0 {
		if skipVal < len(result.Rows) {
			result.Rows = result.Rows[skipVal:]
		} else {
			result.Rows = [][]interface{}{}
		}
	}
	if limitVal > 0 && limitVal < len(result.Rows) {
		result.Rows = result.Rows[:limitVal]
	}

	return result, true, nil
}
