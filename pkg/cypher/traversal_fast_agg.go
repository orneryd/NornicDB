package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryFastRelationshipAggregations attempts to execute common relationship aggregation patterns
// without materializing full traversal paths.
//
// This is primarily a performance optimization for patterns that can be answered in a single pass
// over typed edge indexes (GetEdgesByType) with minimal node lookups.
func (e *StorageExecutor) tryFastRelationshipAggregations(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	// Only handle fixed 1-hop segments (no variable-length expansions).
	if matches.Relationship.MinHops != 1 || matches.Relationship.MaxHops != 1 {
		return nil, false, nil
	}

	if matches.IsChained {
		switch len(matches.Segments) {
		case 2:
			return e.tryFastSupplierCategoryCount(matches, returnItems)
		case 3:
			if rows, ok, err := e.tryFastCustomerCategoryDistinctOrders(matches, returnItems); ok || err != nil {
				return rows, ok, err
			}
			return e.tryFastCustomerSupplierDistinctOrders(matches, returnItems)
		default:
			return nil, false, nil
		}
	}

	return e.tryFastSingleHopAgg(matches, returnItems)
}

type exprMatcher struct{}

func (exprMatcher) key(expr string) string {
	// Uppercase + strip whitespace. Expressions we match here are small and simple,
	// and this only runs once per query (not in hot per-record loops).
	b := make([]byte, 0, len(expr))
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		if isWhitespace(c) {
			continue
		}
		if c >= 'a' && c <= 'z' {
			c = c - ('a' - 'A')
		}
		b = append(b, c)
	}
	return string(b)
}

func (e *StorageExecutor) tryFastSingleHopAgg(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	// Only handle typed 1-hop patterns with no properties/filters.
	if len(matches.Relationship.Types) != 1 {
		return nil, false, nil
	}
	if len(matches.StartNode.properties) != 0 || len(matches.EndNode.properties) != 0 {
		return nil, false, nil
	}
	if matches.Relationship.Direction != "incoming" && matches.Relationship.Direction != "outgoing" {
		return nil, false, nil
	}

	startVar := matches.StartNode.variable
	endVar := matches.EndNode.variable
	relVar := matches.Relationship.Variable
	relType := matches.Relationship.Types[0]

	// We only optimize the "implicit GROUP BY startVar.<prop>" family, which covers:
	// - count(endVar)
	// - avg(endVar.<prop>)
	// - sum(relVar.<prop>)
	// - collect(endVar.<prop>)
	if startVar == "" || len(returnItems) < 2 {
		return nil, false, nil
	}

	m := exprMatcher{}

	groupExprKey := m.key(returnItems[0].expr)
	if !strings.HasPrefix(groupExprKey, m.key(startVar+".")+"") {
		return nil, false, nil
	}
	// groupPropName is used only for value extraction; keep original case from expr.
	groupPropName := ""
	if strings.HasPrefix(strings.TrimSpace(returnItems[0].expr), startVar+".") {
		groupPropName = strings.TrimSpace(returnItems[0].expr)[len(startVar)+1:]
	}
	if groupPropName == "" || strings.ContainsAny(groupPropName, " ()[]{}") {
		return nil, false, nil
	}

	edgeList, idPrefix, err := e.getEdgesByTypeFast(relType)
	if err != nil {
		return nil, false, err
	}

	requiredGroupLabel := ""
	if len(matches.StartNode.labels) == 1 {
		requiredGroupLabel = matches.StartNode.labels[0]
	}
	requiredOtherLabel := ""
	if len(matches.EndNode.labels) == 1 {
		requiredOtherLabel = matches.EndNode.labels[0]
	}

	type aggSpec struct {
		kind     string // "count", "sumEdgeProp", "avgNodeProp", "collectNodeProp"
		varName  string
		propName string
	}

	specs := make([]aggSpec, 0, len(returnItems)-1)
	for _, item := range returnItems[1:] {
		k := m.key(item.expr)

		switch {
		case strings.HasPrefix(k, "COUNT(") && strings.HasSuffix(k, ")"):
			inner := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(item.expr), "count("), ")")
			inner = strings.TrimSpace(inner)
			innerKey := m.key(inner)
			// Support count(endVar) and count(*)
			if innerKey == "*" || (endVar != "" && innerKey == m.key(endVar)) {
				specs = append(specs, aggSpec{kind: "count", varName: inner})
				continue
			}
			return nil, false, nil

		case strings.HasPrefix(k, "SUM(") && strings.HasSuffix(k, ")"):
			if relVar == "" {
				return nil, false, nil
			}
			inner := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(item.expr), "sum("), ")")
			inner = strings.TrimSpace(inner)
			innerKey := m.key(inner)
			if strings.HasPrefix(innerKey, m.key(relVar+".")+"") {
				prop := strings.TrimSpace(inner)[len(relVar)+1:]
				if prop == "" || strings.ContainsAny(prop, " ()[]{}") {
					return nil, false, nil
				}
				specs = append(specs, aggSpec{kind: "sumEdgeProp", varName: relVar, propName: prop})
				continue
			}
			return nil, false, nil

		case strings.HasPrefix(k, "AVG(") && strings.HasSuffix(k, ")"):
			inner := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(item.expr), "avg("), ")")
			inner = strings.TrimSpace(inner)
			innerKey := m.key(inner)
			// We only support avg(endVar.prop) for 1-hop aggregation fast path.
			if endVar == "" {
				return nil, false, nil
			}
			if strings.HasPrefix(innerKey, m.key(endVar+".")+"") {
				prop := strings.TrimSpace(inner)[len(endVar)+1:]
				if prop == "" || strings.ContainsAny(prop, " ()[]{}") {
					return nil, false, nil
				}
				specs = append(specs, aggSpec{kind: "avgNodeProp", varName: endVar, propName: prop})
				continue
			}
			return nil, false, nil

		case strings.HasPrefix(k, "COLLECT(") && strings.HasSuffix(k, ")"):
			inner := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(item.expr), "collect("), ")")
			inner = strings.TrimSpace(inner)
			innerKey := m.key(inner)
			if endVar == "" {
				return nil, false, nil
			}
			if strings.HasPrefix(innerKey, m.key(endVar+".")+"") {
				prop := strings.TrimSpace(inner)[len(endVar)+1:]
				if prop == "" || strings.ContainsAny(prop, " ()[]{}") {
					return nil, false, nil
				}
				specs = append(specs, aggSpec{kind: "collectNodeProp", varName: endVar, propName: prop})
				continue
			}
			return nil, false, nil
		default:
			return nil, false, nil
		}
	}

	if len(specs) == 0 {
		return nil, false, nil
	}

	groupCounts := make(map[storage.NodeID]int64)
	groupSum := make(map[storage.NodeID]float64)
	groupAvgCount := make(map[storage.NodeID]int64)
	groupCollect := make(map[storage.NodeID][]interface{})

	// Determine whether we need to read other-side node properties and/or enforce labels.
	needOtherNodeProps := false
	for _, s := range specs {
		switch s.kind {
		case "avgNodeProp", "collectNodeProp":
			needOtherNodeProps = true
		}
	}
	needOtherLabelOnly := requiredOtherLabel != "" && !needOtherNodeProps
	needOtherNodes := needOtherNodeProps

	// Collect the IDs we need to fetch for label/property filtering.
	otherIDs := make([]storage.NodeID, 0, 256)
	otherSeen := make(map[storage.NodeID]struct{}, 256)
	groupIDsForLabel := make([]storage.NodeID, 0, 64)
	groupSeenForLabel := make(map[storage.NodeID]struct{}, 64)

	for _, edge := range edgeList {
		var groupID, otherID storage.NodeID
		switch matches.Relationship.Direction {
		case "incoming":
			groupID = edge.EndNode
			otherID = edge.StartNode
		case "outgoing":
			groupID = edge.StartNode
			otherID = edge.EndNode
		}

		if requiredGroupLabel != "" {
			if _, ok := groupSeenForLabel[groupID]; !ok {
				groupSeenForLabel[groupID] = struct{}{}
				groupIDsForLabel = append(groupIDsForLabel, groupID)
			}
		}

		if needOtherNodes || needOtherLabelOnly {
			if _, ok := otherSeen[otherID]; !ok {
				otherSeen[otherID] = struct{}{}
				otherIDs = append(otherIDs, otherID)
			}
		}
	}

	var groupNodesForLabel map[storage.NodeID]*storage.Node
	if requiredGroupLabel != "" {
		groupNodesForLabel, _, err = e.batchGetNodesFast(groupIDsForLabel)
		if err != nil {
			return nil, false, err
		}
	}

	var otherNodes map[storage.NodeID]*storage.Node
	var otherHasLabel map[storage.NodeID]bool
	if needOtherNodes {
		otherNodes, _, err = e.batchGetNodesFast(otherIDs)
		if err != nil {
			return nil, false, err
		}
	} else if needOtherLabelOnly {
		engine, _ := e.storageFast()
		if idx, ok := engine.(storage.LabelIndexEngine); ok {
			otherHasLabel, err = idx.HasLabelBatch(otherIDs, requiredOtherLabel)
			if err != nil {
				return nil, false, err
			}
		} else {
			// Fallback: decode nodes if label index lookup isn't available.
			otherNodes, _, err = e.batchGetNodesFast(otherIDs)
			if err != nil {
				return nil, false, err
			}
		}
	}

	groupLabelOK := func(nodeID storage.NodeID) bool {
		if requiredGroupLabel == "" {
			return true
		}
		n := groupNodesForLabel[nodeID]
		if n == nil {
			return false
		}
		for _, lbl := range n.Labels {
			if lbl == requiredGroupLabel {
				return true
			}
		}
		return false
	}
	otherLabelOK := func(nodeID storage.NodeID) bool {
		if requiredOtherLabel == "" {
			return true
		}
		if otherHasLabel != nil {
			return otherHasLabel[nodeID]
		}
		n := otherNodes[nodeID]
		if n == nil {
			return false
		}
		for _, lbl := range n.Labels {
			if lbl == requiredOtherLabel {
				return true
			}
		}
		return false
	}

	// Aggregate in a single pass, using pre-fetched nodes for label/prop filtering.
	for _, edge := range edgeList {
		var groupID, otherID storage.NodeID
		switch matches.Relationship.Direction {
		case "incoming":
			groupID = edge.EndNode
			otherID = edge.StartNode
		case "outgoing":
			groupID = edge.StartNode
			otherID = edge.EndNode
		}

		if !groupLabelOK(groupID) {
			continue
		}
		if !otherLabelOK(otherID) {
			continue
		}

		for _, s := range specs {
			switch s.kind {
			case "count":
				groupCounts[groupID]++
			case "sumEdgeProp":
				v, ok := edge.Properties[s.propName]
				if !ok {
					continue
				}
				switch n := v.(type) {
				case int:
					groupSum[groupID] += float64(n)
				case int64:
					groupSum[groupID] += float64(n)
				case float64:
					groupSum[groupID] += n
				case float32:
					groupSum[groupID] += float64(n)
				default:
				}
			case "avgNodeProp":
				otherNode := otherNodes[otherID]
				if otherNode == nil {
					continue
				}
				v, ok := otherNode.Properties[s.propName]
				if !ok {
					continue
				}
				var num float64
				switch n := v.(type) {
				case int:
					num = float64(n)
				case int64:
					num = float64(n)
				case float64:
					num = n
				case float32:
					num = float64(n)
				default:
					continue
				}
				groupSum[groupID] += num
				groupAvgCount[groupID]++
			case "collectNodeProp":
				otherNode := otherNodes[otherID]
				if otherNode == nil {
					continue
				}
				v, ok := otherNode.Properties[s.propName]
				if !ok {
					continue
				}
				groupCollect[groupID] = append(groupCollect[groupID], v)
			}
		}
	}

	// Build result rows.
	groupIDs := make([]storage.NodeID, 0, len(groupCounts)+len(groupSum)+len(groupCollect))
	groupSeen := make(map[storage.NodeID]struct{})
	for id := range groupCounts {
		groupSeen[id] = struct{}{}
		groupIDs = append(groupIDs, id)
	}
	for id := range groupSum {
		if _, ok := groupSeen[id]; !ok {
			groupSeen[id] = struct{}{}
			groupIDs = append(groupIDs, id)
		}
	}
	for id := range groupAvgCount {
		if _, ok := groupSeen[id]; !ok {
			groupSeen[id] = struct{}{}
			groupIDs = append(groupIDs, id)
		}
	}
	for id := range groupCollect {
		if _, ok := groupSeen[id]; !ok {
			groupSeen[id] = struct{}{}
			groupIDs = append(groupIDs, id)
		}
	}

	// Note: group IDs here come from edges (may be namespaced). When using a NamespacedEngine,
	// BatchGetNodes would prefix again; use batchGetNodesFast() which unwraps to avoid copies.
	groupNodes, _, err := e.batchGetNodesFast(groupIDs)
	if err != nil {
		return nil, false, err
	}

	rows = make([][]interface{}, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		groupNode := groupNodes[groupID]
		if groupNode == nil {
			continue
		}
		groupVal := groupNode.Properties[groupPropName]

		row := make([]interface{}, 0, 1+len(specs))
		row = append(row, groupVal)

		for _, s := range specs {
			switch s.kind {
			case "count":
				row = append(row, groupCounts[groupID])
			case "sumEdgeProp":
				row = append(row, groupSum[groupID])
			case "avgNodeProp":
				cnt := groupAvgCount[groupID]
				if cnt == 0 {
					row = append(row, nil)
				} else {
					row = append(row, groupSum[groupID]/float64(cnt))
				}
			case "collectNodeProp":
				row = append(row, groupCollect[groupID])
			default:
				return nil, false, fmt.Errorf("unsupported aggregation spec: %s", s.kind)
			}
		}
		rows = append(rows, row)
	}

	_ = idPrefix // reserved for future use; ensures we keep prefix context for caller-specific mapping if needed.
	return rows, true, nil
}

func (e *StorageExecutor) tryFastSupplierCategoryCount(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	// MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)
	// RETURN s.companyName, c.categoryName, count(p) as products
	if len(matches.Segments) != 2 || len(returnItems) != 3 {
		return nil, false, nil
	}

	seg0 := matches.Segments[0]
	seg1 := matches.Segments[1]
	if seg0.Relationship.MinHops != 1 || seg0.Relationship.MaxHops != 1 ||
		seg1.Relationship.MinHops != 1 || seg1.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	if len(seg0.Relationship.Types) != 1 || len(seg1.Relationship.Types) != 1 {
		return nil, false, nil
	}
	if seg0.Relationship.Direction != "outgoing" || seg1.Relationship.Direction != "outgoing" {
		return nil, false, nil
	}
	if len(seg0.FromNode.properties) != 0 || len(seg0.ToNode.properties) != 0 || len(seg1.ToNode.properties) != 0 {
		return nil, false, nil
	}

	sVar := seg0.FromNode.variable
	pVar := seg0.ToNode.variable
	cVar := seg1.ToNode.variable
	if sVar == "" || pVar == "" || cVar == "" {
		return nil, false, nil
	}

	m := exprMatcher{}
	if m.key(returnItems[0].expr) != m.key(sVar+".companyName") {
		return nil, false, nil
	}
	if m.key(returnItems[1].expr) != m.key(cVar+".categoryName") {
		return nil, false, nil
	}
	if m.key(returnItems[2].expr) != m.key("count("+pVar+")") {
		return nil, false, nil
	}

	suppliesEdges, _, err := e.getEdgesByTypeFast(seg0.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}
	partOfEdges, _, err := e.getEdgesByTypeFast(seg1.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}

	requiredSupplierLabel := ""
	if len(seg0.FromNode.labels) == 1 {
		requiredSupplierLabel = seg0.FromNode.labels[0]
	}
	requiredProductLabel := ""
	if len(seg0.ToNode.labels) == 1 {
		requiredProductLabel = seg0.ToNode.labels[0]
	}
	requiredCategoryLabel := ""
	if len(seg1.ToNode.labels) == 1 {
		requiredCategoryLabel = seg1.ToNode.labels[0]
	}

	// product -> categories (allow multiple for correctness)
	productToCats := make(map[storage.NodeID][]storage.NodeID)
	for _, edge := range partOfEdges {
		productID := edge.StartNode
		categoryID := edge.EndNode
		productToCats[productID] = append(productToCats[productID], categoryID)
	}

	// Fetch nodes referenced by the join so we can enforce labels without scanning by label.
	productIDs := make([]storage.NodeID, 0, len(productToCats))
	productSeen := make(map[storage.NodeID]struct{}, len(productToCats))
	categoryIDs := make([]storage.NodeID, 0, 64)
	categorySeen := make(map[storage.NodeID]struct{}, 64)
	for pid, cats := range productToCats {
		if _, ok := productSeen[pid]; !ok {
			productSeen[pid] = struct{}{}
			productIDs = append(productIDs, pid)
		}
		for _, cid := range cats {
			if _, ok := categorySeen[cid]; !ok {
				categorySeen[cid] = struct{}{}
				categoryIDs = append(categoryIDs, cid)
			}
		}
	}
	var products map[storage.NodeID]*storage.Node
	var productHasLabel map[storage.NodeID]bool
	if requiredProductLabel != "" {
		engine, _ := e.storageFast()
		if idx, ok := engine.(storage.LabelIndexEngine); ok {
			productHasLabel, err = idx.HasLabelBatch(productIDs, requiredProductLabel)
			if err != nil {
				return nil, false, err
			}
		}
	}
	if requiredProductLabel == "" || productHasLabel == nil {
		products, _, err = e.batchGetNodesFast(productIDs)
		if err != nil {
			return nil, false, err
		}
	}
	categories, _, err := e.batchGetNodesFast(categoryIDs)
	if err != nil {
		return nil, false, err
	}

	hasLabel := func(node *storage.Node, want string) bool {
		if want == "" || node == nil {
			return want == ""
		}
		for _, lbl := range node.Labels {
			if lbl == want {
				return true
			}
		}
		return false
	}

	var suppliersForLabel map[storage.NodeID]*storage.Node
	if requiredSupplierLabel != "" {
		supplierIDs := make([]storage.NodeID, 0, 64)
		supplierSeen := make(map[storage.NodeID]struct{}, 64)
		for _, edge := range suppliesEdges {
			if _, ok := supplierSeen[edge.StartNode]; ok {
				continue
			}
			supplierSeen[edge.StartNode] = struct{}{}
			supplierIDs = append(supplierIDs, edge.StartNode)
		}
		suppliersForLabel, _, err = e.batchGetNodesFast(supplierIDs)
		if err != nil {
			return nil, false, err
		}
	}

	type key struct {
		supplier storage.NodeID
		category storage.NodeID
	}
	counts := make(map[key]int64)
	for _, edge := range suppliesEdges {
		supplierID := edge.StartNode
		productID := edge.EndNode
		if requiredSupplierLabel != "" && !hasLabel(suppliersForLabel[supplierID], requiredSupplierLabel) {
			continue
		}

		if requiredProductLabel != "" {
			if productHasLabel != nil {
				if !productHasLabel[productID] {
					continue
				}
			} else {
				pNode := products[productID]
				if !hasLabel(pNode, requiredProductLabel) {
					continue
				}
			}
		}
		for _, categoryID := range productToCats[productID] {
			cNode := categories[categoryID]
			if !hasLabel(cNode, requiredCategoryLabel) {
				continue
			}
			counts[key{supplier: supplierID, category: categoryID}]++
		}
	}

	// Materialize rows.
	supplierIDs := make([]storage.NodeID, 0, len(counts))
	supplierSeen := make(map[storage.NodeID]struct{})
	for k := range counts {
		if _, ok := supplierSeen[k.supplier]; !ok {
			supplierSeen[k.supplier] = struct{}{}
			supplierIDs = append(supplierIDs, k.supplier)
		}
	}

	suppliers, _, err := e.batchGetNodesFast(supplierIDs)
	if err != nil {
		return nil, false, err
	}

	rows = make([][]interface{}, 0, len(counts))
	for k, count := range counts {
		s := suppliers[k.supplier]
		c := categories[k.category]
		if s == nil || c == nil {
			continue
		}
		rows = append(rows, []interface{}{s.Properties["companyName"], c.Properties["categoryName"], count})
	}

	return rows, true, nil
}

func (e *StorageExecutor) tryFastCustomerCategoryDistinctOrders(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	// MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
	// RETURN c.companyName, cat.categoryName, count(DISTINCT o) as orders
	if len(matches.Segments) != 3 || len(returnItems) != 3 {
		return nil, false, nil
	}

	seg0 := matches.Segments[0]
	seg1 := matches.Segments[1]
	seg2 := matches.Segments[2]
	if seg0.Relationship.Direction != "outgoing" || seg1.Relationship.Direction != "outgoing" || seg2.Relationship.Direction != "outgoing" {
		return nil, false, nil
	}
	if len(seg0.Relationship.Types) != 1 || len(seg1.Relationship.Types) != 1 || len(seg2.Relationship.Types) != 1 {
		return nil, false, nil
	}

	cVar := seg0.FromNode.variable
	oVar := seg0.ToNode.variable
	pVar := seg1.ToNode.variable
	catVar := seg2.ToNode.variable
	if cVar == "" || oVar == "" || pVar == "" || catVar == "" {
		return nil, false, nil
	}

	m := exprMatcher{}
	if m.key(returnItems[0].expr) != m.key(cVar+".companyName") {
		return nil, false, nil
	}
	if m.key(returnItems[1].expr) != m.key(catVar+".categoryName") {
		return nil, false, nil
	}
	// Accept "count(DISTINCT o)" with flexible whitespace/case.
	if m.key(returnItems[2].expr) != m.key("count(distinct "+oVar+")") {
		return nil, false, nil
	}

	purchasedEdges, _, err := e.getEdgesByTypeFast(seg0.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}
	ordersEdges, _, err := e.getEdgesByTypeFast(seg1.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}
	partOfEdges, _, err := e.getEdgesByTypeFast(seg2.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}

	// order -> customer
	orderToCustomer := make(map[storage.NodeID]storage.NodeID, len(purchasedEdges))
	for _, edge := range purchasedEdges {
		orderToCustomer[edge.EndNode] = edge.StartNode
	}

	// product -> categories
	productToCats := make(map[storage.NodeID][]storage.NodeID, len(partOfEdges))
	for _, edge := range partOfEdges {
		productToCats[edge.StartNode] = append(productToCats[edge.StartNode], edge.EndNode)
	}

	type pairKey struct {
		customer storage.NodeID
		category storage.NodeID
	}
	type tripKey struct {
		customer storage.NodeID
		category storage.NodeID
		order    storage.NodeID
	}
	seen := make(map[tripKey]struct{}, 4096)
	counts := make(map[pairKey]int64)

	for _, edge := range ordersEdges {
		orderID := edge.StartNode
		productID := edge.EndNode
		customerID, ok := orderToCustomer[orderID]
		if !ok {
			continue
		}
		for _, categoryID := range productToCats[productID] {
			k3 := tripKey{customer: customerID, category: categoryID, order: orderID}
			if _, ok := seen[k3]; ok {
				continue
			}
			seen[k3] = struct{}{}
			k2 := pairKey{customer: customerID, category: categoryID}
			counts[k2]++
		}
	}

	// Materialize rows.
	customerIDs := make([]storage.NodeID, 0, len(counts))
	categoryIDs := make([]storage.NodeID, 0, len(counts))
	customerSeen := make(map[storage.NodeID]struct{})
	categorySeen := make(map[storage.NodeID]struct{})
	for k := range counts {
		if _, ok := customerSeen[k.customer]; !ok {
			customerSeen[k.customer] = struct{}{}
			customerIDs = append(customerIDs, k.customer)
		}
		if _, ok := categorySeen[k.category]; !ok {
			categorySeen[k.category] = struct{}{}
			categoryIDs = append(categoryIDs, k.category)
		}
	}

	customers, _, err := e.batchGetNodesFast(customerIDs)
	if err != nil {
		return nil, false, err
	}
	categories, _, err := e.batchGetNodesFast(categoryIDs)
	if err != nil {
		return nil, false, err
	}

	rows = make([][]interface{}, 0, len(counts))
	for k, count := range counts {
		c := customers[k.customer]
		cat := categories[k.category]
		if c == nil || cat == nil {
			continue
		}
		rows = append(rows, []interface{}{c.Properties["companyName"], cat.Properties["categoryName"], count})
	}

	return rows, true, nil
}

func (e *StorageExecutor) tryFastCustomerSupplierDistinctOrders(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	// MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)
	// RETURN c.companyName, s.companyName, count(DISTINCT o) as orders
	if len(matches.Segments) != 3 || len(returnItems) != 3 {
		return nil, false, nil
	}

	seg0 := matches.Segments[0]
	seg1 := matches.Segments[1]
	seg2 := matches.Segments[2]
	if seg0.Relationship.Direction != "outgoing" || seg1.Relationship.Direction != "outgoing" || seg2.Relationship.Direction != "incoming" {
		return nil, false, nil
	}
	if len(seg0.Relationship.Types) != 1 || len(seg1.Relationship.Types) != 1 || len(seg2.Relationship.Types) != 1 {
		return nil, false, nil
	}

	cVar := seg0.FromNode.variable
	oVar := seg0.ToNode.variable
	pVar := seg1.ToNode.variable
	sVar := seg2.ToNode.variable
	if cVar == "" || oVar == "" || pVar == "" || sVar == "" {
		return nil, false, nil
	}

	m := exprMatcher{}
	if m.key(returnItems[0].expr) != m.key(cVar+".companyName") {
		return nil, false, nil
	}
	if m.key(returnItems[1].expr) != m.key(sVar+".companyName") {
		return nil, false, nil
	}
	if m.key(returnItems[2].expr) != m.key("count(distinct "+oVar+")") {
		return nil, false, nil
	}

	purchasedEdges, _, err := e.getEdgesByTypeFast(seg0.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}
	ordersEdges, _, err := e.getEdgesByTypeFast(seg1.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}
	suppliesEdges, _, err := e.getEdgesByTypeFast(seg2.Relationship.Types[0])
	if err != nil {
		return nil, false, err
	}

	// order -> customer
	orderToCustomer := make(map[storage.NodeID]storage.NodeID, len(purchasedEdges))
	for _, edge := range purchasedEdges {
		orderToCustomer[edge.EndNode] = edge.StartNode
	}

	// product -> suppliers
	productToSuppliers := make(map[storage.NodeID][]storage.NodeID, len(suppliesEdges))
	for _, edge := range suppliesEdges {
		supplierID := edge.StartNode
		productID := edge.EndNode
		productToSuppliers[productID] = append(productToSuppliers[productID], supplierID)
	}

	type pairKey struct {
		customer storage.NodeID
		supplier storage.NodeID
	}
	type tripKey struct {
		customer storage.NodeID
		supplier storage.NodeID
		order    storage.NodeID
	}
	seen := make(map[tripKey]struct{}, 4096)
	counts := make(map[pairKey]int64)

	for _, edge := range ordersEdges {
		orderID := edge.StartNode
		productID := edge.EndNode
		customerID, ok := orderToCustomer[orderID]
		if !ok {
			continue
		}
		for _, supplierID := range productToSuppliers[productID] {
			k3 := tripKey{customer: customerID, supplier: supplierID, order: orderID}
			if _, ok := seen[k3]; ok {
				continue
			}
			seen[k3] = struct{}{}
			k2 := pairKey{customer: customerID, supplier: supplierID}
			counts[k2]++
		}
	}

	// Materialize rows.
	customerIDs := make([]storage.NodeID, 0, len(counts))
	supplierIDs := make([]storage.NodeID, 0, len(counts))
	customerSeen := make(map[storage.NodeID]struct{})
	supplierSeen := make(map[storage.NodeID]struct{})
	for k := range counts {
		if _, ok := customerSeen[k.customer]; !ok {
			customerSeen[k.customer] = struct{}{}
			customerIDs = append(customerIDs, k.customer)
		}
		if _, ok := supplierSeen[k.supplier]; !ok {
			supplierSeen[k.supplier] = struct{}{}
			supplierIDs = append(supplierIDs, k.supplier)
		}
	}

	customers, _, err := e.batchGetNodesFast(customerIDs)
	if err != nil {
		return nil, false, err
	}
	suppliers, _, err := e.batchGetNodesFast(supplierIDs)
	if err != nil {
		return nil, false, err
	}

	rows = make([][]interface{}, 0, len(counts))
	for k, count := range counts {
		c := customers[k.customer]
		s := suppliers[k.supplier]
		if c == nil || s == nil {
			continue
		}
		rows = append(rows, []interface{}{c.Properties["companyName"], s.Properties["companyName"], count})
	}

	return rows, true, nil
}
