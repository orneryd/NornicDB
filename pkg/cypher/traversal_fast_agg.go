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
	if matches.StartNode.variable != "" && matches.StartNode.variable == matches.EndNode.variable {
		return nil, false, nil
	}

	if matches.IsChained {
		return e.tryFastChainedRelationshipAggregation(matches, returnItems)
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

	requiredGroupLabels := append([]string(nil), matches.StartNode.labels...)
	requiredOtherLabels := append([]string(nil), matches.EndNode.labels...)

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
	needOtherLabelOnly := len(requiredOtherLabels) > 0 && !needOtherNodeProps
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

		if len(requiredGroupLabels) > 0 {
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
	if len(requiredGroupLabels) > 0 {
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
	} else if needOtherLabelOnly && len(requiredOtherLabels) == 1 {
		engine, _ := e.storageFast()
		if idx, ok := engine.(storage.LabelIndexEngine); ok {
			otherHasLabel, err = idx.HasLabelBatch(otherIDs, requiredOtherLabels[0])
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
		if len(requiredGroupLabels) == 0 {
			return true
		}
		n := groupNodesForLabel[nodeID]
		return n != nil && mergeNodeHasLabels(n, requiredGroupLabels)
	}
	otherLabelOK := func(nodeID storage.NodeID) bool {
		if len(requiredOtherLabels) == 0 {
			return true
		}
		if otherHasLabel != nil {
			return otherHasLabel[nodeID]
		}
		n := otherNodes[nodeID]
		return n != nil && mergeNodeHasLabels(n, requiredOtherLabels)
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

type chainedAggregationPlan struct {
	kind      string
	leftVar   string
	leftProp  string
	rightVar  string
	rightProp string
	countVar  string
	distinct  bool
}

func parseTraversalPropertyProjection(expr string) (variable, property string, ok bool) {
	expr = strings.TrimSpace(expr)
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	variable = strings.TrimSpace(expr[:dot])
	property = normalizePropertyKey(strings.TrimSpace(expr[dot+1:]))
	if variable == "" || property == "" {
		return "", "", false
	}
	return variable, property, true
}

func traversalNodesHaveNoProperties(segments []TraversalSegment) bool {
	for _, seg := range segments {
		if len(seg.FromNode.properties) != 0 || len(seg.ToNode.properties) != 0 {
			return false
		}
	}
	return true
}

func buildChainedAggregationPlan(matches *TraversalMatch, returnItems []returnItem) (chainedAggregationPlan, bool) {
	if matches == nil || len(returnItems) != 3 || len(matches.Segments) < 2 || len(matches.Segments) > 3 {
		return chainedAggregationPlan{}, false
	}
	for _, seg := range matches.Segments {
		if seg.Relationship.MinHops != 1 || seg.Relationship.MaxHops != 1 || len(seg.Relationship.Types) != 1 {
			return chainedAggregationPlan{}, false
		}
	}
	if !traversalNodesHaveNoProperties(matches.Segments) {
		return chainedAggregationPlan{}, false
	}

	leftVar, leftProp, ok := parseTraversalPropertyProjection(returnItems[0].expr)
	if !ok {
		return chainedAggregationPlan{}, false
	}
	rightVar, rightProp, ok := parseTraversalPropertyProjection(returnItems[1].expr)
	if !ok {
		return chainedAggregationPlan{}, false
	}
	agg := ParseAggregation(returnItems[2].expr)
	if agg == nil || !strings.EqualFold(agg.Function, "COUNT") || agg.IsStar || agg.Property != "" || agg.Variable == "" {
		return chainedAggregationPlan{}, false
	}

	plan := chainedAggregationPlan{
		leftVar:   leftVar,
		leftProp:  leftProp,
		rightVar:  rightVar,
		rightProp: rightProp,
		countVar:  agg.Variable,
		distinct:  agg.Distinct,
	}

	seg0 := matches.Segments[0]
	switch len(matches.Segments) {
	case 2:
		seg1 := matches.Segments[1]
		if seg0.Relationship.Direction != "outgoing" || seg1.Relationship.Direction != "outgoing" {
			return chainedAggregationPlan{}, false
		}
		if plan.leftVar != seg0.FromNode.variable || plan.rightVar != seg1.ToNode.variable || plan.countVar != seg0.ToNode.variable || plan.distinct {
			return chainedAggregationPlan{}, false
		}
		plan.kind = "two-hop-boundary-count"
	case 3:
		seg1 := matches.Segments[1]
		seg2 := matches.Segments[2]
		if seg0.Relationship.Direction != "outgoing" || seg1.Relationship.Direction != "outgoing" {
			return chainedAggregationPlan{}, false
		}
		if plan.leftVar != seg0.FromNode.variable || plan.rightVar != seg2.ToNode.variable || plan.countVar != seg0.ToNode.variable || !plan.distinct {
			return chainedAggregationPlan{}, false
		}
		switch seg2.Relationship.Direction {
		case "outgoing":
			plan.kind = "three-hop-linear-distinct"
		case "incoming":
			plan.kind = "three-hop-converging-distinct"
		default:
			return chainedAggregationPlan{}, false
		}
	default:
		return chainedAggregationPlan{}, false
	}

	return plan, true
}

func chainedNodeHasLabels(node *storage.Node, labels []string) bool {
	if len(labels) == 0 {
		return true
	}
	return node != nil && mergeNodeHasLabels(node, labels)
}

func appendUniqueNodeID(ids []storage.NodeID, seen map[storage.NodeID]struct{}, id storage.NodeID) []storage.NodeID {
	if _, ok := seen[id]; ok {
		return ids
	}
	seen[id] = struct{}{}
	return append(ids, id)
}

func (e *StorageExecutor) tryFastChainedRelationshipAggregation(matches *TraversalMatch, returnItems []returnItem) (rows [][]interface{}, ok bool, err error) {
	plan, ok := buildChainedAggregationPlan(matches, returnItems)
	if !ok {
		return nil, false, nil
	}

	seg0 := matches.Segments[0]
	seg1 := matches.Segments[1]
	requiredLeftLabels := append([]string(nil), seg0.FromNode.labels...)
	requiredCountLabels := append([]string(nil), seg0.ToNode.labels...)
	requiredJoinLabels := append([]string(nil), seg1.ToNode.labels...)

	type pairKey struct {
		left  storage.NodeID
		right storage.NodeID
	}
	type tripKey struct {
		left    storage.NodeID
		right   storage.NodeID
		counted storage.NodeID
	}

	leftToRightCounts := make(map[pairKey]int64)
	var rightNodes map[storage.NodeID]*storage.Node

	switch plan.kind {
	case "two-hop-boundary-count":
		edges0, _, err := e.getEdgesByTypeFast(seg0.Relationship.Types[0])
		if err != nil {
			return nil, false, err
		}
		edges1, _, err := e.getEdgesByTypeFast(seg1.Relationship.Types[0])
		if err != nil {
			return nil, false, err
		}

		countNodeToRight := make(map[storage.NodeID][]storage.NodeID, len(edges1))
		countIDs := make([]storage.NodeID, 0, len(edges1))
		countSeen := make(map[storage.NodeID]struct{}, len(edges1))
		rightIDs := make([]storage.NodeID, 0, len(edges1))
		rightSeen := make(map[storage.NodeID]struct{}, len(edges1))
		for _, edge := range edges1 {
			countNodeToRight[edge.StartNode] = append(countNodeToRight[edge.StartNode], edge.EndNode)
			countIDs = appendUniqueNodeID(countIDs, countSeen, edge.StartNode)
			rightIDs = appendUniqueNodeID(rightIDs, rightSeen, edge.EndNode)
		}
		countNodes, _, err := e.batchGetNodesFast(countIDs)
		if err != nil {
			return nil, false, err
		}
		rightNodes, _, err = e.batchGetNodesFast(rightIDs)
		if err != nil {
			return nil, false, err
		}

		leftIDs := make([]storage.NodeID, 0, len(edges0))
		leftSeen := make(map[storage.NodeID]struct{}, len(edges0))
		for _, edge := range edges0 {
			leftIDs = appendUniqueNodeID(leftIDs, leftSeen, edge.StartNode)
			if !chainedNodeHasLabels(countNodes[edge.EndNode], requiredCountLabels) {
				continue
			}
			for _, rightID := range countNodeToRight[edge.EndNode] {
				if !chainedNodeHasLabels(rightNodes[rightID], requiredJoinLabels) {
					continue
				}
				leftToRightCounts[pairKey{left: edge.StartNode, right: rightID}]++
			}
		}
	case "three-hop-linear-distinct", "three-hop-converging-distinct":
		seg2 := matches.Segments[2]
		requiredRightLabels := append([]string(nil), seg2.ToNode.labels...)

		edges0, _, err := e.getEdgesByTypeFast(seg0.Relationship.Types[0])
		if err != nil {
			return nil, false, err
		}
		edges1, _, err := e.getEdgesByTypeFast(seg1.Relationship.Types[0])
		if err != nil {
			return nil, false, err
		}
		edges2, _, err := e.getEdgesByTypeFast(seg2.Relationship.Types[0])
		if err != nil {
			return nil, false, err
		}

		countNodeToLeft := make(map[storage.NodeID]storage.NodeID, len(edges0))
		countIDs := make([]storage.NodeID, 0, len(edges0))
		countSeen := make(map[storage.NodeID]struct{}, len(edges0))
		for _, edge := range edges0 {
			countNodeToLeft[edge.EndNode] = edge.StartNode
			countIDs = appendUniqueNodeID(countIDs, countSeen, edge.EndNode)
		}

		joinNodeToRight := make(map[storage.NodeID][]storage.NodeID, len(edges2))
		joinIDs := make([]storage.NodeID, 0, len(edges2))
		joinSeen := make(map[storage.NodeID]struct{}, len(edges2))
		rightIDs := make([]storage.NodeID, 0, len(edges2))
		rightSeen := make(map[storage.NodeID]struct{}, len(edges2))
		for _, edge := range edges2 {
			joinID := edge.StartNode
			rightID := edge.EndNode
			if plan.kind == "three-hop-converging-distinct" {
				joinID = edge.EndNode
				rightID = edge.StartNode
			}
			joinNodeToRight[joinID] = append(joinNodeToRight[joinID], rightID)
			joinIDs = appendUniqueNodeID(joinIDs, joinSeen, joinID)
			rightIDs = appendUniqueNodeID(rightIDs, rightSeen, rightID)
		}

		countNodes, _, err := e.batchGetNodesFast(countIDs)
		if err != nil {
			return nil, false, err
		}
		joinNodes, _, err := e.batchGetNodesFast(joinIDs)
		if err != nil {
			return nil, false, err
		}
		rightNodes, _, err = e.batchGetNodesFast(rightIDs)
		if err != nil {
			return nil, false, err
		}

		seen := make(map[tripKey]struct{}, 4096)
		for _, edge := range edges1 {
			countID := edge.StartNode
			joinID := edge.EndNode
			leftID, found := countNodeToLeft[countID]
			if !found {
				continue
			}
			if !chainedNodeHasLabels(countNodes[countID], requiredCountLabels) || !chainedNodeHasLabels(joinNodes[joinID], requiredJoinLabels) {
				continue
			}
			for _, rightID := range joinNodeToRight[joinID] {
				if !chainedNodeHasLabels(rightNodes[rightID], requiredRightLabels) {
					continue
				}
				trip := tripKey{left: leftID, right: rightID, counted: countID}
				if _, exists := seen[trip]; exists {
					continue
				}
				seen[trip] = struct{}{}
				leftToRightCounts[pairKey{left: leftID, right: rightID}]++
			}
		}
	default:
		return nil, false, nil
	}

	leftIDs := make([]storage.NodeID, 0, len(leftToRightCounts))
	rightIDs := make([]storage.NodeID, 0, len(leftToRightCounts))
	leftSeen := make(map[storage.NodeID]struct{}, len(leftToRightCounts))
	rightSeen := make(map[storage.NodeID]struct{}, len(leftToRightCounts))
	for key := range leftToRightCounts {
		leftIDs = appendUniqueNodeID(leftIDs, leftSeen, key.left)
		rightIDs = appendUniqueNodeID(rightIDs, rightSeen, key.right)
	}
	leftNodes, _, err := e.batchGetNodesFast(leftIDs)
	if err != nil {
		return nil, false, err
	}
	if rightNodes == nil {
		rightNodes, _, err = e.batchGetNodesFast(rightIDs)
		if err != nil {
			return nil, false, err
		}
	}

	rows = make([][]interface{}, 0, len(leftToRightCounts))
	for key, count := range leftToRightCounts {
		leftNode := leftNodes[key.left]
		rightNode := rightNodes[key.right]
		if !chainedNodeHasLabels(leftNode, requiredLeftLabels) || rightNode == nil {
			continue
		}
		rows = append(rows, []interface{}{leftNode.Properties[plan.leftProp], rightNode.Properties[plan.rightProp], count})
	}

	return rows, true, nil
}
