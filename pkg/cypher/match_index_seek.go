package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryCollectNodesFromIDEquality attempts to satisfy:
//
//	MATCH (n[:Label]) WHERE id(n) = <id>
//	MATCH (n[:Label]) WHERE elementId(n) = <element-id>
//
// via a direct node lookup instead of scanning all nodes.
func (e *StorageExecutor) tryCollectNodesFromIDEquality(ctx context.Context, nodePattern nodePatternInfo, whereClause string) ([]*storage.Node, bool, error) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return nil, false, nil
	}

	upper := strings.ToUpper(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " IN ") {
		return nil, false, nil
	}

	eqIdx := strings.Index(clause, "=")
	if eqIdx <= 0 {
		return nil, false, nil
	}
	// Ignore >=, <=, !=, <>
	if eqIdx > 0 {
		prev := clause[eqIdx-1]
		if prev == '!' || prev == '<' || prev == '>' {
			return nil, false, nil
		}
	}
	if eqIdx+1 < len(clause) {
		next := clause[eqIdx+1]
		if next == '=' {
			return nil, false, nil
		}
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return nil, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := strings.ToLower(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return nil, false, nil
	}

	if varName == "" || varName != nodePattern.variable {
		return nil, false, nil
	}

	rawVal := e.parseValue(ctx, right)
	idValue, ok := rawVal.(string)
	if !ok || strings.TrimSpace(idValue) == "" {
		return []*storage.Node{}, true, nil
	}
	idValue = strings.TrimSpace(idValue)

	lookupID := idValue
	if kind == "elementId" {
		// Accept canonical element IDs ("4:<db>:<id>") by extracting the ID payload.
		parts := strings.SplitN(idValue, ":", 3)
		if len(parts) == 3 && parts[0] == "4" {
			lookupID = parts[2]
		}
	}
	// Be permissive for id(n) lookups that accidentally pass an elementId() value.
	if strings.HasPrefix(lookupID, "4:") {
		if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 {
			lookupID = parts[2]
		}
	}

	node, err := e.storage.GetNode(storage.NodeID(lookupID))
	if err != nil || node == nil {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.properties) > 0 {
		for k, v := range nodePattern.properties {
			if node.Properties[k] != v {
				return []*storage.Node{}, true, nil
			}
		}
	}

	return []*storage.Node{node}, true, nil
}

// tryCollectNodesFromIDEqualityParam attempts to satisfy:
//
//	MATCH (n[:Label]) WHERE id(n) = $param
//	MATCH (n[:Label]) WHERE elementId(n) = $param
//
// via a direct node lookup using the parameter value. This complements
// tryCollectNodesFromIDEquality which only handles literal values on the RHS.
// The parameter-aware variant is critical for app workloads that always pass
// IDs as parameters (e.g., attach/read-by-id paths).
func (e *StorageExecutor) tryCollectNodesFromIDEqualityParam(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	if params == nil || strings.TrimSpace(whereClause) == "" {
		return nil, false, nil
	}
	clause := strings.TrimSpace(whereClause)

	upper := strings.ToUpper(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " IN ") {
		return nil, false, nil
	}

	eqIdx := strings.Index(clause, "=")
	if eqIdx <= 0 {
		return nil, false, nil
	}
	if eqIdx > 0 {
		prev := clause[eqIdx-1]
		if prev == '!' || prev == '<' || prev == '>' {
			return nil, false, nil
		}
	}
	if eqIdx+1 < len(clause) {
		next := clause[eqIdx+1]
		if next == '=' {
			return nil, false, nil
		}
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return nil, false, nil
	}

	// RHS must be a parameter reference ($paramName).
	if !strings.HasPrefix(right, "$") {
		return nil, false, nil
	}
	paramName := strings.TrimSpace(right[1:])
	if paramName == "" {
		return nil, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := strings.ToLower(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return nil, false, nil
	}

	if varName == "" || varName != nodePattern.variable {
		return nil, false, nil
	}

	raw, ok := params[paramName]
	if !ok {
		return []*storage.Node{}, true, nil
	}
	idValue, ok := raw.(string)
	if !ok || strings.TrimSpace(idValue) == "" {
		return []*storage.Node{}, true, nil
	}
	idValue = strings.TrimSpace(idValue)

	lookupID := idValue
	if kind == "elementId" {
		parts := strings.SplitN(idValue, ":", 3)
		if len(parts) == 3 && parts[0] == "4" {
			lookupID = parts[2]
		}
	}
	// Permissive: strip canonical element ID prefix for id() lookups too.
	if strings.HasPrefix(lookupID, "4:") {
		if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 {
			lookupID = parts[2]
		}
	}

	node, err := e.storage.GetNode(storage.NodeID(lookupID))
	if err != nil || node == nil {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.properties) > 0 {
		for k, v := range nodePattern.properties {
			if node.Properties[k] != v {
				return []*storage.Node{}, true, nil
			}
		}
	}

	return []*storage.Node{node}, true, nil
}

// tryCollectNodesFromIDEqualityCompound attempts ID/elementId direct seeks from
// simple and compound predicates over a single MATCH variable.
//
// Supported safe forms:
//   - id(n) = '...'
//   - elementId(n) = '...'
//   - id(n) = $param
//   - (... AND id(n) = ...)
//   - (id(n) = ... OR n.id = ... ) where every OR branch is an ID-equality form
//
// For OR predicates, this helper only engages when all disjuncts are recognized
// ID-equality predicates; otherwise it returns (nil, false, nil) to avoid unsafe
// pruning.
func (e *StorageExecutor) tryCollectNodesFromIDEqualityCompound(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil, false, nil
	}

	// 1) Simple clause (literal or param).
	if nodes, used, err := e.tryCollectNodesFromIDEquality(ctx, nodePattern, clause); used || err != nil {
		return nodes, used, err
	}
	if nodes, used, err := e.tryCollectNodesFromIDEqualityParam(ctx, nodePattern, clause, params); used || err != nil {
		return nodes, used, err
	}

	// 2) Conjunction: any recognized id-conjunct is safe to use as a pruning seek.
	if findTopLevelKeyword(clause, " AND ") > 0 {
		for _, raw := range splitTopLevelAndConjuncts(clause) {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			if nodes, used, err := e.tryCollectNodesFromIDEquality(ctx, nodePattern, term); used || err != nil {
				return nodes, used, err
			}
			if nodes, used, err := e.tryCollectNodesFromIDEqualityParam(ctx, nodePattern, term, params); used || err != nil {
				return nodes, used, err
			}
		}
	}

	// 3) Disjunction: only safe when every branch is recognized as an ID seek.
	orTerms := splitTopLevelOrTerms(clause)
	if len(orTerms) > 1 {
		merged := make([]*storage.Node, 0, len(orTerms))
		seen := make(map[storage.NodeID]struct{}, len(orTerms))
		for _, raw := range orTerms {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				return nil, false, nil
			}

			nodes, used, err := e.tryCollectNodesFromIDEquality(ctx, nodePattern, term)
			if err != nil {
				return nil, true, err
			}
			if !used {
				nodes, used, err = e.tryCollectNodesFromIDEqualityParam(ctx, nodePattern, term, params)
				if err != nil {
					return nil, true, err
				}
			}
			if !used {
				// At least one OR branch is not a recognized ID-equality predicate.
				// Bail out to preserve semantics.
				return nil, false, nil
			}

			for _, node := range nodes {
				if node == nil {
					continue
				}
				if _, ok := seen[node.ID]; ok {
					continue
				}
				seen[node.ID] = struct{}{}
				merged = append(merged, node)
			}
		}
		return merged, true, nil
	}

	return nil, false, nil
}

// tryCollectNodesFromPropertyIndex attempts to satisfy MATCH node candidates from a schema property index.
// It only applies to simple equality predicates:
//   - <var>.<prop> = <value>
//   - <value> = <var>.<prop>
//
// It returns (nodes, true, nil) when index planning was used (including empty matches),
// and (nil, false, nil) when the predicate is not eligible for index lookup.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndex(ctx context.Context, nodePattern nodePatternInfo, whereClause string) ([]*storage.Node, bool, error) {
	property, value, ok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{})
	for _, label := range labels {
		for _, id := range schema.PropertyIndexLookup(label, property, value) {
			idSet[id] = struct{}{}
		}
	}

	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexIn attempts to satisfy simple IN-list predicates:
//
//	<var>.<prop> IN $param
//
// where $param is a list value from params. This path is used heavily by Fabric
// batched correlated APPLY lookups.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexIn(
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	property, listValues, ok := e.parseSimpleIndexedInParam(nodePattern.variable, whereClause, params)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range labels {
		for _, value := range listValues {
			for _, id := range schema.PropertyIndexLookup(label, property, value) {
				idSet[id] = struct{}{}
			}
		}
	}
	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexInLiteral attempts to satisfy simple IN-list
// predicates with literal lists:
//
//	<var>.<prop> IN ['a', 'b', ...]
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexInLiteral(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
) ([]*storage.Node, bool, error) {
	property, listValues, ok := e.parseSimpleIndexedInLiteral(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range labels {
		for _, value := range listValues {
			for _, id := range schema.PropertyIndexLookup(label, property, value) {
				idSet[id] = struct{}{}
			}
		}
	}
	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexInOrParam attempts to satisfy OR-combined IN
// predicates backed by indexes, e.g.:
//
//	n.textKey IN $keys OR n.textKey128 IN $keys
//
// This is important for cleanup/read paths that pass large key lists and would
// otherwise full-scan labels.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexInOrParam(
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" || params == nil {
		return nil, false, nil
	}
	orIdx := findTopLevelKeyword(clause, " OR ")
	if orIdx <= 0 {
		return nil, false, nil
	}
	left := strings.TrimSpace(clause[:orIdx])
	right := strings.TrimSpace(clause[orIdx+4:])
	if left == "" || right == "" {
		return nil, false, nil
	}

	lprop, lvals, lok := e.parseSimpleIndexedInParam(nodePattern.variable, left, params)
	rprop, rvals, rok := e.parseSimpleIndexedInParam(nodePattern.variable, right, params)
	if !lok || !rok {
		return nil, false, nil
	}

	// Merge values (keep deterministic and duplicate-free).
	merged := make([]interface{}, 0, len(lvals)+len(rvals))
	seenVals := make(map[string]struct{}, len(lvals)+len(rvals))
	for _, v := range append(lvals, rvals...) {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, ok := seenVals[k]; ok {
			continue
		}
		seenVals[k] = struct{}{}
		merged = append(merged, v)
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	leftLabels := e.indexCandidateLabels(schema, nodePattern.labels, lprop)
	rightLabels := e.indexCandidateLabels(schema, nodePattern.labels, rprop)
	if len(leftLabels) == 0 || len(rightLabels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range leftLabels {
		for _, v := range merged {
			for _, id := range schema.PropertyIndexLookup(label, lprop, v) {
				idSet[id] = struct{}{}
			}
		}
	}
	for _, label := range rightLabels {
		for _, v := range merged {
			for _, id := range schema.PropertyIndexLookup(label, rprop, v) {
				idSet[id] = struct{}{}
			}
		}
	}
	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexOrEquality attempts to satisfy OR-combined
// equality predicates backed by indexes, e.g.:
//
//	a.prop1 = $x OR a.prop2 = $x
//	a.prop1 = 'val1' OR a.prop2 = 'val2'
//
// It splits the OR into two branches, attempts index lookup on each, and merges
// results with deduplication. This removes a major source of slow scans for
// dual-key lookup workloads.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexOrEquality(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return nil, false, nil
	}
	orIdx := findTopLevelKeyword(clause, " OR ")
	if orIdx <= 0 {
		return nil, false, nil
	}
	left := strings.TrimSpace(clause[:orIdx])
	right := strings.TrimSpace(clause[orIdx+4:])
	if left == "" || right == "" {
		return nil, false, nil
	}

	// Try to parse each branch as a simple indexed equality predicate.
	lprop, lval, lok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, left)
	rprop, rval, rok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, right)
	if !lok || !rok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	leftLabels := e.indexCandidateLabels(schema, nodePattern.labels, lprop)
	rightLabels := e.indexCandidateLabels(schema, nodePattern.labels, rprop)
	if len(leftLabels) == 0 && len(rightLabels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 64)
	for _, label := range leftLabels {
		for _, id := range schema.PropertyIndexLookup(label, lprop, lval) {
			idSet[id] = struct{}{}
		}
	}
	for _, label := range rightLabels {
		for _, id := range schema.PropertyIndexLookup(label, rprop, rval) {
			idSet[id] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// tryCollectNodesFromIDInParam attempts to satisfy simple id/elementId IN-list predicates:
//
//	id(<var>) IN $param
//	elementId(<var>) IN $param
//
// where $param is a list from query parameters. This provides direct node seeks
// for batched correlated lookups and avoids full scans.
func (e *StorageExecutor) tryCollectNodesFromIDInParam(
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	if params == nil || strings.TrimSpace(whereClause) == "" {
		return nil, false, nil
	}
	clause := strings.TrimSpace(whereClause)
	upper := strings.ToUpper(clause)
	inIdx := strings.Index(upper, " IN ")
	if inIdx <= 0 {
		return nil, false, nil
	}
	// Keep this strict/simple to avoid semantic drift for complex predicates.
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") {
		return nil, false, nil
	}

	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+4:])
	if left == "" || right == "" || !strings.HasPrefix(right, "$") {
		return nil, false, nil
	}
	paramName := strings.TrimSpace(strings.TrimPrefix(right, "$"))
	if paramName == "" {
		return nil, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := strings.ToLower(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return nil, false, nil
	}
	if varName == "" || varName != nodePattern.variable {
		return nil, false, nil
	}

	raw, ok := params[paramName]
	if !ok {
		return []*storage.Node{}, true, nil
	}
	list, ok := raw.([]interface{})
	if !ok {
		// Keep behavior explicit: IN parameter must be list-like for this path.
		return []*storage.Node{}, true, nil
	}
	if len(list) == 0 {
		return []*storage.Node{}, true, nil
	}

	seen := make(map[string]struct{}, len(list))
	nodes := make([]*storage.Node, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok {
			continue
		}
		lookupID := strings.TrimSpace(s)
		if lookupID == "" {
			continue
		}
		if kind == "elementId" {
			parts := strings.SplitN(lookupID, ":", 3)
			if len(parts) == 3 && parts[0] == "4" {
				lookupID = parts[2]
			}
		}
		if strings.HasPrefix(lookupID, "4:") {
			if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 {
				lookupID = parts[2]
			}
		}
		if _, exists := seen[lookupID]; exists {
			continue
		}
		seen[lookupID] = struct{}{}

		node, err := e.storage.GetNode(storage.NodeID(lookupID))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		if len(nodePattern.properties) > 0 && !e.nodeMatchesProps(node, nodePattern.properties) {
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexOrderLimit attempts to satisfy ORDER BY + LIMIT
// queries using a property index for sorted iteration, even when the WHERE clause
// is not a simple IS NOT NULL predicate. It over-fetches from the index (up to 4x
// the requested limit) and applies any WHERE filter post-hoc. This avoids full
// label scan + in-memory sort for common pagination patterns like:
//
//	MATCH (n:Label) WHERE <filter> ORDER BY n.createdAt DESC LIMIT 30
//
// It only applies when:
//   - The first ORDER BY property with a matching index exists
//   - The node pattern has at least one label
//   - limit > 0
//
// If the ORDER BY has multiple keys, the indexed property is used to fetch
// a candidate window and the remaining keys are applied with the in-memory
// top-K comparator.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexOrderLimit(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
) ([]*storage.Node, bool, error) {
	if limit <= 0 || len(nodePattern.labels) == 0 {
		return nil, false, nil
	}

	orderSpecs := e.parseNodeOrderSpecs(orderExpr, nodePattern.variable)
	if len(orderSpecs) == 0 {
		return nil, false, nil
	}
	spec := orderSpecs[0]

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, spec.propName)
	if len(labels) != 1 {
		return nil, false, nil
	}
	label := labels[0]

	// Over-fetch: grab more candidates than needed to account for WHERE filtering.
	// Use 4x multiplier with a reasonable ceiling to avoid loading too much.
	overFetch := limit * 4
	if overFetch < 200 {
		overFetch = 200
	}

	ids := schema.PropertyIndexTopK(label, spec.propName, overFetch, spec.descending)
	if len(ids) == 0 {
		return nil, false, nil // No index data — fall back to generic path
	}

	hasWhere := strings.TrimSpace(whereClause) != ""
	nodes := make([]*storage.Node, 0, limit)
	for _, id := range ids {
		node, err := e.storage.GetNode(id)
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		if len(nodePattern.properties) > 0 && !e.nodeMatchesProps(node, nodePattern.properties) {
			continue
		}
		// Apply WHERE filter if present.
		if hasWhere && !e.evaluateWhere(ctx, node, nodePattern.variable, whereClause) {
			continue
		}
		nodes = append(nodes, node)
		if len(nodes) >= limit {
			break
		}
	}
	if len(nodes) > limit {
		if topK, ok := e.selectTopKNodesByOrder(nodes, nodePattern.variable, orderExpr, limit); ok {
			nodes = topK
		}
	}

	// If we exhausted the over-fetched set without reaching the limit, the index
	// didn't have enough qualifying rows. Return false to fall back to the generic
	// path which will scan all candidates.
	if len(nodes) < limit && len(ids) >= overFetch {
		return nil, false, nil
	}

	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexNotNullOrderLimit attempts to satisfy:
//
//	MATCH (n:Label) WHERE n.prop IS NOT NULL RETURN ... ORDER BY n.prop [ASC|DESC] LIMIT K
//
// using the property index directly (top-K by indexed key) without label scan.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexNotNullOrderLimit(
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
) ([]*storage.Node, bool, error) {
	if limit <= 0 {
		return nil, false, nil
	}

	whereProp, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	orderSpecs := e.parseNodeOrderSpecs(orderExpr, nodePattern.variable)
	if len(orderSpecs) == 0 {
		return nil, false, nil
	}
	spec := orderSpecs[0]
	if !strings.EqualFold(strings.TrimSpace(spec.propName), strings.TrimSpace(whereProp)) {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, whereProp)
	if len(labels) != 1 {
		// Keep semantics simple/deterministic for now; multi-label merge ordering can be added later.
		return nil, false, nil
	}
	label := labels[0]
	ids := schema.PropertyIndexTopK(label, whereProp, limit, spec.descending)
	if len(ids) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(ids))
	for _, id := range ids {
		node, err := e.storage.GetNode(id)
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
		if len(nodes) >= limit {
			break
		}
	}
	if len(nodes) > limit {
		if topK, ok := e.selectTopKNodesByOrder(nodes, nodePattern.variable, orderExpr, limit); ok {
			nodes = topK
		}
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexNotNull attempts to satisfy:
//
//	MATCH (n:Label) WHERE n.prop IS NOT NULL
//
// from schema index entries, avoiding label scans.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexNotNull(
	nodePattern nodePatternInfo,
	whereClause string,
) ([]*storage.Node, bool, error) {
	property, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{})
	ordered := make([]storage.NodeID, 0)
	for _, label := range labels {
		ids := schema.PropertyIndexAllNonNil(label, property, false)
		for _, id := range ids {
			if _, exists := idSet[id]; exists {
				continue
			}
			idSet[id] = struct{}{}
			ordered = append(ordered, id)
		}
	}
	if len(ordered) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(ordered))
	for _, id := range ordered {
		node, err := e.storage.GetNode(id)
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

func (e *StorageExecutor) indexCandidateLabels(schema *storage.SchemaManager, queryLabels []string, property string) []string {
	if len(queryLabels) > 0 {
		out := make([]string, 0, len(queryLabels))
		for _, label := range queryLabels {
			if _, exists := schema.GetPropertyIndex(label, property); exists {
				out = append(out, label)
			}
		}
		return out
	}

	labels := make(map[string]struct{})
	for _, raw := range schema.GetIndexes() {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := idx["type"].(string)
		if !strings.EqualFold(strings.TrimSpace(typ), "PROPERTY") {
			continue
		}
		label, _ := idx["label"].(string)
		if strings.TrimSpace(label) == "" {
			continue
		}
		prop := ""
		if p, ok := idx["property"].(string); ok {
			prop = p
		}
		if prop == "" {
			if props, ok := idx["properties"].([]string); ok && len(props) == 1 {
				prop = props[0]
			}
			if prop == "" {
				if vals, ok := idx["properties"].([]interface{}); ok && len(vals) == 1 {
					if ps, ok := vals[0].(string); ok {
						prop = ps
					}
				}
			}
		}
		if !strings.EqualFold(strings.TrimSpace(prop), property) {
			continue
		}
		labels[label] = struct{}{}
	}

	out := make([]string, 0, len(labels))
	for label := range labels {
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func (e *StorageExecutor) parseSimpleIndexedEquality(ctx context.Context, variable, whereClause string) (property string, value interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}

	// Only simple predicates are index-eligible.
	for _, kw := range []string{"AND", "OR", "NOT", " IN ", " IS ", "<>", "!=", ">=", "<=", ">", "<"} {
		if topLevelKeywordIndex(clause, kw) >= 0 {
			return "", nil, false
		}
	}

	eqIdx := topLevelEqualsIndex(clause)
	if eqIdx <= 0 || eqIdx >= len(clause)-1 {
		return "", nil, false
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return "", nil, false
	}

	prop, isLeftVarProp := parseVariableProperty(left, variable)
	if isLeftVarProp {
		return prop, e.parseValue(ctx, right), true
	}
	prop, isRightVarProp := parseVariableProperty(right, variable)
	if isRightVarProp {
		return prop, e.parseValue(ctx, left), true
	}
	return "", nil, false
}

func (e *StorageExecutor) parseSimpleIndexedInParam(variable, whereClause string, params map[string]interface{}) (property string, values []interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}
	// Keep this optimization deterministic and safe: only simple standalone IN predicates.
	if containsFold(clause, " AND ") || containsFold(clause, " OR ") {
		return "", nil, false
	}
	inIdx := keywordIndexFrom(clause, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx >= len(clause)-2 {
		return "", nil, false
	}
	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+2:])
	if !strings.HasPrefix(right, "$") {
		return "", nil, false
	}
	paramName := strings.TrimSpace(strings.TrimPrefix(right, "$"))
	if paramName == "" || params == nil {
		return "", nil, false
	}
	parsedProp, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", nil, false
	}
	raw, exists := params[paramName]
	if !exists || raw == nil {
		return "", nil, false
	}
	list := coerceInterfaceList(raw)
	if len(list) == 0 {
		return "", []interface{}{}, true
	}
	out := make([]interface{}, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, v := range list {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return parsedProp, out, true
}

func (e *StorageExecutor) parseSimpleIndexedInLiteral(ctx context.Context, variable, whereClause string) (property string, values []interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}
	// Keep this optimization deterministic and safe: only simple standalone IN predicates.
	if containsFold(clause, " AND ") || containsFold(clause, " OR ") {
		return "", nil, false
	}
	inIdx := keywordIndexFrom(clause, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx >= len(clause)-2 {
		return "", nil, false
	}
	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+2:])
	if left == "" || right == "" {
		return "", nil, false
	}
	parsedProp, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", nil, false
	}
	if !strings.HasPrefix(right, "[") || !strings.HasSuffix(right, "]") {
		return "", nil, false
	}
	rawList := e.parseValue(ctx, right)
	list := coerceInterfaceList(rawList)
	if len(list) == 0 {
		return parsedProp, []interface{}{}, true
	}
	out := make([]interface{}, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, v := range list {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return parsedProp, out, true
}

func coerceInterfaceList(v interface{}) []interface{} {
	switch x := v.(type) {
	case []interface{}:
		return x
	case []string:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []int:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []int64:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []float64:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	default:
		return nil
	}
}

func (e *StorageExecutor) parseSimpleIndexedIsNotNull(variable, whereClause string) (property string, ok bool) {
	clause := strings.TrimSpace(whereClause)
	clause = unwrapOuterParens(clause)
	if clause == "" {
		return "", false
	}
	parts := splitTopLevelAndConjuncts(clause)
	targetProp := ""
	for _, raw := range parts {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		part = unwrapOuterParens(part)
		if p, ok := parseSimpleSingleIndexedIsNotNull(variable, part); ok {
			if targetProp != "" && !strings.EqualFold(targetProp, p) {
				return "", false
			}
			targetProp = p
			continue
		}
		// Allow only constant boolean conjuncts in addition to var.prop IS NOT NULL.
		// This keeps top-K semantics correct while supporting cache-buster style predicates.
		if _, isConst := e.tryEvaluateConstantBooleanConjunct(part); isConst {
			continue
		}
		return "", false
	}
	if targetProp == "" {
		return "", false
	}
	return targetProp, true
}

func parseSimpleSingleIndexedIsNotNull(variable, clause string) (property string, ok bool) {
	upper := strings.ToUpper(strings.TrimSpace(clause))
	sfx := " IS NOT NULL"
	if !strings.HasSuffix(upper, sfx) {
		return "", false
	}
	left := strings.TrimSpace(clause[:len(clause)-len(sfx)])
	if left == "" {
		return "", false
	}
	prop, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", false
	}
	return prop, true
}

func unwrapOuterParens(clause string) string {
	out := strings.TrimSpace(clause)
	for strings.HasPrefix(out, "(") && strings.HasSuffix(out, ")") && len(out) >= 2 {
		inner := strings.TrimSpace(out[1 : len(out)-1])
		if inner == out {
			break
		}
		out = inner
	}
	return out
}

func splitTopLevelAndConjuncts(clause string) []string {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	parts := make([]string, 0, 2)
	start := 0
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		}
		if paren == 0 && bracket == 0 && brace == 0 && i+3 <= len(clause) && strings.EqualFold(clause[i:i+3], "AND") {
			prevOK := i == 0 || isWhitespace(clause[i-1]) || clause[i-1] == '('
			nextIdx := i + 3
			nextOK := nextIdx >= len(clause) || isWhitespace(clause[nextIdx]) || clause[nextIdx] == ')'
			if prevOK && nextOK {
				parts = append(parts, strings.TrimSpace(clause[start:i]))
				start = nextIdx
				i = nextIdx - 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(clause[start:]))
	return parts
}

func (e *StorageExecutor) tryEvaluateConstantBooleanConjunct(clause string) (value bool, ok bool) {
	expr := strings.TrimSpace(clause)
	if expr == "" {
		return false, false
	}
	if strings.EqualFold(expr, "TRUE") {
		return true, true
	}
	if strings.EqualFold(expr, "FALSE") {
		return false, true
	}
	// If expression mentions variables/properties, treat as non-constant.
	if strings.Contains(expr, ".") || strings.Contains(expr, "$") {
		return false, false
	}
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := topLevelSymbolIndex(expr, op); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op):])
			lv, lok := parseLiteralValue(left)
			rv, rok := parseLiteralValue(right)
			if !lok || !rok {
				return false, false
			}
			return compareLiteralValues(lv, rv, op)
		}
	}
	return false, false
}

func topLevelSymbolIndex(expr, sym string) int {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i <= len(expr)-len(sym); i++ {
		ch := expr[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		}
		if paren == 0 && bracket == 0 && brace == 0 && expr[i:i+len(sym)] == sym {
			return i
		}
	}
	return -1
}

func parseLiteralValue(raw string) (interface{}, bool) {
	s := strings.TrimSpace(raw)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1], true
		}
	}
	if strings.EqualFold(s, "true") {
		return true, true
	}
	if strings.EqualFold(s, "false") {
		return false, true
	}
	if strings.EqualFold(s, "null") {
		return nil, true
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}
	return nil, false
}

func compareLiteralValues(left, right interface{}, op string) (bool, bool) {
	switch op {
	case "=", "!=", "<>":
		eq := fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
		if op == "=" {
			return eq, true
		}
		return !eq, true
	}
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if !lok || !rok {
		return false, false
	}
	switch op {
	case ">":
		return lf > rf, true
	case "<":
		return lf < rf, true
	case ">=":
		return lf >= rf, true
	case "<=":
		return lf <= rf, true
	default:
		return false, false
	}
}

func parseVariableProperty(expr, variable string) (string, bool) {
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", false
	}
	lhs := strings.TrimSpace(expr[:dot])
	if !strings.EqualFold(lhs, strings.TrimSpace(variable)) {
		return "", false
	}
	prop := normalizePropertyKey(strings.TrimSpace(expr[dot+1:]))
	if prop == "" {
		return "", false
	}
	return prop, true
}

func topLevelEqualsIndex(clause string) int {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		case '=':
			if paren != 0 || bracket != 0 || brace != 0 {
				continue
			}
			if i > 0 {
				prev := clause[i-1]
				if prev == '!' || prev == '<' || prev == '>' {
					continue
				}
			}
			if i+1 < len(clause) {
				next := clause[i+1]
				if next == '=' {
					continue
				}
			}
			return i
		}
	}
	return -1
}

func nodeHasAnyLabel(node *storage.Node, labels []string) bool {
	for _, want := range labels {
		for _, have := range node.Labels {
			if strings.EqualFold(strings.TrimSpace(want), strings.TrimSpace(have)) {
				return true
			}
		}
	}
	return false
}

// tryRewriteNullNormalizedPredicate rewrites coalesce-wrapped equality predicates
// into OR-expanded forms that are visible to the index seek cascade and compiled
// WHERE fast-paths.
//
// Rewrites:
//
//	coalesce(var.prop, <default>) = <value>  →  (var.prop = <value> OR var.prop IS NULL)
//	                                            when <value> == <default>
//
//	coalesce(var.prop, <default>) <> <value> →  (var.prop <> <value> AND var.prop IS NOT NULL)
//	                                            when <value> == <default>
//
// This is safe because coalesce(x, d) = d is semantically equivalent to
// (x = d OR x IS NULL).
func tryRewriteNullNormalizedPredicate(whereClause string) string {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return clause
	}

	// Only rewrite simple top-level predicates (no AND/OR at the top level).
	// Nested coalesce inside AND/OR conjuncts would need recursive rewrite
	// which risks semantic drift — keep it strict.
	upper := strings.ToUpper(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") {
		return clause
	}

	// Look for coalesce( at the start.
	lowerClause := strings.ToLower(clause)
	if !strings.HasPrefix(lowerClause, "coalesce(") {
		return clause
	}

	// Find matching closing paren for coalesce(...).
	depth := 0
	closeIdx := -1
	for i := 9; i < len(clause); i++ { // start after "coalesce("
		switch clause[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				closeIdx = i
				goto found
			}
			depth--
		}
	}
found:
	if closeIdx < 0 {
		return clause
	}

	inner := strings.TrimSpace(clause[9:closeIdx])
	afterCoalesce := strings.TrimSpace(clause[closeIdx+1:])

	// Parse inner: expect "var.prop, <default>"
	commaIdx := strings.Index(inner, ",")
	if commaIdx <= 0 {
		return clause
	}
	varProp := strings.TrimSpace(inner[:commaIdx])
	defaultVal := strings.TrimSpace(inner[commaIdx+1:])
	if varProp == "" || defaultVal == "" {
		return clause
	}
	// varProp must look like variable.property
	if !strings.Contains(varProp, ".") {
		return clause
	}

	// Parse operator and RHS: expect "= <value>" or "<> <value>" or "!= <value>"
	op := ""
	rhs := ""
	if strings.HasPrefix(afterCoalesce, "<>") {
		op = "<>"
		rhs = strings.TrimSpace(afterCoalesce[2:])
	} else if strings.HasPrefix(afterCoalesce, "!=") {
		op = "<>"
		rhs = strings.TrimSpace(afterCoalesce[2:])
	} else if strings.HasPrefix(afterCoalesce, "=") {
		op = "="
		rhs = strings.TrimSpace(afterCoalesce[1:])
	} else {
		return clause
	}

	if rhs == "" {
		return clause
	}

	// Only rewrite when the comparison value matches the default value.
	// coalesce(x, false) = false → rewrite
	// coalesce(x, false) = true  → don't rewrite (different semantics)
	if !strings.EqualFold(rhs, defaultVal) {
		return clause
	}

	if op == "=" {
		return fmt.Sprintf("(%s = %s OR %s IS NULL)", varProp, rhs, varProp)
	}
	// <> case: coalesce(x, d) <> d means x <> d AND x IS NOT NULL
	return fmt.Sprintf("(%s <> %s AND %s IS NOT NULL)", varProp, rhs, varProp)
}
