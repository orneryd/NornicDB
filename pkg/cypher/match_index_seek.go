package cypher

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// indexSeekConstant evaluates the value side of a predicate an index seek
// answers. The seek may use literals, parameters and values already bound
// in the incoming scope, including functions and property access over them.
// Unbound candidate variables are not constants of that incoming scope.
// Otherwise ok is false and the predicate is left to the row filter. Reading
// the value with the literal parser looked up a function call as its own
// text and returned no rows (#844).
func (e *StorageExecutor) indexSeekConstant(ctx context.Context, expr string) (value interface{}, ok bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	for _, variable := range expressionFreeVariables(expr) {
		if _, bound := e.boundValue(ctx, variable); !bound {
			return nil, false
		}
	}
	value, ok = e.evaluateRowExpressionWithContext(ctx, expr, e.fabricRecordBindings)
	if !ok {
		return nil, false
	}
	return normalizePropValue(value), true
}

// idEqualityLookup reads a WHERE that is one id(<variable>) = <value> or
// elementId(<variable>) = <value> comparison: the variable, and the storage
// ID the value names (an element ID's payload; elementKind is "4" for nodes,
// "5" for relationships). empty is true when the value is null, not a string
// or blank, which matches nothing; ok is false for any other WHERE.
func (e *StorageExecutor) idEqualityLookup(ctx context.Context, whereClause, elementKind string) (variable, lookupID string, empty, ok bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return "", "", false, false
	}

	upper := upperASCII(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " IN ") {
		return "", "", false, false
	}

	eqIdx := strings.Index(clause, "=")
	if eqIdx <= 0 {
		return "", "", false, false
	}
	// Ignore >=, <=, !=, <>
	if prev := clause[eqIdx-1]; prev == '!' || prev == '<' || prev == '>' {
		return "", "", false, false
	}
	if eqIdx+1 < len(clause) && clause[eqIdx+1] == '=' {
		return "", "", false, false
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return "", "", false, false
	}

	kind := ""
	lowerLeft := lowerASCII(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		variable = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		variable = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return "", "", false, false
	}
	if variable == "" {
		return "", "", false, false
	}

	rawVal, constant := e.indexSeekConstant(ctx, right)
	if !constant {
		return "", "", false, false
	}
	idValue, isString := rawVal.(string)
	if !isString || strings.TrimSpace(idValue) == "" {
		return variable, "", true, true
	}
	lookupID = strings.TrimSpace(idValue)
	// A canonical element ID ("<kind>:<db>:<id>") names its ID payload; an
	// id() lookup that passes an element ID is read the same way.
	if kind == "elementId" || strings.HasPrefix(lookupID, elementKind+":") {
		if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 && parts[0] == elementKind {
			lookupID = parts[2]
		}
	}
	return variable, lookupID, false, true
}

// tryCollectNodesFromIDEquality attempts to satisfy:
//
//	MATCH (n[:Label]) WHERE id(n) = <id>
//	MATCH (n[:Label]) WHERE elementId(n) = <element-id>
//
// via a direct node lookup instead of scanning all nodes.
func (e *StorageExecutor) tryCollectNodesFromIDEquality(ctx context.Context, nodePattern nodePatternInfo, whereClause string) ([]*storage.Node, bool, error) {
	varName, lookupID, empty, ok := e.idEqualityLookup(ctx, whereClause, "4")
	if !ok || varName != nodePattern.variable {
		return nil, false, nil
	}
	if empty {
		return []*storage.Node{}, true, nil
	}

	node, err := e.getQueryNode(e.storage, storage.NodeID(lookupID))
	if err != nil || node == nil {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.labels) > 0 && !mergeNodeHasLabels(node, nodePattern.labels) {
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

	upper := upperASCII(clause)
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
	lowerLeft := lowerASCII(left)
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

	node, err := e.getQueryNode(e.storage, storage.NodeID(lookupID))
	if err != nil || node == nil {
		return []*storage.Node{}, true, nil
	}

	if len(nodePattern.labels) > 0 && !mergeNodeHasLabels(node, nodePattern.labels) {
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
	return e.tryCollectNodesFromPropertyIndexAnyOf(ctx, nodePattern, whereClause, nil)
}

// tryCollectNodesFromPropertyIndexAnyOf is tryCollectNodesFromPropertyIndex
// for a node that must carry one of anyOf (a WHERE label disjunction, #858;
// nil when there is none): see seekCandidateLabels.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexAnyOf(ctx context.Context, nodePattern nodePatternInfo, whereClause string, anyOf []string) ([]*storage.Node, bool, error) {
	property, value, ok := e.parseSimpleIndexedEquality(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels, anyOf := e.seekCandidateLabels(schema, nodePattern.labels, property, anyOf)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{})
	for _, label := range labels {
		for _, id := range propertyIndexLookup(e.storage, schema, label, property, value) {
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
		node, err := e.getQueryNode(e.storage, storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if !seekCandidateHasLabels(node, nodePattern.labels, anyOf) {
			continue
		}
		actual, exists := node.Properties[property]
		if !exists {
			continue
		}
		equal := cypherEquality(actual, value)
		if matched, known := equal.(bool); !known || !matched {
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
	return e.tryCollectNodesFromPropertyIndexInAnyOf(nodePattern, whereClause, params, nil)
}

// tryCollectNodesFromPropertyIndexInAnyOf is tryCollectNodesFromPropertyIndexIn
// for a node that must carry one of anyOf (see seekCandidateLabels).
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexInAnyOf(
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
	anyOf []string,
) ([]*storage.Node, bool, error) {
	property, listValues, ok := e.parseSimpleIndexedInParam(nodePattern.variable, whereClause, params)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	labels, anyOf := e.seekCandidateLabels(schema, nodePattern.labels, property, anyOf)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range labels {
		for _, value := range listValues {
			for _, id := range propertyIndexLookup(e.storage, schema, label, property, value) {
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
		node, err := e.getQueryNode(e.storage, storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if !seekCandidateHasLabels(node, nodePattern.labels, anyOf) {
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
	return e.tryCollectNodesFromPropertyIndexInLiteralAnyOf(ctx, nodePattern, whereClause, nil)
}

// tryCollectNodesFromPropertyIndexInLiteralAnyOf is
// tryCollectNodesFromPropertyIndexInLiteral for a node that must carry one of
// anyOf (see seekCandidateLabels).
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexInLiteralAnyOf(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	anyOf []string,
) ([]*storage.Node, bool, error) {
	property, listValues, ok := e.parseSimpleIndexedInLiteral(ctx, nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}
	labels, anyOf := e.seekCandidateLabels(schema, nodePattern.labels, property, anyOf)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range labels {
		for _, value := range listValues {
			for _, id := range propertyIndexLookup(e.storage, schema, label, property, value) {
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
		node, err := e.getQueryNode(e.storage, storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if !seekCandidateHasLabels(node, nodePattern.labels, anyOf) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexInCompound attempts to satisfy an IN-list
// start-node predicate from a schema property index, including when the
// IN-list conjunct is combined with other predicates via AND — e.g. a
// relationship-pattern traversal's WHERE clause that also filters a bound
// relationship variable, as in `WHERE s.uid IN $u AND rel.evidence_source =
// $e`. Only a single AND conjunct needs to match an indexed property IN-list
// for pruning to be safe: the caller (executeMatchWithRelationshipsWithPath)
// always re-applies the full WHERE clause via filterPathsByWhere afterward,
// so seeding from one recognized conjunct can only over-fetch, never
// under-fetch, and correctness does not depend on this helper understanding
// the rest of the clause.
//
// Mirrors tryCollectNodesFromIDEqualityCompound's AND-conjunct handling
// (match_index_seek.go) for the IN-list case. Tries the param-bound form
// first, then the literal-list form, at both the top level and inside each
// top-level AND conjunct.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexInCompound(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil, false, nil
	}

	// 1) Simple clause (param or literal list).
	if nodes, used, err := e.tryCollectNodesFromPropertyIndexIn(nodePattern, clause, params); used || err != nil {
		return nodes, used, err
	}
	if nodes, used, err := e.tryCollectNodesFromPropertyIndexInLiteral(ctx, nodePattern, clause); used || err != nil {
		return nodes, used, err
	}

	// 2) Conjunction: any recognized IN-list conjunct is safe to use as a
	// pruning seek (see over-fetch-only argument in the doc comment above).
	// A label disjunction among the conjuncts names the labels a match must
	// carry, for a pattern without labels (#858).
	if findTopLevelKeyword(clause, " AND ") > 0 {
		conjuncts := splitTopLevelAndConjuncts(clause)
		anyOf := whereLabelDisjunctionConjunct(nodePattern.variable, conjuncts)
		for _, raw := range conjuncts {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			if nodes, used, err := e.tryCollectNodesFromPropertyIndexInAnyOf(nodePattern, term, params, anyOf); used || err != nil {
				return nodes, used, err
			}
			if nodes, used, err := e.tryCollectNodesFromPropertyIndexInLiteralAnyOf(ctx, nodePattern, term, anyOf); used || err != nil {
				return nodes, used, err
			}
		}
	}

	return nil, false, nil
}

// tryCollectNodesFromPropertyIndexEqualityCompound attempts to satisfy a
// start-node predicate from a schema property index when an equality on an
// indexed property is combined with other predicates via AND — e.g.
// `WHERE n.repo_id = $repo_id AND n.evidence_source = 'x' AND
// n.generation_id <> $gen`. Only a single AND conjunct needs to match an
// indexed property equality for pruning to be safe: every caller
// re-applies the full WHERE clause afterward, so seeding from one
// recognized conjunct can only over-fetch, never under-fetch, and
// correctness does not depend on this helper understanding the rest of the
// clause.
//
// Mirrors tryCollectNodesFromPropertyIndexInCompound's AND-conjunct handling
// (match_index_seek.go) for the plain-equality case. Tries the whole clause
// first (the simple form), then each top-level AND conjunct in order.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexEqualityCompound(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
) ([]*storage.Node, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil, false, nil
	}

	// 1) Simple clause (a lone equality, no conjunction).
	if nodes, used, err := e.tryCollectNodesFromPropertyIndex(ctx, nodePattern, clause); used || err != nil {
		return nodes, used, err
	}

	// 2) Conjunction: any equality conjunct on an indexed property is safe
	// to use as a pruning seek (see over-fetch-only argument in the doc
	// comment above). A label disjunction among the conjuncts
	// (n.id = $id AND (n:A OR n:B)) names the labels a match must carry, for
	// a pattern without labels (#858).
	if findTopLevelKeyword(clause, " AND ") > 0 {
		conjuncts := splitTopLevelAndConjuncts(clause)
		anyOf := whereLabelDisjunctionConjunct(nodePattern.variable, conjuncts)
		for _, raw := range conjuncts {
			term := unwrapOuterParens(strings.TrimSpace(raw))
			if term == "" {
				continue
			}
			if nodes, used, err := e.tryCollectNodesFromPropertyIndexAnyOf(ctx, nodePattern, term, anyOf); used || err != nil {
				return nodes, used, err
			}
		}
	}

	return nil, false, nil
}

// indexSeekTerm is a WHERE term on a node variable that an arity-1 property
// index answers: the property, the values it selects, and whether it tests
// membership (value IN n.property) or equality (n.property = value).
type indexSeekTerm struct {
	property  string
	values    []interface{}
	member    bool
	contained bool // values holds the text a contained-key index looks up
}

// parseIndexSeekTerm is the one reader of the WHERE terms a property index
// answers for variable:
//
//	n.p = v, v = n.p                   equality
//	n.p IN $list, n.p IN [v, …]        equality with each element
//	v IN n.p                           membership
//	any(x IN <list> WHERE x IN n.p)    membership of each element
//	any(x IN n.p WHERE x IN <list>)    the same, mirrored; n.p may be
//	                                   coalesce(n.p, [])
//
// v and the lists are constants of the incoming scope (indexSeekConstant).
// A membership seek lists the nodes whose property is a list holding the
// value, or equals it (propertyIndexMemberLookup); it is used only for
// strings, booleans and numbers, whose index keys are equal exactly when
// the values are, so it never misses a match.
func (e *StorageExecutor) parseIndexSeekTerm(ctx context.Context, variable, term string, params map[string]interface{}) (indexSeekTerm, bool) {
	term = unwrapOuterParens(strings.TrimSpace(term))
	if term == "" {
		return indexSeekTerm{}, false
	}
	if property, value, ok := e.parseSimpleIndexedEquality(ctx, variable, term); ok {
		return indexSeekTerm{property: property, values: []interface{}{value}}, true
	}
	if property, values, ok := e.parseSimpleIndexedInParam(variable, term, params); ok {
		return indexSeekTerm{property: property, values: values}, true
	}
	if property, values, ok := e.parseSimpleIndexedInLiteral(ctx, variable, term); ok {
		return indexSeekTerm{property: property, values: values}, true
	}
	if property, values, ok := e.parseIndexedMembership(ctx, variable, term); ok {
		return indexSeekTerm{property: property, values: values, member: true}, true
	}
	if property, values, ok := e.parseIndexedContained(ctx, variable, term); ok {
		return indexSeekTerm{property: property, values: values, contained: true}, true
	}
	return indexSeekTerm{}, false
}

// parseIndexedContained reads the terms a contained-key index answers
// (storage.ContainedIndex):
//
//	<text> CONTAINS n.p
//	any(x IN n.p WHERE ... AND <text> CONTAINS x AND ...)
//
// with n.p optionally coalesce(n.p, []) and <text> a constant of the
// incoming scope that doesn't mention x. The any() form needs only one
// conjunct of that shape: the others narrow it, and the caller re-applies the
// WHERE to the candidates. A null or non-string text makes CONTAINS null, so
// the term selects nothing (values is empty); a non-string key is never
// contained, and the index files none.
func (e *StorageExecutor) parseIndexedContained(ctx context.Context, variable, term string) (string, []interface{}, bool) {
	var property, textExpr string
	if name, inner, ok := parseFunctionCallWS(term); ok && strings.EqualFold(name, "any") && strings.HasSuffix(term, ")") {
		whereIdx := keywordIndexFrom(inner, "WHERE", 0, defaultKeywordScanOpts())
		if whereIdx <= 0 {
			return "", nil, false
		}
		local, bound, ok := splitTopLevelIn(inner[:whereIdx])
		if !ok || !isValidIdentifier(local) || strings.EqualFold(local, variable) {
			return "", nil, false
		}
		if property, ok = membershipListProperty(bound, variable); !ok {
			return "", nil, false
		}
		for _, conjunct := range splitTopLevelAndConjuncts(unwrapOuterParens(strings.TrimSpace(inner[whereIdx+len("WHERE"):]))) {
			left, right, ok := splitTopLevelContains(conjunct)
			if ok && right == local && !slices.Contains(expressionFreeVariables(left), local) {
				textExpr = left
				break
			}
		}
		if textExpr == "" {
			return "", nil, false
		}
	} else {
		if hasTopLevelLogicalOperator(term) || topLevelKeywordIndex(term, "NOT") >= 0 {
			return "", nil, false
		}
		left, right, ok := splitTopLevelContains(term)
		if !ok {
			return "", nil, false
		}
		if property, ok = membershipProperty(right, variable); !ok {
			return "", nil, false
		}
		textExpr = left
	}
	text, constant := e.indexSeekConstant(ctx, textExpr)
	if !constant {
		return "", nil, false
	}
	if value, isString := text.(string); isString {
		return property, []interface{}{value}, true
	}
	return property, nil, true
}

// splitTopLevelContains splits "<left> CONTAINS <right>" at its one
// top-level CONTAINS.
func splitTopLevelContains(expr string) (left, right string, ok bool) {
	expr = unwrapOuterParens(strings.TrimSpace(expr))
	idx := keywordIndexFrom(expr, "CONTAINS", 0, defaultKeywordScanOpts())
	if idx <= 0 || idx+len("CONTAINS") >= len(expr) {
		return "", "", false
	}
	left = strings.TrimSpace(expr[:idx])
	right = strings.TrimSpace(expr[idx+len("CONTAINS"):])
	if left == "" || right == "" || keywordIndexFrom(right, "CONTAINS", 0, defaultKeywordScanOpts()) >= 0 {
		return "", "", false
	}
	return left, right, true
}

// parseIndexedMembership reads v IN n.p, any(x IN <list> WHERE x IN n.p) and
// its mirror any(x IN n.p WHERE x IN <list>): the property and the values
// whose membership selects a node. A null value
// or list selects nothing (the test is never true). ok is false for any
// other term, for a list that is not a list (the evaluator reports it), and
// for a value that is not a string, boolean or number.
func (e *StorageExecutor) parseIndexedMembership(ctx context.Context, variable, term string) (string, []interface{}, bool) {
	var property string
	var values []interface{}
	if name, inner, ok := parseFunctionCallWS(term); ok && strings.EqualFold(name, "any") && strings.HasSuffix(term, ")") {
		whereIdx := keywordIndexFrom(inner, "WHERE", 0, defaultKeywordScanOpts())
		if whereIdx <= 0 {
			return "", nil, false
		}
		local, bound, ok := splitTopLevelIn(inner[:whereIdx])
		if !ok || !isValidIdentifier(local) || strings.EqualFold(local, variable) {
			return "", nil, false
		}
		element, tested, ok := splitTopLevelIn(inner[whereIdx+len("WHERE"):])
		if !ok || element != local {
			return "", nil, false
		}
		// any(x IN <list> WHERE x IN n.p), or the mirrored
		// any(x IN n.p WHERE x IN <list>): both are true exactly when some
		// element of the list is in n.p, a scalar n.p counting as a
		// one-element list.
		var listExpr string
		if property, ok = membershipProperty(tested, variable); ok {
			listExpr = bound
		} else if property, ok = membershipListProperty(bound, variable); ok && !slices.Contains(expressionFreeVariables(tested), local) {
			listExpr = tested
		} else {
			return "", nil, false
		}
		list, constant := e.indexSeekConstant(ctx, listExpr)
		if !constant {
			return "", nil, false
		}
		if list != nil {
			items, isList := cypherListValue(list)
			if !isList {
				return "", nil, false
			}
			values = items
		}
	} else {
		if hasTopLevelLogicalOperator(term) || topLevelKeywordIndex(term, "NOT") >= 0 {
			return "", nil, false
		}
		left, propertyExpr, ok := splitTopLevelIn(term)
		if !ok {
			return "", nil, false
		}
		if property, ok = membershipProperty(propertyExpr, variable); !ok {
			return "", nil, false
		}
		value, constant := e.indexSeekConstant(ctx, left)
		if !constant {
			return "", nil, false
		}
		values = []interface{}{value}
	}
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		if !propertyIndexMissIsAuthoritative(value) {
			return "", nil, false
		}
		out = append(out, value)
	}
	return property, out, true
}

// splitTopLevelIn splits "<left> IN <right>" at its one top-level IN.
func splitTopLevelIn(expr string) (left, right string, ok bool) {
	expr = strings.TrimSpace(expr)
	inIdx := keywordIndexFrom(expr, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx+2 >= len(expr) {
		return "", "", false
	}
	left = strings.TrimSpace(expr[:inIdx])
	right = strings.TrimSpace(expr[inIdx+2:])
	if left == "" || right == "" || keywordIndexFrom(right, "IN", 0, defaultKeywordScanOpts()) >= 0 {
		return "", "", false
	}
	return left, right, true
}

// membershipProperty reads n.p, the list side of a membership seek: a plain
// property of variable.
func membershipProperty(expr, variable string) (string, bool) {
	property, ok := parseVariableProperty(strings.TrimSpace(expr), variable)
	if !ok || !isValidIdentifier(property) {
		return "", false
	}
	return property, true
}

// membershipListProperty reads the list a mirrored membership iterates:
// n.p, or coalesce(n.p, []), which differs from n.p only for a null property,
// and there makes any() false instead of null; a WHERE keeps the row in
// neither case.
func membershipListProperty(expr, variable string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if name, inner, ok := parseFunctionCallWS(expr); ok && strings.EqualFold(name, "coalesce") && strings.HasSuffix(expr, ")") {
		args := splitTopLevelComma(inner)
		if len(args) != 2 || strings.ReplaceAll(args[1], " ", "") != "[]" {
			return "", false
		}
		expr = args[0]
	}
	return membershipProperty(expr, variable)
}

// collectIndexSeekTermNodes returns the nodes the property indexes list for
// any of terms: the union of one lookup per value of each term, sorted by ID,
// each carrying the pattern's labels and one of anyOf (when set). ok is false
// when a term's property has no index usable for the pattern
// (seekCandidateLabels): the union could then miss a match. The candidates
// are a superset of the matches; the caller applies the WHERE to them.
func (e *StorageExecutor) collectIndexSeekTermNodes(nodePattern nodePatternInfo, terms []indexSeekTerm, anyOf []string) ([]*storage.Node, bool) {
	schema := e.storage.GetSchema()
	if schema == nil || len(terms) == 0 {
		return nil, false
	}
	termLabels := make([][]string, len(terms))
	termContained := make([][]*storage.ContainedIndex, len(terms))
	for i, term := range terms {
		if term.contained {
			termContained[i] = containedSeekIndexes(schema, nodePattern.labels, term.property, anyOf)
			if len(termContained[i]) == 0 {
				return nil, false
			}
			continue
		}
		labels, _ := e.seekCandidateLabels(schema, nodePattern.labels, term.property, anyOf)
		if len(labels) == 0 {
			return nil, false
		}
		termLabels[i] = labels
	}
	idSet := make(map[storage.NodeID]struct{})
	for i, term := range terms {
		for _, idx := range termContained[i] {
			for _, value := range term.values {
				ids, ok := containedIndexLookup(e.storage, schema, idx, value.(string))
				if !ok {
					return nil, false
				}
				for _, id := range ids {
					idSet[id] = struct{}{}
				}
			}
		}
		for _, label := range termLabels[i] {
			for _, value := range term.values {
				var ids []storage.NodeID
				if term.member {
					ids = propertyIndexMemberLookup(e.storage, schema, label, term.property, value)
				} else {
					ids = propertyIndexLookup(e.storage, schema, label, term.property, value)
				}
				for _, id := range ids {
					idSet[id] = struct{}{}
				}
			}
		}
	}
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	nodes := make([]*storage.Node, 0, len(ids))
	for _, id := range ids {
		node, err := e.getQueryNode(e.storage, storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if !seekCandidateHasLabels(node, nodePattern.labels, anyOf) {
			continue
		}
		if len(nodePattern.properties) > 0 && !e.nodeMatchesProps(node, nodePattern.properties) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true
}

// containedSeekIndexes returns the filled contained-key indexes covering
// property that list every node a seek may return: one index of a pattern
// label (a candidate carries all of them), or, for a label-less pattern whose
// WHERE requires one of anyOf, an index of every one of those labels. Nil
// when there is none.
func containedSeekIndexes(schema *storage.SchemaManager, patternLabels []string, property string, anyOf []string) []*storage.ContainedIndex {
	if len(patternLabels) > 0 {
		for _, label := range patternLabels {
			if idx, ok := schema.ContainedIndexFor(label, property); ok {
				return []*storage.ContainedIndex{idx}
			}
		}
		return nil
	}
	if len(anyOf) == 0 {
		return nil
	}
	out := make([]*storage.ContainedIndex, 0, len(anyOf))
	for _, label := range anyOf {
		idx, ok := schema.ContainedIndexFor(label, property)
		if !ok {
			return nil
		}
		out = append(out, idx)
	}
	return out
}

// indexSeekDisjunction reads clause as one index seek term or a disjunction
// of them (t1 OR t2 OR …), every one of them read by parseIndexSeekTerm. A
// disjunct that is a conjunction is answered by its first conjunct
// parseIndexSeekTerm reads (any(v IN n.keys WHERE v IN $values) AND
// <residual>): its other conjuncts only narrow it, and the caller applies
// the whole WHERE.
func (e *StorageExecutor) indexSeekDisjunction(ctx context.Context, variable, clause string, params map[string]interface{}) ([]indexSeekTerm, bool) {
	disjuncts := splitTopLevelOrTerms(unwrapOuterParens(strings.TrimSpace(clause)))
	if len(disjuncts) == 0 {
		return nil, false
	}
	terms := make([]indexSeekTerm, 0, len(disjuncts))
	for _, disjunct := range disjuncts {
		term, ok := e.parseIndexSeekTerm(ctx, variable, disjunct, params)
		if !ok && len(disjuncts) > 1 {
			for _, conjunct := range splitTopLevelAndConjuncts(unwrapOuterParens(strings.TrimSpace(disjunct))) {
				if term, ok = e.parseIndexSeekTerm(ctx, variable, conjunct, params); ok {
					break
				}
			}
		}
		if !ok {
			return nil, false
		}
		terms = append(terms, term)
	}
	return terms, true
}

// tryCollectNodesFromPropertyIndexDisjunction seeds a MATCH from the property
// indexes when its WHERE is a disjunction of index seek terms
// (parseIndexSeekTerm), e.g.
//
//	n.id IN $ids OR n.name_key = $name OR any(k IN $keys WHERE k IN n.alias_keys)
//
// with the union of one seek per value of every term. Every disjunct must be
// answered by an index: a term the indexes can't answer could select a node
// none of the seeks lists.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexDisjunction(ctx context.Context, nodePattern nodePatternInfo, whereClause string, params map[string]interface{}) ([]*storage.Node, bool, error) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" || findTopLevelKeyword(clause, " OR ") <= 0 {
		return nil, false, nil
	}
	terms, ok := e.indexSeekDisjunction(ctx, nodePattern.variable, clause, params)
	if !ok {
		return nil, false, nil
	}
	nodes, used := e.collectIndexSeekTermNodes(nodePattern, terms, nil)
	return nodes, used, nil
}

// tryCollectNodesFromPropertyIndexConjunctTerms seeds a MATCH whose WHERE, or
// one of its top-level AND conjuncts, is a membership seek term or a
// disjunction of index seek terms (indexSeekDisjunction): the shapes the
// equality and IN-list seeks before it don't read, e.g.
//
//	(n.id IN $ids OR any(k IN $keys WHERE k IN n.alias_keys)) AND n.current
//
// Seeding from one conjunct can only over-fetch; the caller applies the
// whole WHERE.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexConjunctTerms(ctx context.Context, nodePattern nodePatternInfo, whereClause string, params map[string]interface{}) ([]*storage.Node, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" || (!containsFold(clause, " IN ") && !containsFold(clause, " OR ") && !containsFold(clause, " CONTAINS ")) {
		return nil, false, nil
	}
	conjuncts := splitTopLevelAndConjuncts(clause)
	anyOf := whereLabelDisjunctionConjunct(nodePattern.variable, conjuncts)
	for _, conjunct := range conjuncts {
		terms, ok := e.indexSeekDisjunction(ctx, nodePattern.variable, conjunct, params)
		if !ok || (len(terms) == 1 && !terms[0].member && !terms[0].contained) {
			continue
		}
		if nodes, used := e.collectIndexSeekTermNodes(nodePattern, terms, anyOf); used {
			return nodes, true, nil
		}
	}
	return nil, false, nil
}

// tryCollectNodesFromIDIn attempts to satisfy simple id/elementId IN-list predicates:
//
//	id(<var>) IN $param
//	elementId(<var>) IN $param
//	id(<var>) IN ['id-1', $other, …]
//
// The list is a query parameter, or any constant list: a literal, a list of
// parameters, or a parameter the executor already substituted into the query
// text (#940). A missing or non-list $param selects nothing, as before; a
// constant that isn't a list leaves the predicate to the evaluator. This
// provides direct node seeks for batched correlated lookups and avoids full
// scans.
func (e *StorageExecutor) tryCollectNodesFromIDIn(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return nil, false, nil
	}
	upper := upperASCII(clause)
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
	if left == "" || right == "" {
		return nil, false, nil
	}

	kind := ""
	varName := ""
	lowerLeft := lowerASCII(left)
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

	var list []interface{}
	if paramName := strings.TrimPrefix(right, "$"); paramName != right && isValidIdentifier(paramName) {
		if params == nil {
			return nil, false, nil
		}
		raw, ok := params[paramName]
		if !ok {
			return []*storage.Node{}, true, nil
		}
		list = toAnySlice(raw)
	} else {
		value, constant := e.indexSeekConstant(ctx, right)
		if !constant {
			return nil, false, nil
		}
		if value != nil {
			if list = toAnySlice(value); list == nil {
				return nil, false, nil
			}
		}
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

		node, err := e.getQueryNode(e.storage, storage.NodeID(lookupID))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !mergeNodeHasLabels(node, nodePattern.labels) {
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
// is not a simple IS NOT NULL predicate. It filters complete index-key groups
// into a bounded ordered window. This avoids a full label scan and sort when
// the non-null index can fill common pagination requests like:
//
//	MATCH (n:Label) WHERE <filter> ORDER BY n.createdAt DESC LIMIT 30
//
// It only applies when:
//   - The first ORDER BY property with a matching index exists
//   - The node pattern has at least one label
//   - limit > 0
//
// If the ORDER BY has multiple keys, the indexed property is used to fetch
// complete primary-key groups and all keys select the retained top-K window.
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

	orderSpecs, ok := e.parseIndexedNodeOrderSpecs(orderExpr, nodePattern.variable)
	if !ok {
		return nil, false, nil
	}
	spec := orderSpecs[0]
	// A WHERE that bounds the first sort property (n.t > $last, the keyset
	// form n.t > $t OR (n.t = $t AND n.id > $id)) starts the index scan at
	// the bound, and rules out rows where it is null (#939).
	bounds := impliedPropertyBounds(nodePattern.variable, spec.propName, whereClause, getParamsFromContext(ctx))
	primaryNonNull := bounds.HasLower || bounds.HasUpper || e.indexedOrderRequiresNonNull(nodePattern.variable, spec.propName, whereClause)
	if spec.descending && !primaryNonNull {
		return nil, false, nil
	}

	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, spec.propName)
	if len(labels) != 1 {
		return nil, false, nil
	}
	label := labels[0]

	nodes, used, err := e.collectIndexedOrderWindow(ctx, nodePattern, whereClause, orderSpecs, label, limit, bounds)
	if err == nil && used && len(nodes) < limit {
		if primaryNonNull {
			return nodes, true, nil
		}
		// The property index omits null keys. An exhausted partial window is
		// not a complete general MATCH result: let normal selection include
		// qualifying nodes whose first sort property is absent.
		return nil, false, nil
	}
	return nodes, used, err
}

// tryCollectNodesFromPropertyIndexNotNullOrderLimit attempts to satisfy:
//
//	MATCH (n:Label) WHERE n.prop IS NOT NULL RETURN ... ORDER BY n.prop [ASC|DESC] LIMIT K
//
// using the property index directly (top-K by indexed key) without label scan.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexNotNullOrderLimit(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
) ([]*storage.Node, bool, error) {
	if limit <= 0 || len(nodePattern.labels) == 0 {
		return nil, false, nil
	}

	whereProp, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	orderSpecs, ok := e.parseIndexedNodeOrderSpecs(orderExpr, nodePattern.variable)
	if !ok {
		return nil, false, nil
	}
	spec := orderSpecs[0]
	if !strings.EqualFold(strings.TrimSpace(spec.propName), strings.TrimSpace(whereProp)) {
		return nil, false, nil
	}

	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, whereProp)
	if len(labels) != 1 {
		// Keep semantics simple/deterministic for now; multi-label merge ordering can be added later.
		return nil, false, nil
	}
	label := labels[0]
	return e.collectIndexedOrderWindow(ctx, nodePattern, whereClause, orderSpecs, label, limit, storage.PropertyIndexBounds{})
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
	if len(nodePattern.labels) == 0 {
		return nil, false, nil
	}
	property, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}
	nodes, used := e.collectNodesFromIndexedProperty(schema, nodePattern, property, func(label string) ([]storage.NodeID, bool) {
		return schema.PropertyIndexAllNonNil(label, property, false), true
	})
	return nodes, used, nil
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
	if !labellessPropertyIndexUsable(e.storage, out...) {
		return nil
	}
	return out
}

// seekCandidateLabels returns the labels whose property indexes on property
// list every node a seek may return, and the label disjunction the candidates
// must satisfy (nil when none applies).
//
// A pattern with labels uses its indexed labels (indexCandidateLabels). A
// pattern without labels whose WHERE requires one of anyOf (n:A OR n:B, #858)
// uses those labels when every one of them has an index on property: any node
// the WHERE can match carries one of them, so the union of their index
// entries lists it, and Neo4j plans the same predicate as one index seek per
// label. When one of them has no index, the indexes can miss a match and the
// label-less route decides (labellessPropertyIndexUsable).
func (e *StorageExecutor) seekCandidateLabels(schema *storage.SchemaManager, patternLabels []string, property string, anyOf []string) ([]string, []string) {
	if len(patternLabels) == 0 && len(anyOf) > 0 {
		indexed := true
		for _, label := range anyOf {
			if _, exists := schema.GetPropertyIndex(label, property); !exists {
				indexed = false
				break
			}
		}
		if indexed {
			return anyOf, anyOf
		}
	}
	return e.indexCandidateLabels(schema, patternLabels, property), nil
}

// seekCandidateHasLabels reports whether a node an index listed still carries
// the pattern's labels and one of anyOf (when set): an index can list a node
// that has since lost the label.
func seekCandidateHasLabels(node *storage.Node, patternLabels, anyOf []string) bool {
	if len(patternLabels) > 0 && !mergeNodeHasLabels(node, patternLabels) {
		return false
	}
	return len(anyOf) == 0 || nodeHasAnyLabel(node, anyOf)
}

// whereLabelDisjunctionConjunct returns the labels of the first conjunct that
// is a label disjunction on variable (whereLabelDisjunction), or nil.
func whereLabelDisjunctionConjunct(variable string, conjuncts []string) []string {
	for _, conjunct := range conjuncts {
		if labels, ok := whereLabelDisjunction(variable, conjunct); ok {
			return labels
		}
	}
	return nil
}

// whereLabelDisjunction parses a WHERE term that requires variable to carry
// one of a set of labels: v:A, v:A OR v:B …, or the label expression v:A|B,
// with labels plain or backtick-quoted. A conjunction of labels (v:A:B) or any
// other term is not one.
func whereLabelDisjunction(variable, term string) ([]string, bool) {
	want, ok := isOneSymbolicName(variable)
	if !ok {
		return nil, false
	}
	var labels []string
	for _, alternative := range splitTopLevelOrTerms(unwrapOuterParens(strings.TrimSpace(term))) {
		alternative = unwrapOuterParens(strings.TrimSpace(alternative))
		written, end, ok := scanSymbolicName(alternative, 0)
		if !ok || symbolicNameValue(written) != want {
			return nil, false
		}
		rest := strings.TrimSpace(alternative[end:])
		if !strings.HasPrefix(rest, ":") {
			return nil, false
		}
		names := strings.TrimSpace(rest[1:])
		for {
			written, end, ok := scanSymbolicName(names, 0)
			if !ok {
				return nil, false
			}
			labels = append(labels, symbolicNameValue(written))
			names = strings.TrimSpace(names[end:])
			if names == "" {
				break
			}
			if names[0] != '|' {
				return nil, false
			}
			names = strings.TrimSpace(names[1:])
		}
	}
	return labels, len(labels) > 0
}

func (e *StorageExecutor) parseSimpleIndexedEquality(ctx context.Context, variable, whereClause string) (property string, value interface{}, ok bool) {
	shape := simpleIndexedEqualityShapeFor(ctx, variable, whereClause)
	if !shape.ok {
		return "", nil, false
	}
	value, constant := e.indexSeekConstant(ctx, shape.constant)
	return shape.property, value, constant
}

// simpleIndexedEqualityShape is the text part of parseSimpleIndexedEquality:
// the property of variable that a lone equality compares, and the
// expression it compares with, which is evaluated per use.
type simpleIndexedEqualityShape struct {
	property string
	constant string
	ok       bool
}

func parseSimpleIndexedEqualityShape(variable, whereClause string) simpleIndexedEqualityShape {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return simpleIndexedEqualityShape{}
	}

	// Only simple predicates are index-eligible: a = 1 XOR true is not an
	// equality with (1 XOR true).
	if hasTopLevelLogicalOperator(clause) {
		return simpleIndexedEqualityShape{}
	}
	for _, kw := range []string{"NOT", " IN ", " IS ", "<>", "!=", ">=", "<=", ">", "<"} {
		if topLevelKeywordIndex(clause, kw) >= 0 {
			return simpleIndexedEqualityShape{}
		}
	}

	eqIdx := topLevelEqualsIndex(clause)
	if eqIdx <= 0 || eqIdx >= len(clause)-1 {
		return simpleIndexedEqualityShape{}
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return simpleIndexedEqualityShape{}
	}

	if prop, isLeftVarProp := parseVariableProperty(left, variable); isLeftVarProp {
		return simpleIndexedEqualityShape{property: prop, constant: right, ok: true}
	}
	if prop, isRightVarProp := parseVariableProperty(right, variable); isRightVarProp {
		return simpleIndexedEqualityShape{property: prop, constant: left, ok: true}
	}
	return simpleIndexedEqualityShape{}
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
	rawList, constant := e.indexSeekConstant(ctx, right)
	if !constant {
		return "", nil, false
	}
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

// coerceInterfaceList returns a parameter list's elements as Cypher values
// (cypherListValue), or nil when the value isn't a list; a non-list IN
// parameter is rejected before execution (validateMembershipParameters).
func coerceInterfaceList(v interface{}) []interface{} {
	items, _ := cypherListValue(v)
	return items
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
	upper := upperASCII(strings.TrimSpace(clause))
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
	// Only a pair that encloses the whole clause: (a) OR (b) starts and ends
	// with a parenthesis, but they don't match each other.
	for strings.HasPrefix(out, "(") && findMatchingDelimiter(out, 0, '(', ')') == len(out)-1 {
		out = strings.TrimSpace(out[1 : len(out)-1])
	}
	return out
}

// splitTopLevelAndConjuncts splits a WHERE clause into its top-level AND
// conjuncts. An AND inside a string, brackets or a CASE … END block (a WHEN
// condition, #699) belongs to its operand. It is the one AND splitter of the
// WHERE planners (index seek, cartesian and CREATE / MERGE filters, CALL
// subquery correlation).
func splitTopLevelAndConjuncts(clause string) []string {
	if topLevelKeywordIndex(clause, "OR") >= 0 || topLevelKeywordIndex(clause, "XOR") >= 0 {
		return []string{strings.TrimSpace(clause)}
	}
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace, caseDepth := 0, 0, 0, 0
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
		case 'C', 'c':
			if caseKeywordAt(clause, i, "CASE") {
				caseDepth++
				i += len("CASE") - 1
				continue
			}
		case 'E', 'e':
			if caseDepth > 0 && caseKeywordAt(clause, i, "END") {
				caseDepth--
				i += len("END") - 1
				continue
			}
		}
		if paren == 0 && bracket == 0 && brace == 0 && caseDepth == 0 && i+3 <= len(clause) && strings.EqualFold(clause[i:i+3], "AND") {
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
		return compareCypherPredicateValues(left, right, op), true
	case ">", "<", ">=", "<=":
		if _, comparable := compareCypherOrderedValues(left, right); !comparable {
			return false, false
		}
		return compareCypherPredicateValues(left, right, op), true
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
	upper := upperASCII(clause)
	if strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") {
		return clause
	}

	// Look for coalesce( at the start.
	lowerClause := lowerASCII(clause)
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

// collectNodesFromIndexedProperty is the shared tail of the index seeks that
// read one indexed property per candidate label: it gathers lookup(label)
// for every label of the pattern that indexes property, de-duplicates the
// IDs in lookup order, and loads the nodes that carry all pattern labels.
// used is false when no candidate label indexes property.
func (e *StorageExecutor) collectNodesFromIndexedProperty(
	schema *storage.SchemaManager,
	nodePattern nodePatternInfo,
	property string,
	lookup func(label string) ([]storage.NodeID, bool),
) (nodes []*storage.Node, used bool) {
	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false
	}
	idSet := make(map[storage.NodeID]struct{})
	ordered := make([]storage.NodeID, 0)
	for _, label := range labels {
		ids, ok := lookup(label)
		if !ok {
			return nil, false
		}
		for _, id := range ids {
			if _, exists := idSet[id]; exists {
				continue
			}
			idSet[id] = struct{}{}
			ordered = append(ordered, id)
		}
	}
	nodes = make([]*storage.Node, 0, len(ordered))
	for _, id := range ordered {
		// An ID the index still lists for a node that is gone or lost a
		// pattern label is skipped.
		if node, err := e.getQueryNode(e.storage, id); err == nil && node != nil && mergeNodeHasLabels(node, nodePattern.labels) {
			nodes = append(nodes, node)
		}
	}
	return nodes, true
}

// tryCollectNodesFromPropertyIndexRange seeds a MATCH from a property index
// when its WHERE bounds an indexed property with <, <=, > or >= against a
// number or string literal or parameter (p.age > 30, 18 <= p.age < 65,
// p.name >= $from AND p.name < $to). Only the bounds narrow the seed; the
// caller still evaluates the whole WHERE on every candidate (#820).
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexRange(nodePattern nodePatternInfo, whereClause string, params map[string]interface{}) ([]*storage.Node, bool, error) {
	if len(nodePattern.labels) == 0 {
		return nil, false, nil
	}
	property, bounds, ok := parseIndexedRangeBounds(nodePattern.variable, whereClause, params)
	if !ok {
		return nil, false, nil
	}
	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}
	nodes, used := e.collectNodesFromIndexedProperty(schema, nodePattern, property, func(label string) ([]storage.NodeID, bool) {
		return schema.PropertyIndexRange(label, property, bounds)
	})
	return nodes, used, nil
}

// parseIndexedRangeBounds finds, among the top-level AND conjuncts of a
// WHERE, comparisons of variable.property with a literal or parameter, and
// returns the bounds they place on the first such property.
func parseIndexedRangeBounds(variable, whereClause string, params map[string]interface{}) (property string, bounds storage.PropertyIndexBounds, ok bool) {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return "", bounds, false
	}
	for _, raw := range splitTopLevelAndConjuncts(clause) {
		operands, operators, isChain := splitComparisonChain(unwrapOuterParens(strings.TrimSpace(raw)))
		if !isChain {
			continue
		}
		for index, operator := range operators {
			left, right := strings.TrimSpace(operands[index]), strings.TrimSpace(operands[index+1])
			if leftProperty, isProperty := parseVariableProperty(left, variable); isProperty {
				if value, resolved := rangeBoundValue(right, params); resolved && (property == "" || property == leftProperty) {
					property = leftProperty
					addIndexedRangeBound(&bounds, operator, value)
				}
				continue
			}
			if rightProperty, isProperty := parseVariableProperty(right, variable); isProperty {
				if value, resolved := rangeBoundValue(left, params); resolved && (property == "" || property == rightProperty) {
					property = rightProperty
					addIndexedRangeBound(&bounds, mirroredComparison(operator), value)
				}
			}
		}
	}
	return property, bounds, property != "" && (bounds.HasLower || bounds.HasUpper)
}

// rangeBoundValue resolves a literal or $parameter operand of a range
// comparison.
func rangeBoundValue(operand string, params map[string]interface{}) (interface{}, bool) {
	if strings.HasPrefix(operand, "$") {
		value, exists := params[strings.TrimPrefix(operand, "$")]
		return value, exists
	}
	value, ok := parseLiteralValue(operand)
	if !ok {
		return nil, false
	}
	return value, true
}

// addIndexedRangeBound records "property operator value". A second bound on
// the same side is not combined: keeping one still selects every match.
func addIndexedRangeBound(bounds *storage.PropertyIndexBounds, operator string, value interface{}) {
	switch operator {
	case ">", ">=":
		if !bounds.HasLower {
			bounds.Lower, bounds.HasLower, bounds.LowerInclusive = value, true, operator == ">="
		}
	case "<", "<=":
		if !bounds.HasUpper {
			bounds.Upper, bounds.HasUpper, bounds.UpperInclusive = value, true, operator == "<="
		}
	}
}

// mirroredComparison is the operator with its operands swapped (5 < p.x is
// p.x > 5); others are returned unchanged and ignored by the caller.
func mirroredComparison(operator string) string {
	switch operator {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return operator
}

// collectPipelineIndexedNodeCandidates is the index half of
// collectPipelineInitialNodeCandidates: the candidates of the first index
// operator that can narrow the MATCH, with used false when none can. Row
// reads and the filtered count (tryStreamPipelineFilteredNodeCount) both seed
// from it, so a lookup that uses an index for rows uses it for count(*) too
// (#820).
func (e *StorageExecutor) collectPipelineIndexedNodeCandidates(ctx context.Context, nodePattern nodePatternInfo, whereClause string, hint pipelineMatchPhysicalHint) (nodes []*storage.Node, whereApplied bool, used bool, err error) {
	params := getParamsFromContext(ctx)
	if hint.limit > 0 && hint.orderExpr != "" {
		orderedPlans := []func() ([]*storage.Node, bool, error){
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexNotNullOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit)
			},
			func() ([]*storage.Node, bool, error) {
				return e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, hint.orderExpr, hint.limit)
			},
		}
		for _, plan := range orderedPlans {
			nodes, used, err := plan()
			if err != nil {
				return nil, false, false, err
			}
			if used {
				e.markOuterIndexTopKUsed()
				return nodes, false, true, nil
			}
		}
	}
	if nodes, used, err := e.tryCollectNodesFromIDEqualityCompound(ctx, nodePattern, whereClause, params); err != nil || used {
		return nodes, false, used, err
	}
	// When an id IN list predicate is the whole WHERE, its seek returns
	// exactly the nodes it selects: the caller need not
	// evaluate the WHERE again on each of them. Evaluating it per row checked
	// every node against the whole list: CALL { … } IN TRANSACTIONS batches,
	// which select their rows by id(n) IN $ids, were quadratic in the batch
	// size (#703).
	if nodes, used, err := e.tryCollectNodesFromIDIn(ctx, nodePattern, whereClause, params); err != nil || used {
		return nodes, used, used, err
	}
	// An id IN list among AND conditions seeks the same way; the caller
	// still evaluates the whole WHERE on the nodes it selects.
	if findTopLevelKeyword(whereClause, " AND ") > 0 {
		for _, conjunct := range splitTopLevelAndConjuncts(whereClause) {
			if nodes, used, err := e.tryCollectNodesFromIDIn(ctx, nodePattern, conjunct, params); err != nil || used {
				return nodes, false, used, err
			}
		}
	}
	// inList marks the plan that answers a WHERE that is exactly an IN list.
	indexedPlans := []struct {
		collect func() ([]*storage.Node, bool, error)
		inList  bool
	}{
		{collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexDisjunction(ctx, nodePattern, whereClause, params)
		}},
		{inList: true, collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern, whereClause, params)
		}},
		{collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexEqualityCompound(ctx, nodePattern, whereClause)
		}},
		{collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexConjunctTerms(ctx, nodePattern, whereClause, params)
		}},
		{collect: func() ([]*storage.Node, bool, error) {
			// Inline pattern properties ({id: $id}) on an indexed property.
			nodes, used := e.lookupPatternCandidatesUsingPropertyIndex(nodePattern, e.getStorage(ctx))
			if used {
				e.markMergeSchemaLookupUsed()
			}
			if !used || len(nodePattern.labels) < 2 {
				return nodes, used, nil
			}
			labelled := nodes[:0]
			for _, node := range nodes {
				if mergeNodeHasLabels(node, nodePattern.labels) {
					labelled = append(labelled, node)
				}
			}
			return labelled, true, nil
		}},
		{collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexRange(nodePattern, whereClause, params)
		}},
		{collect: func() ([]*storage.Node, bool, error) {
			return e.tryCollectNodesFromPropertyIndexNotNull(nodePattern, whereClause)
		}},
	}
	for _, plan := range indexedPlans {
		nodes, used, err := plan.collect()
		if err != nil {
			return nil, false, false, err
		}
		if used {
			if len(nodePattern.properties) > 0 {
				nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
			}
			// A WHERE that is exactly <variable>.<property> IN <list> on an
			// indexed property is answered by the index: one lookup per list
			// value returns exactly the nodes it selects, so the caller
			// doesn't test each of them against the whole list again
			// (quadratic in the list for a list that names every node).
			// Checked only once the index answered, so a read that takes
			// another route doesn't parse its WHERE for it.
			exact := plan.inList && e.whereIsSimpleIndexedIn(ctx, nodePattern.variable, whereClause, params)
			return nodes, exact, true, nil
		}
	}
	return nil, false, false, nil
}
