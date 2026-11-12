package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const deleteStreamingBatchSize = 500

// ========================================
// WITH Clause
// ========================================

// executeMatch handles MATCH queries.
func (e *StorageExecutor) parseMergePattern(ctx context.Context, pattern string) (string, []string, map[string]interface{}, error) {
	return e.parseMergeNodePattern(ctx, pattern, nil, nil)
}

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.

type deleteProjectionKind uint8

const (
	deleteProjectionUnknown deleteProjectionKind = iota
	deleteProjectionNode
	deleteProjectionRelationship
)

type deleteTargetValue struct {
	kind   deleteProjectionKind
	nodeID storage.NodeID
	edgeID storage.EdgeID
}

func classifyDeleteTargetValue(val interface{}) deleteTargetValue {
	switch v := val.(type) {
	case map[string]interface{}:
		if id, ok := v["_edgeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionRelationship, edgeID: storage.EdgeID(id)}
		}
		if id, ok := v["_nodeId"].(string); ok && id != "" {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: storage.NodeID(id)}
		}
	case *storage.Node:
		if v != nil {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: v.ID}
		}
	case *storage.Edge:
		if v != nil {
			return deleteTargetValue{kind: deleteProjectionRelationship, edgeID: v.ID}
		}
	case string:
		if v != "" {
			return deleteTargetValue{kind: deleteProjectionNode, nodeID: storage.NodeID(v)}
		}
	}
	return deleteTargetValue{}
}

func (e *StorageExecutor) isDeleteStreamingEligible(ctx context.Context, matchSegment, deleteVars string, detach bool) bool {
	if !detach {
		return false
	}
	if strings.TrimSpace(matchSegment) == "" {
		return false
	}
	// Keep streaming path conservative to avoid semantic drift.
	for _, blocked := range []string{"WITH", "LIMIT", "SKIP", "ORDER", "CALL", "UNWIND"} {
		if containsKeywordOutsideStrings(ctx, matchSegment, blocked) {
			return false
		}
	}
	// Keep variable parsing simple and deterministic for now.
	if strings.Contains(deleteVars, ",") {
		return false
	}
	deleteVars = strings.TrimSpace(deleteVars)
	if deleteVars == "" {
		return false
	}
	return cyphertext.IsIdentifier(deleteVars)
}

func (e *StorageExecutor) normalizeSetMatchRowsToNodes(ctx context.Context, matchResult *ExecuteResult, store storage.Engine) {
	if matchResult == nil {
		return
	}
	for rowIdx := range matchResult.Rows {
		row := matchResult.Rows[rowIdx]
		for colIdx, val := range row {
			m, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			rawID, ok := m["_nodeId"]
			if !ok {
				continue
			}
			nodeID, ok := rawID.(string)
			if !ok || nodeID == "" {
				continue
			}
			node, err := e.getQueryNode(ctx, store, storage.NodeID(nodeID))
			if err != nil || node == nil {
				continue
			}
			row[colIdx] = node
		}
	}
}

func (e *StorageExecutor) normalizeSetMatchRowsToEdges(ctx context.Context, matchResult *ExecuteResult, store storage.Engine) {
	if matchResult == nil {
		return
	}
	for rowIdx := range matchResult.Rows {
		row := matchResult.Rows[rowIdx]
		for colIdx, val := range row {
			m, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			rawID, ok := m["_edgeId"]
			if !ok {
				continue
			}
			edgeID, ok := rawID.(string)
			if !ok || edgeID == "" {
				continue
			}
			edge, err := store.GetEdge(ctx, storage.EdgeID(edgeID))
			if err != nil || edge == nil {
				continue
			}
			row[colIdx] = edge
		}
	}
}

var setScopeVarPattern = regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:\.|\+=|=)`)

// extractScopeVariablesFromSetAndReturn returns variable names referenced in
// SET assignments and RETURN items so the pre-SET MATCH query includes all
// variables needed for mutation and projection.
func extractScopeVariablesFromSetAndReturn(ctx context.Context, setPart, returnPart string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 4)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if !cyphertext.IsIdentifier(v) {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	for _, m := range setScopeVarPattern.FindAllStringSubmatch(setPart, -1) {
		if len(m) > 1 {
			add(m[1])
		}
	}

	if strings.TrimSpace(returnPart) != "" {
		for _, raw := range splitTopLevelCommaKeepEmpty(returnPart) {
			expr := strings.TrimSpace(raw)
			if expr == "" {
				continue
			}
			if asIdx := findKeywordIndex(ctx, expr, "AS"); asIdx > 0 {
				expr = strings.TrimSpace(expr[:asIdx])
			}
			add(extractVariableNameFromReturnItem(expr))
		}
	}
	return out
}

// collapseChainedSetClauses rewrites chained SET keywords into comma-separated assignments.
// Example: "n += $props SET n.x = 1 SET n.y = 2" -> "n += $props, n.x = 1, n.y = 2".
// Counting what a SET writes needs the clause boundaries, so the SET
// applicators walk the clauses with nextChainedSetClause instead.
func collapseChainedSetClauses(ctx context.Context, setPart string) string {
	setPart = strings.TrimSpace(setPart)
	first, next, ok := nextChainedSetClause(ctx, setPart, 0)
	if !ok {
		return setPart
	}
	clause, next, ok := nextChainedSetClause(ctx, setPart, next)
	if !ok {
		return first
	}
	clauses := []string{first}
	for ; ok; clause, next, ok = nextChainedSetClause(ctx, setPart, next) {
		clauses = append(clauses, clause)
	}
	return strings.Join(clauses, ", ")
}

// nextChainedSetClause returns the next non-empty clause at or after start in
// setPart, a SET assignment list that may chain further SET clauses
// ("n += $props SET n.x = 1"), and the index where the clause after it
// starts. ok is false when no clause is left.
func nextChainedSetClause(ctx context.Context, setPart string, start int) (clause string, next int, ok bool) {
	for start < len(setPart) {
		end, after := len(setPart), len(setPart)
		if idx := keywordIndexFrom(ctx, setPart, "SET", start, defaultKeywordScanOpts()); idx >= 0 {
			end, after = idx, idx+len("SET")
		}
		if clause = strings.TrimSpace(setPart[start:end]); clause != "" {
			return clause, after, true
		}
		start = after
	}
	return "", len(setPart), false
}

// firstPostSetClauseIndex returns the first index of a clause keyword that can
// legally follow a SET assignment list. Returns -1 when none are present.
func firstPostSetClauseIndex(ctx context.Context, setTail string) int {
	opts := defaultKeywordScanOpts()
	first := -1
	for _, kw := range []string{
		"REMOVE", "UNWIND", "WITH", "RETURN",
		"CREATE", "MATCH", "MERGE", "DELETE", "CALL",
		"ORDER BY", "LIMIT", "SKIP",
	} {
		if idx := keywordIndexFrom(ctx, setTail, kw, 0, opts); idx >= 0 {
			if first == -1 || idx < first {
				first = idx
			}
		}
	}
	return first
}

func (e *StorageExecutor) resolveUnwindValueFromExpr(ctx context.Context, unwindExpr string, nodeVars map[string]*storage.Node) interface{} {
	expr := normalizeUnwindExpression(unwindExpr)
	if strings.HasPrefix(expr, "$") {
		paramName := strings.TrimSpace(expr[1:])
		if paramName != "" {
			if params := getParamsFromContext(ctx); params != nil {
				if v, ok := params[paramName]; ok {
					return v
				}
			}
		}
	}
	return e.evaluateExpressionWithContext(ctx, expr, nodeVars, nil)
}

// normalizeUnwindExpression removes syntactic wrapper parentheses around a valid
// UNWIND expression, e.g. "($vals)" -> "$vals", while preserving inner
// expression content for evaluation.
func normalizeUnwindExpression(expr string) string {
	trimmed := strings.TrimSpace(expr)
	for hasOuterParens(trimmed) {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	return trimmed
}

func hasOuterParens(s string) bool {
	return len(s) >= 2 && s[0] == '(' && cyphertext.FindMatchingDelimiter(s, 0, '(', ')', cyphertext.OpeningDelimiter) == len(s)-1
}

// coerceToUnwindItems is the one UNWIND coercion, as in Neo4j (#693): null
// unwinds to no rows, a list to its elements (cypherListValue), and any other
// value to one row holding it.
func coerceToUnwindItems(value interface{}) []interface{} {
	if value == nil {
		return nil
	}
	if items, isList := cypherListValue(value); isList {
		return items
	}
	return []interface{}{value}
}

func dedupeNonEmpty(groups ...[]string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, group := range groups {
		for _, item := range group {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	return out
}

func normalizePropsMap(value interface{}, source string) (map[string]interface{}, error) {
	propsMap, ok := value.(map[string]interface{})
	if ok {
		for k, v := range propsMap {
			propsMap[k] = normalizePropValue(v)
		}
		return propsMap, nil
	}
	if genericMap, ok := value.(map[interface{}]interface{}); ok {
		propsMap = make(map[string]interface{}, len(genericMap))
		for k, v := range genericMap {
			keyStr, ok := k.(string)
			if !ok {
				return nil, localizedError(localization.CypherMutationsSetMergeStringKeysRequired(source, fmt.Sprintf("%T", k)), nil)
			}
			propsMap[keyStr] = normalizePropValue(v)
		}
		return propsMap, nil
	}
	return nil, localizedError(localization.CypherMutationsSetMergeMapRequired(source, fmt.Sprintf("%T", value)), nil)
}

func normalizePropValue(value interface{}) interface{} {
	normalized, _ := normalizedPropValue(value)
	return normalized
}

// normalizedPropValue is the one property value normalizer: Go integers
// become int64, float32 float64 and Go times DateTimes, in lists and maps
// too. A container is copied only when a descendant changes; rewritten
// describes a value conversion, independently of a writer's ownership copy.
func normalizedPropValue(value interface{}) (normalized interface{}, rewritten bool) {
	return normalizedCypherValue(value, true)
}

// normalizeExpressionValue admits a runtime value without storage's numeric
// coercion. Both boundaries share container, temporal and null normalization.
func normalizeExpressionValue(value interface{}) interface{} {
	normalized, _ := normalizedCypherValue(value, false)
	return normalized
}

func normalizedCypherValue(value interface{}, property bool) (normalized interface{}, rewritten bool) {
	if value != nil && cypherfn.RuntimeValue(value) == nil {
		return nil, true
	}
	if number, numeric := valueconvert.ReadNumber(value); numeric {
		if !property {
			return value, false
		}
		if number.IsInteger() {
			if integer, fits := number.Int64(valueconvert.IntegerOnly); fits {
				_, canonical := value.(int64)
				return integer, !canonical
			}
		}
		if _, canonical := value.(float64); canonical {
			return value, false
		}
		// Preserve storage's existing unsigned-overflow-to-Double policy;
		// a failed signed conversion must never wrap to a negative Long.
		return number.Float64(), true
	}
	switch v := value.(type) {
	case time.Time:
		return dateTimeFromGoTime(v), true
	case *time.Time:
		if v == nil {
			return nil, true
		}
		return dateTimeFromGoTime(*v), true
	case []interface{}:
		var out []interface{}
		for i, item := range v {
			stored, changed := normalizedCypherValue(item, property)
			if changed && out == nil {
				out = make([]interface{}, len(v))
				copy(out, v)
			}
			if out != nil {
				out[i] = stored
			}
		}
		if out != nil {
			return out, true
		}
		return value, false
	case map[string]interface{}:
		var out map[string]interface{}
		for k, item := range v {
			stored, changed := normalizedCypherValue(item, property)
			if changed && out == nil {
				out = cloneStringAnyMap(v)
			}
			if out != nil {
				out[k] = stored
			}
		}
		if out != nil {
			return out, true
		}
		return value, false
	default:
		return value, false
	}
}

func removeNodeLabels(existing []string, labelsToRemove []string) ([]string, int64) {
	if len(existing) == 0 || len(labelsToRemove) == 0 {
		return existing, 0
	}
	removeSet := make(map[string]struct{}, len(labelsToRemove))
	for _, label := range labelsToRemove {
		removeSet[label] = struct{}{}
	}
	next := make([]string, 0, len(existing))
	var removed int64
	for _, label := range existing {
		if _, ok := removeSet[label]; ok {
			removed++
			continue
		}
		next = append(next, label)
	}
	return next, removed
}

// executeCall handles CALL procedure queries.

// smartSplitReturnItems adapts callers to canonical top-level comma splitting.
func (e *StorageExecutor) smartSplitReturnItems(returnPart string) []string {
	return splitTopLevelComma(returnPart)
}

func (e *StorageExecutor) parseReturnItems(ctx context.Context, returnPart string) []returnItem {
	if strings.TrimSpace(returnPart) == "" {
		returnPart = "*"
	}
	plan := returnProjectionPlanFor(ctx, "RETURN "+returnPart)
	if plan.star {
		items := []returnItem{{expr: "*", alias: "*"}}
		for _, item := range plan.starItems {
			itemSource := readProjectionItemSource(ctx, item, projectionAdmitted)
			expr, alias := itemSource.expression, itemSource.alias
			items = append(items, returnItem{expr: expr, alias: alias})
		}
		return items
	}
	items := make([]returnItem, 0, len(plan.projections))
	for _, projection := range plan.projections {
		items = append(items, returnItem{expr: projection.expr, alias: projection.alias})
	}
	return items
}

func normalizeProjectionColumnName(raw string) string {
	return symbolicNameValue(strings.TrimSpace(raw))
}

func (e *StorageExecutor) generateID() string {
	// Use UUID for globally unique IDs
	// This prevents ID collisions across server restarts which caused
	// the race condition where CREATE would cancel pending DELETEs
	return uuid.New().String()
}

// Deprecated: Sequential counter replaced with UUID generation
var idCounter int64

func (e *StorageExecutor) idCounter() int64 {
	// Keep for backward compatibility but not used in generateID anymore
	atomic.AddInt64(&idCounter, 1)
	return atomic.LoadInt64(&idCounter)
}

// forEachRelationshipSource frames relationship declarations in pattern text.
// The relationship grammar remains here; opaque, name and delimiter extents
// belong to the shared lexical owners and the existing declaration reader.
func forEachRelationshipSource(pattern string, visit func(start, open, close, end int, incoming, outgoing bool) bool) {
	for index := 0; index < len(pattern); index++ {
		if end, opaque := cyphertext.SkipToken(pattern, index); opaque {
			index = end - 1
			continue
		}
		if pattern[index] == '(' {
			end := cyphertext.FindMatchingDelimiter(pattern, index, '(', ')', cyphertext.OpeningDelimiter)
			if end < 0 {
				return
			}
			index = end
			continue
		}
		if pattern[index] != '[' || index == 0 || pattern[index-1] != '-' {
			continue
		}
		close := cyphertext.FindMatchingDelimiter(pattern, index, '[', ']', cyphertext.OpeningDelimiter)
		if close < 0 {
			return
		}
		end, _ := cyphertext.GapEnd(pattern, close+1)
		if end >= len(pattern) || pattern[end] != '-' {
			index = close
			continue
		}
		end++
		outgoing := end < len(pattern) && pattern[end] == '>'
		if outgoing {
			end++
		}
		start := index - 1
		incoming := start > 0 && pattern[start-1] == '<'
		if incoming {
			start--
		}
		if !visit(start, index, close, end, incoming, outgoing) {
			return
		}
		index = end - 1
	}
}

// extractVariableNameFromReturnItem extracts the variable name from a return item expression.
// Examples:
//   - "n" -> "n"
//   - "n.name" -> "n"
//   - "id(n)" -> "n"
//   - "n.age + 1" -> "n"
func extractVariableNameFromReturnItem(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}

	// Handle function calls like id(n), elementId(n), etc.
	if strings.Contains(expr, "(") {
		// Extract variable from function call: id(n) -> n
		openParen := strings.Index(expr, "(")
		closeParen := strings.LastIndex(expr, ")")
		if openParen > 0 && closeParen > openParen {
			inner := strings.TrimSpace(expr[openParen+1 : closeParen])
			// If inner contains a dot, it's property access: id(n.name) -> n
			if dotIdx := strings.Index(inner, "."); dotIdx > 0 {
				return strings.TrimSpace(inner[:dotIdx])
			}
			return inner
		}
	}

	// Handle property access: n.name -> n
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		return strings.TrimSpace(parts[0])
	}

	// Simple variable name
	return expr
}

// extractRelTypesFromPattern consumes the existing relationship grammar.
func (e *StorageExecutor) extractRelTypesFromPattern(pattern, prefix string) []string {
	var types []string
	forEachRelationshipSource(pattern, func(start, open, close, _ int, _, _ bool) bool {
		if !strings.HasPrefix(pattern[start:], prefix) {
			return true
		}
		types = relationshipDeclarationOf(StripComments(pattern[open+1 : close])).types
		return false
	})
	return types
}

func (e *StorageExecutor) relationshipExistencePatternDirections(pattern, variable string) (incoming, outgoing bool, relTypes []string) {
	if groupStart := strings.Index(pattern, "("+variable+")"); groupStart >= 0 || strings.Contains(pattern, "("+variable+":") {
		if groupStart < 0 {
			groupStart = strings.Index(pattern, "("+variable+":")
		}
		groupEnd := groupStart + len(variable) + 2
		if strings.HasPrefix(pattern[groupStart:], "("+variable+":") {
			closeRel := strings.Index(pattern[groupStart:], ")")
			if closeRel >= 0 {
				groupEnd = groupStart + closeRel + 1
			}
		}
		before := pattern[:groupStart]
		after := pattern[groupEnd:]

		switch {
		case strings.HasPrefix(after, "<-["):
			incoming = true
			relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
		case strings.HasPrefix(after, "-["):
			relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			if strings.Contains(after, "]->") {
				outgoing = true
			} else if strings.Contains(after, "]-") {
				incoming = true
				outgoing = true
			}
		}

		switch {
		case strings.HasSuffix(before, "]->"):
			incoming = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			}
		case strings.Contains(before, "<-[") && strings.HasSuffix(before, "]-"):
			outgoing = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
			}
		case strings.HasSuffix(before, "]-"):
			incoming = true
			outgoing = true
			if len(relTypes) == 0 {
				relTypes = e.extractRelTypesFromPattern(pattern, "-[")
			}
		}
	}

	if !incoming && !outgoing {
		if in, out, ok := bareRelDirection(pattern, variable); ok {
			return in, out, nil
		}
	}
	return incoming, outgoing, relTypes
}

// edgeTypeMatches checks if an edge type matches any of the allowed types
func (e *StorageExecutor) edgeTypeMatches(edgeType string, allowedTypes []string) bool {
	for _, t := range allowedTypes {
		if edgeType == t {
			return true
		}
	}
	return false
}

// validatePolicyOnLabelChange checks RELATIONSHIP_POLICY constraints when a node's labels
// change. It validates all adjacent edges (outgoing and incoming) against the current
// policy constraints to ensure no DISALLOWED pair is formed and any ALLOWED whitelist
// is still satisfied.
func validatePolicyOnLabelChange(ctx context.Context, store storage.Engine, node *storage.Node, oldLabels []string) error {
	schema := store.GetSchema()
	if schema == nil {
		return nil
	}

	// Collect all policy constraints.
	constraints := schema.GetAllConstraints()
	var policies []storage.Constraint
	for _, c := range constraints {
		if c.Type == storage.ConstraintPolicy {
			policies = append(policies, c)
		}
	}
	if len(policies) == 0 {
		return nil
	}

	// Check outgoing edges (node is the source).
	outgoing, _ := store.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: node.ID})
	for _, edge := range outgoing {
		targetNode, err := store.GetNode(ctx, storage.NodeID(edge.EndNode))
		if err != nil || targetNode == nil {
			continue
		}
		if err := checkPolicyForEdge(edge.Type, node.Labels, targetNode.Labels, policies); err != nil {
			return err
		}
	}

	// Check incoming edges (node is the target).
	incoming, _ := store.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: node.ID})
	for _, edge := range incoming {
		sourceNode, err := store.GetNode(ctx, storage.NodeID(edge.StartNode))
		if err != nil || sourceNode == nil {
			continue
		}
		if err := checkPolicyForEdge(edge.Type, sourceNode.Labels, node.Labels, policies); err != nil {
			return err
		}
	}

	return nil
}

// checkPolicyForEdge validates a single edge against all policy constraints for its type.
// DISALLOWED policies are checked first (they take precedence).
func checkPolicyForEdge(edgeType string, sourceLabels, targetLabels []string, policies []storage.Constraint) error {
	// Gather policies for this edge type.
	var relevantAllowed []storage.Constraint
	for _, p := range policies {
		if p.Label != edgeType {
			continue
		}
		if p.PolicyMode == "DISALLOWED" {
			if sliceContains(sourceLabels, p.SourceLabel) && sliceContains(targetLabels, p.TargetLabel) {
				return localizedError(localization.CypherResidualPolicyDisallowed(p.Name, p.SourceLabel, edgeType, p.TargetLabel), nil)
			}
		} else if p.PolicyMode == "ALLOWED" {
			relevantAllowed = append(relevantAllowed, p)
		}
	}

	// If ALLOWED policies exist for this edge type, at least one must match.
	if len(relevantAllowed) > 0 {
		matched := false
		for _, p := range relevantAllowed {
			if sliceContains(sourceLabels, p.SourceLabel) && sliceContains(targetLabels, p.TargetLabel) {
				matched = true
				break
			}
		}
		if !matched {
			return localizedError(localization.CypherResidualPolicyAllowedRequired(edgeType), nil)
		}
	}

	return nil
}

func sliceContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Patterns compiled once (#591): these helpers run per statement or per row.
var (
	subqueryWherePattern = regexp.MustCompile(`(?i)\s+WHERE\s+`)
)
