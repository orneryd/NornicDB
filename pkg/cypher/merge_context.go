// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"


	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// emptyRelationshipContexts is n rows' relationship bindings for a pattern
// without relationship variables.
func emptyRelationshipContexts(n int) []map[string]*storage.Edge {
	contexts := make([]map[string]*storage.Edge, n)
	for index := range contexts {
		contexts[index] = map[string]*storage.Edge{}
	}
	return contexts
}

// lookupPatternCandidatesUsingPropertyIndex returns a narrowed node candidate
// set when the pattern carries inline equality on one or more indexed
// properties. It is the pattern-inline counterpart of
// `lookupWhereCandidatesUsingPropertyIndex`: same correctness contract, same
// safety story, but the source of the equality is the parsed `nodeInfo`
// rather than a WHERE-clause string.
//
// Returned `(nodes, true)` means: "use these as the candidate set; you still
// need to apply the full `nodeMatchesProps` filter to enforce residual
// non-indexed predicates". Returned `(nil, false)` means: "no usable index;
// fall back to the existing label-scan / AllNodes path".
//
// The function is shape-agnostic: it works for labelled and labelless
// patterns, single-property and N-property patterns, and any property type
// the index can key (string / int / float / bool — same set
// `indexValueKey` accepts on insert). It deliberately does NOT
// attempt to short-circuit when the residual filter is empty; the caller's
// uniform `nodeMatchesProps` step keeps the post-condition trivially
// correct even when the index narrows partially.
func (e *StorageExecutor) lookupPatternCandidatesUsingPropertyIndex(nodeInfo nodePatternInfo, store storage.Engine) ([]*storage.Node, bool) {
	if len(nodeInfo.properties) == 0 {
		return nil, false
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false
	}

	// Collect per-property candidate ID sets, one per inline equality that an
	// index can serve. Each set narrows the result via intersection.
	var idSets []map[storage.NodeID]struct{}
	usedAnyIndex := false

	for prop, val := range nodeInfo.properties {
		var ids []storage.NodeID
		probed := false
		if len(nodeInfo.labels) > 0 {
			// Labelled: probe the (label, prop) index when one exists.
			if _, ok := schema.GetPropertyIndex(nodeInfo.labels[0], prop); ok {
				ids = propertyIndexLookup(store, schema, nodeInfo.labels[0], prop, val)
				probed = true
			}
		} else {
			labels := e.indexCandidateLabels(schema, nil, prop)
			if labellessPropertyIndexUsable(store, labels...) {
				ids = schema.PropertyIndexLookupAnyLabel(prop, val)
				probed = true
			}
		}
		// Property has no covering index — record nothing for this prop.
		// The residual `nodeMatchesProps` step still enforces it. A probe
		// that finds nothing for a string, boolean or number means no node
		// holds the value (#821); for other values the scan decides.
		if ids == nil && !(probed && propertyIndexMissIsAuthoritative(val)) {
			continue
		}
		usedAnyIndex = true
		set := make(map[storage.NodeID]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		idSets = append(idSets, set)
	}

	if !usedAnyIndex || len(idSets) == 0 {
		return nil, false
	}

	// Intersect across all indexed-prop sets.
	smallest := 0
	for i, s := range idSets {
		if len(s) < len(idSets[smallest]) {
			smallest = i
		}
	}
	out := make([]*storage.Node, 0, len(idSets[smallest]))
	for id := range idSets[smallest] {
		hit := true
		for i, s := range idSets {
			if i == smallest {
				continue
			}
			if _, ok := s[id]; !ok {
				hit = false
				break
			}
		}
		if !hit {
			continue
		}
		n, err := store.GetNode(id)
		if err != nil || n == nil {
			continue
		}
		out = append(out, n)
	}
	return out, true
}

// propertyIndexMissIsAuthoritative reports whether a property index lookup
// that finds no node for value proves that no node holds an equal value:
// strings, booleans and numbers are filed under keys that are equal exactly
// when the values are equal in Cypher (1 and 1.0 share a key).
func propertyIndexMissIsAuthoritative(value interface{}) bool {
	switch value.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func (e *StorageExecutor) lookupWhereCandidatesUsingPropertyIndex(nodeInfo nodePatternInfo, wherePart string, store storage.Engine) ([]*storage.Node, bool) {
	if len(nodeInfo.labels) == 0 {
		return nil, false
	}
	// Safety: don't index-narrow top-level OR expressions.
	// A single property index lookup can under-approximate OR semantics when one
	// branch is non-selective or when an index backend is non-unique for a value.
	// Fall back to normal filtering to preserve openCypher correctness.
	if findTopLevelKeyword(wherePart, " OR ") >= 0 {
		return nil, false
	}
	schema := store.GetSchema()
	if schema == nil {
		return nil, false
	}
	label := nodeInfo.labels[0]
	terms := splitTopLevelOrTerms(wherePart)
	if len(terms) == 0 {
		terms = []string{wherePart}
	}

	idSet := make(map[storage.NodeID]struct{}, 8)
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		prop, lit, ok := e.extractIndexedEqualityFromWhereTerm(nodeInfo.variable, term)
		if !ok {
			continue
		}
		ids := propertyIndexLookup(store, schema, label, prop, lit)
		for _, id := range ids {
			idSet[id] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return nil, false
	}
	out := make([]*storage.Node, 0, len(idSet))
	for id := range idSet {
		n, err := store.GetNode(id)
		if err == nil && n != nil {
			out = append(out, n)
		}
	}
	return out, true
}

func splitTopLevelOrTerms(expr string) []string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	orIdx := findTopLevelKeyword(expr, " OR ")
	if orIdx < 0 {
		return []string{expr}
	}
	left := strings.TrimSpace(expr[:orIdx])
	right := strings.TrimSpace(expr[orIdx+4:])
	out := make([]string, 0, 4)
	out = append(out, splitTopLevelOrTerms(left)...)
	out = append(out, splitTopLevelOrTerms(right)...)
	return out
}

func (e *StorageExecutor) extractIndexedEqualityFromWhereTerm(variable, term string) (string, interface{}, bool) {
	term = strings.TrimSpace(term)
	if term == "" {
		return "", nil, false
	}
	if strings.HasPrefix(term, "(") && strings.HasSuffix(term, ")") {
		inner := strings.TrimSpace(term[1 : len(term)-1])
		if inner != "" {
			term = inner
		}
	}

	if prop, lit, ok := e.parseVarPropEqualsLiteral(variable, term); ok {
		return prop, lit, true
	}

	parts := splitTopLevelAndConjuncts(term)
	if len(parts) < 2 {
		return "", nil, false
	}
	var (
		prop   string
		lit    interface{}
		haveEq bool
	)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if eqProp, eqLit, ok := e.parseVarPropEqualsLiteral(variable, p); ok {
			prop, lit, haveEq = eqProp, eqLit, true
			continue
		}
		if !e.isLiteralIsNotNullExpr(p) {
			return "", nil, false
		}
	}
	if haveEq {
		return prop, lit, true
	}
	return "", nil, false
}

func (e *StorageExecutor) parseVarPropEqualsLiteral(variable, expr string) (string, interface{}, bool) {
	expr = strings.TrimSpace(expr)
	if strings.Count(expr, "=") != 1 ||
		strings.Contains(expr, ">=") || strings.Contains(expr, "<=") ||
		strings.Contains(expr, "!=") || strings.Contains(expr, "<>") {
		return "", nil, false
	}
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return "", nil, false
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	prefix := variable + "."
	if !strings.HasPrefix(left, prefix) {
		return "", nil, false
	}
	prop := strings.TrimSpace(left[len(prefix):])
	if prop == "" {
		return "", nil, false
	}
	lit, ok := parseWhereLiteral(right)
	if !ok {
		return "", nil, false
	}
	return prop, lit, true
}

func (e *StorageExecutor) isLiteralIsNotNullExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	up := upperASCII(expr)
	needle := " IS NOT NULL"
	idx := strings.Index(up, needle)
	if idx <= 0 {
		return false
	}
	lit := strings.TrimSpace(expr[:idx])
	v, ok := parseWhereLiteral(lit)
	if !ok {
		return false
	}
	return v != nil
}

func parseWhereLiteral(token string) (interface{}, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, false
	}
	if strings.EqualFold(token, "null") {
		return nil, true
	}
	if strings.EqualFold(token, "true") {
		return true, true
	}
	if strings.EqualFold(token, "false") {
		return false, true
	}
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		s := token[1 : len(token)-1]
		s = strings.ReplaceAll(s, "''", "'")
		s = strings.ReplaceAll(s, "\\\\", "\\")
		return s, true
	}
	if i, err := strconv.ParseInt(token, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(token, 64); err == nil {
		return f, true
	}
	return nil, false
}

func lookupNodeMapProperty(nodeMap map[string]*storage.Node, expr string) (interface{}, bool) {
	parts := strings.SplitN(strings.TrimSpace(expr), ".", 2)
	if len(parts) != 2 {
		return nil, false
	}
	v := strings.TrimSpace(parts[0])
	p := strings.TrimSpace(parts[1])
	n, ok := nodeMap[v]
	if !ok || n == nil {
		return nil, false
	}
	val, exists := n.Properties[p]
	if !exists {
		return nil, false
	}
	return val, true
}

func normalizeWhereList(v interface{}) ([]interface{}, bool) {
	switch t := v.(type) {
	case []interface{}:
		return t, true
	case []string:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []int:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []int64:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	case []float64:
		out := make([]interface{}, 0, len(t))
		for _, x := range t {
			out = append(out, x)
		}
		return out, true
	default:
		return nil, false
	}
}

// extractVariableNamesFromPattern extracts variable names from a Cypher pattern.
// e.g., "(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)" returns ["p", "poc", "a"]
func (e *StorageExecutor) extractVariableNamesFromPattern(pattern string) []string {
	var varNames []string
	seen := make(map[string]bool)

	// Find all node patterns (...)
	inParen := false
	inBracket := false
	var current strings.Builder

	for _, c := range pattern {
		switch c {
		case '(':
			inParen = true
			current.Reset()
		case ')':
			if inParen {
				nodeContent := current.String()
				// Extract variable name (before : or end)
				varName, _, _ := splitNodeHead(nodeContent)
				// Remove any property part
				if idx := strings.Index(varName, "{"); idx > 0 {
					varName = strings.TrimSpace(varName[:idx])
				}
				if varName != "" && !seen[varName] {
					varNames = append(varNames, varName)
					seen[varName] = true
				}
			}
			inParen = false
		case '[':
			inBracket = true
		case ']':
			inBracket = false
		default:
			if inParen && !inBracket {
				current.WriteRune(c)
			}
		}
	}

	return varNames
}

// findNodeByProperties finds a node by matching its properties.
func (e *StorageExecutor) findNodeByProperties(props map[string]interface{}) *storage.Node {
	// Get all nodes and try to match
	allNodes := e.storage.GetAllNodes()
	for _, node := range allNodes {
		// Check if name property matches (common identifier)
		if name, ok := props["name"]; ok {
			if nodeName, ok := node.Properties["name"]; ok && nodeName == name {
				return node
			}
		}
		// Check if all provided properties match
		matches := true
		for k, v := range props {
			if k == "_labels" || k == "_id" {
				continue
			}
			if nodeVal, ok := node.Properties[k]; !ok || nodeVal != v {
				matches = false
				break
			}
		}
		if matches && len(props) > 0 {
			return node
		}
	}
	return nil
}

// buildCartesianProduct creates all combinations of node matches
func (e *StorageExecutor) buildCartesianProduct(patternMatches []struct {
	variable string
	nodes    []*storage.Node
}) []map[string]*storage.Node {
	if len(patternMatches) == 0 {
		return nil
	}

	// Start with first pattern's nodes
	var result []map[string]*storage.Node
	for _, node := range patternMatches[0].nodes {
		result = append(result, map[string]*storage.Node{
			patternMatches[0].variable: node,
		})
	}

	// For each subsequent pattern, expand the combinations
	for i := 1; i < len(patternMatches); i++ {
		pm := patternMatches[i]
		var expanded []map[string]*storage.Node

		for _, existing := range result {
			for _, node := range pm.nodes {
				// Copy existing map and add new variable
				newMap := make(map[string]*storage.Node)
				for k, v := range existing {
					newMap[k] = v
				}
				newMap[pm.variable] = node
				expanded = append(expanded, newMap)
			}
		}

		result = expanded
	}

	return result
}

// mergeNodeAbsentKey marks a context whose caller has just scanned the
// MERGE node pattern's whole label and found no matching node
// (pipelineApplyMerge, findMergeNodesScanned): the MERGE creates the node
// without scanning the label a second time (#823).
type mergeNodeAbsentKey struct{}

func withMergeNodeAbsent(ctx context.Context) context.Context {
	return context.WithValue(ctx, mergeNodeAbsentKey{}, true)
}

// executeMergeWithContext runs one MERGE pattern, the text "MERGE pattern"
// without ON CREATE / ON MATCH actions (pipelineApplyMerge splits them off and
// applies them), with the row's bound nodes and relationships: it binds what
// it matched or created into nodeContext / relContext. Its stats report what
// it created, which tells the caller whether ON CREATE or ON MATCH applies.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	// The caller's finding applies to this MERGE's node pattern only, not to
	// the clauses that follow it.
	nodeKnownAbsent, _ := ctx.Value(mergeNodeAbsentKey{}).(bool)
	if nodeKnownAbsent {
		ctx = context.WithValue(ctx, mergeNodeAbsentKey{}, false)
	}
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	mergePattern := strings.TrimSpace(cypher)
	if startsWithKeywordFold(mergePattern, "MERGE") {
		mergePattern = strings.TrimSpace(mergePattern[len("MERGE"):])
	}

	// Check if this is a relationship pattern: (a)-[r:TYPE]->(b)
	if strings.Contains(mergePattern, "->") || strings.Contains(mergePattern, "<-") || strings.Contains(mergePattern, "]-") {
		return e.executeMergeRelationshipWithContext(ctx, mergePattern, nodeContext, relContext)
	}

	// Parse node pattern
	varName, labels, matchProps, err := e.parseMergeNodePattern(ctx, mergePattern, nodeContext, relContext)
	if err != nil {
		return nil, &classifiedCypherError{
			cause:  err,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
	if err := validateMergePatternProperties(matchProps, "node"); err != nil {
		return nil, err
	}

	// Try to find existing node
	var existingNode *storage.Node
	if candidate := nodeContext[varName]; candidate != nil && mergeNodeMatches(candidate, labels, matchProps) {
		existingNode = candidate
	}
	if existingNode == nil && !nodeKnownAbsent {
		if err := prepareMergeKeys(ctx, store, labels, matchProps); err != nil {
			return nil, err
		}
		existingNode, err = e.findMergeNode(store, labels, matchProps)
		if err != nil {
			return nil, err
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
	} else {
		node = &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		if err := validatePropertyValues(node.Properties); err != nil {
			return nil, err
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			if !mergeCreateConflict(err) {
				return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
			}
			// A concurrent MERGE created it first: this MERGE matched it.
			recoveredNode, findErr := e.findMergeNode(store, labels, matchProps)
			if findErr != nil {
				return nil, findErr
			}
			if recoveredNode == nil {
				return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
			}
			node = recoveredNode
		} else {
			node.ID = actualID
			e.notifyNodeMutated(string(node.ID))
			result.Stats.NodesCreated = 1
			countCreatedEntity(result.Stats, node.Labels, node.Properties)
		}
	}
	e.cacheMergeNode(labels, matchProps, node)

	// Anonymous pattern nodes do not introduce a variable into scope.
	if varName != "" {
		nodeContext[varName] = node
	}
	return result, nil
}

// parseMergeProperties parses a MERGE pattern's property map text
// ("{k: v, …}") with the statement's bound nodes and relationships. A value
// the parser resolves (a literal, a parameter, an evaluated expression) is
// that value: a string literal is a string, whatever it reads. An expression
// the parser leaves as its own text and that reads a bound variable's
// property (a.id) is evaluated against the bound entities. Null values remain
// in the map so the shared MERGE validator rejects them before matching.
func (e *StorageExecutor) parseMergeProperties(ctx context.Context, propsText string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) map[string]interface{} {
	props := make(map[string]interface{})
	propsText = strings.TrimSpace(propsText)
	if strings.HasPrefix(propsText, "{") && strings.HasSuffix(propsText, "}") {
		propsText = propsText[1 : len(propsText)-1]
	}
	if strings.TrimSpace(propsText) == "" {
		return props
	}
	for _, pair := range e.splitPropertyPairs(propsText) {
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			continue
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:colon]))
		valueText := strings.TrimSpace(pair[colon+1:])
		value := e.parsePropertyValue(ctx, valueText)
		if text, unresolved := value.(string); unresolved && text == valueText && !isWholeCypherQuotedString(valueText) {
			if dot := strings.Index(text, "."); dot > 0 {
				variable := strings.TrimSpace(text[:dot])
				_, boundNode := nodeContext[variable]
				_, boundRel := relContext[variable]
				_, boundValue := valueBindingsFromContext(ctx)[variable]
				if boundNode || boundRel || boundValue {
					value = e.evaluateExpressionWithContext(ctx, text, nodeContext, relContext)
				}
			}
		}
		props[key] = value
	}
	return props
}

// parseMergeNodePattern is parseMergePattern with the property map read by
// parseMergeProperties.
func (e *StorageExecutor) parseMergeNodePattern(ctx context.Context, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (string, []string, map[string]interface{}, error) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", nil, nil, localizedError(localization.CypherResidualMergePatternInvalid(pattern), nil)
	}
	head, props := splitNodePatternProperties(pattern)
	variable, labels, labelErr := parseNodeHead(head)
	if labelErr != nil {
		return "", nil, nil, labelErr
	}
	return variable, labels, e.parseMergeProperties(ctx, props, nodeContext, relContext), nil
}

// executeMergeRelationshipWithContext runs a relationship MERGE pattern (see
// executeMergeWithContext): it binds the relationship it matched or created,
// and the endpoints, into relContext / nodeContext.
func (e *StorageExecutor) executeMergeRelationshipWithContext(ctx context.Context, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	parsedPattern, err := e.parseMergeRelationshipPattern(ctx, pattern, nodeContext, relContext)
	if err != nil {
		return nil, err
	}
	if err := validateMergePatternProperties(parsedPattern.startNodePattern.properties, "node"); err != nil {
		return nil, err
	}
	if err := validateMergePatternProperties(parsedPattern.endNodePattern.properties, "node"); err != nil {
		return nil, err
	}
	if err := validateMergePatternProperties(parsedPattern.properties, "relationship"); err != nil {
		return nil, err
	}

	// Get start and end nodes from context
	startNode := nodeContext[parsedPattern.startVariable]
	endNode := nodeContext[parsedPattern.endVariable]

	// Whole-pattern MERGE semantics (#640): when neither endpoint is bound by
	// an earlier clause, an existing relationship whose endpoints satisfy both
	// node patterns is the match; when none exists, every endpoint node is
	// created fresh — even if a node matching its pattern already exists.
	// Neo4j does not reuse such a node. The one-bound and both-bound forms
	// keep their get-or-create semantics below (the issue confirms those
	// already agree with Neo4j).
	var existingEdge *storage.Edge
	if startNode == nil && endNode == nil ||
		startNode == nil && len(parsedPattern.startNodePattern.labels) == 0 && len(parsedPattern.startNodePattern.properties) == 0 ||
		endNode == nil && len(parsedPattern.endNodePattern.labels) == 0 && len(parsedPattern.endNodePattern.properties) == 0 {
		// Self-referencing pattern (a)-[:R]->(a): the end is the same node as
		// the start; resolve it with get-or-create semantics.
		if parsedPattern.startVariable != "" && parsedPattern.startVariable == parsedPattern.endVariable {
			var created bool
			startNode, created, err = e.resolveMergeRelationshipEndpoint(ctx, store, parsedPattern.startNodePattern)
			if err != nil {
				return nil, err
			}
			if startNode == nil {
				return nil, localizedError(localization.CypherMergeStartVariableNotBound(parsedPattern.startVariable, getKeys(nodeContext)), nil)
			}
			if created {
				result.Stats.NodesCreated++
				countCreatedEntity(result.Stats, startNode.Labels, startNode.Properties)
			}
			endNode = startNode
			if parsedPattern.startVariable != "" {
				nodeContext[parsedPattern.startVariable] = startNode
			}
		} else {
			startPattern := parsedPattern.startNodePattern
			endPattern := parsedPattern.endNodePattern
			startCandidates := []*storage.Node{startNode}
			if startNode == nil {
				if err := prepareMergeKeys(ctx, store, startPattern.labels, startPattern.properties); err != nil {
					return nil, err
				}
				startCandidates, err = e.findMergeNodes(store, startPattern.labels, startPattern.properties)
				if err != nil {
					return nil, err
				}
			}
			endCandidates := []*storage.Node{endNode}
			if endNode == nil {
				if err := prepareMergeKeys(ctx, store, endPattern.labels, endPattern.properties); err != nil {
					return nil, err
				}
				endCandidates, err = e.findMergeNodes(store, endPattern.labels, endPattern.properties)
				if err != nil {
					return nil, err
				}
			}
		search:
			for _, candidateStart := range startCandidates {
				for _, candidateEnd := range endCandidates {
					if candidateStart == nil || candidateEnd == nil {
						continue
					}
					// Distinct endpoint variables may bind the same node: an
					// existing self-loop whose endpoints satisfy both node
					// patterns is a valid whole-pattern match, so a same-ID
					// pair must still be searched (#640 whole-pattern
					// semantics).
					var found *storage.Edge
					switch parsedPattern.direction {
					case mergeRelationshipIncoming:
						found, err = findRelationshipForMerge(store, candidateEnd.ID, candidateStart.ID, parsedPattern.relType, parsedPattern.properties)
					default:
						found, err = findRelationshipForMerge(store, candidateStart.ID, candidateEnd.ID, parsedPattern.relType, parsedPattern.properties)
						if found == nil && err == nil && parsedPattern.direction == mergeRelationshipUndirected {
							found, err = findRelationshipForMerge(store, candidateEnd.ID, candidateStart.ID, parsedPattern.relType, parsedPattern.properties)
						}
					}
					if err != nil {
						return nil, localizedError(localization.CypherMergeFindRelationshipFailed(err), err)
					}
					if found != nil {
						existingEdge = found
						startNode, endNode = candidateStart, candidateEnd
						if parsedPattern.startVariable != "" {
							nodeContext[parsedPattern.startVariable] = startNode
						}
						if parsedPattern.endVariable != "" {
							nodeContext[parsedPattern.endVariable] = endNode
						}
						break search
					}
				}
			}
			if existingEdge == nil {
				// Create the whole pattern fresh.
				if startNode == nil {
					startNode, err = e.createMergeRelationshipEndpointNode(store, startPattern)
					if err != nil {
						return nil, err
					}
					result.Stats.NodesCreated++
					countCreatedEntity(result.Stats, startNode.Labels, startNode.Properties)
					if parsedPattern.startVariable != "" {
						nodeContext[parsedPattern.startVariable] = startNode
					}
				}
				if endNode == nil {
					endNode, err = e.createMergeRelationshipEndpointNode(store, endPattern)
					if err != nil {
						return nil, err
					}
					result.Stats.NodesCreated++
					countCreatedEntity(result.Stats, endNode.Labels, endNode.Properties)
					if parsedPattern.endVariable != "" {
						nodeContext[parsedPattern.endVariable] = endNode
					}
				}
			}
		}
	}

	if startNode == nil {
		var created bool
		startNode, created, err = e.resolveMergeRelationshipEndpoint(ctx, store, parsedPattern.startNodePattern)
		if err != nil {
			return nil, err
		}
		if startNode == nil {
			return nil, localizedError(localization.CypherMergeStartVariableNotBound(parsedPattern.startVariable, getKeys(nodeContext)), nil)
		}
		if created {
			result.Stats.NodesCreated++
			countCreatedEntity(result.Stats, startNode.Labels, startNode.Properties)
		}
		if parsedPattern.startVariable != "" {
			nodeContext[parsedPattern.startVariable] = startNode
		}
	}
	if endNode == nil && parsedPattern.endVariable != "" {
		endNode = nodeContext[parsedPattern.endVariable]
	}
	if endNode == nil {
		var created bool
		endNode, created, err = e.resolveMergeRelationshipEndpoint(ctx, store, parsedPattern.endNodePattern)
		if err != nil {
			return nil, err
		}
		if endNode == nil {
			return nil, localizedError(localization.CypherMergeEndVariableNotBound(parsedPattern.endVariable, getKeys(nodeContext)), nil)
		}
		if created {
			result.Stats.NodesCreated++
			countCreatedEntity(result.Stats, endNode.Labels, endNode.Properties)
		}
		if parsedPattern.endVariable != "" {
			nodeContext[parsedPattern.endVariable] = endNode
		}
	}
	mergeStartNode, mergeEndNode := startNode, endNode
	if parsedPattern.direction == mergeRelationshipIncoming {
		mergeStartNode, mergeEndNode = endNode, startNode
	}
	// Cypher relationship properties inside the MERGE pattern are identity
	// fields. Scan the bounded endpoint pair so same-type relationships with
	// different property identities remain distinct.
	if existingEdge == nil {
		existingEdge, err = findRelationshipForMerge(store, mergeStartNode.ID, mergeEndNode.ID, parsedPattern.relType, parsedPattern.properties)
		if err != nil {
			return nil, localizedError(localization.CypherMergeFindRelationshipFailed(err), err)
		}
		if existingEdge == nil && parsedPattern.direction == mergeRelationshipUndirected {
			existingEdge, err = findRelationshipForMerge(store, mergeEndNode.ID, mergeStartNode.ID, parsedPattern.relType, parsedPattern.properties)
			if err != nil {
				return nil, localizedError(localization.CypherMergeFindRelationshipFailed(err), err)
			}
		}
	}

	var edge *storage.Edge
	if existingEdge != nil {
		edge = existingEdge
	} else {
		// Create new relationship
		edge = &storage.Edge{
			ID:         e.newRelationshipMergeEdgeID(mergeStartNode.ID, mergeEndNode.ID, parsedPattern.relType, parsedPattern.properties),
			Type:       parsedPattern.relType,
			StartNode:  mergeStartNode.ID,
			EndNode:    mergeEndNode.ID,
			Properties: parsedPattern.properties,
		}
		createdEdge, created, createErr := createRelationshipForMerge(e, store, edge, parsedPattern.properties)
		if createErr != nil {
			return nil, localizedError(localization.CypherMergeCreateRelationshipFailed(createErr), createErr)
		}
		edge = createdEdge
		if created {
			result.Stats.RelationshipsCreated = 1
			countCreatedEntity(result.Stats, nil, edge.Properties)
			e.notifyEdgeMutated(string(edge.ID))
			e.notifyNodeMutated(string(edge.StartNode))
			if edge.EndNode != edge.StartNode {
				e.notifyNodeMutated(string(edge.EndNode))
			}
		}
	}

	// Store in context
	if parsedPattern.relVariable != "" {
		relContext[parsedPattern.relVariable] = edge
	}
	return result, nil
}

// createMergeRelationshipEndpointNode creates a fresh node for a whole-pattern
// MERGE relationship (#640): when no relationship matching the whole pattern
// exists, every unbound endpoint node is created even if a node matching its
// pattern already exists.
func (e *StorageExecutor) createMergeRelationshipEndpointNode(store storage.Engine, pattern nodePatternInfo) (*storage.Node, error) {
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     pattern.labels,
		Properties: pattern.properties,
	}
	if err := validatePropertyValues(node.Properties); err != nil {
		return nil, err
	}
	actualID, err := store.CreateNode(node)
	if err != nil {
		return nil, localizedError(localization.CypherMergeCreateNodeFailed(err), err)
	}
	node.ID = actualID
	e.notifyNodeMutated(string(node.ID))
	e.cacheMergeNode(pattern.labels, pattern.properties, node)
	return node, nil
}

// resolveMergeRelationshipEndpoint resolves a MERGE relationship endpoint with
// get-or-create semantics, used when at least one endpoint is already bound
// (the forms the issue confirms match Neo4j).
func (e *StorageExecutor) resolveMergeRelationshipEndpoint(ctx context.Context, store storage.Engine, pattern nodePatternInfo) (*storage.Node, bool, error) {
	if len(pattern.labels) == 0 && len(pattern.properties) == 0 {
		return nil, false, nil
	}
	if err := prepareMergeKeys(ctx, store, pattern.labels, pattern.properties); err != nil {
		return nil, false, err
	}

	node, err := e.findMergeNode(store, pattern.labels, pattern.properties)
	if err != nil || node != nil {
		return node, false, err
	}

	// Same creation path as the whole-pattern branch (#640): one helper for
	// every MERGE node creation (validate, CreateNode, notify, cache).
	node, err = e.createMergeRelationshipEndpointNode(store, pattern)
	if err != nil {
		if !mergeCreateConflict(err) {
			return nil, false, err
		}
		// A concurrent MERGE created the node between the lookup and this
		// create: re-find it instead of failing.
		recovered, findErr := e.findMergeNode(store, pattern.labels, pattern.properties)
		if findErr != nil {
			return nil, false, findErr
		}
		if recovered == nil {
			return nil, false, err
		}
		return recovered, false, nil
	}
	return node, true, nil
}

// applySetToRelationshipWithContext is the single per-relationship SET
// applicator (pipeline, MERGE, CREATE ... SET). It applies every assignment in
// setClause that targets varName: r = map / entity (replace), r += map / entity
// (merge) and r.p = value. A null value removes the key. setClause may chain
// several SET clauses (a SET b). It returns the number of properties written
// by Neo4j's properties_set rule (setWrites), or an error for a value Cypher
// cannot store (replacing with a non-map, a map or entity as a property value).
func (e *StorageExecutor) applySetToRelationshipWithContext(ctx context.Context, edge *storage.Edge, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (int, error) {
	if edge == nil || varName == "" {
		return 0, nil
	}
	fullRelContext := make(map[string]*storage.Edge)
	for k, v := range relContext {
		fullRelContext[k] = v
	}
	fullRelContext[varName] = edge

	var writes setWrites
	var run setPropertyRun
	for segment, next, ok := nextChainedSetClause(setClause, 0); ok; segment, next, ok = nextChainedSetClause(setClause, next) {
		run.applyToRelationship(edge, &writes)
		writes.endRun()
		for _, assignment := range splitSetAssignments(segment) {
			assignment = strings.TrimSpace(assignment)
			if assignment == "" {
				continue
			}

			target, propName, operator, right := splitSetAssignment(assignment)
			if target != varName || propName == "" {
				run.applyToRelationship(edge, &writes)
				writes.endRun()
			}
			if target != varName {
				continue
			}
			switch {
			case operator == "=" && propName == "":
				props, err := e.setSourceMap(ctx, right, "=", nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				// A source without properties (an empty node) clears them.
				writes.mapEntries(edge.Properties, props, true)
				edge.Properties = setPropertyMap(props)
			case operator == "+=":
				props, err := e.setSourceMap(ctx, right, "+=", nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				writes.mapEntries(edge.Properties, props, false)
				for k, v := range props {
					setRelationshipProperty(edge, k, v)
				}
			case operator == "=":
				// Direct $param resolution (in setPropertyValue) preserves declared
				// types (e.g. []string, []float64) end-to-end. The run's values
				// are written together (setPropertyRun).
				value, err := e.setPropertyValue(ctx, right, nodeContext, fullRelContext)
				if err != nil {
					return writes.count, err
				}
				run.add(propName, value)
			}
		}
	}
	run.applyToRelationship(edge, &writes)
	return writes.count, nil
}

// applySetToNodeWithContext is the single per-node SET applicator used by
// every SET route (MATCH / pipeline, MERGE in all its forms, CREATE ... SET).
// It applies every assignment in setClause that targets varName: n = map /
// entity (replace), n += map / entity (merge), n.p = value and n:L1:L2 labels.
// A null value removes the key. setClause may chain several SET clauses
// (a SET b). It returns the number of properties written by Neo4j's
// properties_set rule (setWrites), or an error for a value Cypher cannot
// store (replacing with a non-map, a map or entity as a property value);
// assignments are validated statically by validatePipelineSetAssignments.
func (e *StorageExecutor) applySetToNodeWithContext(ctx context.Context, node *storage.Node, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (int, error) {
	// Add current node to context for self-references
	fullContext := make(map[string]*storage.Node)
	for k, v := range nodeContext {
		fullContext[k] = v
	}
	fullContext[varName] = node

	var writes setWrites
	var run setPropertyRun
	for segment, next, ok := nextChainedSetClause(setClause, 0); ok; segment, next, ok = nextChainedSetClause(setClause, next) {
		writes.endRun()
		if err := e.applyNodeSetClause(ctx, node, varName, segment, fullContext, relContext, &writes, &run); err != nil {
			return writes.count, err
		}
	}
	return writes.count, nil
}

// applyNodeSetClause applies the assignments of one SET clause that target
// varName to node (applySetToNodeWithContext), recording what they write.
func (e *StorageExecutor) applyNodeSetClause(ctx context.Context, node *storage.Node, varName string, setClause string, fullContext map[string]*storage.Node, relContext map[string]*storage.Edge, writes *setWrites, run *setPropertyRun) error {
	defer run.applyToNode(node, writes)
	for _, assignment := range splitSetAssignments(setClause) {
		assignment = strings.TrimSpace(assignment)

		target, propName, operator, right := splitSetAssignment(assignment)
		if target != varName || propName == "" {
			run.applyToNode(node, writes)
			writes.endRun()
		}
		if target != varName {
			continue
		}
		switch {
		case operator == "+=":
			if err := e.applySetMapMergeToNode(ctx, node, varName, right, fullContext, relContext, writes); err != nil {
				return err
			}
		case operator == "=" && propName == "":
			props, err := e.setSourceMap(ctx, right, "=", fullContext, relContext)
			if err != nil {
				return err
			}
			// A source without properties (an empty node) clears them.
			writes.mapEntries(node.Properties, props, true)
			node.Properties = setPropertyMap(props)
		case operator == ":":
			labelExpr := right
			if labelExpr == "" {
				continue
			}
			if strings.HasPrefix(labelExpr, "$(") && strings.HasSuffix(labelExpr, ")") {
				innerExpr := strings.TrimSpace(labelExpr[2 : len(labelExpr)-1])
				labelValue, ok := resolveContextPathRef(ctx, innerExpr)
				if !ok {
					labelValue = e.evaluateExpressionWithContext(ctx, innerExpr, fullContext, relContext)
				}
				labels := toStringSlice(labelValue)
				if len(labels) == 0 {
					labels = toStringSlice(e.parseValue(ctx, innerExpr))
				}
				for _, label := range labels {
					if label == "" || !isValidIdentifier(label) || containsReservedKeyword(label) || containsString(node.Labels, label) {
						continue
					}
					node.Labels = append(node.Labels, label)
				}
				continue
			}
			labels, err := setLabelChain(labelExpr)
			if err != nil {
				continue // rejected by validatePipelineSetAssignments before execution
			}
			for _, label := range labels {
				if !containsString(node.Labels, label) {
					node.Labels = append(node.Labels, label)
				}
			}
		case operator == "=":
			// Direct $param resolution (in setPropertyValue) preserves declared
			// types end-to-end. The run's values are written together
			// (setPropertyRun).
			value, err := e.setPropertyValue(ctx, right, fullContext, relContext)
			if err != nil {
				return err
			}
			run.add(propName, value)
		}
	}
	return nil
}

// setPropertyValue evaluates the right-hand side of SET x.p = <expr>. A direct
// $param keeps its declared Go type. A map, entity or nested list is rejected
// with a TypeError, as in Neo4j.
func (e *StorageExecutor) setPropertyValue(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, error) {
	if err := requireSetParameter(ctx, expr); err != nil {
		return nil, err
	}
	var value interface{}
	if v, ok := resolveDirectParamRef(ctx, expr); ok {
		value = normalizePropValue(v)
	} else {
		value = e.evaluateSetExpressionWithContext(ctx, expr, nodes, rels)
	}
	if err := validateSetPropertyValue(value); err != nil {
		return nil, err
	}
	return value, nil
}

// setSourceMap evaluates the right-hand side of SET x = <expr> or
// SET x += <expr> (operator): a map, or a node / relationship whose
// properties are copied. Any other value, null included, is a TypeError
// (setPropertyMapValue, #907), as is a map holding a value a property cannot
// store. An inline literal the evaluator hands back unresolved, as its own
// text, goes through the literal parser. Both operators read their source
// the same way; every SET route (pipeline, MERGE actions, FOREACH, the UNWIND
// batch routes) calls this.
func (e *StorageExecutor) setSourceMap(ctx context.Context, expr, operator string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (map[string]interface{}, error) {
	if err := requireSetParameter(ctx, expr); err != nil {
		return nil, err
	}
	value, ok := resolveDirectParamRef(ctx, expr)
	if !ok {
		value, ok = resolveContextPathRef(ctx, expr)
	}
	if !ok {
		value = e.evaluateSetExpressionWithContext(ctx, expr, nodes, rels)
		if text, isText := value.(string); isText && strings.TrimSpace(text) == strings.TrimSpace(expr) {
			value = e.parseValue(ctx, strings.TrimSpace(expr))
		}
	}
	return setPropertyMapValue(value, operator)
}

// splitSetAssignment splits one SET assignment into its target variable,
// property (for x.p = v), operator ("=", "+=" or ":" for labels) and
// right-hand side. The operator is the first top-level "=" (preceded by "+"
// for +=), so "=" or "+=" inside a value string does not change the form.
// Targets may be parenthesized: SET (n).p = v. Unrecognized text yields an
// empty operator.
func splitSetAssignment(assignment string) (target, property, operator, right string) {
	assignment = strings.TrimSpace(assignment)
	eq := strings.IndexByte(assignment, '=')
	if colon := strings.IndexByte(assignment, ':'); colon > 0 && (eq < 0 || colon < eq) {
		// Label form x:L1:L2 / x:$(expr); a dynamic label expression may
		// itself contain "=".
		if target := strings.TrimSpace(assignment[:colon]); isValidIdentifier(target) {
			return target, "", ":", strings.TrimSpace(assignment[colon+1:])
		}
	}
	if eq < 0 {
		return "", "", "", ""
	}
	left := assignment[:eq]
	operator = "="
	if eq > 0 && assignment[eq-1] == '+' {
		left = assignment[:eq-1]
		operator = "+="
	}
	target, property, hasProperty := parseSetAssignmentTarget(left)
	if operator == "+=" && hasProperty {
		return "", "", "", ""
	}
	return target, property, operator, strings.TrimSpace(assignment[eq+1:])
}

// requireSetParameter reports a SET value that is a bare $name parameter not
// supplied with the query, as Neo4j does, instead of letting the unresolved
// "$name" text be stored. Other expressions pass through unchanged.
func requireSetParameter(ctx context.Context, expr string) error {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "$") {
		return nil
	}
	name := expr[1:]
	if !isValidIdentifier(name) {
		return nil
	}
	params := getParamsFromContext(ctx)
	if len(params) == 0 {
		return localizedError(localization.CypherMutationsSetAssignmentParametersRequired(name), nil)
	}
	if _, ok := params[name]; !ok {
		return localizedError(localization.CypherMutationsSetAssignmentParameterNotFound(name), nil)
	}
	return nil
}

// setPropertyMapValue converts a SET x = / x += source value into the property
// map to write: maps as-is, nodes and relationships by their properties
// (propertyMapForSetValue). Anything else is a TypeError, null included (Neo4j
// 5.26: "Expected Null() to be a map", #907), as is a map holding a value a
// property cannot store.
func setPropertyMapValue(value interface{}, operator string) (map[string]interface{}, error) {
	typeName := "NULL"
	if value != nil {
		typeName = cypherTypeName(value)
	}
	props, ok := propertyMapForSetValue(value)
	if value == nil || !ok {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherMutationsSetSourceNotMap(operator, typeName))
	}
	for _, v := range props {
		if err := validateSetPropertyValue(v); err != nil {
			return nil, err
		}
	}
	return props, nil
}

// evaluateSetExpressionWithContext evaluates SET clause expressions with context.
func (e *StorageExecutor) evaluateSetExpressionWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
}

// mergeBindingRow is the row a MERGE's RETURN reads: the values bound in
// ctx (fabric record and child-context bindings, such as an UNWIND variable)
// and the MERGE's node and relationship bindings.
func (e *StorageExecutor) mergeBindingRow(ctx context.Context, nodes map[string]*storage.Node, rels map[string]*storage.Edge) pipelineRow {
	bindings := valueBindingsFromContext(ctx)
	row := make(pipelineRow, len(e.fabricRecordBindings)+len(bindings)+len(nodes)+len(rels))
	for name, value := range e.fabricRecordBindings {
		row[name] = value
	}
	for name, value := range bindings {
		row[name] = value
	}
	// An unbound variable (an OPTIONAL MATCH without a match) is null.
	for name, node := range nodes {
		if name == "" {
			continue
		}
		if node == nil {
			row[name] = nil
		} else {
			row[name] = node
		}
	}
	for name, edge := range rels {
		if name == "" {
			continue
		}
		if edge == nil {
			row[name] = nil
		} else {
			row[name] = edge
		}
	}
	return row
}

// projectMergeReturn projects a MERGE statement's binding rows through the
// shared RETURN projection (pipelineApplyReturn), so DISTINCT, aggregation,
// ORDER BY, SKIP and LIMIT apply over all the statement's rows (#640).
// returnClause starts with RETURN.
func (e *StorageExecutor) projectMergeReturn(ctx context.Context, rows []pipelineRow, returnClause string) (*ExecuteResult, error) {
	return e.projectMergeReturnSource(ctx, rows, returnClause, pipelineRowsSource(rows))
}

func (e *StorageExecutor) projectMergeReturnSource(ctx context.Context, rows []pipelineRow, returnClause string, source pipelineRowSource, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, error) {
	returnClause = strings.TrimSpace(returnClause)
	return e.projectMergeReturnPlan(ctx, rows, returnClause, returnProjectionPlanFor(returnClause), source, preparedGroups...)
}

func (e *StorageExecutor) projectMergeReturnPlan(ctx context.Context, rows []pipelineRow, returnClause string, plan *returnProjectionPlan, source pipelineRowSource, preparedGroups ...[]*pipelineAggregateGroup) (*ExecuteResult, error) {
	priorFailure := getExpressionFailure(ctx)
	result, handled := e.pipelineApplyReturnPlan(ctx, rows, plan, source, false, preparedGroups...)
	if failure := getExpressionFailure(ctx); failure != nil && (!handled || priorFailure == nil) {
		return nil, failure
	}
	if handled {
		return result, nil
	}
	err := newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnexpectedSyntax",
		"could not parse RETURN expression: "+strings.TrimSpace(returnClause[len("RETURN"):]),
	)
	recordExpressionFailure(ctx, err)
	return nil, err
}
