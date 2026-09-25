// CREATE clause implementation for NornicDB.
// This file contains CREATE execution for nodes and relationships.

package cypher

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// createOutcome is what one CREATE pattern produced.
type createOutcome struct {
	result    *ExecuteResult
	nodes     map[string]*storage.Node
	edges     map[string]*storage.Edge
	paths     map[string]PathResult
	cypher    string // the statement after parameter substitution
	returnIdx int    // index of RETURN in cypher, or -1
}

// createFromPattern is the single CREATE pattern executor: parameter
// substitution, validation (empty / invalid / reserved labels, property keys
// and values, relationship types), property references to variables created
// earlier in the same pattern, relationship chains and named paths (p = ...).
// Every CREATE route that creates from pattern text - CREATE (executeCreate),
// CREATE ... SET, CREATE ... WITH ... DELETE and the pipeline's CREATE step
// (executeCreateWithRefs) - uses it, so they create the same graph and reject
// the same patterns.
func (e *StorageExecutor) createFromPattern(ctx context.Context, cypher string) (*createOutcome, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse CREATE pattern
	pattern := cypher[6:] // Skip "CREATE"

	// Use word boundary detection to avoid matching substrings
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx > 0 {
		pattern = cypher[6:returnIdx]
	}
	pattern = strings.TrimSpace(pattern)

	createdNodes := make(map[string]*storage.Node)
	createdEdges := make(map[string]*storage.Edge)
	createdPaths, err := e.createPatternsInScope(ctx, pattern, createdNodes, createdEdges, result)
	if err != nil {
		return nil, err
	}
	return &createOutcome{
		result:    result,
		nodes:     createdNodes,
		edges:     createdEdges,
		paths:     createdPaths,
		cypher:    cypher,
		returnIdx: returnIdx,
	}, nil
}

// createPlan is what one CREATE clause writes: every node and relationship,
// fully parsed, evaluated and validated, in creation order.
type createPlan struct {
	nodes []*storage.Node
	edges []*storage.Edge
}

// createPlanPool recycles plans so planning a CREATE allocates nothing in
// steady state. Storage bulk writes don't retain the slices they are given.
var createPlanPool = sync.Pool{New: func() any { return &createPlan{} }}

func acquireCreatePlan() *createPlan {
	return createPlanPool.Get().(*createPlan)
}

// release clears the plan's references and returns it to the pool; plans
// that grew unusually large are dropped instead of being kept alive.
func (p *createPlan) release() {
	if cap(p.nodes) > 256 || cap(p.edges) > 256 {
		return
	}
	clear(p.nodes)
	clear(p.edges)
	p.nodes = p.nodes[:0]
	p.edges = p.edges[:0]
	createPlanPool.Put(p)
}

// createPatternsInScope is the single CREATE executor for pattern text (the
// part after CREATE, comma-separated patterns). nodes and edges hold the
// variables already in scope (from MATCH, WITH, UNWIND or an earlier CREATE);
// relationship endpoints that name a bound variable reuse it, and every
// created node / relationship is bound into them. Nodes - standalone and
// inline endpoints - go through planCreateNode, so every route validates
// labels and properties and resolves property references the same way. Stats
// go to result. It returns the named paths (p = ...).
//
// The clause is planned completely (planCreatePatterns) before anything is
// written: a property expression that fails (1 / 0, recorded in ctx), a
// rejected property value or pattern anywhere in the clause returns the error
// with nothing written. The plan is then written by applyCreatePlan with one
// bulk node write and one bulk relationship write, so a storage-level
// rejection (a unique constraint) also writes nothing. This keeps CREATE
// atomic on routes that write without a transaction (the async auto-commit
// route, #628) as well as inside one.
func (e *StorageExecutor) createPatternsInScope(ctx context.Context, pattern string, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, result *ExecuteResult) (map[string]PathResult, error) {
	plan := acquireCreatePlan()
	defer plan.release()
	createdPaths, err := e.planCreatePatterns(ctx, pattern, createdNodes, createdEdges, plan)
	if err != nil {
		return nil, err
	}
	if err := e.applyCreatePlan(ctx, plan, result); err != nil {
		return nil, err
	}
	return createdPaths, nil
}

// planCreatePatterns plans one CREATE clause into plan without writing:
// every node and relationship is parsed, evaluated and validated, and bound
// into createdNodes / createdEdges for later patterns and clauses. A
// statement of several CREATE clauses plans them all into one plan and
// writes it once (executeMultipleCreates), so the statement is atomic.
func (e *StorageExecutor) planCreatePatterns(ctx context.Context, pattern string, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, plan *createPlan) (map[string]PathResult, error) {

	// Split into individual patterns (nodes and relationships)
	allPatterns := e.splitCreatePatterns(pattern)

	// Separate node patterns from relationship patterns
	var nodePatterns []string
	var relPatterns []string
	for _, p := range allPatterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Use string-literal-aware checks to avoid matching arrows inside content strings
		// e.g., 'Data -> Output' should NOT be treated as a relationship
		if patternHasRelationship(p) {
			relPatterns = append(relPatterns, p)
		} else {
			nodePatterns = append(nodePatterns, p)
		}
	}

	// First, create all nodes
	createdPaths := make(map[string]PathResult)
	for _, nodePatternStr := range nodePatterns {
		nodePatternStr = strings.TrimSpace(nodePatternStr)
		if nodePatternStr == "" {
			continue
		}
		if _, err := e.planCreateNode(ctx, nodePatternStr, createdNodes, createdEdges, plan); err != nil {
			return nil, err
		}
	}

	// Then, create all relationships using variable references or inline node definitions
	for _, relPatternStr := range relPatterns {
		relPatternStr = strings.TrimSpace(relPatternStr)
		if relPatternStr == "" {
			continue
		}

		// Process relationship chains - keep going until no remainder
		pathVar, currentPattern := parseCreatePathAssignment(relPatternStr)
		var pathNodes []*storage.Node
		var pathEdges []*storage.Edge
		var chainedSourceNode *storage.Node
		for currentPattern != "" {
			// Parse the relationship pattern: (varA)-[:TYPE {props}]->(varB)
			sourceContent, relStr, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(currentPattern)
			if err != nil {
				return nil, err
			}

			// The relationship is parsed and validated before its endpoints
			// are created, so a rejected pattern writes nothing.
			// Parse relationship type and properties
			relType, relProps := e.parseRelationshipTypeAndProps(ctx, relStr)

			// Extract relationship variable if present (e.g., "r:TYPE" -> "r").
			relVar := ""
			if colonIdx := strings.Index(relStr, ":"); colonIdx > 0 {
				relVar = strings.TrimSpace(relStr[:colonIdx])
			} else if !strings.Contains(relStr, "{") {
				// No colon and no props - entire string might be variable
				relVar = strings.TrimSpace(relStr)
			}

			// CREATE needs exactly one relationship type.
			if relType == "" {
				return nil, localizedError(localization.CypherMergeRelationshipTypeRequired(), nil)
			}
			// SECURITY: Validate relationship type
			if !isValidIdentifier(relType) {
				return nil, localizedError(localization.CypherMutationsInvalidRelationshipType(relType), nil)
			}

			// SECURITY: Validate relationship property keys
			for key := range relProps {
				if !isValidIdentifier(key) {
					return nil, localizedError(localization.CypherMutationsInvalidRelationshipPropertyKey(key), nil)
				}
			}

			if err := validatePropertyValues(relProps); err != nil {
				return nil, err
			}

			// Endpoints: a bound variable is reused; anything else is planned
			// through the shared node planner.
			sourceNode := chainedSourceNode
			if sourceNode == nil {
				sourceNode, err = e.planCreateEndpoint(ctx, sourceContent, createdNodes, createdEdges, plan)
				if err != nil {
					return nil, err
				}
			}
			targetNode, err := e.planCreateEndpoint(ctx, targetContent, createdNodes, createdEdges, plan)
			if err != nil {
				return nil, err
			}

			// Handle reverse direction
			startNode, endNode := sourceNode, targetNode
			if isReverse {
				startNode, endNode = targetNode, sourceNode
			}

			// Create relationship
			edge := &storage.Edge{
				ID:         storage.EdgeID(e.generateID()),
				StartNode:  startNode.ID,
				EndNode:    endNode.ID,
				Type:       relType,
				Properties: relProps,
			}
			plan.edges = append(plan.edges, edge)
			if relVar != "" {
				createdEdges[relVar] = edge
			}
			if pathVar != "" {
				if len(pathNodes) == 0 {
					pathNodes = append(pathNodes, startNode)
				}
				pathEdges = append(pathEdges, edge)
				pathNodes = append(pathNodes, endNode)
			}
			// If there's more chain to process, continue with target as new source
			if remainder != "" && (strings.HasPrefix(remainder, "-[") || strings.HasPrefix(remainder, "<-[")) {
				// Build the next pattern: (targetContent) + remainder
				chainedSourceNode = targetNode
				currentPattern = "(" + targetContent + ")" + remainder
			} else {
				currentPattern = ""
			}
		}

		if pathVar != "" {
			createdPaths[pathVar] = PathResult{
				Nodes:         pathNodes,
				Relationships: pathEdges,
				Length:        len(pathEdges),
			}
		}
	}
	return createdPaths, nil
}

// applyCreatePlan writes a planned CREATE clause. A property expression that
// failed while the clause was planned (recorded in ctx) aborts it before any
// write. Nodes are written before relationships, each set in one bulk call,
// so storage rejects a clause as a whole; stats, optimistic IDs and mutation
// notifications are recorded only for what was written. A set of exactly one
// node or one relationship has nothing to keep together and is written with
// the single create, which every engine implements most directly. When the
// relationships are rejected after the nodes were written, the nodes are
// removed again, so the clause is all-or-nothing without a transaction too.
func (e *StorageExecutor) applyCreatePlan(ctx context.Context, plan *createPlan, result *ExecuteResult) error {
	if failure := getExpressionFailure(ctx); failure != nil {
		return failure
	}
	store := e.getStorage(ctx)
	switch len(plan.nodes) {
	case 0:
	case 1:
		node := plan.nodes[0]
		actualID, err := store.CreateNode(node)
		if err != nil {
			return localizedError(localization.CypherMutationsCreateNodeFailed(err), err)
		}
		if actualID != "" {
			node.ID = actualID
		}
	default:
		if err := store.BulkCreateNodes(plan.nodes); err != nil {
			return localizedError(localization.CypherMutationsCreateNodeFailed(err), err)
		}
	}
	var edgeErr error
	switch len(plan.edges) {
	case 0:
	case 1:
		edgeErr = store.CreateEdge(plan.edges[0])
	default:
		edgeErr = store.BulkCreateEdges(plan.edges)
	}
	if edgeErr != nil {
		// The clause's nodes are already written; remove them so the clause
		// leaves nothing behind on a route without a transaction.
		if len(plan.nodes) > 0 {
			ids := make([]storage.NodeID, len(plan.nodes))
			for i, node := range plan.nodes {
				ids[i] = node.ID
			}
			_ = store.BulkDeleteNodes(ids)
		}
		return localizedError(localization.CypherMutationsCreateRelationshipFailed(edgeErr), edgeErr)
	}
	for _, node := range plan.nodes {
		e.notifyNodeMutated(string(node.ID))
		addOptimisticNodeID(result, node.ID)
		countCreatedEntity(result.Stats, node.Labels, node.Properties)
	}
	result.Stats.NodesCreated += len(plan.nodes)
	for _, edge := range plan.edges {
		e.notifyEdgeMutated(string(edge.ID))
		addOptimisticRelationshipID(result, edge.ID)
		countCreatedEntity(result.Stats, nil, edge.Properties)
	}
	result.Stats.RelationshipsCreated += len(plan.edges)
	return nil
}

// planCreateNode plans one node from a CREATE node pattern through
// prepareCreateNodePattern (validation, property references), adds it to plan
// and binds its variable in nodes.
func (e *StorageExecutor) planCreateNode(ctx context.Context, pattern string, nodes map[string]*storage.Node, edges map[string]*storage.Edge, plan *createPlan) (*storage.Node, error) {
	nodePattern, err := e.prepareCreateNodePattern(ctx, pattern, nodes, edges)
	if err != nil {
		return nil, err
	}
	if nodePattern.properties == nil {
		nodePattern.properties = make(map[string]interface{})
	}
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     nodePattern.labels,
		Properties: nodePattern.properties,
	}
	plan.nodes = append(plan.nodes, node)
	if nodePattern.variable != "" {
		nodes[nodePattern.variable] = node
	}
	return node, nil
}

// planCreateEndpoint resolves a relationship endpoint in a CREATE pattern:
// a variable already bound in nodes is reused, anything else is planned with
// planCreateNode.
func (e *StorageExecutor) planCreateEndpoint(ctx context.Context, content string, nodes map[string]*storage.Node, edges map[string]*storage.Edge, plan *createPlan) (*storage.Node, error) {
	content = strings.TrimSpace(content)
	variable := content
	if end := strings.IndexAny(content, ":{ "); end >= 0 {
		variable = strings.TrimSpace(content[:end])
	}
	if node := nodes[variable]; variable != "" && node != nil {
		return node, nil
	}
	return e.planCreateNode(ctx, "("+content+")", nodes, edges, plan)
}

// projectCreateReturn fills out.result with the RETURN row of a CREATE
// statement, if it has one, using projectCreatedReturnItem over the created
// nodes, relationships and named paths.
func (e *StorageExecutor) projectCreateReturn(ctx context.Context, out *createOutcome) {
	if out.returnIdx <= 0 {
		return
	}
	returnItems := e.parseReturnItems(strings.TrimSpace(out.cypher[out.returnIdx+6:]))
	out.result.Columns = make([]string, len(returnItems))
	row := make([]interface{}, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			out.result.Columns[i] = item.alias
		} else {
			out.result.Columns[i] = item.expr
		}
		row[i] = e.projectCreatedReturnItem(ctx, item, out.nodes, out.edges, out.paths)
	}
	out.result.Rows = [][]interface{}{row}
}

func (e *StorageExecutor) executeCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	out, err := e.createFromPattern(ctx, cypher)
	if err != nil {
		return nil, err
	}
	e.projectCreateReturn(ctx, out)
	return out.result, nil
}

// projectCreatedReturnItem evaluates one RETURN item of a CREATE ... RETURN
// statement against what the CREATE produced: count(...), created paths,
// relationships and nodes by variable, and every other item (literals,
// parameters, arithmetic, expressions over several variables) as an expression.
// It is the single RETURN projection for the CREATE routes that project their
// own RETURN - executeCreate, executeCreateWithRefs and the auto-commit
// node-only fast path tryAsyncCreateNodeBatch - so they return the same values.
func (e *StorageExecutor) projectCreatedReturnItem(ctx context.Context, item returnItem, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, createdPaths map[string]PathResult) interface{} {
	if isAggregateFuncName(item.expr, "count") {
		inner := strings.TrimSpace(extractFuncInner(item.expr))
		if inner == "*" {
			return int64(1)
		}
		if value := e.evaluateExpressionWithContext(ctx, inner, createdNodes, createdEdges); value != nil {
			return int64(1)
		}
		return int64(0)
	}

	// Items over a created path (RETURN p, length(p) + 1, size(nodes(p)),
	// nodes(p)[0].id, ...) are evaluated with a path context holding the
	// created paths, nodes and relationships - the same evaluator the MATCH
	// path routes use.
	if pathVar, ok := referencedCreatedPath(item.expr, createdPaths); ok {
		if item.expr == pathVar {
			return e.pathToMap(createdPaths[pathVar])
		}
		return e.evaluateExpressionWithPathContext(ctx, item.expr, createdPathContext(createdNodes, createdEdges, createdPaths))
	}

	// Relationship variables next (RETURN r, r.prop, id(r), type(r), ...)
	//
	// This matches Neo4j expectations that `r` is returned as a relationship
	// structure and can be used in functions like id(r)/type(r).
	if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
		if edge, ok := createdEdges[varName]; ok && edge != nil && !referencesOtherCreatedVariable(item.expr, varName, createdNodes, createdEdges) {
			// Direct relationship reference.
			if item.expr == varName {
				return edge
			}
			// Relationship property access: r.someProp
			if strings.HasPrefix(item.expr, varName+".") {
				propName := strings.TrimSpace(item.expr[len(varName)+1:])
				return edge.Properties[propName]
			}
			// Functions over relationships (id(r), type(r), properties(r), ...)
			return e.evaluateExpressionWithContext(ctx, item.expr, createdNodes, createdEdges)
		}
	}

	// Resolve the return expression against the created variables.
	//
	// The RETURN item may be:
	// - variable: a
	// - property access: a.name
	// - function call: id(a), elementId(a), labels(a)
	// - other expressions that reference a single variable
	varName := extractVariableNameFromReturnItem(item.expr)
	if varName != "" {
		if node, ok := createdNodes[varName]; ok && !referencesOtherCreatedVariable(item.expr, varName, createdNodes, createdEdges) {
			return e.resolveReturnItem(ctx, item, varName, node)
		}
	}

	// Anything else (literals, parameters, arithmetic, expressions
	// over several created variables) is evaluated as an expression
	// against the created nodes and relationships.
	return e.evaluateExpressionWithContext(ctx, item.expr, createdNodes, createdEdges)
}

// referencesOtherCreatedVariable reports whether expr mentions a created node or
// relationship other than varName, e.g. a.v + b.v. Such items need the full
// expression evaluator; the single-variable shortcuts would see only varName.
func referencesOtherCreatedVariable(expr, varName string, nodes map[string]*storage.Node, edges map[string]*storage.Edge) bool {
	for name := range nodes {
		if name != varName && containsIdentifierToken(expr, name) {
			return true
		}
	}
	for name := range edges {
		if name != varName && containsIdentifierToken(expr, name) {
			return true
		}
	}
	return false
}

// referencedCreatedPath returns the created path variable an item mentions.
func referencedCreatedPath(expr string, createdPaths map[string]PathResult) (string, bool) {
	for name := range createdPaths {
		if containsIdentifierToken(expr, name) {
			return name, true
		}
	}
	return "", false
}

// createdPathContext is the evaluation scope of a CREATE ... RETURN item that
// uses a created path: the created paths by name plus the created nodes and
// relationships.
func createdPathContext(nodes map[string]*storage.Node, edges map[string]*storage.Edge, createdPaths map[string]PathResult) PathContext {
	paths := make(map[string]*PathResult, len(createdPaths))
	for name := range createdPaths {
		path := createdPaths[name]
		paths[name] = &path
	}
	return PathContext{nodes: nodes, rels: edges, paths: paths}
}

// prepareCreateNodePattern parses and validates one CREATE node pattern. It is
// shared by every CREATE route (createFromPattern and the auto-commit bulk fast
// path tryAsyncCreateNodeBatch) so they reject the same patterns: malformed
// property maps, an empty label after ':', invalid or reserved labels, invalid
// property keys and values. Property values that reference variables created
// earlier in the statement (b {name: a.name}) are resolved against nodes and
// relationships.
func (e *StorageExecutor) prepareCreateNodePattern(ctx context.Context, pattern string, nodes map[string]*storage.Node, relationships map[string]*storage.Edge) (nodePatternInfo, error) {
	if err := e.validateCreatePatternPropertyMap(ctx, pattern); err != nil {
		return nodePatternInfo{}, err
	}
	nodePattern := e.parseNodePattern(ctx, pattern)
	e.resolveCreatePropertyReferences(ctx, pattern, nodePattern.properties, nodes, relationships)

	// An empty label (e.g. "n:" or ":") - only check before properties.
	head, _ := splitNodePatternProperties(pattern)
	if indexByteOutsideBackticks(head, ':') >= 0 && len(nodePattern.labels) == 0 {
		return nodePatternInfo{}, localizedError(localization.CypherResidualEmptyLabelAfterColon(pattern), nil)
	}
	// SECURITY: labels follow the label rules (quoted labels may hold any
	// character; unquoted labels are identifiers, not reserved words).
	if nodePattern.labelErr != nil {
		return nodePatternInfo{}, nodePattern.labelErr
	}
	// SECURITY: Validate property keys and values
	for key, val := range nodePattern.properties {
		if !isValidIdentifier(key) {
			return nodePatternInfo{}, localizedError(localization.CypherMutationsInvalidPropertyKey(key), nil)
		}
		if _, ok := val.(invalidPropertyValue); ok {
			return nodePatternInfo{}, localizedError(localization.CypherMutationsInvalidPropertyValue(key), nil)
		}
	}
	if err := validatePropertyValues(nodePattern.properties); err != nil {
		return nodePatternInfo{}, err
	}
	return nodePattern, nil
}

func (e *StorageExecutor) resolveCreatePropertyReferences(
	ctx context.Context,
	pattern string,
	properties map[string]interface{},
	nodes map[string]*storage.Node,
	relationships map[string]*storage.Edge,
) {
	if len(nodes) == 0 && len(relationships) == 0 && valueBindingsFromContext(ctx) == nil {
		return // no variable in scope that a property could reference
	}
	open := indexByteOutsideBackticks(pattern, '{')
	if open < 0 || strings.IndexByte(pattern[open:], '.') < 0 {
		return
	}
	close := e.findMatchingBrace(pattern, open)
	if close < 0 {
		return
	}
	for _, pair := range e.splitPropertyPairs(pattern[open+1 : close]) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
		expression := strings.TrimSpace(pair[separator+1:])
		if expression == "" || isWholeCypherQuotedString(expression) {
			continue
		}
		dot := strings.IndexByte(expression, '.')
		if dot <= 0 {
			continue
		}
		root := strings.TrimSpace(expression[:dot])
		if nodes[root] == nil && relationships[root] == nil {
			// Bound loop variables (FOREACH x, UNWIND rows): a dotted
			// reference whose root lives in the value scope resolves
			// through the binding-aware evaluator.
			if _, ok := e.boundValue(ctx, root); !ok {
				continue
			}
		}
		value := e.evaluateExpressionWithContext(ctx, expression, nodes, relationships)
		if value == nil {
			delete(properties, key)
			continue
		}
		properties[key] = normalizePropValue(value)
	}
}

func (e *StorageExecutor) validateCreatePatternPropertyMap(ctx context.Context, pattern string) error {
	_, props := splitNodePatternProperties(pattern)
	if props == "" {
		return nil
	}
	propsEnd := strings.LastIndex(props, "}")
	if propsEnd < 0 {
		return localizedError(localization.CypherResidualPropertyMapSyntaxInvalid(pattern), nil)
	}
	propsLiteral := strings.TrimSpace(props[:propsEnd+1])
	// Syntax only: the values are parsed by parseNodePattern.
	if err := validateMapLiteralSyntax(propsLiteral); err != nil {
		return localizedError(localization.CypherMutationsInvalidPropertyMapCause(err), err)
	}
	return nil
}

func parseCreatePathAssignment(pattern string) (string, string) {
	trimmed := strings.TrimSpace(pattern)
	eqIdx := strings.Index(trimmed, "=")
	if eqIdx <= 0 {
		return "", pattern
	}
	left := strings.TrimSpace(trimmed[:eqIdx])
	right := strings.TrimSpace(trimmed[eqIdx+1:])
	if left == "" || !isValidIdentifier(left) || !strings.HasPrefix(right, "(") {
		return "", pattern
	}
	return left, right
}

// executeCreateWithRefs is like executeCreate but also returns the created nodes and edges maps.
// This is used by compound queries like CREATE...WITH...DELETE to avoid expensive O(n) scans
// when looking up the created entities.
func (e *StorageExecutor) executeCreateWithRefs(ctx context.Context, cypher string) (*ExecuteResult, map[string]*storage.Node, map[string]*storage.Edge, error) {
	out, err := e.createFromPattern(ctx, cypher)
	if err != nil {
		return nil, nil, nil, err
	}
	e.projectCreateReturn(ctx, out)
	return out.result, out.nodes, out.edges, nil
}

// splitCreatePatterns splits a CREATE pattern into individual patterns (nodes and relationships)
// respecting parentheses depth, string literals, and handling relationship syntax.
// IMPORTANT: This properly handles content inside string literals (single/double quotes)
// so that Cypher-like content inside strings is not parsed as relationship patterns.
func (e *StorageExecutor) splitCreatePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	inRelationship := false
	inString := false
	stringChar := byte(0) // Track which quote character started the string
	braceDepth := 0

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		// Handle string literal boundaries
		if (c == '\'' || c == '"') && !isBackslashEscaped(pattern, i) {
			if !inString {
				// Starting a string literal
				inString = true
				stringChar = c
			} else if c == stringChar {
				// Ending the string literal (same quote type)
				inString = false
				stringChar = 0
			}
			current.WriteByte(c)
			continue
		}

		// If inside a string literal, add character without parsing
		if inString {
			current.WriteByte(c)
			continue
		}

		// Normal parsing outside string literals
		switch c {
		case '{':
			braceDepth++
			current.WriteByte(c)
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			current.WriteByte(c)
		case '(':
			if braceDepth == 0 {
				depth++
				current.WriteByte(c)
			} else {
				current.WriteByte(c)
			}
		case ')':
			if braceDepth == 0 {
				depth--
				current.WriteByte(c)
				if depth == 0 {
					// Check if next non-whitespace is a relationship operator
					j := i + 1
					for j < len(pattern) && (pattern[j] == ' ' || pattern[j] == '\t' || pattern[j] == '\n' || pattern[j] == '\r') {
						j++
					}
					if j < len(pattern) && (pattern[j] == '-' || pattern[j] == '<') {
						// This is part of a relationship pattern, continue accumulating
						inRelationship = true
					} else if !inRelationship {
						// End of a standalone node pattern
						patterns = append(patterns, current.String())
						current.Reset()
					} else {
						// End of a relationship pattern
						patterns = append(patterns, current.String())
						current.Reset()
						inRelationship = false
					}
				}
			} else {
				current.WriteByte(c)
			}
		case ',':
			if depth == 0 && !inRelationship {
				// Skip comma between patterns
				continue
			}
			current.WriteByte(c)
		case ' ', '\t', '\n', '\r':
			if depth > 0 || inRelationship {
				// Only keep whitespace inside patterns
				current.WriteByte(c)
			}
		default:
			if depth > 0 || inRelationship || c == '-' || c == '<' || c == '[' || c == ']' || c == '>' || c == ':' {
				current.WriteByte(c)
				if c == '-' || c == '<' {
					inRelationship = true
				}
				continue
			}
			// Preserve path assignment prefixes like "p=(:A)-[:R]->(:B)".
			// We drop whitespace, but keep identifiers and "=" before the first "(".
			if depth == 0 && !inRelationship {
				if isWordChar(byte(c)) || c == '=' {
					current.WriteByte(c)
				}
			}
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		patterns = append(patterns, current.String())
	}

	return patterns
}

// parseCreateRelPatternWithVars parses patterns like (varA)-[r:TYPE {props}]->(varB)
// where varA and varB are variable references (not full node definitions)
// Returns: sourceVar, relContent, targetVar, isReverse, remainder, error
// remainder is any content after the target node (for chained patterns)
func (e *StorageExecutor) parseCreateRelPatternWithVars(pattern string) (string, string, string, bool, string, error) {
	pattern = strings.TrimSpace(pattern)
	findNodeEnd := func(input string, start int) int {
		depth := 0
		var quote byte
		for index := start; index < len(input); index++ {
			current := input[index]
			if quote != 0 {
				if current == '\\' && index+1 < len(input) {
					index++
					continue
				}
				if current == quote {
					if index+1 < len(input) && input[index+1] == quote {
						index++
						continue
					}
					quote = 0
				}
				continue
			}
			switch current {
			case '\'', '"':
				quote = current
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return index
				}
			}
		}
		return -1
	}

	// Find the first node: (varA)
	if !strings.HasPrefix(pattern, "(") {
		return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternMustStartNode(), nil)
	}

	// Find end of first node
	firstNodeEnd := findNodeEnd(pattern, 0)
	if firstNodeEnd < 0 {
		return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternUnmatchedParen(), nil)
	}

	sourceVar := strings.TrimSpace(pattern[1:firstNodeEnd])
	rest := pattern[firstNodeEnd+1:]
	rest = strings.TrimSpace(rest) // Remove any whitespace before -[ or <-[

	// Detect direction and find relationship bracket
	isReverse := false
	var relStart int

	if strings.HasPrefix(rest, "-[") {
		relStart = 2 // Skip "-["
	} else if strings.HasPrefix(rest, "<-[") {
		isReverse = true
		relStart = 3 // Skip "<-["
	} else {
		return "", "", "", false, "", localizedError(localization.CypherResidualRelationshipConnectorExpected(rest[:min(20, len(rest))]), nil)
	}

	// Find matching ] considering nested brackets in properties
	depth := 1
	relEnd := -1
	inQuote := false
	quoteChar := rune(0)
	for i := relStart; i < len(rest); i++ {
		c := rune(rest[i])
		if !inQuote {
			if c == '\'' || c == '"' {
				inQuote = true
				quoteChar = c
			} else if c == '[' {
				depth++
			} else if c == ']' {
				depth--
				if depth == 0 {
					relEnd = i
					break
				}
			}
		} else if c == quoteChar {
			if i > 0 && !isBackslashEscaped(rest, i) {
				inQuote = false
			}
		}
	}
	if relEnd < 0 {
		return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternUnmatchedBracket(), nil)
	}

	relContent := rest[relStart:relEnd]
	afterRel := strings.TrimSpace(rest[relEnd+1:])

	// Now find the second node
	var secondNodeStart int
	if isReverse {
		if !strings.HasPrefix(afterRel, "-(") {
			return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternForwardExpected(), nil)
		}
		secondNodeStart = 2
	} else {
		if !strings.HasPrefix(afterRel, "->(") {
			return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternArrowExpected(), nil)
		}
		secondNodeStart = 3
	}

	// Find end of second node
	secondNodeEnd := findNodeEnd(afterRel, secondNodeStart-1)
	if secondNodeEnd < 0 {
		return "", "", "", false, "", localizedError(localization.CypherMutationsRelationshipPatternSecondUnmatched(), nil)
	}

	targetVar := strings.TrimSpace(afterRel[secondNodeStart:secondNodeEnd])
	remainder := strings.TrimSpace(afterRel[secondNodeEnd+1:])

	return sourceVar, relContent, targetVar, isReverse, remainder, nil
}

// splitNodePatterns splits a CREATE pattern into individual node patterns
// (Used for simple node-only patterns and by other parts of the system)
func (e *StorageExecutor) splitNodePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	braceDepth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if (c == '\'' || c == '"') && !isBackslashEscaped(pattern, i) {
			if !inString {
				inString = true
				stringChar = c
			} else if c == stringChar {
				inString = false
				stringChar = 0
			}
			if depth > 0 {
				current.WriteByte(c)
			}
			continue
		}
		if inString {
			if depth > 0 {
				current.WriteByte(c)
			}
			continue
		}
		switch c {
		case '{':
			if depth > 0 {
				braceDepth++
				current.WriteByte(c)
			}
		case '}':
			if depth > 0 {
				if braceDepth > 0 {
					braceDepth--
				}
				current.WriteByte(c)
			}
		case '(':
			if braceDepth == 0 {
				depth++
				current.WriteByte(c)
			} else {
				current.WriteByte(c)
			}
		case ')':
			if braceDepth == 0 {
				depth--
				current.WriteByte(c)
				if depth == 0 {
					patterns = append(patterns, current.String())
					current.Reset()
				}
			} else {
				current.WriteByte(c)
			}
		case ',':
			if depth == 0 {
				// Skip comma between patterns
				continue
			}
			current.WriteByte(c)
		default:
			if depth > 0 {
				current.WriteByte(c)
			}
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		patterns = append(patterns, current.String())
	}

	return patterns
}

// parseRelationshipTypeAndProps parses "r:TYPE {props}" or ":TYPE {props}". A pattern with no type ("r", ":") yields an empty type.
// Returns the type and properties map
func (e *StorageExecutor) parseRelationshipTypeAndProps(ctx context.Context, relStr string) (string, map[string]interface{}) {
	relStr = strings.TrimSpace(relStr)
	relType := ""
	var relProps map[string]interface{}

	// Find properties block if present
	propsStart := strings.Index(relStr, "{")
	if propsStart >= 0 {
		// Find matching }
		depth := 0
		propsEnd := -1
		inQuote := false
		quoteChar := rune(0)
		for i := propsStart; i < len(relStr); i++ {
			c := rune(relStr[i])
			if !inQuote {
				if c == '\'' || c == '"' {
					inQuote = true
					quoteChar = c
				} else if c == '{' {
					depth++
				} else if c == '}' {
					depth--
					if depth == 0 {
						propsEnd = i
						break
					}
				}
			} else if c == quoteChar && !isBackslashEscaped(relStr, i) {
				inQuote = false
			}
		}
		if propsEnd > propsStart {
			relProps = e.parseProperties(ctx, relStr[propsStart:propsEnd+1])
		}
		relStr = strings.TrimSpace(relStr[:propsStart])
	}

	// Parse type: "r:TYPE" or ":TYPE" - if no colon, it's just a variable (use default type)
	if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
		// Has colon - everything after is the type
		relType = strings.TrimSpace(relStr[colonIdx+1:])
	}
	// No colon ("r") or nothing after it (":") leaves the type empty; the
	// CREATE core rejects a relationship without exactly one type.

	if relProps == nil {
		relProps = make(map[string]interface{})
	}

	return relType, relProps
}

// executeCompoundMatchCreate handles MATCH ... CREATE queries.
// This handles multiple scenarios:
// 1. Create relationships between matched nodes
// 2. Create new nodes and relationships referencing matched nodes
// 3. Multiple MATCH...CREATE blocks in a single query
//
// Example 1: Create relationship only
//
//	MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//	CREATE (a)-[:KNOWS]->(b)
//
// Example 2: Create new node with relationships to matched nodes
//
//	MATCH (s:Supplier {supplierID: 1}), (c:Category {categoryID: 1})
//	CREATE (p:Product {productName: 'Chai'})
//	CREATE (p)-[:PART_OF]->(c)
//	CREATE (s)-[:SUPPLIES]->(p)
//
// Example 3: Multiple MATCH...CREATE blocks
//
//	MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
//	CREATE (p1:Product {...})
//	MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
//	CREATE (p2:Product {...})
func (e *StorageExecutor) executeCompoundMatchCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result, _, _, err := e.executeCompoundMatchCreateWithRefs(ctx, cypher)
	return result, err
}

// executeCompoundMatchCreateWithRefs executes a compound MATCH/CREATE query
// and retains the entity bindings produced by the mutation. Callers that
// continue with later clauses need these references so the query is not
// partially executed and then retried through another mutation handler.
func (e *StorageExecutor) executeCompoundMatchCreateWithRefs(ctx context.Context, cypher string) (*ExecuteResult, map[string]*storage.Node, map[string]*storage.Edge, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues.
	// If the query includes SET += (map merge), keep the SET segment intact
	// so $props can be resolved from context instead of string-substitution.
	if params := getParamsFromContext(ctx); params != nil {
		if containsKeywordOutsideStrings(cypher, "SET") && strings.Contains(cypher, "+=") {
			setIdx := findKeywordIndex(cypher, "SET")
			if setIdx >= 0 {
				returnIdx := findKeywordIndex(cypher, "RETURN")
				prefix := cypher[:setIdx]
				setSegment := ""
				suffix := ""
				if returnIdx > setIdx {
					setSegment = cypher[setIdx:returnIdx]
					suffix = cypher[returnIdx:]
				} else {
					setSegment = cypher[setIdx:]
				}
				prefix = e.substituteParams(prefix, params)
				if suffix != "" {
					suffix = e.substituteParams(suffix, params)
				}
				cypher = prefix + setSegment + suffix
			} else {
				cypher = e.substituteParams(cypher, params)
			}
		} else {
			cypher = e.substituteParams(cypher, params)
		}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Check if this query has multiple MATCH blocks (each starting a new scope)
	// Split into independent MATCH...CREATE blocks
	blocks := e.splitMatchCreateBlocks(cypher)

	// Track all created nodes across blocks (for cross-block references)
	allNodeVars := make(map[string]*storage.Node)
	allEdgeVars := make(map[string]*storage.Edge)

	for _, block := range blocks {
		blockResult, err := e.executeMatchCreateBlock(ctx, block, allNodeVars, allEdgeVars)
		if err != nil {
			return nil, nil, nil, err
		}
		// Accumulate stats
		addQueryStats(result.Stats, blockResult.Stats)

		// Copy Columns and Rows from last block with RETURN
		if len(blockResult.Columns) > 0 {
			result.Columns = blockResult.Columns
			result.Rows = append(result.Rows, blockResult.Rows...)
		}
	}

	return result, allNodeVars, allEdgeVars, nil
}

// splitMatchCreateBlocks splits a query into independent MATCH...CREATE blocks
// Consecutive MATCH clauses without intervening CREATE are merged into a single block
func (e *StorageExecutor) splitMatchCreateBlocks(cypher string) []string {
	var blocks []string

	// Find all MATCH and CREATE keyword positions
	matchPositions := findAllKeywordPositions(cypher, "MATCH")
	createPositions := findAllKeywordPositions(cypher, "CREATE")

	if len(matchPositions) == 0 {
		return []string{cypher}
	}

	// If there's only one MATCH or no CREATEs, return as single block
	if len(matchPositions) == 1 || len(createPositions) == 0 {
		return []string{cypher}
	}

	// Group consecutive MATCHes that share a CREATE
	// A new block starts when there's a CREATE between two MATCH positions
	var blockStarts []int
	blockStarts = append(blockStarts, matchPositions[0])

	for i := 1; i < len(matchPositions); i++ {
		prevMatchPos := matchPositions[i-1]
		currMatchPos := matchPositions[i]

		// Check if there's a CREATE between the previous MATCH and current MATCH
		hasCreateBetween := false
		for _, createPos := range createPositions {
			if createPos > prevMatchPos && createPos < currMatchPos {
				hasCreateBetween = true
				break
			}
		}

		// Only start a new block if there was a CREATE between MATCHes
		if hasCreateBetween {
			blockStarts = append(blockStarts, currMatchPos)
		}
	}

	// Create blocks from start positions
	for i, startPos := range blockStarts {
		var endPos int
		if i+1 < len(blockStarts) {
			endPos = blockStarts[i+1]
		} else {
			endPos = len(cypher)
		}

		block := strings.TrimSpace(cypher[startPos:endPos])
		if block != "" {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

// findAllKeywordPositions finds all positions of a keyword in the query
func findAllKeywordPositions(cypher string, keyword string) []int {
	var positions []int
	keywordLen := len(keyword)

	for i := 0; i <= len(cypher)-keywordLen; i++ {
		// Check if keyword matches at this position (case insensitive)
		if strings.EqualFold(cypher[i:i+keywordLen], keyword) {
			// Check word boundary before
			if i > 0 {
				prevChar := cypher[i-1]
				if isAlphaNumericByte(prevChar) {
					continue // Part of another word
				}
			}
			// Check word boundary after
			if i+keywordLen < len(cypher) {
				nextChar := cypher[i+keywordLen]
				if isAlphaNumericByte(nextChar) {
					continue // Part of another word
				}
			}
			positions = append(positions, i)
		}
	}

	// Handle nested MATCH in strings - check if position is inside quotes
	var validPositions []int
	for _, pos := range positions {
		if !isInsideQuotes(cypher, pos) {
			validPositions = append(validPositions, pos)
		}
	}

	return validPositions
}

// isAlphaNumericByte checks if a byte is alphanumeric or underscore
func isAlphaNumericByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// isInsideQuotes checks if a position is inside quotes
func isInsideQuotes(s string, pos int) bool {
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < pos; i++ {
		c := s[i]
		if c == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		} else if c == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}
	}

	return inSingleQuote || inDoubleQuote
}

// executeMatchCreateBlock executes a single MATCH...CREATE block
func (e *StorageExecutor) executeMatchCreateBlock(ctx context.Context, block string, allNodeVars map[string]*storage.Node, allEdgeVars map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Split into MATCH and CREATE parts
	createIdx := findKeywordIndex(block, "CREATE")
	if createIdx < 0 {
		// No CREATE in this block, just MATCH - skip
		return result, nil
	}

	matchPart := strings.TrimSpace(block[:createIdx])
	createPart := strings.TrimSpace(block[createIdx:]) // Keep "CREATE" for splitting

	// allCombinations is filled either by the MATCH+WITH pipeline (when WITH is non-trivial) or by MATCH parsing below
	var allCombinations []map[string]*storage.Node

	// Extract WITH clause with LIMIT/SKIP from matchPart (handles MATCH ... WITH ... LIMIT 1 CREATE ...)
	// When WITH is non-trivial (projections like expr AS var, collect, UNWIND, etc.), we run the full
	// pipeline and pass its bindings into CREATE so variables such as "pharmacy" from WITH are available.
	var withLimit, withSkip int
	fullMatchPart := matchPart
	if withInMatch := findKeywordIndex(matchPart, "WITH"); withInMatch > 0 {
		withPart := strings.TrimSpace(matchPart[withInMatch:])
		// Extract LIMIT and SKIP from WITH clause (for simple WITH ... LIMIT n CREATE)
		limitIdx := findKeywordIndex(withPart, "LIMIT")
		skipIdx := findKeywordIndex(withPart, "SKIP")

		if limitIdx > 0 {
			limitPart := strings.TrimSpace(withPart[limitIdx+5:])
			if fields := strings.Fields(limitPart); len(fields) > 0 {
				if l, err := strconv.Atoi(fields[0]); err == nil {
					withLimit = l
				}
			}
		}
		if skipIdx > 0 && (limitIdx < 0 || skipIdx < limitIdx) {
			skipPart := strings.TrimSpace(withPart[skipIdx+4:])
			if fields := strings.Fields(skipPart); len(fields) > 0 {
				if s, err := strconv.Atoi(fields[0]); err == nil {
					withSkip = s
				}
			}
		}

		// Check if WITH is non-trivial (has AS, collect, UNWIND, or multiple WITH) — then run pipeline
		createVars := extractCreateVariableRefs(createPart)
		hasNonTrivialWith := strings.Contains(strings.ToUpper(withPart), " AS ") ||
			strings.Contains(strings.ToUpper(withPart), "COLLECT(") ||
			strings.Contains(strings.ToUpper(withPart), "UNWIND ") ||
			findKeywordIndex(withPart, "WITH") > 0
		if hasNonTrivialWith && len(createVars) > 0 {
			// Run MATCH+WITH pipeline and use its rows to drive CREATE (so WITH bindings like "pharmacy" are available)
			pipelineRows, pipelineErr := e.executeMatchWithPipelineToRows(ctx, fullMatchPart, createVars, store)
			if pipelineErr == nil && len(pipelineRows) > 0 {
				allCombinations = make([]map[string]*storage.Node, 0, len(pipelineRows))
				for _, row := range pipelineRows {
					combo := make(map[string]*storage.Node)
					for k, v := range row {
						if node, ok := v.(*storage.Node); ok {
							combo[k] = node
						}
					}
					if len(combo) > 0 {
						allCombinations = append(allCombinations, combo)
					}
				}
			}
		}

		// Strip WITH clause from matchPart for building combinations from MATCH only (when not using pipeline)
		if len(allCombinations) == 0 {
			matchPart = strings.TrimSpace(matchPart[:withInMatch])
		}
	}

	// Find WITH clause (for MATCH...CREATE...WITH...DELETE pattern)
	withIdx := findKeywordIndex(createPart, "WITH")
	deleteIdx := findKeywordIndex(createPart, "DELETE")
	var deleteTarget string
	hasWithDelete := withIdx > 0 && deleteIdx > withIdx
	hasDirectDelete := deleteIdx > 0 && withIdx <= 0 // DELETE without WITH in createPart

	// Find RETURN clause if present (only in last block typically)
	// Note: findKeywordIndex returns -1 if not found, 0 if at start
	returnIdx := findKeywordIndex(createPart, "RETURN")
	var returnPart string
	if returnIdx >= 0 {
		returnPart = strings.TrimSpace(createPart[returnIdx+6:])
		if hasWithDelete {
			// WITH...DELETE...RETURN - extract delete target and strip
			withDeletePart := createPart[withIdx:returnIdx]
			deletePartIdx := findKeywordIndex(withDeletePart, "DELETE")
			if deletePartIdx > 0 {
				deleteTarget = strings.TrimSpace(withDeletePart[deletePartIdx+6:])
			}
			createPart = strings.TrimSpace(createPart[:withIdx])
		} else if hasDirectDelete {
			// CREATE...DELETE...RETURN (DELETE without WITH)
			deleteTarget = strings.TrimSpace(createPart[deleteIdx+6 : returnIdx])
			createPart = strings.TrimSpace(createPart[:deleteIdx])
		} else {
			createPart = strings.TrimSpace(createPart[:returnIdx])
		}
	} else if hasWithDelete {
		// WITH...DELETE without RETURN
		withDeletePart := createPart[withIdx:]
		deletePartIdx := findKeywordIndex(withDeletePart, "DELETE")
		if deletePartIdx > 0 {
			deleteTarget = strings.TrimSpace(withDeletePart[deletePartIdx+6:])
		}
		createPart = strings.TrimSpace(createPart[:withIdx])
	} else if hasDirectDelete {
		// CREATE...DELETE without WITH or RETURN
		deleteTarget = strings.TrimSpace(createPart[deleteIdx+6:])
		createPart = strings.TrimSpace(createPart[:deleteIdx])
	}

	// Extract SET clause (e.g., MATCH ... CREATE ... SET r += $props)
	setIdx := findKeywordIndex(createPart, "SET")
	var setPart string
	if setIdx > 0 {
		setPart = strings.TrimSpace(createPart[setIdx+3:])
		createPart = strings.TrimSpace(createPart[:setIdx])
	}

	// Parse all node patterns from MATCH clause and find matching nodes (skip when pipeline already filled allCombinations)
	nodeVars := make(map[string]*storage.Node)
	for k, v := range allNodeVars {
		nodeVars[k] = v
	}
	edgeVars := make(map[string]*storage.Edge)
	for k, v := range allEdgeVars {
		edgeVars[k] = v
	}

	if len(allCombinations) == 0 {
		// Collect all patterns and their matching nodes for cartesian product.
		// Parse per-MATCH WHERE: each "MATCH ... WHERE ..." segment keeps its own WHERE
		// so we do not truncate matchPart after the first WHERE (which would drop
		// subsequent MATCH clauses and leave variables like 'a' or 'b' unbound).
		hadMatchPatterns := false
		anyPatternUnmatched := false
		patternMatches := make([]struct {
			variable string
			nodes    []*storage.Node
		}, 0)
		var postFilterWhere string // WHERE that references multiple vars in one segment, applied after cartesian product

		// Split by MATCH keyword; matchPart is left intact (includes all WHERE clauses)
		matchClauses := SplitByMatch(matchPart)
		nonEmptyMatchClauses := make([]string, 0, len(matchClauses))
		for _, clause := range matchClauses {
			clause = strings.TrimSpace(clause)
			if clause != "" {
				nonEmptyMatchClauses = append(nonEmptyMatchClauses, clause)
			}
		}

		// Fast path: resolve nodes directly from WHERE elementId/id equality
		// predicates. This avoids label scans for the common "MATCH by id +
		// CREATE rel" pattern (e.g. MATCH (a), (b) WHERE elementId(a) = $x
		// AND elementId(b) = $y CREATE (a)-[:REL]->(b)).
		if len(allNodeVars) == 0 && len(allEdgeVars) == 0 {
			if combo, ok := e.tryResolveMatchNodesByIDFromWhere(ctx, nonEmptyMatchClauses, getParamsFromContext(ctx)); ok {
				allCombinations = []map[string]*storage.Node{combo}
				hadMatchPatterns = true
			}
		}

		if len(allNodeVars) == 0 && len(allEdgeVars) == 0 && len(nonEmptyMatchClauses) == 1 && allCombinations == nil {
			shortcutSafe := true
			if whereIdx := findKeywordIndex(nonEmptyMatchClauses[0], "WHERE"); whereIdx > 0 {
				whereClause := strings.TrimSpace(nonEmptyMatchClauses[0][whereIdx+5:])
				for _, term := range splitTopLevelAndConjuncts(whereClause) {
					if _, _, _, ok := parseNotRelationshipExistenceTerm(strings.TrimSpace(term)); ok {
						shortcutSafe = false
						break
					}
				}
			}
			if shortcutSafe {
				clauseMatches, _, err := e.executeMatchForContext(ctx, "MATCH "+nonEmptyMatchClauses[0])
				if err != nil {
					return nil, err
				}
				// Only accept the shortcut when it actually produced rows.
				// If it yields zero rows, fall back to the explicit MATCH pattern scan below
				// so relationship-create shapes like MATCH (...) CREATE (new)-[:R]->(...) don't
				// silently no-op when shortcut parsing is overly conservative.
				if len(clauseMatches) > 0 {
					allCombinations = clauseMatches
					hadMatchPatterns = true
				}
			}
		}

		for _, clause := range nonEmptyMatchClauses {
			if allCombinations != nil {
				break
			}

			// Extract this segment's WHERE (only the first WHERE in this segment)
			whereForClause := ""
			if whereIdx := findKeywordIndex(clause, "WHERE"); whereIdx > 0 {
				whereForClause = strings.TrimSpace(clause[whereIdx+5:])
				clause = strings.TrimSpace(clause[:whereIdx])
			}

			// Split by comma but respect parentheses
			patterns := e.splitNodePatterns(clause)
			numPatternsInSegment := 0
			for _, p := range patterns {
				if strings.TrimSpace(p) != "" {
					hadMatchPatterns = true
					numPatternsInSegment++
				}
			}
			addedClauseWhereToPostFilter := false
			if whereForClause != "" && numPatternsInSegment > 1 {
				if postFilterWhere != "" {
					postFilterWhere = postFilterWhere + " AND " + whereForClause
				} else {
					postFilterWhere = whereForClause
				}
				addedClauseWhereToPostFilter = true
			}

			// Collect ALL matched nodes from this MATCH clause (for cartesian product)
			for _, pattern := range patterns {
				pattern = strings.TrimSpace(pattern)
				if pattern == "" {
					continue
				}

				nodeInfo := e.parseNodePattern(ctx, pattern)
				if nodeInfo.variable == "" {
					continue
				}

				// Check if this variable already exists from previous blocks
				if existingNode, exists := nodeVars[nodeInfo.variable]; exists {
					// Already have this variable, use the existing node (single match from prior context)
					patternMatches = append(patternMatches, struct {
						variable string
						nodes    []*storage.Node
					}{
						variable: nodeInfo.variable,
						nodes:    []*storage.Node{existingNode},
					})
					continue
				}

				// Find ALL matching nodes for this pattern (for cartesian product)
				var matchingNodes []*storage.Node
				if len(nodeInfo.labels) > 0 {
					var err error
					matchingNodes, err = store.GetNodesByLabel(nodeInfo.labels[0])
					if err != nil {
						return nil, localizedError(localization.CypherMutationsMatchLabelLookupFailed(nodeInfo.labels[0], err), err)
					}
				} else {
					var err error
					matchingNodes, err = store.AllNodes()
					if err != nil {
						return nil, localizedError(localization.CypherMutationsMatchAllNodesFailed(err), err)
					}
				}

				// Filter by additional labels
				if len(nodeInfo.labels) > 1 {
					var filtered []*storage.Node
					for _, node := range matchingNodes {
						hasAll := true
						for _, reqLabel := range nodeInfo.labels[1:] {
							found := false
							for _, nodeLabel := range node.Labels {
								if nodeLabel == reqLabel {
									found = true
									break
								}
							}
							if !found {
								hasAll = false
								break
							}
						}
						if hasAll {
							filtered = append(filtered, node)
						}
					}
					matchingNodes = filtered
				}

				// Filter by properties
				if len(nodeInfo.properties) > 0 {
					var filtered []*storage.Node
					for _, node := range matchingNodes {
						if e.nodeMatchesProps(node, nodeInfo.properties) {
							filtered = append(filtered, node)
						}
					}
					matchingNodes = filtered
				}

				// Apply per-node WHERE only when the predicate references this variable.
				// Predicates involving other variables must be evaluated post-cartesian.
				applyWherePerNode := false
				if whereForClause != "" && numPatternsInSegment == 1 {
					if whereClauseReferencesOnlyVariable(whereForClause, nodeInfo.variable) {
						applyWherePerNode = true
					} else if !addedClauseWhereToPostFilter {
						if postFilterWhere != "" {
							postFilterWhere = postFilterWhere + " AND " + whereForClause
						} else {
							postFilterWhere = whereForClause
						}
						addedClauseWhereToPostFilter = true
					}
				}

				if applyWherePerNode {
					var filtered []*storage.Node
					for _, node := range matchingNodes {
						if e.evaluateWhere(ctx, node, nodeInfo.variable, whereForClause) {
							filtered = append(filtered, node)
						}
					}
					matchingNodes = filtered
				}

				if len(matchingNodes) > 0 {
					patternMatches = append(patternMatches, struct {
						variable string
						nodes    []*storage.Node
					}{
						variable: nodeInfo.variable,
						nodes:    matchingNodes,
					})
				} else {
					anyPatternUnmatched = true
				}
			}
		}

		// Build combinations of all pattern matches.
		// Prefer a join-aware construction for common equality-join WHERE shapes
		// to avoid full cartesian expansion in MATCH...CREATE hot paths.
		if anyPatternUnmatched {
			allCombinations = []map[string]*storage.Node{}
		} else {
			// Push down selective multi-variable WHERE predicates before
			// cartesian expansion to avoid combinatorial blow-ups on join-shapes.
			if postFilterWhere != "" && len(patternMatches) > 1 {
				patternMatches = e.applyCartesianWherePushdown(patternMatches, postFilterWhere)
				if joined, ok := e.buildCombinationsUsingWhereJoin(patternMatches, postFilterWhere); ok {
					allCombinations = joined
				}
			}
			if allCombinations == nil {
				allCombinations = e.buildCartesianProduct(patternMatches)
			}
		}

		// Apply WITH LIMIT/SKIP if present (from MATCH ... WITH ... LIMIT ... CREATE pattern)
		// This limits how many matched rows are processed before CREATE
		if withLimit > 0 || withSkip > 0 {
			startIdx := withSkip
			if startIdx > len(allCombinations) {
				startIdx = len(allCombinations)
			}
			endIdx := len(allCombinations)
			if withLimit > 0 && startIdx+withLimit < endIdx {
				endIdx = startIdx + withLimit
			}
			if startIdx < len(allCombinations) {
				allCombinations = allCombinations[startIdx:endIdx]
			} else {
				allCombinations = []map[string]*storage.Node{} // No matches after SKIP
			}
		}

		// Apply post-filter WHERE (multi-variable conditions from a single MATCH segment) if present
		if postFilterWhere != "" {
			relChecks := make([]struct {
				startVar string
				edgeType string
				endVar   string
			}, 0, 2)
			remainingTerms := make([]string, 0, 4)
			for _, term := range splitTopLevelAndConjuncts(postFilterWhere) {
				term = strings.TrimSpace(term)
				if term == "" {
					continue
				}
				if sv, et, ev, ok := parseNotRelationshipExistenceTerm(term); ok {
					relChecks = append(relChecks, struct {
						startVar string
						edgeType string
						endVar   string
					}{startVar: sv, edgeType: et, endVar: ev})
					continue
				}
				remainingTerms = append(remainingTerms, term)
			}
			remainingWhere := strings.Join(remainingTerms, " AND ")

			var filtered []map[string]*storage.Node
			for _, combination := range allCombinations {
				keep := true
				if remainingWhere != "" {
					keep = e.evaluateWhereForContext(ctx, remainingWhere, combination)
				}
				if keep && len(relChecks) > 0 {
					for _, rc := range relChecks {
						startNode := combination[rc.startVar]
						endNode := combination[rc.endVar]
						if startNode == nil || endNode == nil {
							keep = false
							break
						}
						exists, err := hasRelationshipOfType(store, startNode.ID, endNode.ID, rc.edgeType)
						if err != nil {
							return nil, localizedError(localization.CypherMutationsRelationshipExistenceCheckFailed(rc.startVar, rc.edgeType, rc.endVar, err), err)
						}
						if exists {
							keep = false
							break
						}
					}
				}
				if keep {
					filtered = append(filtered, combination)
				}
			}
			allCombinations = filtered
		}

		// If no combinations, fall back to using existing nodeVars
		if len(allCombinations) == 0 {
			// openCypher semantics: MATCH producing zero rows short-circuits the CREATE path
			// and returns no rows unless RETURN is aggregation-only (then one row).
			if hadMatchPatterns {
				if returnPart != "" {
					returnItems := e.parseReturnItems(returnPart)
					result.Columns = make([]string, len(returnItems))
					for i, item := range returnItems {
						if item.alias != "" {
							result.Columns[i] = item.alias
						} else {
							result.Columns[i] = item.expr
						}
					}
					if len(returnItems) > 0 {
						allAggregates := true
						row := make([]interface{}, len(returnItems))
						for i, item := range returnItems {
							if !isAggregateFunc(item.expr) {
								allAggregates = false
								break
							}
							if isAggregateFuncName(item.expr, "count") {
								row[i] = int64(0)
							} else {
								row[i] = nil
							}
						}
						if allAggregates {
							result.Rows = [][]interface{}{row}
						}
					}
				}
				return result, nil
			}
			allCombinations = []map[string]*storage.Node{nodeVars}
		}
	}

	// Split CREATE part into individual CREATE statements
	createClauses := SplitByCreate(createPart)

	// For each combination in the cartesian product, execute CREATE
	for _, combination := range allCombinations {
		// Merge combination with existing nodeVars (combination takes precedence for cartesian product vars)
		combinedNodeVars := make(map[string]*storage.Node)
		for k, v := range nodeVars {
			combinedNodeVars[k] = v
		}
		for k, v := range combination {
			combinedNodeVars[k] = v
		}

		// Each CREATE clause runs through the shared CREATE core in the scope
		// of this row's bindings.
		for _, clause := range createClauses {
			if clause = strings.TrimSpace(clause); clause == "" {
				continue
			}
			if _, err := e.createPatternsInScope(ctx, clause, combinedNodeVars, edgeVars, result); err != nil {
				return nil, err
			}
		}

		// Apply SET through the shared per-entity applicator, as MATCH ... SET,
		// MERGE ... SET and CREATE ... SET do.
		if setPart != "" {
			setPart = collapseChainedSetClauses(setPart)
			row := make(pipelineRow, len(combinedNodeVars)+len(edgeVars))
			for name, node := range combinedNodeVars {
				row[name] = node
			}
			for name, edge := range edgeVars {
				row[name] = edge
			}
			for _, variable := range pipelineSetTargetVariables(e.splitSetAssignments(setPart)) {
				if row[variable] == nil {
					return nil, localizedError(localization.CypherMutationsUnknownSetVariable(variable), nil)
				}
			}
			setStats, _, err := e.pipelineApplySet(ctx, []pipelineRow{row}, "SET "+setPart)
			if err != nil {
				return nil, err
			}
			if setStats != nil {
				addQueryStats(result.Stats, setStats)
			}
		}

		// Copy created nodes AND matched nodes back to nodeVars for RETURN clause
		for k, v := range combinedNodeVars {
			nodeVars[k] = v
		}
	}

	// Ensure all matched nodes from last combination are in nodeVars for RETURN
	// (in case there were multiple combinations, use the last one)
	if len(allCombinations) > 0 {
		lastCombination := allCombinations[len(allCombinations)-1]
		for k, v := range lastCombination {
			nodeVars[k] = v
		}
	}

	// Copy new vars back to allNodeVars for use in later blocks
	for k, v := range nodeVars {
		allNodeVars[k] = v
	}
	for k, v := range edgeVars {
		allEdgeVars[k] = v
	}

	// Execute DELETE if present (MATCH...CREATE...WITH...DELETE pattern)
	if deleteTarget != "" {
		if edge, exists := edgeVars[deleteTarget]; exists {
			if err := store.DeleteEdge(edge.ID); err == nil {
				result.Stats.RelationshipsDeleted++
			}
		} else if node, exists := nodeVars[deleteTarget]; exists {
			// Delete connected edges first
			outEdges, _ := store.GetOutgoingEdges(node.ID)
			inEdges, _ := store.GetIncomingEdges(node.ID)
			for _, edge := range outEdges {
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			for _, edge := range inEdges {
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			if err := store.DeleteNode(node.ID); err == nil {
				result.Stats.NodesDeleted++
				e.removeNodeFromSearch(string(node.ID))
			}
		}
	}

	// Handle RETURN clause
	if returnPart != "" {
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}

			// Handle count() after DELETE
			upperExpr := strings.ToUpper(item.expr)
			if strings.HasPrefix(upperExpr, "COUNT(") && deleteTarget != "" {
				row[i] = int64(1) // count of deleted items
				continue
			}
			if strings.HasPrefix(upperExpr, "COUNT(") {
				inner := strings.TrimSpace(extractFuncInner(item.expr))
				if inner == "" || inner == "*" {
					row[i] = int64(len(allCombinations))
					continue
				}
				var cnt int64
				for _, combination := range allCombinations {
					// COUNT(expr) after MATCH...CREATE should be able to see both matched
					// bindings (from combination) and newly-created bindings (nodeVars).
					// Using combination alone makes COUNT(newVar) incorrectly return 0.
					combined := make(map[string]*storage.Node, util.SafePreallocSum(len(nodeVars), len(combination)))
					for k, v := range nodeVars {
						combined[k] = v
					}
					for k, v := range combination {
						combined[k] = v
					}
					if e.evaluateExpressionWithContext(ctx, inner, combined, edgeVars) != nil {
						cnt++
					}
				}
				row[i] = cnt
				continue
			}

			// Everything else goes through the shared CREATE RETURN projection,
			// over the matched and the newly created variables.
			row[i] = e.projectCreatedReturnItem(ctx, item, nodeVars, edgeVars, nil)
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

func parseCreateRelationshipContent(content string) (relVar string, relType string, relPropsStr string, err error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", "", "", nil
	}

	head := content
	if braceStart := indexByteOutsideBackticks(content, '{'); braceStart >= 0 {
		braceEnd := strings.LastIndex(content, "}")
		if braceEnd < braceStart {
			return "", "", "", localizedError(localization.CypherMutationsRelationshipPropertiesInvalid(), nil)
		}
		relPropsStr = strings.TrimSpace(content[braceStart : braceEnd+1])
		if strings.TrimSpace(content[braceEnd+1:]) != "" {
			return "", "", "", localizedError(localization.CypherMutationsRelationshipPropertiesInvalid(), nil)
		}
		head = strings.TrimSpace(content[:braceStart])
	}

	if head == "" {
		return "", "", relPropsStr, nil
	}

	if strings.HasPrefix(head, ":") {
		relType = strings.TrimSpace(head[1:])
		return relVar, relType, relPropsStr, nil
	}

	if colon := strings.Index(head, ":"); colon >= 0 {
		relVar = strings.TrimSpace(head[:colon])
		relType = strings.TrimSpace(head[colon+1:])
		return relVar, relType, relPropsStr, nil
	}

	relVar = strings.TrimSpace(head)
	return relVar, "", relPropsStr, nil
}

// isSimpleVariable checks if content is just a variable name (alphanumeric + underscore)
func isSimpleVariable(content string) bool {
	if content == "" {
		return false
	}
	for _, r := range content {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// getKeys returns the keys of a map as a slice
func getKeys(m map[string]*storage.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// executeCompoundCreateWithDelete handles CREATE ... WITH ... DELETE queries.
// This pattern creates a node/relationship, passes it through WITH, then deletes it.
// Example: CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t RETURN count(t)
func (e *StorageExecutor) executeCompoundCreateWithDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Find clause boundaries
	withIdx := findKeywordIndex(cypher, "WITH")
	deleteIdx := findKeywordIndex(cypher, "DELETE")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if withIdx < 0 || deleteIdx < 0 {
		return nil, localizedError(localization.CypherMutationsCreateWithDeleteInvalid(), nil)
	}

	// Extract CREATE part (everything before WITH)
	createPart := strings.TrimSpace(cypher[:withIdx])

	// Extract WITH variables (between WITH and DELETE)
	withPart := strings.TrimSpace(cypher[withIdx+4 : deleteIdx])

	// Extract DELETE target (between DELETE and RETURN, or end)
	var deletePart string
	if returnIdx > 0 {
		deletePart = strings.TrimSpace(cypher[deleteIdx+6 : returnIdx])
	} else {
		deletePart = strings.TrimSpace(cypher[deleteIdx+6:])
	}

	// Execute the CREATE part and get the created nodes/edges directly
	// This avoids expensive O(n) scans of GetNodesByLabel/AllEdges
	createResult, createdVars, createdEdges, err := e.executeCreateWithRefs(ctx, createPart)
	if err != nil {
		return nil, localizedError(localization.CypherMutationsCreateFailed(err), err)
	}
	addQueryStats(result.Stats, createResult.Stats)

	// Parse WITH clause to see what variables are passed through
	withVars := strings.Split(withPart, ",")
	for i := range withVars {
		withVars[i] = strings.TrimSpace(withVars[i])
	}

	// Execute DELETE
	deleteTarget := strings.TrimSpace(deletePart)
	if node, exists := createdVars[deleteTarget]; exists {
		// Node was JUST created in this query, so it has no pre-existing edges.
		// Only need to check for edges created in the same query (in createdEdges).
		// This avoids 2 unnecessary storage lookups per delete operation.
		for varName, edge := range createdEdges {
			if edge.StartNode == node.ID || edge.EndNode == node.ID {
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
					delete(createdEdges, varName) // Mark as deleted
				}
			}
		}
		if err := store.DeleteNode(node.ID); err != nil {
			return nil, localizedError(localization.CypherMutationsDeleteFailed(err), err)
		}
		result.Stats.NodesDeleted++
		e.removeNodeFromSearch(string(node.ID))
	} else if edge, exists := createdEdges[deleteTarget]; exists {
		if err := store.DeleteEdge(edge.ID); err != nil {
			return nil, localizedError(localization.CypherMutationsDeleteFailed(err), err)
		}
		result.Stats.RelationshipsDeleted++
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])

		// Parse return expression
		if strings.Contains(strings.ToLower(returnPart), "count(") {
			// count() after delete should return 1 (counted before delete conceptually)
			// But actually in Neo4j, count(t) after DELETE t returns 1 (the count of deleted items)
			result.Columns = []string{"count(" + deleteTarget + ")"}
			result.Rows = [][]interface{}{{int64(1)}}
		} else {
			result.Columns = []string{returnPart}
			result.Rows = [][]interface{}{{nil}}
		}
	}

	return result, nil
}

// executeCreateSet handles CREATE ... SET queries (Neo4j compatibility).
// Neo4j allows SET immediately after CREATE to set additional properties
// on newly created nodes/relationships.
// Example: CREATE (n:Node {id: 'test'}) SET n.content = 'value' RETURN n
func (e *StorageExecutor) executeCreateSet(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Normalize whitespace for index finding (newlines/tabs become spaces)
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Find clause boundaries
	setIdx := findKeywordIndex(normalized, "SET")

	if setIdx < 0 {
		return nil, localizedError(localization.CypherMutationsCreateSetClauseMissing(), nil)
	}

	// Extract CREATE part (everything before SET)
	createPart := strings.TrimSpace(normalized[:setIdx])

	// Extract SET part and any trailing clauses after SET.
	setTail := strings.TrimSpace(normalized[setIdx+4:])
	setPart := setTail
	trailingPart := ""
	if postSetIdx := firstPostSetClauseIndex(setTail); postSetIdx >= 0 {
		setPart = strings.TrimSpace(setTail[:postSetIdx])
		trailingPart = strings.TrimSpace(setTail[postSetIdx:])
	}
	setPart = collapseChainedSetClauses(setPart)

	// Create through the shared CREATE core (validation, property references,
	// named paths), then apply SET through the shared per-entity applicator
	// (pipelineApplySet -> applySetToNodeWithContext /
	// applySetToRelationshipWithContext) - the same code MATCH ... SET and
	// MERGE ... SET use, so null removal, label chains, map / entity sources
	// and value errors behave identically on every route. Assignment shapes
	// and functions are validated for the whole statement beforehand
	// (validateSetClauseScope).
	created, err := e.createFromPattern(ctx, createPart)
	if err != nil {
		return nil, localizedError(localization.CypherMutationsCreateInCreateSetFailed(err), err)
	}
	createdNodes, createdEdges, createdPaths := created.nodes, created.edges, created.paths
	addQueryStats(result.Stats, created.result.Stats)

	row := make(pipelineRow, len(createdNodes)+len(createdEdges))
	for name, node := range createdNodes {
		row[name] = node
	}
	for name, edge := range createdEdges {
		row[name] = edge
	}
	for _, variable := range pipelineSetTargetVariables(e.splitSetAssignments(setPart)) {
		if row[variable] == nil {
			return nil, localizedError(localization.CypherMutationsUnknownSetVariable(variable), nil)
		}
	}
	setStats, _, err := e.pipelineApplySet(ctx, []pipelineRow{row}, "SET "+setPart)
	if err != nil {
		return nil, err
	}
	if setStats != nil {
		addQueryStats(result.Stats, setStats)
	}

	// Process trailing clauses in CREATE...SET pipelines.
	// Supported forms:
	//   CREATE (...) SET ... CREATE (...) RETURN ...
	//   CREATE (...) SET ... WITH x CREATE (...) RETURN ...
	remainingTrailing := strings.TrimSpace(trailingPart)
	for remainingTrailing != "" {
		upperTrailing := strings.ToUpper(remainingTrailing)

		if strings.HasPrefix(upperTrailing, "RETURN ") {
			break
		}

		if strings.HasPrefix(upperTrailing, "WITH ") {
			afterWith := strings.TrimSpace(remainingTrailing[len("WITH "):])
			nextClauseIdx := -1
			for _, kw := range []string{"CREATE", "RETURN"} {
				if idx := findKeywordIndex(afterWith, kw); idx >= 0 {
					if nextClauseIdx == -1 || idx < nextClauseIdx {
						nextClauseIdx = idx
					}
				}
			}
			if nextClauseIdx < 0 {
				return nil, localizedError(localization.CypherMutationsCreateSetWithContinuationRequired(), nil)
			}
			withProjection := strings.TrimSpace(afterWith[:nextClauseIdx])
			remainingTrailing = strings.TrimSpace(afterWith[nextClauseIdx:])
			if withProjection == "" {
				return nil, localizedError(localization.CypherMutationsWithClauseEmpty(), nil)
			}

			projectedNodes := make(map[string]*storage.Node)
			projectedEdges := make(map[string]*storage.Edge)
			projectedPaths := make(map[string]PathResult)
			for _, item := range strings.Split(withProjection, ",") {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				alias := ""
				expr := item
				if asIdx := findKeywordIndex(item, "AS"); asIdx > 0 {
					expr = strings.TrimSpace(item[:asIdx])
					alias = strings.TrimSpace(item[asIdx+2:])
				}
				if alias == "" {
					alias = strings.TrimSpace(expr)
				}
				if alias == "" {
					return nil, localizedError(localization.CypherResidualWithItemInvalid(item), nil)
				}

				if node, ok := createdNodes[strings.TrimSpace(expr)]; ok {
					projectedNodes[alias] = node
					continue
				}
				if edge, ok := createdEdges[strings.TrimSpace(expr)]; ok {
					projectedEdges[alias] = edge
					continue
				}
				if path, ok := createdPaths[strings.TrimSpace(expr)]; ok {
					projectedPaths[alias] = path
					continue
				}

				evaluated := e.evaluateExpressionWithContext(ctx, expr, createdNodes, createdEdges)
				switch v := evaluated.(type) {
				case *storage.Node:
					if v != nil {
						projectedNodes[alias] = v
					}
				case *storage.Edge:
					if v != nil {
						projectedEdges[alias] = v
					}
				default:
					return nil, localizedError(localization.CypherResidualCreateSetScopeEntityRequired(item), nil)
				}
			}

			clear(createdNodes)
			for k, v := range projectedNodes {
				createdNodes[k] = v
			}
			clear(createdEdges)
			for k, v := range projectedEdges {
				createdEdges[k] = v
			}
			createdPaths = projectedPaths
			continue
		}

		if strings.HasPrefix(upperTrailing, "CREATE ") {
			createChunk := remainingTrailing
			if retIdx := findKeywordIndex(remainingTrailing, "RETURN"); retIdx > 0 {
				createChunk = strings.TrimSpace(remainingTrailing[:retIdx])
				remainingTrailing = strings.TrimSpace(remainingTrailing[retIdx:])
			} else {
				remainingTrailing = ""
			}
			if err := e.applyCreateClausesInScope(ctx, createChunk, createdNodes, createdEdges, result, store); err != nil {
				return nil, err
			}
			continue
		}

		return nil, localizedError(localization.CypherMutationsCreateSetTrailingClauseUnsupported(strings.Fields(remainingTrailing)[0]), nil)
	}

	// Handle RETURN clause
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(remainingTrailing)), "RETURN ") {
		returnPart := strings.TrimSpace(strings.TrimSpace(remainingTrailing)[len("RETURN "):])
		returnItems := e.parseReturnItems(returnPart)
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns = append(result.Columns, item.alias)
			} else {
				result.Columns = append(result.Columns, item.expr)
			}
			row[i] = e.projectCreatedReturnItem(ctx, item, createdNodes, createdEdges, createdPaths)
		}

		result.Rows = [][]interface{}{row}
	} else {
		// No RETURN clause - return created entities by default
		for _, node := range createdNodes {
			if len(result.Columns) == 0 {
				result.Columns = append(result.Columns, "node")
			}
			if len(result.Rows) == 0 {
				result.Rows = append(result.Rows, []interface{}{})
			}
			result.Rows[0] = append(result.Rows[0], node)
		}
	}

	return result, nil
}

// applyCreateClausesInScope executes one or more CREATE clauses and mutates the
// provided variable scope maps so trailing CREATE chains in CREATE...SET queries
// can reference previously created variables.
func (e *StorageExecutor) applyCreateClausesInScope(
	ctx context.Context,
	createPart string,
	nodeVars map[string]*storage.Node,
	edgeVars map[string]*storage.Edge,
	result *ExecuteResult,
	store storage.Engine,
) error {
	for _, clause := range SplitByCreate(strings.TrimSpace(createPart)) {
		if clause = strings.TrimSpace(clause); clause == "" {
			continue
		}
		if _, err := e.createPatternsInScope(ctx, clause, nodeVars, edgeVars, result); err != nil {
			return err
		}
	}
	return nil
}

// resolveSetMergeSourceFromParams resolves identifier or dotted-path map sources from params.
// Supported forms:
//   - row
//   - row.properties
func resolveSetMergeSourceFromParams(params map[string]interface{}, source string) (interface{}, bool) {
	if params == nil {
		return nil, false
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, false
	}
	parts, ok := parseParamPathParts(source)
	if !ok || len(parts) == 0 {
		return nil, false
	}

	current, ok := params[parts[0]]
	if !ok {
		return nil, false
	}
	for _, part := range parts[1:] {
		switch m := current.(type) {
		case map[string]interface{}:
			next, exists := m[part]
			if !exists {
				return nil, false
			}
			current = next
		case map[interface{}]interface{}:
			next, exists := m[part]
			if !exists {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}

// parseParamPathParts parses dotted and bracketed map access paths.
// Supported forms:
//   - row
//   - row.props
//   - row['props']
//   - row["props"]
//   - row.meta['inner'].value
func parseParamPathParts(source string) ([]string, bool) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, false
	}

	parts := make([]string, 0, 4)
	i := 0
	readIdent := func(start int) (string, int, bool) {
		if start >= len(source) {
			return "", start, false
		}
		ch := source[start]
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
			return "", start, false
		}
		j := start + 1
		for j < len(source) {
			c := source[j]
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				break
			}
			j++
		}
		return source[start:j], j, true
	}

	root, next, ok := readIdent(i)
	if !ok {
		return nil, false
	}
	parts = append(parts, root)
	i = next

	for i < len(source) {
		switch source[i] {
		case '.':
			i++
			part, j, ok := readIdent(i)
			if !ok {
				return nil, false
			}
			parts = append(parts, part)
			i = j
		case '[':
			i++
			for i < len(source) && isWhitespace(source[i]) {
				i++
			}
			if i >= len(source) {
				return nil, false
			}
			quote := source[i]
			if quote != '\'' && quote != '"' {
				return nil, false
			}
			i++
			start := i
			for i < len(source) && source[i] != quote {
				i++
			}
			if i >= len(source) {
				return nil, false
			}
			key := source[start:i]
			i++ // close quote
			for i < len(source) && isWhitespace(source[i]) {
				i++
			}
			if i >= len(source) || source[i] != ']' {
				return nil, false
			}
			i++ // close bracket
			if key == "" {
				return nil, false
			}
			parts = append(parts, key)
		default:
			return nil, false
		}
	}
	return parts, true
}

// executeMultipleCreates handles queries with multiple CREATE statements.
// Example: CREATE (a:Person {name: "Alice"}) CREATE (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b) RETURN a, b
func (e *StorageExecutor) executeMultipleCreates(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Context to track created nodes and edges for variable references
	nodeContext := make(map[string]*storage.Node)
	edgeContext := make(map[string]*storage.Edge)

	// Every CREATE clause of the statement is planned into one plan, written
	// once before RETURN (or at the end), so a failure in a later clause
	// leaves nothing from an earlier one.
	plan := acquireCreatePlan()
	defer plan.release()

	// Split into CREATE segments
	segments := e.splitMultipleCreates(cypher)

	// Process each CREATE segment
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		upperSeg := strings.ToUpper(segment)

		if strings.HasPrefix(upperSeg, "CREATE") {
			createContent := strings.TrimSpace(segment[6:])
			// A CREATE clause has one scope; the shared CREATE core binds every
			// node of the clause for later patterns, clauses and RETURN.
			if _, err := e.planCreatePatterns(ctx, createContent, nodeContext, edgeContext, plan); err != nil {
				return nil, err
			}
		} else if strings.HasPrefix(upperSeg, "WITH") {
			// Process WITH clause - extract variables and update context
			// WITH a, b means we keep a and b in context, filtering out others
			withClause := strings.TrimSpace(segment[4:]) // Skip "WITH"

			// Remove WHERE, ORDER BY, SKIP, LIMIT if present
			for _, keyword := range []string{" WHERE ", " ORDER BY ", " SKIP ", " LIMIT "} {
				if idx := findKeywordIndex(withClause, keyword); idx >= 0 {
					withClause = withClause[:idx]
				}
			}
			if withClause == "*" {
				continue
			}

			// Parse WITH items (similar to RETURN items)
			withItems := e.parseReturnItems(withClause)

			// Create new context with only the WITH variables
			newNodeContext := make(map[string]*storage.Node)
			newEdgeContext := make(map[string]*storage.Edge)
			row := make(pipelineRow, util.SafePreallocSum(len(nodeContext), len(edgeContext)))
			for name, node := range nodeContext {
				row[name] = node
			}
			for name, edge := range edgeContext {
				row[name] = edge
			}

			for _, item := range withItems {
				alias := item.expr
				if item.alias != "" {
					alias = item.alias
				}
				val, ok := e.evaluateRowExpression(item.expr, row)
				if !ok {
					return nil, localizedError(localization.CypherResidualCreateWithExpressionInvalid(item.expr), nil)
				}
				switch v := val.(type) {
				case *storage.Node:
					if v != nil {
						newNodeContext[alias] = v
					}
				case *storage.Edge:
					if v != nil {
						newEdgeContext[alias] = v
					}
				}
			}

			// Update context
			nodeContext = newNodeContext
			edgeContext = newEdgeContext
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			if err := e.applyCreatePlan(ctx, plan, result); err != nil {
				return nil, err
			}
			clear(plan.nodes)
			clear(plan.edges)
			plan.nodes = plan.nodes[:0]
			plan.edges = plan.edges[:0]
			// Build result from context
			returnClause := strings.TrimSpace(segment[6:])
			items := e.parseReturnItems(returnClause)

			// The CREATE routes' one RETURN projection (count(...) over the
			// created row included, #507).
			row := make([]interface{}, len(items))
			for i, item := range items {
				if item.alias != "" {
					result.Columns = append(result.Columns, item.alias)
				} else {
					result.Columns = append(result.Columns, item.expr)
				}
				row[i] = e.projectCreatedReturnItem(ctx, item, nodeContext, edgeContext, nil)
			}
			result.Rows = append(result.Rows, row)
		}
	}
	if err := e.applyCreatePlan(ctx, plan, result); err != nil {
		return nil, err
	}

	return result, nil
}

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present

func whereClauseReferencesOnlyVariable(whereClause string, variable string) bool {
	clause := strings.TrimSpace(whereClause)
	variable = strings.TrimSpace(variable)
	if clause == "" || variable == "" {
		return false
	}
	for i := 0; i < len(clause); {
		if !isIdentByte(clause[i]) {
			i++
			continue
		}
		start := i
		for i < len(clause) && isIdentByte(clause[i]) {
			i++
		}
		if i < len(clause) && clause[i] == '.' {
			refVar := clause[start:i]
			if !strings.EqualFold(refVar, variable) {
				return false
			}
		}
	}
	return true
}

var notRelationshipExistencePattern = regexp.MustCompile(`(?i)^NOT\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*->\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$`)

func parseNotRelationshipExistenceTerm(term string) (startVar, edgeType, endVar string, ok bool) {
	m := notRelationshipExistencePattern.FindStringSubmatch(strings.TrimSpace(term))
	if len(m) != 4 {
		return "", "", "", false
	}
	return strings.TrimSpace(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3]), true
}

func hasRelationshipOfType(store storage.Engine, startID, endID storage.NodeID, edgeType string) (bool, error) {
	edges, err := store.GetEdgesBetween(startID, endID)
	if err != nil {
		return false, err
	}
	for _, edge := range edges {
		if edge != nil && strings.EqualFold(edge.Type, edgeType) {
			return true, nil
		}
	}
	return false, nil
}

func (e *StorageExecutor) buildCombinationsUsingWhereJoin(
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	whereClause string,
) ([]map[string]*storage.Node, bool) {
	if len(patternMatches) < 2 || strings.TrimSpace(whereClause) == "" {
		return nil, false
	}
	varOrder := make([]string, 0, len(patternMatches))
	varToNodes := make(map[string][]*storage.Node, len(patternMatches))
	for _, pm := range patternMatches {
		if pm.variable == "" {
			return nil, false
		}
		if _, exists := varToNodes[pm.variable]; exists {
			return nil, false
		}
		varOrder = append(varOrder, pm.variable)
		varToNodes[pm.variable] = pm.nodes
	}

	eqConstraints := make([]cartesianEqConstraint, 0, 4)
	for _, term := range splitTopLevelAndConjuncts(whereClause) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		lv, lp, rv, rp, ok := parseCartesianVarPropEqualityTerm(term)
		if !ok {
			continue
		}
		if _, lok := varToNodes[lv]; !lok {
			continue
		}
		if _, rok := varToNodes[rv]; !rok {
			continue
		}
		eqConstraints = append(eqConstraints, cartesianEqConstraint{
			leftVar:   lv,
			leftProp:  lp,
			rightVar:  rv,
			rightProp: rp,
		})
	}
	if len(eqConstraints) == 0 {
		return nil, false
	}

	parent := make(map[string]string, len(varOrder))
	for _, v := range varOrder {
		parent[v] = v
	}
	var find func(string) string
	find = func(v string) string {
		p := parent[v]
		if p != v {
			parent[v] = find(p)
		}
		return parent[v]
	}
	union := func(a, b string) {
		ra := find(a)
		rb := find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	for _, c := range eqConstraints {
		union(c.leftVar, c.rightVar)
	}

	components := make(map[string][]string, len(varOrder))
	componentOrder := make([]string, 0, len(varOrder))
	for _, v := range varOrder {
		root := find(v)
		if _, exists := components[root]; !exists {
			componentOrder = append(componentOrder, root)
		}
		components[root] = append(components[root], v)
	}

	constraintsByRoot := make(map[string][]cartesianEqConstraint, len(componentOrder))
	for _, c := range eqConstraints {
		root := find(c.leftVar)
		constraintsByRoot[root] = append(constraintsByRoot[root], c)
	}

	indexCache := map[string]map[string][]*storage.Node{}
	buildIndex := func(variable, prop string) map[string][]*storage.Node {
		cacheKey := variable + "|" + prop
		if idx, exists := indexCache[cacheKey]; exists {
			return idx
		}
		nodes := varToNodes[variable]
		idx := make(map[string][]*storage.Node, len(nodes))
		for _, n := range nodes {
			if n == nil || n.Properties == nil {
				continue
			}
			v, ok := n.Properties[prop]
			if !ok {
				continue
			}
			k := cartesianValueKey(v)
			idx[k] = append(idx[k], n)
		}
		indexCache[cacheKey] = idx
		return idx
	}

	buildComponentRows := func(vars []string, constraints []cartesianEqConstraint) []map[string]*storage.Node {
		if len(vars) == 0 {
			return []map[string]*storage.Node{{}}
		}
		if len(vars) == 1 {
			v := vars[0]
			rows := make([]map[string]*storage.Node, 0, len(varToNodes[v]))
			for _, n := range varToNodes[v] {
				rows = append(rows, map[string]*storage.Node{v: n})
			}
			return rows
		}

		seed := vars[0]
		rows := make([]map[string]*storage.Node, 0, len(varToNodes[seed]))
		for _, n := range varToNodes[seed] {
			rows = append(rows, map[string]*storage.Node{seed: n})
		}
		added := map[string]struct{}{seed: {}}

		for len(added) < len(vars) {
			progressed := false
			for _, c := range constraints {
				baseVar := ""
				baseProp := ""
				newVar := ""
				newProp := ""
				if _, ok := added[c.leftVar]; ok {
					if _, seen := added[c.rightVar]; !seen {
						baseVar, baseProp = c.leftVar, c.leftProp
						newVar, newProp = c.rightVar, c.rightProp
					}
				}
				if baseVar == "" {
					if _, ok := added[c.rightVar]; ok {
						if _, seen := added[c.leftVar]; !seen {
							baseVar, baseProp = c.rightVar, c.rightProp
							newVar, newProp = c.leftVar, c.leftProp
						}
					}
				}
				if baseVar == "" {
					continue
				}

				idx := buildIndex(newVar, newProp)
				nextRows := make([]map[string]*storage.Node, 0, len(rows))
				for _, row := range rows {
					baseNode := row[baseVar]
					if baseNode == nil || baseNode.Properties == nil {
						continue
					}
					baseVal, ok := baseNode.Properties[baseProp]
					if !ok {
						continue
					}
					for _, matchNode := range idx[cartesianValueKey(baseVal)] {
						joined := make(map[string]*storage.Node, util.SafePreallocSum(len(row), 1))
						for k, v := range row {
							joined[k] = v
						}
						joined[newVar] = matchNode
						nextRows = append(nextRows, joined)
					}
				}
				rows = nextRows
				added[newVar] = struct{}{}
				progressed = true
			}
			if !progressed {
				return nil
			}
		}

		filtered := make([]map[string]*storage.Node, 0, len(rows))
		for _, row := range rows {
			keep := true
			for _, c := range constraints {
				ln := row[c.leftVar]
				rn := row[c.rightVar]
				if ln == nil || rn == nil || ln.Properties == nil || rn.Properties == nil {
					keep = false
					break
				}
				lv, lok := ln.Properties[c.leftProp]
				rv, rok := rn.Properties[c.rightProp]
				if !lok || !rok || cartesianValueKey(lv) != cartesianValueKey(rv) {
					keep = false
					break
				}
			}
			if keep {
				filtered = append(filtered, row)
			}
		}
		return filtered
	}

	componentRows := make([][]map[string]*storage.Node, 0, len(componentOrder))
	for _, root := range componentOrder {
		rows := buildComponentRows(components[root], constraintsByRoot[root])
		if rows == nil {
			return nil, false
		}
		componentRows = append(componentRows, rows)
	}

	out := []map[string]*storage.Node{{}}
	for _, rows := range componentRows {
		if len(rows) == 0 {
			return []map[string]*storage.Node{}, true
		}
		next := make([]map[string]*storage.Node, 0, util.SafePreallocProduct(len(out), len(rows)))
		for _, base := range out {
			for _, row := range rows {
				merged := make(map[string]*storage.Node, util.SafePreallocSum(len(base), len(row)))
				for k, v := range base {
					merged[k] = v
				}
				for k, v := range row {
					merged[k] = v
				}
				next = append(next, merged)
			}
		}
		out = next
	}
	return out, true
}

// tryResolveMatchNodesByIDFromWhere attempts to resolve all MATCH pattern
// variables via direct ID lookup from WHERE elementId(var) = $param or
// id(var) = $param predicates. This is the relationship-create hot path:
// it avoids label scans when both endpoints are identified by ID.
//
// It collects all WHERE clauses across MATCH segments, splits on top-level
// AND, and tries to parse each conjunct as an ID equality predicate. If
// every pattern variable can be resolved this way, it returns a single
// combination map. Otherwise it returns (nil, false) and the caller falls
// through to the generic label-scan path.
func (e *StorageExecutor) tryResolveMatchNodesByIDFromWhere(
	ctx context.Context,
	matchClauses []string,
	params map[string]interface{},
) (map[string]*storage.Node, bool) {
	if len(matchClauses) == 0 {
		return nil, false
	}

	// Collect all pattern variables and their label constraints, plus all
	// WHERE conjuncts across segments.
	type varInfo struct {
		labels     []string
		properties map[string]interface{}
	}
	variables := make(map[string]*varInfo)
	var allWhereTerms []string

	for _, clause := range matchClauses {
		whereForClause := ""
		patternPart := clause
		if whereIdx := findKeywordIndex(clause, "WHERE"); whereIdx > 0 {
			whereForClause = strings.TrimSpace(clause[whereIdx+5:])
			patternPart = strings.TrimSpace(clause[:whereIdx])
		}

		// Extract pattern variables from comma-separated node patterns.
		patterns := e.splitNodePatterns(patternPart)
		for _, p := range patterns {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			// Skip relationship patterns — they contain arrows.
			if patternHasRelationship(p) {
				return nil, false
			}
			info := e.parseNodePattern(ctx, p)
			if info.variable == "" {
				continue
			}
			variables[info.variable] = &varInfo{
				labels:     info.labels,
				properties: info.properties,
			}
		}

		if whereForClause != "" {
			for _, term := range splitTopLevelAndConjuncts(whereForClause) {
				term = strings.TrimSpace(term)
				if term != "" {
					allWhereTerms = append(allWhereTerms, term)
				}
			}
		}
	}

	if len(variables) == 0 || len(allWhereTerms) == 0 {
		return nil, false
	}

	// Try to resolve each variable from an ID equality predicate.
	resolved := make(map[string]*storage.Node, len(variables))
	for _, term := range allWhereTerms {
		varName, node := e.resolveNodeFromIDEqualityTerm(term, params)
		if varName == "" || node == nil {
			continue
		}
		if _, known := variables[varName]; !known {
			continue
		}
		resolved[varName] = node
	}

	// All variables must be resolved for the fast path to apply.
	if len(resolved) != len(variables) {
		return nil, false
	}

	// Validate label and property constraints from the patterns.
	for varName, info := range variables {
		node := resolved[varName]
		if len(info.labels) > 0 && !nodeHasAnyLabel(node, info.labels) {
			return nil, false
		}
		for k, v := range info.properties {
			if node.Properties[k] != v {
				return nil, false
			}
		}
	}

	return resolved, true
}

// resolveNodeFromIDEqualityTerm parses a single WHERE conjunct of the form
// elementId(<var>) = <value> or id(<var>) = <value> and resolves the node.
// Returns the variable name and resolved node, or ("", nil) if the term
// doesn't match the expected shape.
func (e *StorageExecutor) resolveNodeFromIDEqualityTerm(
	term string,
	params map[string]interface{},
) (string, *storage.Node) {
	term = strings.TrimSpace(term)
	if term == "" {
		return "", nil
	}

	eqIdx := strings.Index(term, "=")
	if eqIdx <= 0 || eqIdx >= len(term)-1 {
		return "", nil
	}
	// Reject !=, <=, >=, ==
	if eqIdx > 0 && (term[eqIdx-1] == '!' || term[eqIdx-1] == '<' || term[eqIdx-1] == '>') {
		return "", nil
	}
	if eqIdx+1 < len(term) && term[eqIdx+1] == '=' {
		return "", nil
	}

	left := strings.TrimSpace(term[:eqIdx])
	right := strings.TrimSpace(term[eqIdx+1:])
	if left == "" || right == "" {
		return "", nil
	}

	// LHS must be id(var) or elementId(var).
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
		return "", nil
	}
	if varName == "" {
		return "", nil
	}

	// Resolve the RHS value.
	var idValue string
	if strings.HasPrefix(right, "$") {
		// Parameter reference.
		if params == nil {
			return "", nil
		}
		paramName := strings.TrimSpace(right[1:])
		raw, ok := params[paramName]
		if !ok {
			return "", nil
		}
		s, ok := raw.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", nil
		}
		idValue = strings.TrimSpace(s)
	} else {
		// Literal value — strip surrounding quotes.
		idValue = strings.TrimSpace(right)
		if len(idValue) >= 2 {
			if (idValue[0] == '\'' && idValue[len(idValue)-1] == '\'') ||
				(idValue[0] == '"' && idValue[len(idValue)-1] == '"') {
				idValue = idValue[1 : len(idValue)-1]
			}
		}
		if idValue == "" {
			return "", nil
		}
	}

	// Strip canonical element ID prefix (4:dbname:rawid).
	lookupID := idValue
	if kind == "elementId" || strings.HasPrefix(lookupID, "4:") {
		if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 && parts[0] == "4" {
			lookupID = parts[2]
		}
	}

	node, err := e.storage.GetNode(storage.NodeID(lookupID))
	if err != nil || node == nil {
		return "", nil
	}

	return varName, node
}
