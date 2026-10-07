// CREATE clause implementation for NornicDB.
// This file contains CREATE execution for nodes and relationships.

package cypher

import (
	"context"
	"fmt"
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

// createPlan is what one CREATE clause writes: every node and relationship,
// fully parsed, evaluated and validated, in creation order.
type createPlan struct {
	nodes        []*storage.Node
	edges        []*storage.Edge
	nodeBindings map[string]*storage.Node
	edgeBindings map[string]*storage.Edge
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
	if cap(p.nodes) > 256 || cap(p.edges) > 256 || len(p.nodeBindings) > 64 || len(p.edgeBindings) > 64 {
		return
	}
	clear(p.nodes)
	clear(p.edges)
	clear(p.nodeBindings)
	clear(p.edgeBindings)
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
func (e *StorageExecutor) pipelineCreateSource(ctx context.Context, source pipelineRowSource, clauses []pipelineClause) ([]pipelineRow, *ExecuteResult, bool, error) {
	plan := acquireCreatePlan()
	defer plan.release()
	var out []pipelineRow
	var planningError error
	completed := source(func(row pipelineRow) bool {
		if planningError = ctx.Err(); planningError != nil {
			return false
		}
		clear(plan.nodeBindings)
		clear(plan.edgeBindings)
		var newRow pipelineRow
		newRow, planningError = e.pipelinePlanCreateRow(ctx, row, clauses, plan)
		if planningError != nil {
			return false
		}
		out = append(out, newRow)
		return true
	})
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, nil, true, failure
	}
	if planningError != nil {
		return nil, nil, true, planningError
	}
	if !completed {
		return nil, nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, true, err
	}
	created := &ExecuteResult{Stats: &QueryStats{}}
	if err := e.applyCreatePlan(ctx, plan, created); err != nil {
		return nil, nil, true, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
	}
	return out, created, true, nil
}

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
// into createdNodes / createdEdges for later patterns and clauses. Adjacent
// pipeline CREATE clauses share one plan and are published atomically.
func (e *StorageExecutor) planCreatePatterns(ctx context.Context, pattern string, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, plan *createPlan) (map[string]PathResult, error) {
	for _, fragment := range splitTopLevelComma(pattern) {
		_, body := parseCreatePathAssignment(strings.TrimSpace(fragment))
		if !strings.HasPrefix(body, "(") || !strings.HasSuffix(strings.TrimSpace(body), ")") {
			return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "Invalid CREATE pattern")
		}
	}
	patterns := e.createPatternSplitFor(pattern)

	// First, create all nodes
	var createdPaths map[string]PathResult
	for _, nodePatternStr := range patterns.nodes {
		nodePatternStr = strings.TrimSpace(nodePatternStr)
		if nodePatternStr == "" {
			continue
		}
		if _, err := e.planCreateNode(ctx, nodePatternStr, createdNodes, createdEdges, plan); err != nil {
			return nil, err
		}
	}

	// Then, create all relationships using variable references or inline node definitions
	for _, relPatternStr := range patterns.relationships {
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

			// Property keys follow the map-key rule (a symbolic name, or a
			// backtick-quoted name holding any character).
			if err := validateCreatePropertyKeys("["+relStr+"]", relProps); err != nil {
				return nil, err
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
			if createdPaths == nil {
				createdPaths = make(map[string]PathResult)
			}
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
// statement, if it has one, through the canonical RETURN operator.
func (e *StorageExecutor) projectCreateReturn(ctx context.Context, out *createOutcome) error {
	if out.returnIdx <= 0 {
		return nil
	}
	row := e.mergeBindingRow(ctx, out.nodes, out.edges)
	for name, path := range out.paths {
		row[name] = e.pathToMap(path)
	}
	projected, err := e.projectMergeReturn(ctx, []pipelineRow{row}, out.cypher[out.returnIdx:])
	if err != nil {
		return err
	}
	out.result.Columns = projected.Columns
	out.result.Rows = projected.Rows
	return nil
}

// prepareCreateNodePattern parses and validates one CREATE node pattern. It is
// shared by every CREATE route (createFromPattern and the auto-commit bulk fast
// path tryAsyncCreateNodeBatch) so they reject the same patterns: malformed
// property maps, an empty label after ':', invalid or reserved labels, invalid
// property keys and values. Property values that reference variables created
// earlier in the statement (b {name: a.name}) are resolved against nodes and
// relationships.
func (e *StorageExecutor) prepareCreateNodePattern(ctx context.Context, pattern string, nodes map[string]*storage.Node, relationships map[string]*storage.Edge) (nodePatternInfo, error) {
	var parameterProperties map[string]interface{}
	if head, props := splitNodePatternProperties(pattern); props == "" {
		if parameterAt := indexByteOutsideBackticks(head, '$'); parameterAt >= 0 {
			value, resolved := resolveDirectParamRef(ctx, strings.TrimSpace(head[parameterAt:]))
			properties, isMap := toStringAnyMap(value)
			if !resolved || !isMap {
				return nodePatternInfo{}, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidPropertyValue", "CREATE properties must be a map")
			}
			parameterProperties = cloneNodePropertiesMap(properties)
			pattern = "(" + strings.TrimSpace(head[:parameterAt]) + ")"
		}
	}
	if err := e.validateCreatePatternPropertyMap(ctx, pattern); err != nil {
		return nodePatternInfo{}, err
	}
	nodePattern := e.parseNodePattern(ctx, pattern)
	if parameterProperties != nil {
		nodePattern.properties = parameterProperties
	}
	e.resolveCreatePropertyReferences(ctx, pattern, nodePattern.properties, nodes, relationships)
	for key, value := range nodePattern.properties {
		if value == nil {
			delete(nodePattern.properties, key)
		}
	}

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
	// SECURITY: Validate property keys and values. Keys follow the map-key
	// rule (a symbolic name, or a backtick-quoted name holding any
	// character).
	if err := validateCreatePropertyKeys(pattern, nodePattern.properties); err != nil {
		return nodePatternInfo{}, err
	}
	for key, val := range nodePattern.properties {
		if _, ok := val.(invalidPropertyValue); ok {
			return nodePatternInfo{}, localizedError(localization.CypherMutationsInvalidPropertyValue(key), nil)
		}
	}
	if err := validatePropertyValues(nodePattern.properties); err != nil {
		return nodePatternInfo{}, err
	}
	if err := e.validateNoTextFallthrough(ctx, pattern, nodePattern.properties, nodes, relationships); err != nil {
		return nodePatternInfo{}, err
	}
	return nodePattern, nil
}

// namedPathAssignmentPrefix reports whether pattern begins a named-path
// assignment, `p = (` (spaces optional).
func namedPathAssignmentPrefix(pattern string) bool {
	i := indexByteOutsideBackticks(pattern, '=')
	if i <= 0 {
		return false
	}
	name := strings.TrimSpace(pattern[:i])
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		return strings.TrimSpace(pattern[i+1:]) != ""
	}
	if name == "" || !isCypherIdentifierStart(name[0]) {
		return false
	}
	for k := 1; k < len(name); k++ {
		if !isCypherIdentifierPart(name[k]) {
			return false
		}
	}
	return strings.TrimSpace(pattern[i+1:]) != ""
}

// validateNoTextFallthrough rejects a CREATE property map whose value is the
// expression's own source text: the evaluator's "return as string" fallback
// fires for expressions it cannot evaluate (an unbound variable, a malformed
// expression), and Neo4j rejects those as syntax errors instead of storing
// the query text as data (#514). A quoted string literal never trips this:
// its stored value differs from the expression text by its quotes. Before
// rejecting, the expression is re-run through the error-propagating row
// evaluator with the pattern's bound nodes and relationships in scope: a
// genuine evaluation failure (1 / 0) keeps its ArithmeticError, and an
// expression the row evaluator resolves is fine — only a truly unevaluable
// expression is a syntax error.
func (e *StorageExecutor) validateNoTextFallthrough(ctx context.Context, pattern string, properties map[string]interface{}, nodes map[string]*storage.Node, relationships map[string]*storage.Edge) error {
	open := indexByteOutsideBackticks(pattern, '{')
	if open < 0 {
		return nil
	}
	close := e.findMatchingBrace(pattern, open)
	if close < 0 {
		return nil
	}
	for _, pair := range e.splitPropertyPairs(pattern[open+1 : close]) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:separator]))
		expression := strings.TrimSpace(pair[separator+1:])
		if expression == "" {
			continue
		}
		value, ok := properties[key]
		if !ok {
			continue
		}
		text, isString := value.(string)
		if !isString || text != expression {
			continue
		}
		if isWholeCypherQuotedString(expression) {
			continue
		}
		values := make(map[string]interface{}, len(nodes)+len(relationships)+len(valueBindingsFromContext(ctx)))
		for name, value := range valueBindingsFromContext(ctx) {
			values[name] = value
		}
		for name, node := range nodes {
			values[name] = node.Properties
		}
		for name, edge := range relationships {
			values[name] = edge.Properties
		}
		if _, resolved, evalErr := e.evaluateRowValue(expression, values); evalErr != nil {
			return evalErr
		} else if resolved {
			continue
		}
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			fmt.Sprintf("Invalid input '%s': unknown expression", truncateQuery(expression, 80)))
	}
	return nil
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

// splitCreatePatterns splits a CREATE pattern into individual patterns (nodes and relationships)
// respecting parentheses depth, string literals, and handling relationship syntax.
// IMPORTANT: This properly handles content inside string literals (single/double quotes)
// so that Cypher-like content inside strings is not parsed as relationship patterns.
func (e *StorageExecutor) splitCreatePatterns(pattern string) []string {
	return e.createPatternSplitFor(pattern).all
}

func (e *StorageExecutor) scanCreatePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	inRelationship := false
	braceDepth := 0

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		if c == '\'' || c == '"' || c == '`' {
			end := skipCypherQuotedText(pattern, i, c)
			current.WriteString(pattern[i:end])
			i = end - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(pattern, i); end >= 0 {
				current.WriteString(pattern[i:end])
				i = end - 1
				continue
			}
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

// splitNodePatterns splits a CREATE pattern into individual node patterns
// (Used for simple node-only patterns and by other parts of the system)
func (e *StorageExecutor) splitNodePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	braceDepth := 0

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '\'' || c == '"' || c == '`' {
			end := skipCypherQuotedText(pattern, i, c)
			if depth > 0 {
				current.WriteString(pattern[i:end])
			}
			i = end - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(pattern, i); end >= 0 {
				if depth > 0 {
					current.WriteString(pattern[i:end])
				}
				i = end - 1
				continue
			}
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
		propsEnd := findMatchingDelimiter(relStr, propsStart, '{', '}')
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
		lv, lp, rv, rp, offset, ok := parseCartesianVarPropEqualityTerm(term)
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
			offset:    offset,
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
				// leftVar.leftProp = rightVar.rightProp + offset. Expanding
				// from a base variable to a new variable needs the new
				// variable's value expressed through the base's: from left to
				// right it is left - offset, from right to left it is
				// right + offset.
				lookupOffset := int64(0)
				if _, ok := added[c.leftVar]; ok {
					if _, seen := added[c.rightVar]; !seen {
						baseVar, baseProp = c.leftVar, c.leftProp
						newVar, newProp = c.rightVar, c.rightProp
						lookupOffset = -c.offset
					}
				}
				if baseVar == "" {
					if _, ok := added[c.rightVar]; ok {
						if _, seen := added[c.leftVar]; !seen {
							baseVar, baseProp = c.rightVar, c.rightProp
							newVar, newProp = c.leftVar, c.leftProp
							lookupOffset = c.offset
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
					lookupKey := cartesianValueKey(baseVal)
					if lookupOffset != 0 {
						// Shift the value exactly: integer offsets use integer
						// arithmetic (no float64 rounding above 2^53, #692).
						// Floats keep their float keys so float offset joins
						// still find their index entries.
						if key, ok := cartesianShiftValueKey(baseVal, lookupOffset); ok {
							lookupKey = key
						} else if baseNum, numeric := toFloat64(baseVal); numeric {
							lookupKey = cartesianValueKey(baseNum + float64(lookupOffset))
						} else {
							continue
						}
					}
					for _, matchNode := range idx[lookupKey] {
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
				if !lok || !rok {
					keep = false
					break
				}
				if c.offset == 0 {
					if cartesianValueKey(lv) != cartesianValueKey(rv) {
						keep = false
						break
					}
					continue
				}
				// lv must equal rv + offset exactly (#692): integer values
				// compare through integer arithmetic, others through float64.
				if !cartesianOffsetValuesEqual(lv, rv, c.offset) {
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
	lowerLeft := lowerASCII(left)
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

// validateCreatePropertyKeys checks a CREATE pattern's property keys against
// the map-key rule: a key is a symbolic name, or a backtick-quoted name
// (holding any character) in the pattern text. Only a key that is neither
// is checked on the text by validateStaticMapKeys, which reports it, so a
// pattern with plain keys isn't scanned again.
func validateCreatePropertyKeys(text string, properties map[string]interface{}) error {
	for key := range properties {
		if isValidIdentifier(key) || strings.Contains(text, "`"+strings.ReplaceAll(key, "`", "``")+"`") {
			continue
		}
		if err := validateStaticMapKeys(text); err != nil {
			return err
		}
		return localizedError(localization.CypherMutationsInvalidPropertyKey(key), nil)
	}
	return nil
}
