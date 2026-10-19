package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// A pipeline CREATE clause is planned per row: planCreatePatterns splits and
// parses the pattern text for every UNWIND row (~7% of UNWIND-100 CPU in the
// PR #1043 write-path profiles). pipelineCreateTemplate parses the clause
// once and evaluates each property expression for the row directly — the same
// values, without the text round trip — mirroring pipelineNodeMatchTemplate
// and pipelineMergeTemplate. A shape the template doesn't cover makes the
// row fall back to the general route (and a template-level verdict is
// cached per clause text).

// createNodeTemplate is one CREATE node pattern parsed once:
// `(v:Label {prop: expr})`.
type createNodeTemplate struct {
	variable   string
	labels     []string
	labelErr   error
	properties []pipelinePropertyExpression
}

// createEdgeTemplate is one CREATE relationship chain link parsed once:
// `(source)-[:TYPE {prop: expr}]->(target)`, direction included.
type createEdgeTemplate struct {
	relVar     string
	relType    string
	sourceVar  string
	targetVar  string
	isReverse  bool
	properties []pipelinePropertyExpression
}

// pipelineCreateTemplate is a CREATE clause parsed once per clause text.
type pipelineCreateTemplate struct {
	usable bool
	nodes  []createNodeTemplate
	edges  []createEdgeTemplate
}

var pipelineCreateTemplates = newBoundedCache[string, *pipelineCreateTemplate](512, boundedCacheStringShardIndex)

// pipelineCreateTemplateFor parses a CREATE clause once per clause text. It
// covers the seeding shapes: bare node patterns and relationship chains whose
// endpoints are bound variables, with static labels and types and a literal
// property-map body. Anything else (path assignments, inline endpoint
// definitions, parameter maps, dynamic labels/types, multiple types) makes
// the template unusable and the general route stays authoritative.
func (e *StorageExecutor) pipelineCreateTemplateFor(clause string) *pipelineCreateTemplate {
	if template, ok := pipelineCreateTemplates.get(clause); ok {
		return template
	}
	template := e.buildPipelineCreateTemplate(pipelineClauseBody(clause, "CREATE"))
	pipelineCreateTemplates.put(clause, template)
	return template
}

// buildPipelineCreateTemplate parses one CREATE pattern body into node and
// edge templates. usable=false on any unsupported shape.
func (e *StorageExecutor) buildPipelineCreateTemplate(pattern string) *pipelineCreateTemplate {
	template := &pipelineCreateTemplate{}
	fragments := splitTopLevelComma(pattern)
	for _, raw := range fragments {
		fragment := strings.TrimSpace(raw)
		if fragment == "" {
			continue
		}
		pathVar, body := parseCreatePathAssignment(fragment)
		if pathVar != "" {
			return template
		}
		if !strings.HasPrefix(body, "(") || !strings.HasSuffix(strings.TrimSpace(body), ")") {
			return template
		}
		if strings.Contains(body, "-[") || strings.Contains(body, "]-") {
			edges, ok := e.parseCreateEdgeTemplates(body)
			if !ok {
				return template
			}
			template.edges = append(template.edges, edges...)
			continue
		}
		node, ok := e.parseCreateNodeTemplate(body)
		if !ok {
			return template
		}
		template.nodes = append(template.nodes, node)
	}
	if len(template.nodes) == 0 && len(template.edges) == 0 {
		return template
	}
	// Property keys follow the map-key rule; the general route rejects a
	// bad key per row, the template rejects it once at build (and the
	// general route stays authoritative for the statement).
	for _, node := range template.nodes {
		keys := make(map[string]interface{}, len(node.properties))
		for _, expression := range node.properties {
			keys[expression.key] = nil
		}
		if err := validateCreatePropertyKeys("", keys); err != nil {
			return template
		}
	}
	for _, edge := range template.edges {
		keys := make(map[string]interface{}, len(edge.properties))
		for _, expression := range edge.properties {
			keys[expression.key] = nil
		}
		if err := validateCreatePropertyKeys("", keys); err != nil {
			return template
		}
	}
	template.usable = true
	return template
}

// parseCreateNodeTemplate parses one `(v:Label {exprs})` node pattern. ok is
// false for a parameter map or an unresolvable property-map split.
func (e *StorageExecutor) parseCreateNodeTemplate(body string) (createNodeTemplate, bool) {
	head, props := splitNodePatternProperties(body)
	if props == "" {
		// A parameter map form (($props), (n $props)) is row-independent in
		// the general route but not covered here.
		return createNodeTemplate{}, false
	}
	variable, labels, labelErr := parseNodeHead(head)
	if variable == "" {
		return createNodeTemplate{}, false
	}
	expressions, ok := e.pipelinePropertyExpressions(props)
	if !ok {
		return createNodeTemplate{}, false
	}
	return createNodeTemplate{
		variable:   variable,
		labels:     labels,
		labelErr:   labelErr,
		properties: expressions,
	}, true
}

// parseCreateEdgeTemplates parses one relationship chain into one edge
// template per link. ok is false for inline endpoint definitions, parameter
// maps, variable-length patterns, dynamic or multi types, or a chain with an
// unrecognized remainder.
func (e *StorageExecutor) parseCreateEdgeTemplates(body string) ([]createEdgeTemplate, bool) {
	_, current := parseCreatePathAssignment(body)
	var out []createEdgeTemplate
	for current != "" {
		sourceContent, relStr, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(current)
		if err != nil {
			return nil, false
		}
		sourceVar, ok := bareEndpointVariable("(" + sourceContent + ")")
		if !ok {
			return nil, false
		}
		targetVar, ok := bareEndpointVariable("(" + targetContent + ")")
		if !ok {
			return nil, false
		}
		if rest, _, parameter := splitPatternParameterMap(relStr); parameter {
			_ = rest
			return nil, false
		}
		declaration := relationshipDeclarationOf(relStr)
		if declaration.hasLength || declaration.invalidType != "" {
			return nil, false
		}
		if relVar := declaration.variable; relVar != "" && !isSimpleIdentifier(relVar) {
			return nil, false
		}
		relType := declaration.singleType()
		if relType == "" || strings.Contains(relType, "$") {
			return nil, false
		}
		var expressions []pipelinePropertyExpression
		if declaration.properties != "" {
			var ok bool
			expressions, ok = e.pipelinePropertyExpressions(declaration.properties)
			if !ok {
				return nil, false
			}
		}
		out = append(out, createEdgeTemplate{
			relVar:     declaration.variable,
			relType:    relType,
			sourceVar:  sourceVar,
			targetVar:  targetVar,
			isReverse:  isReverse,
			properties: expressions,
		})
		if remainder == "" {
			return out, true
		}
		if strings.HasPrefix(remainder, "-[") || strings.HasPrefix(remainder, "<-[") {
			current = "(" + targetContent + ")" + remainder
			continue
		}
		return nil, false
	}
	return out, true
}

// bareEndpointVariable reads an endpoint as a bound variable: "(v)" with a
// simple identifier and nothing else. Anything else (inline definitions,
// labels, property maps) is the general route's job.
func bareEndpointVariable(content string) (string, bool) {
	content = strings.TrimSpace(content)
	if !strings.HasPrefix(content, "(") || !strings.HasSuffix(content, ")") {
		return "", false
	}
	variable := strings.TrimSpace(content[1 : len(content)-1])
	if variable == "" || !isSimpleIdentifier(variable) {
		return "", false
	}
	return variable, true
}

// plan evaluates the template for one row and appends its nodes and edges to
// plan, binding variables in nodes/edges. ok=false sends the row to the
// general route (an endpoint not bound, or an expression the row evaluator
// can't resolve — including the string-fallthrough case the general route
// reports as an error). Planning is atomic: on any failure the nodes and
// edges already staged by this call are rolled back, so the general route
// replans the whole clause instead of duplicating the template's prefix.
func (t *pipelineCreateTemplate) plan(ctx context.Context, e *StorageExecutor, row pipelineRow, nodes map[string]*storage.Node, edges map[string]*storage.Edge, plan *createPlan) (map[string]PathResult, bool, error) {
	if !t.usable {
		return nil, false, nil
	}
	nodeStart, edgeStart := len(plan.nodes), len(plan.edges)
	type nodeUndo struct {
		name string
		prev *storage.Node
	}
	type edgeUndo struct {
		name string
		prev *storage.Edge
	}
	var nodesAdded []nodeUndo
	var edgesAdded []edgeUndo
	rollback := func() {
		plan.nodes = plan.nodes[:nodeStart]
		plan.edges = plan.edges[:edgeStart]
		for _, undo := range nodesAdded {
			if undo.prev == nil {
				delete(nodes, undo.name)
			} else {
				nodes[undo.name] = undo.prev
			}
		}
		for _, undo := range edgesAdded {
			if undo.prev == nil {
				delete(edges, undo.name)
			} else {
				edges[undo.name] = undo.prev
			}
		}
	}
	for _, nodeTemplate := range t.nodes {
		if nodeTemplate.labelErr != nil {
			rollback()
			return nil, false, nodeTemplate.labelErr
		}
		var props map[string]interface{}
		if len(nodeTemplate.properties) > 0 {
			var ok bool
			props, ok = e.evaluatePipelineProperties(ctx, nodeTemplate.properties, row)
			if !ok {
				rollback()
				return nil, false, nil
			}
			// CREATE drops null properties, exactly as prepareCreateNodePattern
			// deletes them from the parsed map.
			for key, value := range props {
				if value == nil {
					delete(props, key)
				}
			}
			if !createTemplatePropsRowOK(nodeTemplate.properties, props) {
				rollback()
				return nil, false, nil
			}
			if err := validatePropertyValues(props); err != nil {
				rollback()
				return nil, false, err
			}
			for key, value := range props {
				if _, bad := value.(invalidPropertyValue); bad {
					rollback()
					return nil, false, localizedError(localization.CypherMutationsInvalidPropertyValue(key), nil)
				}
			}
		}
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodeTemplate.labels,
			Properties: props,
		}
		plan.nodes = append(plan.nodes, node)
		nodesAdded = append(nodesAdded, nodeUndo{name: nodeTemplate.variable, prev: nodes[nodeTemplate.variable]})
		nodes[nodeTemplate.variable] = node
	}
	for _, edgeTemplate := range t.edges {
		source := nodes[edgeTemplate.sourceVar]
		if source == nil {
			rollback()
			return nil, false, nil
		}
		target := nodes[edgeTemplate.targetVar]
		if target == nil {
			rollback()
			return nil, false, nil
		}
		var props map[string]interface{}
		if len(edgeTemplate.properties) > 0 {
			var ok bool
			props, ok = e.evaluatePipelineProperties(ctx, edgeTemplate.properties, row)
			if !ok {
				rollback()
				return nil, false, nil
			}
			// CREATE drops null properties on relationships too.
			for key, value := range props {
				if value == nil {
					delete(props, key)
				}
			}
			if !createTemplatePropsRowOK(edgeTemplate.properties, props) {
				rollback()
				return nil, false, nil
			}
			if err := validatePropertyValues(props); err != nil {
				rollback()
				return nil, false, err
			}
		}
		startNode, endNode := source, target
		if edgeTemplate.isReverse {
			startNode, endNode = target, source
		}
		edge := &storage.Edge{
			ID:         storage.EdgeID(e.generateID()),
			
			
			Type:       edgeTemplate.relType,
			Properties: props,
		}
		if err := storage.BindRelationshipEndpoints(edge, startNode, endNode); err != nil { rollback(); return nil, false, err }
		plan.edges = append(plan.edges, edge)
		if edgeTemplate.relVar != "" {
			edgesAdded = append(edgesAdded, edgeUndo{name: edgeTemplate.relVar, prev: edges[edgeTemplate.relVar]})
			edges[edgeTemplate.relVar] = edge
		}
	}
	return nil, true, nil
}

// createTemplatePropsRowOK mirrors validateNoTextFallthrough for one row: a
// string value equal to its own raw expression text is the text-embedding
// fallthrough the general route resolves (or rejects) against its own value
// set — the template defers that row to it.
func createTemplatePropsRowOK(expressions []pipelinePropertyExpression, props map[string]interface{}) bool {
	for _, expression := range expressions {
		value, ok := props[expression.key]
		if !ok {
			continue
		}
		text, isString := value.(string)
		if !isString || text != strings.TrimSpace(expression.expr) {
			continue
		}
		if isWholeCypherQuotedString(expression.expr) {
			continue
		}
		return false
	}
	return true
}
