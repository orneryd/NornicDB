package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A pipeline MATCH or MERGE clause evaluates its pattern's property map for
// every row. The general route renders each row's values into the pattern
// text and parses that text again (materializePipelinePropertyExpressions,
// parseNodePattern). A pattern template parses the clause text once and
// evaluates each property expression for the row directly: the same values,
// without the text round trip. An expression the row evaluator doesn't
// resolve (a parameter path the text parser reads, …) makes that row take
// the general route. TestPipelinePatternTemplateMatchesTextRoute runs both on
// the same values.

// pipelinePropertyExpression is one key: expression pair of a pattern's
// property map.
type pipelinePropertyExpression struct {
	key  string
	expr string
}

// pipelinePropertyExpressions splits a property map's text ("{k: e, …}")
// into its pairs, as parseProperties reads them. ok is false for a pair it
// would skip (no key), which the general route handles.
func (e *StorageExecutor) pipelinePropertyExpressions(props string) ([]pipelinePropertyExpression, bool) {
	props = strings.TrimSpace(props)
	if strings.HasPrefix(props, "{") && strings.HasSuffix(props, "}") {
		props = props[1 : len(props)-1]
	}
	if strings.TrimSpace(props) == "" {
		return nil, true
	}
	pairs := e.splitPropertyPairs(props)
	out := make([]pipelinePropertyExpression, 0, len(pairs))
	for _, pair := range pairs {
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			return nil, false
		}
		out = append(out, pipelinePropertyExpression{
			key:  normalizePropertyKey(strings.TrimSpace(pair[:colon])),
			expr: strings.TrimSpace(pair[colon+1:]),
		})
	}
	return out, true
}

// evaluatePipelineProperties evaluates pattern predicates for a row without
// dropping null-valued equality constraints. ok is false when an expression
// doesn't resolve.
func (e *StorageExecutor) evaluatePipelineProperties(ctx context.Context, expressions []pipelinePropertyExpression, row pipelineRow) (map[string]interface{}, bool) {
	props := make(map[string]interface{}, len(expressions))
	for _, expression := range expressions {
		value, ok := e.evaluateRowExpressionWithContext(ctx, expression.expr, row)
		if !ok {
			return nil, false
		}
		props[expression.key] = normalizePropValue(value)
	}
	return props, true
}

// pipelineNodeMatchTemplate is a single-node MATCH clause parsed once.
type pipelineNodeMatchTemplate struct {
	// usable is false for a clause the template doesn't cover.
	usable       bool
	pattern      string
	where        string
	pathVariable string
	variable     string
	labels       []string
	labelErr     error
	properties   []pipelinePropertyExpression
}

var pipelineNodeMatchTemplates = newBoundedCache[string, *pipelineNodeMatchTemplate](512)

// pipelineNodeMatchTemplateFor parses a MATCH clause's single node pattern
// once per clause text.
func (e *StorageExecutor) pipelineNodeMatchTemplateFor(clause string) *pipelineNodeMatchTemplate {
	if template, ok := pipelineNodeMatchTemplates.get(clause); ok {
		return template
	}
	template := &pipelineNodeMatchTemplate{}
	pattern := strings.TrimSpace(clause[len("MATCH"):])
	if whereIndex := topLevelKeywordIndex(pattern, "WHERE"); whereIndex >= 0 {
		template.where = normalizePipelineWhitespace(pattern[whereIndex+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:whereIndex])
	}
	template.pattern = pattern
	if !strings.Contains(pattern, "-[") && !strings.Contains(pattern, "]-") && len(e.splitNodePatterns(pattern)) == 1 {
		// A path assignment (p = (n)) takes the general route.
		template.pathVariable = extractPathAssignmentVariable(pattern)
		head, props := splitNodePatternProperties(pattern)
		template.variable, template.labels, template.labelErr = parseNodeHead(head)
		template.properties, template.usable = e.pipelinePropertyExpressions(props)
		template.usable = template.usable && template.variable != "" && template.pathVariable == ""
	}
	pipelineNodeMatchTemplates.put(clause, template)
	return template
}

// node returns the row's node pattern, ok false when the row takes the
// general route.
func (t *pipelineNodeMatchTemplate) node(ctx context.Context, e *StorageExecutor, row pipelineRow) (nodePatternInfo, bool) {
	if !t.usable {
		return nodePatternInfo{}, false
	}
	if len(t.properties) == 0 {
		return nodePatternInfo{variable: t.variable, labels: t.labels, labelErr: t.labelErr}, true
	}
	props, ok := e.evaluatePipelineProperties(ctx, t.properties, row)
	if !ok {
		return nodePatternInfo{}, false
	}
	return nodePatternInfo{variable: t.variable, labels: t.labels, properties: props, labelErr: t.labelErr}, true
}

// pipelinePropertiesKey is a key for a node pattern's evaluated properties:
// equal for equal maps (relationshipMergeIdentityValuesKey's encoding). ok
// is false for a value that key can't encode.
func pipelinePropertiesKey(props map[string]interface{}) (string, bool) {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded, ok := relationshipMergeIdentityValuesKey(props, keys)
	if !ok {
		return "", false
	}
	return strings.Join(keys, "\x01") + "\x02" + encoded, true
}

// pipelineMergeTemplate is a relationship MERGE clause whose endpoints are
// bare variables, parsed once. Its rows with both endpoints bound are looked
// up with the relationship's properties evaluated for the row; a row with no
// match takes the general route, which creates.
type pipelineMergeTemplate struct {
	usable     bool
	shape      *mergeRelationshipShape
	properties []pipelinePropertyExpression
}

var pipelineMergeTemplates = newBoundedCache[string, *pipelineMergeTemplate](512)

// pipelineMergeTemplateFor parses a MERGE clause once per clause text. The
// template covers `MERGE (a)-[r:T {…}]->(b)` without ON CREATE / ON MATCH SET.
func (e *StorageExecutor) pipelineMergeTemplateFor(clause string) *pipelineMergeTemplate {
	if template, ok := pipelineMergeTemplates.get(clause); ok {
		return template
	}
	template := &pipelineMergeTemplate{}
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "MERGE") {
		parts := splitMergeClauseActions(strings.TrimSpace(body[len("MERGE"):]))
		pattern := parts.pattern
		if open, _ := firstRelationshipBracket(pattern); open >= 0 && parts.onCreate.len() == 0 && parts.onMatch.len() == 0 {
			if shape, err := e.parseMergeRelationshipShape(pattern); err == nil &&
				isSimpleIdentifier(strings.TrimSpace(shape.startContent)) && isSimpleIdentifier(strings.TrimSpace(shape.endContent)) &&
				shape.relType != "" {
				shape.startContent = strings.TrimSpace(shape.startContent)
				shape.endContent = strings.TrimSpace(shape.endContent)
				template.shape = shape
				template.properties, template.usable = e.pipelinePropertyExpressions(shape.relProps)
			}
		}
	}
	pipelineMergeTemplates.put(clause, template)
	return template
}

// pattern returns the row's relationship pattern, ok false when the row
// takes the general route (an endpoint not bound to a node, a property the
// row evaluator can't evaluate).
func (t *pipelineMergeTemplate) pattern(ctx context.Context, e *StorageExecutor, row pipelineRow) (*mergeRelationshipPattern, *storage.Node, *storage.Node, bool) {
	if !t.usable {
		return nil, nil, nil, false
	}
	startNode, _ := row[t.shape.startContent].(*storage.Node)
	endNode, _ := row[t.shape.endContent].(*storage.Node)
	if startNode == nil || endNode == nil {
		return nil, nil, nil, false
	}
	props, ok := e.evaluatePipelineProperties(ctx, t.properties, row)
	if !ok {
		return nil, nil, nil, false
	}
	return &mergeRelationshipPattern{
		pathVariable:     t.shape.pathVariable,
		direction:        t.shape.direction,
		startVariable:    t.shape.startContent,
		endVariable:      t.shape.endContent,
		startNodePattern: nodePatternInfo{variable: t.shape.startContent},
		endNodePattern:   nodePatternInfo{variable: t.shape.endContent},
		relVariable:      t.shape.relVariable,
		relType:          t.shape.relType,
		properties:       props,
	}, startNode, endNode, true
}
