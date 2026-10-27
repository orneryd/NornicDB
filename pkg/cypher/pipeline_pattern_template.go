package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A pipeline MATCH or MERGE clause evaluates its pattern's property map for
// every row. A pattern template parses the clause text once and evaluates
// each property expression for the row directly. Unsupported syntax or binding
// scope selects the general route before evaluation; expression errors are final.
// TestPipelinePatternTemplateMatchesTextRoute runs both on the same values.

// pipelinePropertyExpression is one key: expression pair of a pattern's
// property map.
type pipelinePropertyExpression struct {
	key  string
	expr string
}

// pipelinePropertyExpressions splits a property map's text ("{k: e, …}")
// into its pairs. Unsupported syntax declines before any value is evaluated.
func (e *StorageExecutor) pipelinePropertyExpressions(props string) ([]pipelinePropertyExpression, bool) {
	expressions, err := parsePropertyExpressions(props)
	return expressions, err == nil
}

// evaluatePipelineProperties evaluates pattern predicates for a row without
// dropping null-valued equality constraints. Failed evaluation returns an error.
func (e *StorageExecutor) evaluatePipelineProperties(ctx context.Context, expressions []pipelinePropertyExpression, row pipelineRow) (map[string]interface{}, error) {
	return e.materializePropertyExpressions(ctx, expressions, row, true, false)
}

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

var pipelineNodeMatchTemplates = newBoundedCache[string, *pipelineNodeMatchTemplate](512, boundedCacheStringShardIndex)

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
	if e.isSingleNodePattern(pattern) {
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
func (t *pipelineNodeMatchTemplate) node(ctx context.Context, e *StorageExecutor, row pipelineRow) (nodePatternInfo, bool, error) {
	if !t.usable {
		return nodePatternInfo{}, false, nil
	}
	props, err := e.evaluatePipelineProperties(ctx, t.properties, row)
	if err != nil {
		return nodePatternInfo{}, true, err
	}
	return nodePatternInfo{variable: t.variable, labels: t.labels, properties: props, labelErr: t.labelErr}, true, nil
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

var pipelineMergeTemplates = newBoundedCache[string, *pipelineMergeTemplate](512, boundedCacheStringShardIndex)

// pipelineMergeTemplateFor parses a MERGE clause once per clause text. The
// template covers `MERGE (a)-[r:T {…}]->(b)` without ON CREATE / ON MATCH SET.
func (e *StorageExecutor) pipelineMergeTemplateFor(clause string) *pipelineMergeTemplate {
	if template, ok := pipelineMergeTemplates.get(clause); ok {
		return template
	}
	template := &pipelineMergeTemplate{}
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "MERGE") {
		parts := splitMergeClauseActions(strings.TrimSpace(body[len("MERGE"):]), nil)
		pattern := parts.pattern
		if open, _ := readRelationshipBracket(pattern, 0, requireClosedRelationship); open >= 0 && parts.onCreate.len() == 0 && parts.onMatch.len() == 0 {
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
// takes the general route because syntax or endpoint bindings are unsupported.
func (t *pipelineMergeTemplate) pattern(ctx context.Context, e *StorageExecutor, row pipelineRow) (*mergeRelationshipPattern, *storage.Node, *storage.Node, bool, error) {
	if !t.usable {
		return nil, nil, nil, false, nil
	}
	startNode, _ := row[t.shape.startContent].(*storage.Node)
	endNode, _ := row[t.shape.endContent].(*storage.Node)
	if startNode == nil || endNode == nil {
		return nil, nil, nil, false, nil
	}
	props, err := e.evaluatePipelineProperties(ctx, t.properties, row)
	if err != nil {
		return nil, nil, nil, true, err
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
	}, startNode, endNode, true, nil
}
