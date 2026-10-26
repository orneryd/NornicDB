package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type mergeRelationshipDirection uint8

const (
	mergeRelationshipOutgoing mergeRelationshipDirection = iota
	mergeRelationshipIncoming
	mergeRelationshipUndirected
)

type mergeRelationshipPattern struct {
	pathVariable     string
	startVariable    string
	endVariable      string
	startNodePattern nodePatternInfo
	endNodePattern   nodePatternInfo
	relVariable      string
	relType          string
	properties       map[string]interface{}
	direction        mergeRelationshipDirection
	// endContent and relProps are the end node's and the relationship's
	// text: a map that reads the pattern's own variables is read again once
	// they are bound (executeMergeWithContext).
	endContent braceView
	relProps   braceView
}

// mergeRelationshipShape is a relationship MERGE pattern's text split into
// its parts; its property maps are still text.
type mergeRelationshipShape struct {
	pathVariable string
	direction    mergeRelationshipDirection
	startContent braceView
	endContent   braceView
	relVariable  string
	relType      string
	relProps     braceView
}

// parseMergeRelationshipShape converts every supported relationship
// direction into one shape. It deliberately reuses the CREATE relationship
// parser so delimiter, quoted-string, and nested-list handling cannot
// diverge.
func (e *StorageExecutor) parseMergeRelationshipShape(view braceView) (*mergeRelationshipShape, error) {
	shape := &mergeRelationshipShape{}
	view = view.trim()
	pattern := view.text()
	if shape.pathVariable = extractPathAssignmentVariable(pattern); shape.pathVariable != "" {
		view = view.window(strings.Index(pattern, "=")+1, len(pattern)).trim()
		pattern = view.text()
	}

	openBracket, closeBracket := readRelationshipBracket(view, 0, requireClosedRelationship)
	if openBracket < 0 || closeBracket < 0 {
		return nil, localizedError(localization.CypherMutationsRelationshipPatternUnmatchedBracket(), nil)
	}

	normalized := view
	afterRelationship := strings.TrimSpace(pattern[closeBracket+1:])
	incoming := strings.HasSuffix(strings.TrimSpace(pattern[:openBracket]), "<-")
	if !incoming && strings.HasPrefix(afterRelationship, "-(") {
		shape.direction = mergeRelationshipUndirected
		target := view.window(len(pattern)-len(afterRelationship)+1, len(pattern)).trim()
		normalized = view.assembleCode([]braceView{view.window(0, closeBracket+1).wrap("", "->"), target}, "")
	}

	startContent, relationshipContent, endContent, reverse, remainder, err := e.parseCreateRelPatternWithVars(normalized)
	if err != nil {
		return nil, err
	}
	if !remainder.empty() {
		return nil, localizedError(localization.CypherMutationsRelationshipPropertiesInvalid(), nil)
	}
	if shape.direction != mergeRelationshipUndirected {
		if reverse {
			shape.direction = mergeRelationshipIncoming
		} else {
			shape.direction = mergeRelationshipOutgoing
		}
	}
	shape.startContent, shape.endContent = startContent, endContent
	shape.relVariable, shape.relType, shape.relProps, err = parseCreateRelationshipContent(relationshipContent)
	if err != nil {
		return nil, err
	}
	return shape, nil
}

// parseMergeRelationshipPattern parses a relationship MERGE pattern's text
// with its property maps read against the bound nodes and relationships
// (parseMergeProperties).
func (e *StorageExecutor) parseMergeRelationshipPattern(ctx context.Context, pattern braceView, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*mergeRelationshipPattern, error) {
	shape, err := e.parseMergeRelationshipShape(pattern)
	if err != nil {
		return nil, err
	}
	start, err := e.parseMergeEndpointPattern(ctx, shape.startContent, nodeContext, relContext)
	if err != nil {
		return nil, err
	}
	end, err := e.parseMergeEndpointPattern(ctx, shape.endContent, nodeContext, relContext)
	if err != nil {
		return nil, err
	}
	props, err := e.parseMergeProperties(ctx, shape.relProps, nodeContext, relContext)
	if err != nil {
		return nil, err
	}
	return &mergeRelationshipPattern{
		pathVariable: shape.pathVariable, direction: shape.direction,
		startNodePattern: start, endNodePattern: end, startVariable: start.variable, endVariable: end.variable,
		relVariable: shape.relVariable, relType: shape.relType, properties: props,
		endContent: shape.endContent, relProps: shape.relProps,
	}, nil
}

// parseMergeEndpointPattern parses an endpoint of a relationship MERGE
// ("a:L {k: v}") with its properties read by parseMergeProperties.
func (e *StorageExecutor) parseMergeEndpointPattern(ctx context.Context, content braceView, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (nodePatternInfo, error) {
	head, props := splitNodePatternProperties(content.wrap("(", ")"))
	info := nodePatternInfo{}
	info.variable, info.labels, info.labelErr = parseNodeHead(head)
	var err error
	info.properties, err = e.parseMergeProperties(ctx, props, nodeContext, relContext)
	return info, err
}

func findParsedMergeRelationships(
	store storage.Engine,
	cache *relationshipMergeIdentityCache,
	pattern *mergeRelationshipPattern,
	startNode *storage.Node,
	endNode *storage.Node,
) ([]*storage.Edge, error) {
	lookupStart, lookupEnd := startNode, endNode
	if pattern.direction == mergeRelationshipIncoming {
		lookupStart, lookupEnd = endNode, startNode
	}
	matches, err := cache.relationships(store, lookupStart.ID, lookupEnd.ID, pattern.relType, pattern.properties)
	if err != nil || pattern.direction != mergeRelationshipUndirected {
		return matches, err
	}
	reverse, err := cache.relationships(store, lookupEnd.ID, lookupStart.ID, pattern.relType, pattern.properties)
	if err != nil {
		return nil, err
	}
	seen := make(map[storage.EdgeID]struct{}, len(matches)+len(reverse))
	combined := make([]*storage.Edge, 0, len(matches)+len(reverse))
	for _, edge := range append(matches, reverse...) {
		if _, exists := seen[edge.ID]; exists {
			continue
		}
		seen[edge.ID] = struct{}{}
		combined = append(combined, edge)
	}
	return combined, nil
}
