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
	// they are bound (executeMergeRelationshipWithContext).
	endContent string
	relProps   string
}

// mergeRelationshipShape is a relationship MERGE pattern's text split into
// its parts; its property maps are still text.
type mergeRelationshipShape struct {
	pathVariable string
	direction    mergeRelationshipDirection
	startContent string
	endContent   string
	relVariable  string
	relType      string
	relProps     string
}

// parseMergeRelationshipShape converts every supported relationship
// direction into one shape. It deliberately reuses the CREATE relationship
// parser so delimiter, quoted-string, and nested-list handling cannot
// diverge.
func (e *StorageExecutor) parseMergeRelationshipShape(pattern string) (*mergeRelationshipShape, error) {
	shape := &mergeRelationshipShape{}
	pattern = strings.TrimSpace(pattern)
	if shape.pathVariable = extractPathAssignmentVariable(pattern); shape.pathVariable != "" {
		pattern = strings.TrimSpace(pattern[strings.Index(pattern, "=")+1:])
	}

	openBracket, closeBracket := firstRelationshipBracket(pattern)
	if openBracket < 0 || closeBracket < 0 {
		return nil, localizedError(localization.CypherMutationsRelationshipPatternUnmatchedBracket(), nil)
	}

	normalized := pattern
	afterRelationship := strings.TrimSpace(pattern[closeBracket+1:])
	incoming := strings.HasSuffix(strings.TrimSpace(pattern[:openBracket]), "<-")
	if !incoming && strings.HasPrefix(afterRelationship, "-(") {
		shape.direction = mergeRelationshipUndirected
		normalized = pattern[:closeBracket+1] + "->" + strings.TrimSpace(afterRelationship[1:])
	}

	startContent, relationshipContent, endContent, reverse, remainder, err := e.parseCreateRelPatternWithVars(normalized)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(remainder) != "" {
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
func (e *StorageExecutor) parseMergeRelationshipPattern(
	ctx context.Context,
	pattern string,
	nodeContext map[string]*storage.Node,
	relContext map[string]*storage.Edge,
) (*mergeRelationshipPattern, error) {
	shape, err := e.parseMergeRelationshipShape(pattern)
	if err != nil {
		return nil, err
	}
	parsed := &mergeRelationshipPattern{
		pathVariable:     shape.pathVariable,
		direction:        shape.direction,
		startNodePattern: e.parseMergeEndpointPattern(ctx, shape.startContent, nodeContext, relContext),
		endNodePattern:   e.parseMergeEndpointPattern(ctx, shape.endContent, nodeContext, relContext),
		relVariable:      shape.relVariable,
		relType:          shape.relType,
		properties:       make(map[string]interface{}),
		endContent:       shape.endContent,
		relProps:         shape.relProps,
	}
	parsed.startVariable = parsed.startNodePattern.variable
	parsed.endVariable = parsed.endNodePattern.variable
	if shape.relProps != "" {
		parsed.properties = e.parseMergeProperties(ctx, shape.relProps, nodeContext, relContext)
	}
	return parsed, nil
}

// parseMergeEndpointPattern parses an endpoint of a relationship MERGE
// ("a:L {k: v}") with its properties read by parseMergeProperties.
func (e *StorageExecutor) parseMergeEndpointPattern(ctx context.Context, content string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) nodePatternInfo {
	head, props := splitNodePatternProperties("(" + content + ")")
	info := nodePatternInfo{}
	info.variable, info.labels, info.labelErr = parseNodeHead(head)
	info.properties = e.parseMergeProperties(ctx, props, nodeContext, relContext)
	return info
}

func findParsedMergeRelationships(
	store storage.Engine,
	cache *relationshipMergeIdentityCache,
	pattern *mergeRelationshipPattern,
	startNode *storage.Node,
	endNode *storage.Node,
, probe *pipelineCancellation) ([]*storage.Edge, error) {
	lookupStart, lookupEnd := startNode, endNode
	if pattern.direction == mergeRelationshipIncoming {
		lookupStart, lookupEnd = endNode, startNode
	}
	matches, err := cache.relationships(store, lookupStart.ID, lookupEnd.ID, pattern.relType, pattern.properties, probe)
	if err != nil || pattern.direction != mergeRelationshipUndirected {
		return matches, err
	}
	reverse, err := cache.relationships(store, lookupEnd.ID, lookupStart.ID, pattern.relType, pattern.properties, probe)
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
