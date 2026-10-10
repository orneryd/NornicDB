package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// normalizeBareRelationshipPatterns gives relationships without square
// brackets the same parser input as their explicit empty-bracket form. Cypher
// permits all three forms: --, -->, and <--.
func normalizeBareRelationshipPatterns(query string) string {
	var output strings.Builder
	output.Grow(len(query))
	quote := byte(0)
	for index := 0; index < len(query); index++ {
		character := query[index]
		if quote != 0 {
			output.WriteByte(character)
			if character == '\\' && quote != '`' && index+1 < len(query) {
				index++
				output.WriteByte(query[index])
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			quote = character
			output.WriteByte(character)
			continue
		}
		if character != ')' {
			output.WriteByte(character)
			continue
		}

		cursor := index + 1
		for cursor < len(query) && isWhitespace(query[cursor]) {
			cursor++
		}
		connectorStart := cursor
		if cursor < len(query) && query[cursor] == '<' {
			cursor++
		}
		if cursor+1 >= len(query) || query[cursor] != '-' || query[cursor+1] != '-' {
			output.WriteByte(character)
			continue
		}
		cursor += 2
		if cursor < len(query) && query[cursor] == '>' {
			cursor++
		}
		for cursor < len(query) && isWhitespace(query[cursor]) {
			cursor++
		}
		if cursor >= len(query) || query[cursor] != '(' {
			output.WriteByte(character)
			continue
		}

		output.WriteByte(')')
		connector := strings.ReplaceAll(query[connectorStart:cursor], " ", "")
		connector = strings.ReplaceAll(connector, "\t", "")
		connector = strings.ReplaceAll(connector, "\n", "")
		switch connector {
		case "--":
			output.WriteString("-[]-")
		case "-->":
			output.WriteString("-[]->")
		case "<--":
			output.WriteString("<-[]-")
		default:
			output.WriteString(query[index+1 : cursor])
		}
		index = cursor - 1
	}
	return output.String()
}

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

// mergeMapReads reports whether the property map in text (a node pattern's
// content "b:L {k: v}", or a relationship's "{k: v}") has a value that reads
// one of variables: a MERGE pattern's own node or relationship, which the
// value can read only once it is bound.
func (e *StorageExecutor) mergeMapReads(text string, variables ...string) bool {
	if text == "" || strings.IndexByte(text, '{') < 0 {
		return false
	}
	properties := text
	if !strings.HasPrefix(strings.TrimSpace(text), "{") {
		_, properties = splitNodePatternProperties("(" + text + ")")
	}
	properties = strings.TrimSpace(properties)
	if len(properties) < 2 || properties[0] != '{' {
		return false
	}
	for _, pair := range e.splitPropertyPairs(properties[1 : len(properties)-1]) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		for _, variable := range expressionFreeVariables(pair[separator+1:]) {
			for _, name := range variables {
				if name != "" && variable == name {
					return true
				}
			}
		}
	}
	return false
}
