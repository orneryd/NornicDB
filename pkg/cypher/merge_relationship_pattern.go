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
}

// parseMergeRelationshipPattern converts every supported relationship direction
// into one execution shape. It deliberately reuses the CREATE relationship
// parser so delimiter, quoted-string, and nested-list handling cannot diverge.
func (e *StorageExecutor) parseMergeRelationshipPattern(
	ctx context.Context,
	pattern string,
	nodeContext map[string]*storage.Node,
	relContext map[string]*storage.Edge,
) (*mergeRelationshipPattern, error) {
	parsed := &mergeRelationshipPattern{properties: make(map[string]interface{})}
	pattern = strings.TrimSpace(pattern)
	if parsed.pathVariable = extractPathAssignmentVariable(pattern); parsed.pathVariable != "" {
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
		parsed.direction = mergeRelationshipUndirected
		normalized = pattern[:closeBracket+1] + "->" + strings.TrimSpace(afterRelationship[1:])
	}

	startContent, relationshipContent, endContent, reverse, remainder, err := e.parseCreateRelPatternWithVars(normalized)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(remainder) != "" {
		return nil, localizedError(localization.CypherMutationsRelationshipPropertiesInvalid(), nil)
	}
	if parsed.direction != mergeRelationshipUndirected {
		if reverse {
			parsed.direction = mergeRelationshipIncoming
		} else {
			parsed.direction = mergeRelationshipOutgoing
		}
	}

	parsed.startNodePattern = e.parseNodePattern(ctx, "("+startContent+")")
	parsed.endNodePattern = e.parseNodePattern(ctx, "("+endContent+")")
	parsed.startVariable = parsed.startNodePattern.variable
	parsed.endVariable = parsed.endNodePattern.variable
	parsed.startNodePattern.properties = e.resolveMergePropsWithContext(ctx, parsed.startNodePattern.properties, nodeContext, relContext)
	parsed.endNodePattern.properties = e.resolveMergePropsWithContext(ctx, parsed.endNodePattern.properties, nodeContext, relContext)
	parsed.relVariable, parsed.relType, remainder, err = parseCreateRelationshipContent(relationshipContent)
	if err != nil {
		return nil, err
	}
	if remainder != "" {
		parsed.properties = e.parseProperties(ctx, remainder)
	}
	parsed.properties = e.resolveMergePropsWithContext(ctx, parsed.properties, nodeContext, relContext)
	return parsed, nil
}

func findParsedMergeRelationships(
	store storage.Engine,
	pattern *mergeRelationshipPattern,
	startNode *storage.Node,
	endNode *storage.Node,
) ([]*storage.Edge, error) {
	lookupStart, lookupEnd := startNode, endNode
	if pattern.direction == mergeRelationshipIncoming {
		lookupStart, lookupEnd = endNode, startNode
	}
	matches, err := findRelationshipsForMerge(store, lookupStart.ID, lookupEnd.ID, pattern.relType, pattern.properties)
	if err != nil || pattern.direction != mergeRelationshipUndirected {
		return matches, err
	}
	reverse, err := findRelationshipsForMerge(store, lookupEnd.ID, lookupStart.ID, pattern.relType, pattern.properties)
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
