package cypher

import (
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// cypher25ValueText is a value as Neo4j 2026.09's toString() writes it in a
// Cypher 25 statement: a scalar, temporal value, duration or point as
// toString() always does (convertToStringOrNull); a list as [a, b] and a map
// as {k: v}, keys sorted and backtick-quoted when they aren't plain names,
// with their strings unquoted and null as null; a node as its labels (:A:B),
// a relationship as its type [:T], and a path as its nodes and relationships
// in path order, each relationship pointing the way it is stored:
// (:A)-[:T]->(:B), (:B)<-[:T]-(:A). ok is false for a value of no Cypher
// type and for null.
func cypher25ValueText(value interface{}) (string, bool) {
	var out strings.Builder
	if value == nil || !writeCypher25ValueText(&out, value) {
		return "", false
	}
	return out.String(), true
}

func writeCypher25ValueText(out *strings.Builder, value interface{}) bool {
	switch cypherValueKindOf(value) {
	case valueKindNull:
		out.WriteString("null")
	case valueKindList:
		out.WriteByte('[')
		for index, item := range toAnySlice(value) {
			if index > 0 {
				out.WriteString(", ")
			}
			if !writeCypher25ValueText(out, item) {
				return false
			}
		}
		out.WriteByte(']')
	case valueKindMap:
		entries, isMap := value.(map[string]interface{})
		if !isMap {
			return false
		}
		keys := make([]string, 0, len(entries))
		for key := range entries {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				out.WriteString(", ")
			}
			out.WriteString(cypher25NameText(key))
			out.WriteString(": ")
			if !writeCypher25ValueText(out, entries[key]) {
				return false
			}
		}
		out.WriteByte('}')
	case valueKindNode:
		node, isNode := value.(*storage.Node)
		if !isNode || node == nil {
			return false
		}
		writeCypher25NodeText(out, node)
	case valueKindRelationship:
		edge, isEdge := value.(*storage.Edge)
		if !isEdge || edge == nil {
			return false
		}
		out.WriteString("[:" + cypher25NameText(edge.Type) + "]")
	case valueKindPath:
		return writeCypher25PathText(out, value)
	default:
		text, isText := convertToStringOrNull(value).(string)
		if !isText {
			return false
		}
		out.WriteString(text)
	}
	return true
}

func writeCypher25NodeText(out *strings.Builder, node *storage.Node) {
	out.WriteByte('(')
	for _, label := range node.Labels {
		out.WriteString(":" + cypher25NameText(label))
	}
	out.WriteByte(')')
}

// writeCypher25PathText writes a path value (a PathResult, or a path map
// carrying one) as cypher25ValueText describes.
func writeCypher25PathText(out *strings.Builder, value interface{}) bool {
	var path *PathResult
	switch typed := value.(type) {
	case PathResult:
		path = &typed
	case *PathResult:
		path = typed
	case map[string]interface{}:
		nodes, relationships, hasNodes, _ := pathValueParts(typed)
		if !hasNodes {
			return false
		}
		path = &PathResult{}
		for _, item := range nodes {
			node, isNode := item.(*storage.Node)
			if !isNode {
				return false
			}
			path.Nodes = append(path.Nodes, node)
		}
		for _, item := range relationships {
			edge, isEdge := item.(*storage.Edge)
			if !isEdge {
				return false
			}
			path.Relationships = append(path.Relationships, edge)
		}
	}
	if path == nil || len(path.Nodes) == 0 || len(path.Relationships) != len(path.Nodes)-1 {
		return false
	}
	writeCypher25NodeText(out, path.Nodes[0])
	for index, edge := range path.Relationships {
		relationship := "[:" + cypher25NameText(edge.Type) + "]"
		if edge.StartNode == path.Nodes[index].ID {
			out.WriteString("-" + relationship + "->")
		} else {
			out.WriteString("<-" + relationship + "-")
		}
		writeCypher25NodeText(out, path.Nodes[index+1])
	}
	return true
}

// convertToStringInVersion is toStringOrNull in a statement of the version,
// and what toStringList makes of each element: in Cypher 25 lists, maps and
// graph entities have text too (cypher25ValueText) and a null element is
// 'null' (toString(null) is null before it gets here); in Cypher 5 they are
// null (convertToStringOrNull).
func convertToStringInVersion(value interface{}, cypher25 bool) interface{} {
	if !cypher25 {
		return convertToStringOrNull(value)
	}
	var out strings.Builder
	if !writeCypher25ValueText(&out, value) {
		return nil
	}
	return out.String()
}

// cypher25NameText is a key, label or type as cypher25ValueText writes it:
// as it is when it is a plain name, else backtick-quoted (`a b`).
func cypher25NameText(name string) string {
	if isSimpleCypherMapKey(name) {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
