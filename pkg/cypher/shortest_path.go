// Shortest path Cypher syntax support for NornicDB.
// Implements shortestPath() and allShortestPaths() functions.
//
// Syntax:
//   MATCH p = shortestPath((start)-[*]-(end)) RETURN p
//   MATCH p = allShortestPaths((start)-[*]-(end)) RETURN p

package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// bfsCancelCheckMask is the bitmask used to check ctx.Done() periodically
// inside hot BFS loops. Checking on every iteration is measurable overhead
// for traversals over large fan-out graphs; checking once every 256 dequeues
// is enough to react to client disconnects/server shutdown within a few ms
// while keeping the per-iteration cost negligible. Power of two so the
// compiler turns the modulo into a single AND.
const bfsCancelCheckMask = 0xFF

func extractShortestPathCall(cypher string) (string, string, int, bool) {
	upperCypher := upperASCII(cypher)
	// Upper-case literals avoid upperASCII(funcName) allocations on every
	// statement; the compiler places this constant slice in static data.
	for _, upperFunc := range []string{"ALLSHORTESTPATHS", "SHORTESTPATH"} {
		searchStart := 0
		for searchStart < len(cypher) {
			idx := strings.Index(upperCypher[searchStart:], upperFunc)
			if idx < 0 {
				break
			}
			idx += searchStart
			if idx > 0 && isIdentByte(cypher[idx-1]) {
				searchStart = idx + 1
				continue
			}
			openParen := idx + len(upperFunc)
			for openParen < len(cypher) && (cypher[openParen] == ' ' || cypher[openParen] == '\t' || cypher[openParen] == '\n' || cypher[openParen] == '\r') {
				openParen++
			}
			if openParen >= len(cypher) || cypher[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(cypher, openParen)
			if closeParen < 0 {
				if upperFunc == "ALLSHORTESTPATHS" {
					return "allShortestPaths", "", idx, false
				}
				return "shortestPath", "", idx, false
			}
			if upperFunc == "ALLSHORTESTPATHS" {
				return "allShortestPaths", strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
			}
			return "shortestPath", strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
		}
	}
	return "", "", -1, false
}

// evaluateShortestPathValue evaluates shortestPath(...) / allShortestPaths(...)
// in expression position for one row. As in Neo4j, both endpoints must be
// variables bound by the row (the statement rewrite rejects an anonymous one,
// shortestPathExpressionError), and the labels and properties written on a
// bound endpoint filter it. The search is
// the MATCH clause's (shortestPathsBetween, #863). The value evaluator and
// the row evaluator (WHERE) both call it. handled=false lets the ordinary
// evaluator report its own error when the argument is not a traversal
// pattern.
func (e *StorageExecutor) evaluateShortestPathValue(ctx context.Context, funcName, pattern string, nodes map[string]*storage.Node) (interface{}, bool, error) {
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil {
		return nil, false, nil
	}
	if err := shortestPathPatternError(funcName, pattern, match); err != nil {
		return nil, true, err
	}
	findAll := strings.EqualFold(funcName, "allShortestPaths")
	start, end := nodes[match.StartNode.variable], nodes[match.EndNode.variable]
	if start == nil || end == nil || !e.matchesEndPattern(start, &match.StartNode) || !e.matchesEndPattern(end, &match.EndNode) {
		return nil, true, nil
	}
	paths, err := e.shortestPathsBetween(ctx, &shortestPathMatch{findAll: findAll, traversal: match}, start, end, nil)
	if err != nil {
		return nil, true, err
	}
	if findAll {
		values := make([]interface{}, 0, len(paths))
		for _, path := range paths {
			values = append(values, e.pathToMap(path))
		}
		return values, true, nil
	}
	if len(paths) == 0 {
		return nil, true, nil
	}
	return e.pathToMap(paths[0]), true, nil
}

func nodeToValue(n *storage.Node) interface{} {
	props := make(map[string]interface{}, len(n.Properties))
	for k, v := range n.Properties {
		props[k] = v
	}
	return map[string]interface{}{
		"elementId":  string(n.ID),
		"labels":     labelStringSlice(n.Labels),
		"properties": props,
	}
}

func labelStringSlice(in []string) []interface{} {
	out := make([]interface{}, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// pathToMap converts a PathResult to a map representation
func (e *StorageExecutor) pathToMap(path PathResult) map[string]interface{} {
	nodes := make([]interface{}, len(path.Nodes))
	for i, n := range path.Nodes {
		nodes[i] = n
	}

	rels := make([]interface{}, len(path.Relationships))
	for i, r := range path.Relationships {
		rels[i] = e.edgeToMap(r)
	}

	return map[string]interface{}{
		"_pathResult":   path,
		"nodes":         nodes,
		"relationships": rels,
		"length":        int64(path.Length),
	}
}

// pathValueParts returns the nodes and relationships of a path value. Path
// values come in two map shapes - pathToMap's ("relationships" holds
// relationship maps) and the path-context evaluator's ("rels" holds
// relationships) - and both carry the PathResult under "_pathResult", which
// is authoritative: nodes are *storage.Node and relationships *storage.Edge
// whichever shape the value has. The "nodes" / "rels" keys are read only when
// "_pathResult" is absent. hasNodes / hasRelationships report whether the
// value carried them.
func pathValueParts(path map[string]interface{}) (nodes, relationships []interface{}, hasNodes, hasRelationships bool) {
	var result *PathResult
	switch typed := path["_pathResult"].(type) {
	case PathResult:
		result = &typed
	case *PathResult:
		result = typed
	}
	if result != nil {
		nodes = make([]interface{}, len(result.Nodes))
		for index, node := range result.Nodes {
			nodes[index] = node
		}
		relationships = make([]interface{}, len(result.Relationships))
		for index, relationship := range result.Relationships {
			relationships[index] = relationship
		}
		return nodes, relationships, true, true
	}
	if raw, found := path["nodes"]; found {
		nodes, hasNodes = toAnySlice(raw), true
	}
	if raw, found := path["rels"]; found {
		relationships, hasRelationships = toAnySlice(raw), true
	}
	return nodes, relationships, hasNodes, hasRelationships
}
