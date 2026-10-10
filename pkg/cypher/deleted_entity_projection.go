package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// deletedEntities is what a statement has deleted so far: the nodes and
// relationships its DELETE clauses removed (a DETACH DELETE's relationships
// and a deleted path's parts included). It is the statement's, not a row's,
// so it holds across WITH, aliases, collect() and the row-at-a-time runs of
// a write stretch: Neo4j 5.26 reads a deleted entity the same way wherever
// it is reached in the rest of the statement (#907).
type deletedEntities struct {
	nodes map[storage.NodeID]struct{}
	edges map[storage.EdgeID]struct{}
}

type deletedEntitiesKey struct{}

// withDeletedEntities gives the statement a deletedEntities, unless ctx has
// one: a subquery, FOREACH or row-at-a-time run shares the statement's.
func withDeletedEntities(ctx context.Context) context.Context {
	if deletedEntitiesOf(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, deletedEntitiesKey{}, &deletedEntities{})
}

// deletedEntitiesOf is the statement's deletedEntities, nil when it has none.
func deletedEntitiesOf(ctx context.Context) *deletedEntities {
	deleted, _ := ctx.Value(deletedEntitiesKey{}).(*deletedEntities)
	return deleted
}

// pipelineClausesMayDelete reports whether clauses can delete: a DELETE, or
// a FOREACH or CALL subquery that may hold one.
func pipelineClausesMayDelete(clauses []pipelineClause) bool {
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseDelete, pipelineClauseForeach, pipelineClauseCallSubquery:
			return true
		}
	}
	return false
}

// add records deleted nodes and relationships.
func (d *deletedEntities) add(nodeIDs []storage.NodeID, edgeIDs map[storage.EdgeID]struct{}) {
	if len(nodeIDs) > 0 && d.nodes == nil {
		d.nodes = make(map[storage.NodeID]struct{}, len(nodeIDs))
	}
	for _, id := range nodeIDs {
		d.nodes[id] = struct{}{}
	}
	if len(edgeIDs) > 0 && d.edges == nil {
		d.edges = make(map[storage.EdgeID]struct{}, len(edgeIDs))
	}
	for id := range edgeIDs {
		d.edges[id] = struct{}{}
	}
}

// empty reports whether nothing is deleted (a nil d included).
func (d *deletedEntities) empty() bool {
	return d == nil || (len(d.nodes) == 0 && len(d.edges) == 0)
}

// replaceDeletedEntityViews replaces, in rows, every node and relationship in
// deleted with what Neo4j 5.26 returns for it after the DELETE: the same
// entity without labels or properties, so RETURN n is an empty node and
// n {.*}, properties(n) and keys(n) are empty. A relationship keeps its type
// and endpoints (type(r) still answers). Paths and lists holding a deleted
// entity are rebuilt with its view (#907). Reading a deleted entity's
// property or labels is an error instead (validateDeletedEntityReads).
func (e *StorageExecutor) replaceDeletedEntityViews(rows []pipelineRow, deleted *deletedEntities) {
	if deleted.empty() {
		return
	}
	views := deletedEntityViews{executor: e, nodes: deleted.nodes, edges: deleted.edges}
	for _, row := range rows {
		for name, value := range row {
			if replaced, changed := views.replace(value); changed {
				row[name] = replaced
			}
		}
	}
}

// deletedEntityViews builds the views of replaceDeletedEntityViews: a copy
// with the ID (a relationship's type and endpoints too) and no labels or
// properties. A view is equal to the entity by ID, as entities compare.
type deletedEntityViews struct {
	executor *StorageExecutor
	nodes    map[storage.NodeID]struct{}
	edges    map[storage.EdgeID]struct{}
}

// replace returns value with its deleted entities replaced by their views,
// and whether anything was replaced.
func (v *deletedEntityViews) replace(value interface{}) (interface{}, bool) {
	switch typed := value.(type) {
	case *storage.Node:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.nodes[typed.ID]; deleted {
			return v.node(typed), true
		}
	case *storage.Edge:
		if typed == nil {
			return value, false
		}
		if _, deleted := v.edges[typed.ID]; deleted {
			return v.edge(typed), true
		}
	case []interface{}:
		var replaced []interface{}
		for index, item := range typed {
			if view, changed := v.replace(item); changed {
				if replaced == nil {
					replaced = append([]interface{}(nil), typed...)
				}
				replaced[index] = view
			}
		}
		if replaced != nil {
			return replaced, true
		}
	case map[string]interface{}:
		var path PathResult
		switch result := typed["_pathResult"].(type) {
		case PathResult:
			path = result
		case *PathResult:
			if result == nil {
				return value, false
			}
			path = *result
		default:
			return value, false
		}
		changed := false
		nodes := make([]*storage.Node, len(path.Nodes))
		for index, node := range path.Nodes {
			nodes[index] = node
			if view, replaced := v.replace(node); replaced {
				nodes[index], changed = view.(*storage.Node), true
			}
		}
		relationships := make([]*storage.Edge, len(path.Relationships))
		for index, relationship := range path.Relationships {
			relationships[index] = relationship
			if view, replaced := v.replace(relationship); replaced {
				relationships[index], changed = view.(*storage.Edge), true
			}
		}
		if changed {
			path.Nodes, path.Relationships = nodes, relationships
			return v.executor.pathToMap(path), true
		}
	}
	return value, false
}

func (v *deletedEntityViews) node(node *storage.Node) *storage.Node {
	return &storage.Node{ID: node.ID}
}

func (v *deletedEntityViews) edge(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Type: edge.Type, StartNode: edge.StartNode, EndNode: edge.EndNode}
}

// validateDeletedEntityReads rejects a clause that, after a DELETE in the
// statement, reads a property or the labels of a deleted node or
// relationship, or the keys or properties() of a deleted relationship,
// anywhere it reads (n.p + 1, WITH n.p AS p, WHERE n.p = 1, ORDER BY n.p, a
// SET value, [x IN deleted | x.p]): Neo4j 5.26's EntityNotFound. keys and
// properties of a deleted node, and n {.*}, are empty instead
// (replaceDeletedEntityViews); n.p IS NULL is true and SET n.p = 1 writes
// nothing, without an error (deletedEntityReadsIn, deletedEntityReadText).
func validateDeletedEntityReads(rows []pipelineRow, clause string, deleted *deletedEntities) error {
	if deleted.empty() {
		return nil
	}
	reads := deletedEntityReadsIn(clause)
	for _, row := range rows {
		for _, read := range reads {
			variable := read.variable
			if _, bound := row[variable]; !bound {
				// A list iteration variable (x IN xs) reads the list's items.
				variable = iterationSourceVariable(clause, variable)
			}
			if deleted.bindsDeleted(row, variable, read.relationshipOnly) {
				return newSemanticError(
					"Neo.ClientError.Statement.EntityNotFound",
					"DeletedEntityAccess",
					"cannot access properties or labels of a deleted entity",
				)
			}
		}
	}
	return nil
}

// deletedEntityRead is a variable an expression reads a property or the
// labels of, or (relationshipOnly) the keys or properties() of: an error only
// for a relationship.
type deletedEntityRead struct {
	variable         string
	relationshipOnly bool
}

// deletedEntityReadsIn returns the variables expression reads a property, the
// labels, the keys or properties() of, outside string literals: v.p,
// labels(v), keys(v), properties(v).
func deletedEntityReadsIn(expression string) []deletedEntityRead {
	var reads []deletedEntityRead
	for index := 0; index < len(expression); {
		switch character := expression[index]; {
		case character == '\'' || character == '"' || character == '`':
			end := strings.IndexByte(expression[index+1:], character)
			if end < 0 {
				return reads
			}
			index += end + 2
			continue
		case !isIdentByte(character) || (index > 0 && (isIdentByte(expression[index-1]) || expression[index-1] == '.')):
			index++
			continue
		}
		start := index
		for index < len(expression) && isIdentByte(expression[index]) {
			index++
		}
		word := expression[start:index]
		next := skipSpaceIndex(expression, index)
		switch {
		case next < len(expression) && expression[next] == '.' && next+1 < len(expression) && expression[next+1] != '.':
			if !propertyNullTestFollows(expression, next+1) {
				reads = append(reads, deletedEntityRead{variable: word})
			}
		case next < len(expression) && expression[next] == '(' && (strings.EqualFold(word, "labels") || strings.EqualFold(word, "keys") || strings.EqualFold(word, "properties")):
			argumentStart := skipSpaceIndex(expression, next+1)
			argumentEnd := argumentStart
			for argumentEnd < len(expression) && isIdentByte(expression[argumentEnd]) {
				argumentEnd++
			}
			if close := skipSpaceIndex(expression, argumentEnd); argumentEnd > argumentStart && close < len(expression) && expression[close] == ')' {
				reads = append(reads, deletedEntityRead{variable: expression[argumentStart:argumentEnd], relationshipOnly: !strings.EqualFold(word, "labels")})
			}
		}
	}
	return reads
}

func skipSpaceIndex(text string, index int) int {
	for index < len(text) && isASCIISpace(text[index]) {
		index++
	}
	return index
}

// bindsDeleted reports whether row binds variable to a deleted node or
// relationship. For a keys() / properties() read (relationshipOnly) only a
// relationship counts: a deleted node's are empty.
func (d *deletedEntities) bindsDeleted(row pipelineRow, variable string, relationshipOnly bool) bool {
	if variable == "" {
		return false
	}
	return d.holdsDeleted(row[variable], relationshipOnly)
}

// holdsDeleted reports whether value is a deleted node or relationship, or a
// list holding one (bindsDeleted).
func (d *deletedEntities) holdsDeleted(value interface{}, relationshipOnly bool) bool {
	switch entity := value.(type) {
	case []interface{}:
		for _, item := range entity {
			if d.holdsDeleted(item, relationshipOnly) {
				return true
			}
		}
		return false
	case *storage.Node:
		if relationshipOnly || entity == nil {
			return false
		}
		_, found := d.nodes[entity.ID]
		return found
	case *storage.Edge:
		if entity == nil {
			return false
		}
		_, found := d.edges[entity.ID]
		return found
	default:
		return false
	}
}

// propertyNullTestFollows reports whether the property name starting at
// index is followed by IS NULL or IS NOT NULL: Neo4j tests a deleted
// entity's property for null without an error (it has none).
func propertyNullTestFollows(expression string, index int) bool {
	for index < len(expression) && isIdentByte(expression[index]) {
		index++
	}
	rest := expression[skipSpaceIndex(expression, index):]
	if !startsWithKeywordFold(rest, "IS") {
		return false
	}
	rest = strings.TrimLeft(rest[len("IS"):], " \t\r\n")
	if startsWithKeywordFold(rest, "NOT") {
		rest = strings.TrimLeft(rest[len("NOT"):], " \t\r\n")
	}
	return startsWithKeywordFold(rest, "NULL")
}

// iterationSourceVariable is the row variable that variable iterates in
// clause (x in [x IN xs | x.p], any(x IN xs WHERE …), reduce(s = 0, x IN xs
// | …)), "" when variable iterates no row variable.
func iterationSourceVariable(clause, variable string) string {
	for offset := 0; offset < len(clause); {
		index := indexIdentifierFold(clause[offset:], variable)
		if index < 0 {
			return ""
		}
		after := offset + index + len(variable)
		offset = after
		rest := clause[skipSpaceIndex(clause, after):]
		if !startsWithKeywordFold(rest, "IN") {
			continue
		}
		rest = strings.TrimLeft(rest[len("IN"):], " \t\r\n")
		end := 0
		for end < len(rest) && isIdentByte(rest[end]) {
			end++
		}
		if end > 0 {
			return rest[:end]
		}
	}
	return ""
}

// indexIdentifierFold is the index of identifier in text as a whole word
// outside string literals, -1 when it isn't there.
func indexIdentifierFold(text, identifier string) int {
	for index := 0; index+len(identifier) <= len(text); {
		switch character := text[index]; {
		case character == '\'' || character == '"' || character == '`':
			end := strings.IndexByte(text[index+1:], character)
			if end < 0 {
				return -1
			}
			index += end + 2
			continue
		case (index > 0 && isIdentByte(text[index-1])) || !strings.EqualFold(text[index:index+len(identifier)], identifier):
			index++
			continue
		}
		if end := index + len(identifier); end < len(text) && isIdentByte(text[end]) {
			index++
			continue
		}
		return index
	}
	return -1
}

// deletedEntityReadText is the part of clause that reads values, for
// validateDeletedEntityReads: a SET's assigned values, dynamic keys and
// dynamic label expressions (its targets are writes, and Neo4j lets SET
// write to a deleted entity), nothing for a DELETE or REMOVE (their
// targets), the whole clause otherwise.
func deletedEntityReadText(clause pipelineClause) string {
	switch clause.kind {
	case pipelineClauseDelete, pipelineClauseRemove:
		return ""
	case pipelineClauseSet:
		body := strings.TrimSpace(clause.text)
		if startsWithKeywordFold(body, "SET") {
			body = body[len("SET"):]
		}
		var values strings.Builder
		for _, assignment := range splitSetAssignments(body) {
			_, property, operator, right := splitSetAssignment(assignment)
			switch operator {
			case "[]=":
				// The key expression of x[key] = v is read too.
				values.WriteString(property)
				values.WriteByte('\n')
				fallthrough
			case "=", "+=":
				values.WriteString(right)
				values.WriteByte('\n')
			case ":":
				// A dynamic label's expression ($(e)) reads e.
				if strings.Contains(right, "$(") {
					values.WriteString(right)
					values.WriteByte('\n')
				}
			}
		}
		return values.String()
	}
	return clause.text
}
