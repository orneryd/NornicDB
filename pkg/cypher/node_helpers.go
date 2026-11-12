// Node and edge conversion helpers for NornicDB Cypher.
//
// This file contains helper functions for converting storage nodes and edges
// to map representations suitable for query results, and for extracting
// information from Cypher patterns.
//
// # Conversion Functions
//
// These functions convert internal storage types to result-friendly formats:
//
//   - nodeToMap: Convert storage.Node to map for RETURN
//   - edgeToMap: Convert storage.Edge to map for RETURN
//
// # Pattern Extraction
//
// These functions extract information from Cypher pattern strings:
//
//   - extractVarName: Get variable name from "(n:Label)"
//   - extractLabels: Get labels from "(n:Label1:Label2)"
//
// # ELI12
//
// When you search for something in NornicDB, you get back "nodes" (like
// people or places) and "edges" (like "knows" or "lives at"). These helper
// functions:
//
//  1. Turn internal storage format into nice readable maps
//  2. Hide the huge embedding arrays (they're just numbers, not useful to see)
//  3. Pull out useful info like variable names and labels from patterns
//
// It's like when you ask someone about their friend - they say "Alice, 30,
// from New York" not the person's entire DNA sequence!
//
// # Neo4j Compatibility
//
// These conversions match Neo4j's result format for compatibility.

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// profileKindNodeByIdSeek names the point node read a PROFILE statement
// actually performed: the plan operator a point read is claimed as.
const profileKindNodeByIdSeek = queryprofile.KindNodeByIDSeek

// getQueryNode follows the admitted statement's node-image contract. Native
// consumers retain full vectors; metadata consumers read settled embedding
// metadata without separately stored vectors. Writes outside native staging
// keep full images because those engines cannot reconcile omitted vectors.
//
// A PROFILE statement counts the point read here: one entity read attempt and
// one row when the node exists. This is the one owner of a point read, so the
// seek that hydrates the entries its index listed, the traversal that reads an
// endpoint and the MERGE that reads its own pattern each report those reads
// here, and never under the operator that named the node.
func (e *StorageExecutor) getQueryNode(ctx context.Context, store storage.Engine, id storage.NodeID) (*storage.Node, error) {
	reader := e.queryNodeReader(ctx, store)
	counter := queryprofile.Access(ctx, profileKindNodeByIdSeek, "")
	var (
		node *storage.Node
		err  error
	)
	if reader != nil {
		node, err = reader.GetQueryNode(ctx, id)
	} else {
		node, err = store.GetNode(ctx, id)
	}
	queryprofile.EntityReadResult(counter, node, err)
	return node, err
}

// queryNodeReader is the shared read decision for point reads and MATCH scans.
func (e *StorageExecutor) queryNodeReader(ctx context.Context, store storage.Engine) storage.QueryNodeReader {
	statement, _ := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext)
	if statement == nil || !statement.nodeImages.resolved || statement.nodeImages.images != MetadataNodeImages {
		return nil
	}
	if !statement.nodeImages.readOnly {
		// Only native staging reconciles a metadata image against stored vectors.
		if _, transactional := store.(*transactionStorageWrapper); !transactional {
			return nil
		}
	}
	reader, _ := store.(storage.QueryNodeReader)
	return reader
}

// streamQueryNodes carries the query-node read contract to storage. Each
// consumer keeps its own row admission and stopping state: a bounded
// traversal already checks labels, visibility and its remaining limit. The
// stream reads labels[0]: callers order a pattern's labels with
// nodeScanLabels.
func streamQueryNodes(ctx context.Context, store storage.Engine, labels []string, visit func(*storage.Node) error) error {
	opts := storage.StreamNodesOptions{QueryNodes: true, ApplyDecayFilter: true}
	if len(labels) > 0 {
		opts.Label = labels[0]
	}
	return store.StreamNodesWithOptions(ctx, opts, visit)
}

// nodeScanLabels orders a node pattern's labels for a label scan: the label
// with the fewest nodes first, the others in their written order. A scan reads
// the first label and checks the others on every node it reads, so a pattern
// such as (n:Large:Small) reads the small label, as Neo4j starts from the
// label with the fewest nodes (NOR-414). The sizes are the count store's
// (countStoreRead); when one is not answered the written order is kept. A
// pattern with fewer than two labels is returned as written, unread.
func nodeScanLabels(ctx context.Context, store storage.Engine, labels []string) []string {
	if len(labels) < 2 {
		return labels
	}
	smallest, smallestCount := 0, int64(0)
	for i, label := range labels {
		count, answered, err := countStoreRead(ctx, store, storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: label})
		if err != nil || !answered.available() {
			return labels
		}
		if i == 0 || count < smallestCount {
			smallest, smallestCount = i, count
		}
	}
	if smallest == 0 {
		return labels
	}
	ordered := make([]string, 0, len(labels))
	ordered = append(ordered, labels[smallest])
	ordered = append(ordered, labels[:smallest]...)
	return append(ordered, labels[smallest+1:]...)
}

type materializedAccessRecorder interface {
	RecordMaterializedAccess(entityID string)
}

// recordMaterializedResultAccess records an access (ON ACCESS policies) for
// every node and relationship a result returns.
func (e *StorageExecutor) recordMaterializedResultAccess(result *ExecuteResult) {
	if result == nil {
		return
	}
	recorder, ok := e.storage.(materializedAccessRecorder)
	if !ok || recorder == nil {
		return
	}
	materializedRows(recorder, result.Rows)
}

// resultHasMaterializedEntities reports whether result returns a node or a
// relationship whose access recordMaterializedResultAccess records. The
// result cache asks once per stored result, so a cached result with none is
// not walked on every hit.
func resultHasMaterializedEntities(result *ExecuteResult) bool {
	return result != nil && materializedRows(nil, result.Rows)
}

// materializedRows walks rows for the nodes and relationships they return:
// with a recorder it records an access for each one and reports whether
// there was any; with a nil recorder it only reports whether there is one,
// stopping at the first.
func materializedRows(recorder materializedAccessRecorder, rows [][]interface{}) bool {
	found := false
	for _, row := range rows {
		if materializedValues(recorder, row) {
			if recorder == nil {
				return true
			}
			found = true
		}
	}
	return found
}

// materializedValues is materializedRows for a list of values.
func materializedValues(recorder materializedAccessRecorder, values []interface{}) bool {
	found := false
	for _, value := range values {
		if materializedValue(recorder, value) {
			if recorder == nil {
				return true
			}
			found = true
		}
	}
	return found
}

// materializedValue is materializedRows for one value, including the nodes
// and relationships inside maps and lists.
func materializedValue(recorder materializedAccessRecorder, value interface{}) bool {
	id := ""
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return false
		}
		id = string(v.ID)
	case *storage.Edge:
		if v == nil {
			return false
		}
		id = string(v.ID)
	case storage.RemotePathValue:
		if _, _, valid := v.Parts(); !valid {
			return false
		}
		found := false
		for _, entity := range v {
			if materializedValue(recorder, entity) {
				if recorder == nil {
					return true
				}
				found = true
			}
		}
		return found
	case storage.RemoteEntityValue:
		if v == nil {
			return false
		}
		id, _ = v["elementId"].(string)
		if id == "" {
			return false
		}
	case PathResult:
		return materializedPathResult(recorder, &v)
	case *PathResult:
		return materializedPathResult(recorder, v)
	case map[string]interface{}:
		if path, ok := cypherPathResultOf(v, v, true); ok {
			return materializedPathResult(recorder, &path)
		}
		found := false
		for _, nested := range v {
			if materializedValue(recorder, nested) {
				if recorder == nil {
					return true
				}
				found = true
			}
		}
		return found
	case []interface{}:
		return materializedValues(recorder, v)
	case []map[string]interface{}:
		found := false
		for _, nested := range v {
			if materializedValue(recorder, nested) {
				if recorder == nil {
					return true
				}
				found = true
			}
		}
		return found
	default:
		return false
	}
	if recorder != nil {
		recorder.RecordMaterializedAccess(id)
	}
	return true
}

// materializedPathResult visits the native entities carried by a local path.
// The typed carrier is authoritative, so its public map fields are not walked
// a second time.
func materializedPathResult(recorder materializedAccessRecorder, path *PathResult) bool {
	if path == nil {
		return false
	}
	found := false
	for _, node := range path.Nodes {
		if materializedValue(recorder, node) {
			if recorder == nil {
				return true
			}
			found = true
		}
	}
	for _, relationship := range path.Relationships {
		if materializedValue(recorder, relationship) {
			if recorder == nil {
				return true
			}
			found = true
		}
	}
	return found
}

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.
//
// # Parameters
//
//   - node: The storage node to convert
//
// # Returns
//
//   - A map suitable for query result rows
//
// # Example
//
//	node := &storage.Node{
//	    ID: "123",
//	    Labels: []string{"Person"},
//	    Properties: map[string]interface{}{"name": "Alice"},
//	}
//	result := exec.nodeToMap(node)
//	// result = {"_nodeId": "123", "labels": ["Person"], "name": "Alice", ...}
func (e *StorageExecutor) nodeToMap(node *storage.Node) map[string]interface{} {
	// Start with node metadata
	// Use _nodeId for internal storage ID to avoid conflicts with user "id" property
	result := map[string]interface{}{
		"_nodeId": string(node.ID), // Internal storage ID for DELETE operations
		"labels":  node.Labels,
	}

	// Add properties both at top level (for Neo4j compatibility) and nested (for standard graph format)
	props := e.decayVisibleNodeProperties(node)
	for k, v := range props {
		result[k] = v
	}
	result["properties"] = props

	// If no user "id" property, use storage ID for backward compatibility
	if _, hasUserID := result["id"]; !hasUserID {
		result["id"] = string(node.ID)
	}

	return result
}

// decayVisibleNodeProperties is the shared property-visibility owner for node
// maps and expression/result projections. It never changes the stored map.
func (e *StorageExecutor) decayVisibleNodeProperties(node *storage.Node) map[string]interface{} {
	if e.statementPropertyViews != nil {
		return e.statementPropertyViews.node(node).Properties
	}
	return storage.NewQueryResultProjection(e.storage).NodeProperties(node)
}

func nodeVersionAtNanos(node *storage.Node, createdAt int64) int64 {
	if node == nil || node.UpdatedAt.IsZero() {
		return createdAt
	}
	return node.UpdatedAt.UnixNano()
}

// edgeToMap converts a storage.Edge to a map for result output.
//
// # Parameters
//
//   - edge: The storage edge to convert
//
// # Returns
//
//   - A map suitable for query result rows
//
// # Example
//
//	edge := &storage.Edge{
//	    ID: "e1",
//	    Type: "KNOWS",
//	    StartNode: "n1",
//	    EndNode: "n2",
//	}
//	result := exec.edgeToMap(edge)
//	// result = {"_edgeId": "e1", "type": "KNOWS", "startNode": "n1", ...}
func (e *StorageExecutor) edgeToMap(edge *storage.Edge) map[string]interface{} {
	return map[string]interface{}{
		"_edgeId":    string(edge.ID),
		"type":       edge.Type,
		"startNode":  string(edge.StartNode),
		"endNode":    string(edge.EndNode),
		"properties": e.decayVisibleEdgeProperties(edge),
	}
}

// decayVisibleEdgeProperties returns edge's properties without the ones decay
// hides at the current scoring time (the edge's own map when decay filtering
// doesn't apply).
func (e *StorageExecutor) decayVisibleEdgeProperties(edge *storage.Edge) map[string]interface{} {
	if e.statementPropertyViews != nil {
		return e.statementPropertyViews.edge(edge).Properties
	}
	return storage.NewQueryResultProjection(e.storage).EdgeProperties(edge)
}

// procedureRelationship is a relationship a procedure yields: the
// relationship itself (Neo4j's RELATIONSHIP, so `rel.prop` and Bolt see a
// relationship, not a map), with the properties decay hides removed.
func (e *StorageExecutor) procedureRelationship(edge *storage.Edge) *storage.Edge {
	visible := *edge
	visible.Properties = e.decayVisibleEdgeProperties(edge)
	return &visible
}

// extractVarName extracts the variable name from a pattern like "(n:Label {...})".
//
// # Parameters
//
//   - pattern: The Cypher pattern string
//
// # Returns
//
//   - The variable name, or "n" as default
//
// # Example
//
//	extractVarName("(person:Person {name: 'Alice'})")
//	// Returns: "person"
//
//	extractVarName("(:Person)")
//	// Returns: "n" (default)
func (e *StorageExecutor) extractVarName(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	pattern = strings.TrimPrefix(pattern, "(")
	// Find first : or { or )
	for i, c := range pattern {
		if c == ':' || c == '{' || c == ')' || cyphertext.IsWhitespace(c) {
			name := strings.TrimSpace(pattern[:i])
			if name != "" {
				return name
			}
			break
		}
	}
	return "n" // Default variable name
}

// extractLabels extracts labels from a pattern like "(n:Label1:Label2 {...})".
//
// # Parameters
//
//   - pattern: The Cypher pattern string
//
// # Returns
//
//   - Slice of label strings
//
// # Example
//
//	extractLabels("(n:Person:Employee {name: 'Alice'})")
//	// Returns: ["Person", "Employee"]
//
//	extractLabels("(n)")
//	// Returns: []
func (e *StorageExecutor) extractLabels(pattern string) []string {
	head, _ := splitNodePatternProperties(pattern)
	_, labels, _ := parseNodeHead(head)
	return labels
}

// admitTraversalStartNodes applies complete pattern admission once, before
// either traversal executor runs. Preserve the candidate slice when all pass;
// a rejected candidate requires a separate slice because seeds may be shared.
func (e *StorageExecutor) admitTraversalStartNodes(ctx context.Context, pattern nodePatternInfo, nodes []*storage.Node) []*storage.Node {
	for i, node := range nodes {
		if e.pipelineNodeMatchesPattern(ctx, node, pattern) {
			continue
		}
		admitted := append([]*storage.Node(nil), nodes[:i]...)
		for _, rest := range nodes[i+1:] {
			if e.pipelineNodeMatchesPattern(ctx, rest, pattern) {
				admitted = append(admitted, rest)
			}
		}
		return admitted
	}
	return nodes
}
