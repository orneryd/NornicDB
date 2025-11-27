// CALL procedure implementations for NornicDB.
// This file contains all CALL procedures for Neo4j compatibility and NornicDB extensions.
//
// Phase 3: Core Procedures Implementation
// =======================================
//
// Critical procedures for Mimir MCP tools:
//   - db.index.vector.queryNodes - Vector similarity search with cosine/euclidean
//   - db.index.fulltext.queryNodes - Full-text search with BM25-like scoring
//   - apoc.path.subgraphNodes - Graph traversal with depth/filter control
//   - apoc.path.expand - Path expansion with relationship filters
//
// These procedures are essential for:
//   - Semantic search (vector similarity)
//   - Text search (full-text indexing)
//   - Knowledge graph traversal
//   - Memory relationship discovery

package cypher

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// yieldClause represents parsed YIELD information from a CALL statement.
// Syntax: CALL procedure() YIELD var1, var2 AS alias WHERE condition
type yieldClause struct {
	items      []yieldItem // List of yielded items (possibly with aliases)
	yieldAll   bool        // YIELD * - return all columns
	where      string      // Optional WHERE condition after YIELD
	hasReturn  bool        // Whether there's a RETURN clause after
	returnExpr string      // The RETURN expression if present
}

// yieldItem represents a single item in a YIELD clause
type yieldItem struct {
	name  string // Original column name from procedure
	alias string // Alias (empty if no AS clause)
}

// parseYieldClause extracts YIELD information from a CALL statement.
// Handles: YIELD *, YIELD a, b, YIELD a AS x, b AS y, YIELD a WHERE a.score > 0.5
func parseYieldClause(cypher string) *yieldClause {
	upper := strings.ToUpper(cypher)
	yieldIdx := strings.Index(upper, " YIELD ")
	if yieldIdx == -1 {
		return nil
	}

	result := &yieldClause{
		items: []yieldItem{},
	}

	// Get everything after YIELD
	afterYield := strings.TrimSpace(cypher[yieldIdx+7:])

	// Check for YIELD *
	if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(afterYield)), "*") {
		result.yieldAll = true
		afterYield = strings.TrimSpace(afterYield[1:])
	}

	// Find WHERE and RETURN boundaries
	whereIdx := findKeywordIndexInContext(afterYield, "WHERE")
	returnIdx := findKeywordIndexInContext(afterYield, "RETURN")

	// Extract WHERE clause if present
	if whereIdx != -1 {
		if returnIdx != -1 && returnIdx > whereIdx {
			result.where = strings.TrimSpace(afterYield[whereIdx+5 : returnIdx])
		} else {
			result.where = strings.TrimSpace(afterYield[whereIdx+5:])
		}
	}

	// Extract RETURN clause if present
	if returnIdx != -1 {
		result.hasReturn = true
		result.returnExpr = strings.TrimSpace(afterYield[returnIdx+6:])
	}

	// Parse yield items (if not YIELD *)
	if !result.yieldAll {
		// Get the items part (before WHERE or RETURN)
		itemsEnd := len(afterYield)
		if whereIdx != -1 {
			itemsEnd = whereIdx
		} else if returnIdx != -1 {
			itemsEnd = returnIdx
		}

		itemsStr := strings.TrimSpace(afterYield[:itemsEnd])
		if itemsStr != "" {
			// Split by comma, respecting AS keyword
			for _, item := range strings.Split(itemsStr, ",") {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}

				yi := yieldItem{}
				// Check for AS alias
				upperItem := strings.ToUpper(item)
				asIdx := strings.Index(upperItem, " AS ")
				if asIdx != -1 {
					yi.name = strings.TrimSpace(item[:asIdx])
					yi.alias = strings.TrimSpace(item[asIdx+4:])
				} else {
					yi.name = item
					yi.alias = ""
				}
				result.items = append(result.items, yi)
			}
		}
	}

	return result
}

// findKeywordIndexInContext finds a keyword in context, avoiding matches inside quotes
func findKeywordIndexInContext(s, keyword string) int {
	upper := strings.ToUpper(s)
	keyword = strings.ToUpper(keyword)

	inQuote := false
	quoteChar := rune(0)

	for i := 0; i <= len(s)-len(keyword); i++ {
		c := rune(s[i])

		// Track quote state
		if c == '\'' || c == '"' {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			continue
		}

		if inQuote {
			continue
		}

		// Check for keyword match with word boundary
		if strings.HasPrefix(upper[i:], keyword) {
			// Check left boundary (must be start or non-alphanumeric)
			if i > 0 {
				prev := s[i-1]
				if (prev >= 'A' && prev <= 'Z') || (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' {
					continue
				}
			}
			// Check right boundary
			end := i + len(keyword)
			if end < len(s) {
				next := s[end]
				if (next >= 'A' && next <= 'Z') || (next >= 'a' && next <= 'z') || (next >= '0' && next <= '9') || next == '_' {
					continue
				}
			}
			return i
		}
	}
	return -1
}

// applyYieldFilter applies YIELD clause filtering to procedure results.
// This handles column selection, aliasing, and WHERE filtering.
func (e *StorageExecutor) applyYieldFilter(result *ExecuteResult, yield *yieldClause) (*ExecuteResult, error) {
	if yield == nil {
		return result, nil
	}

	// Apply WHERE filter first
	if yield.where != "" {
		filteredRows := make([][]interface{}, 0)
		for _, row := range result.Rows {
			// Create a context with the row values mapped to column names
			ctx := make(map[string]interface{})
			for i, col := range result.Columns {
				if i < len(row) {
					ctx[col] = row[i]
				}
			}

			// Evaluate the WHERE condition
			passes, err := e.evaluateYieldWhere(yield.where, ctx)
			if err != nil {
				// If evaluation fails, include the row (conservative)
				passes = true
			}
			if passes {
				filteredRows = append(filteredRows, row)
			}
		}
		result.Rows = filteredRows
	}

	// Apply column selection and aliasing (if not YIELD *)
	if !yield.yieldAll && len(yield.items) > 0 {
		// Build column index map
		colIndex := make(map[string]int)
		for i, col := range result.Columns {
			colIndex[col] = i
		}

		// Build new columns and project rows
		newColumns := make([]string, 0, len(yield.items))
		for _, item := range yield.items {
			if item.alias != "" {
				newColumns = append(newColumns, item.alias)
			} else {
				newColumns = append(newColumns, item.name)
			}
		}

		newRows := make([][]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			newRow := make([]interface{}, len(yield.items))
			for i, item := range yield.items {
				if idx, ok := colIndex[item.name]; ok && idx < len(row) {
					newRow[i] = row[idx]
				} else {
					newRow[i] = nil
				}
			}
			newRows = append(newRows, newRow)
		}

		result.Columns = newColumns
		result.Rows = newRows
	}

	return result, nil
}

// evaluateYieldWhere evaluates a WHERE condition in the context of YIELD variables.
func (e *StorageExecutor) evaluateYieldWhere(whereExpr string, ctx map[string]interface{}) (bool, error) {
	// Simple evaluation for common patterns
	whereExpr = strings.TrimSpace(whereExpr)
	if whereExpr == "" {
		return true, nil
	}

	// Convert context to pseudo-nodes for the expression evaluator
	// Each yielded variable becomes a pseudo-node with properties from the context
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)

	for name, val := range ctx {
		// If the value is a map (like a node result), wrap it
		if mapVal, ok := val.(map[string]interface{}); ok {
			props := make(map[string]interface{})
			for k, v := range mapVal {
				props[k] = v
			}
			nodes[name] = &storage.Node{
				ID:         storage.NodeID(name),
				Properties: props,
			}
		} else {
			// For scalar values, create a node with that value as a property
			nodes[name] = &storage.Node{
				ID: storage.NodeID(name),
				Properties: map[string]interface{}{
					"value": val,
				},
			}
			// Also add the scalar value directly to enable direct comparisons like "score > 0.5"
			ctx[name] = val
		}
	}

	// Try to evaluate using the expression evaluator with context
	result := e.evaluateExpressionWithContext(whereExpr, nodes, rels)

	// Convert result to boolean
	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("WHERE expression did not evaluate to boolean: %v", result)
	}
}

func (e *StorageExecutor) executeCall(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(cypher)

	var result *ExecuteResult
	var err error

	switch {
	// Neo4j Vector Index Procedures (CRITICAL for Mimir)
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYNODES"):
		result, err = e.callDbIndexVectorQueryNodes(cypher)
	// Neo4j Fulltext Index Procedures (CRITICAL for Mimir)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYNODES"):
		result, err = e.callDbIndexFulltextQueryNodes(cypher)
	// APOC Procedures (CRITICAL for Mimir graph traversal)
	case strings.Contains(upper, "APOC.PATH.SUBGRAPHNODES"):
		result, err = e.callApocPathSubgraphNodes(cypher)
	case strings.Contains(upper, "APOC.PATH.EXPAND"):
		result, err = e.callApocPathExpand(cypher)
	// NornicDB Extensions
	case strings.Contains(upper, "NORNICDB.VERSION"):
		result, err = e.callNornicDbVersion()
	case strings.Contains(upper, "NORNICDB.STATS"):
		result, err = e.callNornicDbStats()
	case strings.Contains(upper, "NORNICDB.DECAY.INFO"):
		result, err = e.callNornicDbDecayInfo()
	// Neo4j Schema/Metadata Procedures
	case strings.Contains(upper, "DB.SCHEMA.VISUALIZATION"):
		result, err = e.callDbSchemaVisualization()
	case strings.Contains(upper, "DB.SCHEMA.NODEPROPERTIES"):
		result, err = e.callDbSchemaNodeProperties()
	case strings.Contains(upper, "DB.SCHEMA.RELPROPERTIES"):
		result, err = e.callDbSchemaRelProperties()
	case strings.Contains(upper, "DB.LABELS"):
		result, err = e.callDbLabels()
	case strings.Contains(upper, "DB.RELATIONSHIPTYPES"):
		result, err = e.callDbRelationshipTypes()
	case strings.Contains(upper, "DB.INDEXES"):
		result, err = e.callDbIndexes()
	case strings.Contains(upper, "DB.CONSTRAINTS"):
		result, err = e.callDbConstraints()
	case strings.Contains(upper, "DB.PROPERTYKEYS"):
		result, err = e.callDbPropertyKeys()
	// Neo4j GDS Link Prediction Procedures (topological)
	case strings.Contains(upper, "GDS.LINKPREDICTION.ADAMICADAR.STREAM"):
		result, err = e.callGdsLinkPredictionAdamicAdar(cypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.COMMONNEIGHBORS.STREAM"):
		result, err = e.callGdsLinkPredictionCommonNeighbors(cypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.RESOURCEALLOCATION.STREAM"):
		result, err = e.callGdsLinkPredictionResourceAllocation(cypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.PREFERENTIALATTACHMENT.STREAM"):
		result, err = e.callGdsLinkPredictionPreferentialAttachment(cypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.JACCARD.STREAM"):
		result, err = e.callGdsLinkPredictionJaccard(cypher)
	case strings.Contains(upper, "GDS.LINKPREDICTION.PREDICT.STREAM"):
		result, err = e.callGdsLinkPredictionPredict(cypher)
	// Additional Neo4j procedures for compatibility
	case strings.Contains(upper, "DB.INFO"):
		result, err = e.callDbInfo()
	case strings.Contains(upper, "DB.PING"):
		result, err = e.callDbPing()
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexFulltextQueryRelationships(cypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexVectorQueryRelationships(cypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.LISTAVAILABLEANALYZERS"):
		result, err = e.callDbIndexFulltextListAvailableAnalyzers()
	case strings.Contains(upper, "DBMS.INFO"):
		result, err = e.callDbmsInfo()
	case strings.Contains(upper, "DBMS.LISTCONFIG"):
		result, err = e.callDbmsListConfig()
	case strings.Contains(upper, "DBMS.CLIENTCONFIG"):
		result, err = e.callDbmsClientConfig()
	case strings.Contains(upper, "DBMS.LISTCONNECTIONS"):
		result, err = e.callDbmsListConnections()
	case strings.Contains(upper, "DBMS.COMPONENTS"):
		result, err = e.callDbmsComponents()
	case strings.Contains(upper, "DBMS.PROCEDURES"):
		result, err = e.callDbmsProcedures()
	case strings.Contains(upper, "DBMS.FUNCTIONS"):
		result, err = e.callDbmsFunctions()
	default:
		return nil, fmt.Errorf("unknown procedure: %s", cypher)
	}

	// Return error if procedure failed
	if err != nil {
		return nil, err
	}

	// Apply YIELD clause filtering (WHERE, column selection, aliasing)
	if yield != nil {
		return e.applyYieldFilter(result, yield)
	}

	return result, nil
}

func (e *StorageExecutor) callDbLabels() (*ExecuteResult, error) {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"label"},
		Rows:    make([][]interface{}, 0, len(labelSet)),
	}
	for label := range labelSet {
		result.Rows = append(result.Rows, []interface{}{label})
	}
	return result, nil
}

func (e *StorageExecutor) callDbRelationshipTypes() (*ExecuteResult, error) {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}

	result := &ExecuteResult{
		Columns: []string{"relationshipType"},
		Rows:    make([][]interface{}, 0, len(typeSet)),
	}
	for relType := range typeSet {
		result.Rows = append(result.Rows, []interface{}{relType})
	}
	return result, nil
}

func (e *StorageExecutor) callDbIndexes() (*ExecuteResult, error) {
	// Return empty for now - no indexes implemented yet
	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties", "state"},
		Rows:    [][]interface{}{},
	}, nil
}

func (e *StorageExecutor) callDbConstraints() (*ExecuteResult, error) {
	// Return empty for now
	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties"},
		Rows:    [][]interface{}{},
	}, nil
}

func (e *StorageExecutor) callDbmsComponents() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"name", "versions", "edition"},
		Rows: [][]interface{}{
			{"NornicDB", []string{"1.0.0"}, "community"},
		},
	}, nil
}

// NornicDB-specific procedures

func (e *StorageExecutor) callNornicDbVersion() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"version", "build", "edition"},
		Rows: [][]interface{}{
			{"1.0.0", "development", "community"},
		},
	}, nil
}

func (e *StorageExecutor) callNornicDbStats() (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships", "labels", "relationshipTypes"},
		Rows: [][]interface{}{
			{nodeCount, edgeCount, e.countLabels(), e.countRelTypes()},
		},
	}, nil
}

func (e *StorageExecutor) countLabels() int {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return 0
	}
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}
	return len(labelSet)
}

func (e *StorageExecutor) countRelTypes() int {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return 0
	}
	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}
	return len(typeSet)
}

func (e *StorageExecutor) callNornicDbDecayInfo() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"enabled", "halfLifeEpisodic", "halfLifeSemantic", "halfLifeProcedural", "archiveThreshold"},
		Rows: [][]interface{}{
			{true, "7 days", "69 days", "693 days", 0.05},
		},
	}, nil
}

// Neo4j schema procedures

func (e *StorageExecutor) callDbSchemaVisualization() (*ExecuteResult, error) {
	// Return a simplified schema visualization
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	// Collect unique labels and relationship types
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	relTypeSet := make(map[string]bool)
	for _, edge := range edges {
		relTypeSet[edge.Type] = true
	}

	// Build schema nodes (one per label)
	var schemaNodes []map[string]interface{}
	for label := range labelSet {
		schemaNodes = append(schemaNodes, map[string]interface{}{
			"label": label,
		})
	}

	// Build schema relationships
	var schemaRels []map[string]interface{}
	for relType := range relTypeSet {
		schemaRels = append(schemaRels, map[string]interface{}{
			"type": relType,
		})
	}

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships"},
		Rows: [][]interface{}{
			{schemaNodes, schemaRels},
		},
	}, nil
}

func (e *StorageExecutor) callDbSchemaNodeProperties() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()

	// Collect properties per label
	labelProps := make(map[string]map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			if _, ok := labelProps[label]; !ok {
				labelProps[label] = make(map[string]bool)
			}
			for prop := range node.Properties {
				labelProps[label][prop] = true
			}
		}
	}

	result := &ExecuteResult{
		Columns: []string{"nodeLabel", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for label, props := range labelProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{label, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbSchemaRelProperties() (*ExecuteResult, error) {
	edges, _ := e.storage.AllEdges()

	// Collect properties per relationship type
	typeProps := make(map[string]map[string]bool)
	for _, edge := range edges {
		if _, ok := typeProps[edge.Type]; !ok {
			typeProps[edge.Type] = make(map[string]bool)
		}
		for prop := range edge.Properties {
			typeProps[edge.Type][prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"relType", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for relType, props := range typeProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{relType, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbPropertyKeys() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	propSet := make(map[string]bool)
	for _, node := range nodes {
		for prop := range node.Properties {
			propSet[prop] = true
		}
	}
	for _, edge := range edges {
		for prop := range edge.Properties {
			propSet[prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"propertyKey"},
		Rows:    make([][]interface{}, 0, len(propSet)),
	}
	for prop := range propSet {
		result.Rows = append(result.Rows, []interface{}{prop})
	}

	return result, nil
}

func (e *StorageExecutor) callDbmsProcedures() (*ExecuteResult, error) {
	procedures := [][]interface{}{
		{"db.labels", "Lists all labels in the database", "READ"},
		{"db.relationshipTypes", "Lists all relationship types", "READ"},
		{"db.propertyKeys", "Lists all property keys", "READ"},
		{"db.indexes", "Lists all indexes", "READ"},
		{"db.constraints", "Lists all constraints", "READ"},
		{"db.schema.visualization", "Visualizes the database schema", "READ"},
		{"db.schema.nodeProperties", "Lists node properties by label", "READ"},
		{"db.schema.relProperties", "Lists relationship properties by type", "READ"},
		{"dbms.components", "Lists database components", "DBMS"},
		{"dbms.procedures", "Lists available procedures", "DBMS"},
		{"dbms.functions", "Lists available functions", "DBMS"},
		{"nornicdb.version", "Returns NornicDB version", "READ"},
		{"nornicdb.stats", "Returns database statistics", "READ"},
		{"nornicdb.decay.info", "Returns memory decay configuration", "READ"},
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "mode"},
		Rows:    procedures,
	}, nil
}

func (e *StorageExecutor) callDbmsFunctions() (*ExecuteResult, error) {
	functions := [][]interface{}{
		{"count", "Counts items", "Aggregating"},
		{"sum", "Sums numeric values", "Aggregating"},
		{"avg", "Averages numeric values", "Aggregating"},
		{"min", "Returns minimum value", "Aggregating"},
		{"max", "Returns maximum value", "Aggregating"},
		{"collect", "Collects values into a list", "Aggregating"},
		{"id", "Returns internal ID", "Scalar"},
		{"labels", "Returns labels of a node", "Scalar"},
		{"type", "Returns type of relationship", "Scalar"},
		{"properties", "Returns properties map", "Scalar"},
		{"keys", "Returns property keys", "Scalar"},
		{"coalesce", "Returns first non-null value", "Scalar"},
		{"toString", "Converts to string", "Scalar"},
		{"toInteger", "Converts to integer", "Scalar"},
		{"toFloat", "Converts to float", "Scalar"},
		{"toBoolean", "Converts to boolean", "Scalar"},
		{"size", "Returns size of list/string", "Scalar"},
		{"length", "Returns path length", "Scalar"},
		{"head", "Returns first list element", "List"},
		{"tail", "Returns list without first element", "List"},
		{"last", "Returns last list element", "List"},
		{"range", "Creates a range list", "List"},
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "category"},
		Rows:    functions,
	}, nil
}

// ========================================
// Neo4j Vector Index Procedures (CRITICAL for Mimir)
// ========================================

// callDbIndexVectorQueryNodes implements db.index.vector.queryNodes
// Syntax: CALL db.index.vector.queryNodes('indexName', k, queryVector) YIELD node, score
//
// This is the primary vector similarity search procedure used by Mimir for:
//   - Semantic memory retrieval
//   - Similar document discovery
//   - Embedding-based node matching
//
// Parameters:
//   - indexName: Name of the vector index (from CREATE VECTOR INDEX)
//   - k: Number of results to return
//   - queryVector: The query embedding vector ([]float32 or []float64)
//
// Returns:
//   - node: The matched node with all properties
//   - score: Cosine similarity score (0.0 to 1.0)
func (e *StorageExecutor) callDbIndexVectorQueryNodes(cypher string) (*ExecuteResult, error) {
	// Parse parameters from: CALL db.index.vector.queryNodes('indexName', k, $queryVector)
	indexName, k, queryVector, err := e.parseVectorQueryParams(cypher)
	if err != nil {
		return nil, fmt.Errorf("vector query parse error: %w", err)
	}

	result := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows:    [][]interface{}{},
	}

	// Get vector index configuration (if it exists)
	var targetLabel, targetProperty string
	var similarityFunc string = "cosine"
	
	schema := e.storage.GetSchema()
	if schema != nil {
		if vectorIdx, exists := schema.GetVectorIndex(indexName); exists {
			targetLabel = vectorIdx.Label
			targetProperty = vectorIdx.Property
			similarityFunc = vectorIdx.SimilarityFunc
		}
	}

	// Get all nodes and filter to those with embeddings
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	// Collect nodes with embeddings and calculate similarities
	type scoredNode struct {
		node  *storage.Node
		score float64
	}
	var scoredNodes []scoredNode

	for _, node := range nodes {
		// Check label filter if index specifies one
		if targetLabel != "" {
			hasLabel := false
			for _, l := range node.Labels {
				if l == targetLabel {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}

		// Get embedding - check property first, then node.Embedding
		var nodeEmbedding []float32
		if targetProperty != "" {
			if emb, ok := node.Properties[targetProperty]; ok {
				nodeEmbedding = toFloat32Slice(emb)
			}
		}
		if nodeEmbedding == nil && node.Embedding != nil {
			nodeEmbedding = node.Embedding
		}

		if nodeEmbedding == nil || len(nodeEmbedding) == 0 {
			continue
		}

		// Skip if dimensions don't match
		if len(nodeEmbedding) != len(queryVector) {
			continue
		}

		// Calculate similarity
		var score float64
		switch similarityFunc {
		case "euclidean":
			score = euclideanSimilarityFloat32(queryVector, nodeEmbedding)
		case "dot":
			score = dotProductFloat32(queryVector, nodeEmbedding)
		default: // cosine
			score = cosineSimilarityFloat32(queryVector, nodeEmbedding)
		}

		scoredNodes = append(scoredNodes, scoredNode{node: node, score: score})
	}

	// Sort by score descending
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score > scoredNodes[j].score
	})

	// Limit to k results
	if k > 0 && len(scoredNodes) > k {
		scoredNodes = scoredNodes[:k]
	}

	// Convert to result rows
	for _, sn := range scoredNodes {
		result.Rows = append(result.Rows, []interface{}{
			e.nodeToMap(sn.node),
			sn.score,
		})
	}

	return result, nil
}

// parseVectorQueryParams extracts indexName, k, and queryVector from a vector query CALL
func (e *StorageExecutor) parseVectorQueryParams(cypher string) (indexName string, k int, queryVector []float32, err error) {
	// Default values
	k = 10
	indexName = "default"

	// Find the procedure call
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.VECTOR.QUERYNODES")
	if callIdx == -1 {
		return "", 0, nil, fmt.Errorf("vector query procedure not found")
	}

	// Find the opening parenthesis
	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return "", 0, nil, fmt.Errorf("missing parameters")
	}

	// Find matching closing parenthesis
	parenContent := rest[parenIdx+1:]
	depth := 1
	endIdx := -1
	for i, c := range parenContent {
		if c == '(' || c == '[' {
			depth++
		} else if c == ')' || c == ']' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}
	if endIdx == -1 {
		return "", 0, nil, fmt.Errorf("unmatched parenthesis")
	}

	params := parenContent[:endIdx]
	
	// Split parameters (careful with nested brackets)
	parts := splitParamsCarefully(params)
	
	if len(parts) >= 1 {
		// First param is index name (quoted string)
		indexName = strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	}
	
	if len(parts) >= 2 {
		// Second param is k (integer)
		kStr := strings.TrimSpace(parts[1])
		if val, parseErr := strconv.Atoi(kStr); parseErr == nil {
			k = val
		}
	}
	
	if len(parts) >= 3 {
		// Third param is query vector (array or $parameter)
		vectorStr := strings.TrimSpace(parts[2])
		
		// Check if it's a parameter reference
		if strings.HasPrefix(vectorStr, "$") {
			// Parameter will be substituted by caller - for now return nil
			// The actual vector should be passed via context/parameters
			queryVector = nil
		} else if strings.HasPrefix(vectorStr, "[") {
			// Parse inline array
			queryVector = parseInlineVector(vectorStr)
		}
	}

	return indexName, k, queryVector, nil
}

// splitParamsCarefully splits comma-separated parameters while respecting brackets
func splitParamsCarefully(params string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, c := range params {
		if c == '[' || c == '(' || c == '{' {
			depth++
			current.WriteRune(c)
		} else if c == ']' || c == ')' || c == '}' {
			depth--
			current.WriteRune(c)
		} else if c == ',' && depth == 0 {
			result = append(result, current.String())
			current.Reset()
		} else {
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// parseInlineVector parses an inline vector like [0.1, 0.2, 0.3]
func parseInlineVector(s string) []float32 {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	
	parts := strings.Split(s, ",")
	result := make([]float32, 0, len(parts))
	
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if val, err := strconv.ParseFloat(p, 32); err == nil {
			result = append(result, float32(val))
		}
	}
	
	return result
}

// toFloat32Slice converts various types to []float32
func toFloat32Slice(v interface{}) []float32 {
	switch val := v.(type) {
	case []float32:
		return val
	case []float64:
		result := make([]float32, len(val))
		for i, f := range val {
			result[i] = float32(f)
		}
		return result
	case []interface{}:
		result := make([]float32, 0, len(val))
		for _, item := range val {
			switch f := item.(type) {
			case float64:
				result = append(result, float32(f))
			case float32:
				result = append(result, f)
			case int:
				result = append(result, float32(f))
			case int64:
				result = append(result, float32(f))
			}
		}
		return result
	}
	return nil
}

// cosineSimilarityFloat32 calculates cosine similarity between two float32 vectors
func cosineSimilarityFloat32(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProd, normA, normB float64
	for i := range a {
		dotProd += float64(a[i] * b[i])
		normA += float64(a[i] * a[i])
		normB += float64(b[i] * b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProd / (math.Sqrt(normA) * math.Sqrt(normB))
}

// euclideanSimilarityFloat32 calculates similarity based on Euclidean distance
func euclideanSimilarityFloat32(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var sum float64
	for i := range a {
		diff := float64(a[i] - b[i])
		sum += diff * diff
	}

	// Convert distance to similarity (higher is more similar)
	return 1.0 / (1.0 + math.Sqrt(sum))
}

// dotProductFloat32 calculates dot product of two float32 vectors
func dotProductFloat32(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var sum float64
	for i := range a {
		sum += float64(a[i] * b[i])
	}
	return sum
}

// ========================================
// Neo4j Fulltext Index Procedures (CRITICAL for Mimir)
// ========================================

// callDbIndexFulltextQueryNodes implements db.index.fulltext.queryNodes
// Syntax: CALL db.index.fulltext.queryNodes('indexName', query) YIELD node, score
//
// This is the primary text search procedure used by Mimir for:
//   - Keyword-based memory search
//   - Content discovery
//   - Text matching across node properties
//
// Parameters:
//   - indexName: Name of the fulltext index (from CREATE FULLTEXT INDEX)
//   - query: Search query string (supports AND, OR, NOT, wildcards)
//
// Returns:
//   - node: The matched node with all properties
//   - score: BM25-like relevance score
//
// Scoring Algorithm:
//   Uses a simplified BM25-like scoring that considers:
//   - Term frequency (TF): How often query terms appear
//   - Inverse document frequency (IDF): How rare terms are
//   - Field length normalization: Shorter fields score higher
func (e *StorageExecutor) callDbIndexFulltextQueryNodes(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows:    [][]interface{}{},
	}

	// Extract query string and index name
	indexName, query := e.extractFulltextParams(cypher)
	if query == "" {
		return result, nil
	}

	// Get index configuration if it exists
	var targetLabels []string
	var targetProperties []string
	
	schema := e.storage.GetSchema()
	if schema != nil {
		if ftIdx, exists := schema.GetFulltextIndex(indexName); exists {
			targetLabels = ftIdx.Labels
			targetProperties = ftIdx.Properties
		}
	}

	// Default searchable properties if no index config
	if len(targetProperties) == 0 {
		targetProperties = []string{"content", "text", "title", "name", "description", "body", "summary"}
	}

	// Parse query into terms (supports basic AND/OR/NOT)
	queryTerms, excludeTerms, mustHaveTerms := parseFulltextQuery(query)
	if len(queryTerms) == 0 && len(mustHaveTerms) == 0 {
		return result, nil
	}

	// Get all nodes
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	// Calculate IDF for all terms (for BM25-like scoring)
	docFreq := make(map[string]int)
	totalDocs := 0
	for _, node := range nodes {
		if !matchesLabels(node, targetLabels) {
			continue
		}
		totalDocs++
		
		// Count documents containing each term
		content := extractTextContent(node, targetProperties)
		contentLower := strings.ToLower(content)
		
		allTerms := append(queryTerms, mustHaveTerms...)
		for _, term := range allTerms {
			if strings.Contains(contentLower, term) {
				docFreq[term]++
			}
		}
	}

	// Score each node
	type scoredNode struct {
		node  *storage.Node
		score float64
	}
	var scoredNodes []scoredNode

	for _, node := range nodes {
		// Check label filter
		if !matchesLabels(node, targetLabels) {
			continue
		}

		// Get searchable content
		content := extractTextContent(node, targetProperties)
		if content == "" {
			continue
		}
		contentLower := strings.ToLower(content)

		// Check exclude terms
		shouldExclude := false
		for _, term := range excludeTerms {
			if strings.Contains(contentLower, term) {
				shouldExclude = true
				break
			}
		}
		if shouldExclude {
			continue
		}

		// Check must-have terms
		hasMustHave := true
		for _, term := range mustHaveTerms {
			if !strings.Contains(contentLower, term) {
				hasMustHave = false
				break
			}
		}
		if !hasMustHave {
			continue
		}

		// Calculate BM25-like score
		score := calculateBM25Score(contentLower, queryTerms, docFreq, totalDocs)
		
		// Boost for must-have terms
		for _, term := range mustHaveTerms {
			if strings.Contains(contentLower, term) {
				score += 2.0
			}
		}

		if score > 0 {
			scoredNodes = append(scoredNodes, scoredNode{node: node, score: score})
		}
	}

	// Sort by score descending
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score > scoredNodes[j].score
	})

	// Convert to result rows
	for _, sn := range scoredNodes {
		result.Rows = append(result.Rows, []interface{}{
			e.nodeToMap(sn.node),
			sn.score,
		})
	}

	return result, nil
}

// extractFulltextParams extracts index name and query from a fulltext CALL statement
func (e *StorageExecutor) extractFulltextParams(cypher string) (indexName, query string) {
	indexName = "default"
	
	// Find the procedure call
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYNODES")
	if callIdx == -1 {
		return "", ""
	}

	// Find the opening parenthesis
	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return "", ""
	}

	// Find matching closing parenthesis
	parenContent := rest[parenIdx+1:]
	depth := 1
	endIdx := -1
	for i, c := range parenContent {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}
	if endIdx == -1 {
		return "", ""
	}

	params := parenContent[:endIdx]
	parts := splitParamsCarefully(params)
	
	if len(parts) >= 1 {
		indexName = strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	}
	
	if len(parts) >= 2 {
		query = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	}

	return indexName, query
}

// parseFulltextQuery parses a fulltext query into regular terms, exclude terms, and must-have terms
func parseFulltextQuery(query string) (terms, excludeTerms, mustHaveTerms []string) {
	query = strings.ToLower(query)
	
	// Handle quoted phrases
	phraseRegex := regexp.MustCompile(`"([^"]+)"`)
	phrases := phraseRegex.FindAllStringSubmatch(query, -1)
	for _, match := range phrases {
		mustHaveTerms = append(mustHaveTerms, match[1])
	}
	query = phraseRegex.ReplaceAllString(query, "")
	
	// Split by spaces and operators
	words := strings.Fields(query)
	
	for i := 0; i < len(words); i++ {
		word := words[i]
		
		// Handle NOT operator
		if word == "not" && i+1 < len(words) {
			excludeTerms = append(excludeTerms, words[i+1])
			i++
			continue
		}
		
		// Handle - prefix for exclusion
		if strings.HasPrefix(word, "-") && len(word) > 1 {
			excludeTerms = append(excludeTerms, word[1:])
			continue
		}
		
		// Handle + prefix for required
		if strings.HasPrefix(word, "+") && len(word) > 1 {
			mustHaveTerms = append(mustHaveTerms, word[1:])
			continue
		}
		
		// Skip AND/OR operators
		if word == "and" || word == "or" {
			continue
		}
		
		// Regular term
		if len(word) > 0 {
			terms = append(terms, word)
		}
	}
	
	return terms, excludeTerms, mustHaveTerms
}

// matchesLabels checks if a node has any of the target labels (empty = all match)
func matchesLabels(node *storage.Node, targetLabels []string) bool {
	if len(targetLabels) == 0 {
		return true
	}
	for _, nl := range node.Labels {
		for _, tl := range targetLabels {
			if nl == tl {
				return true
			}
		}
	}
	return false
}

// extractTextContent extracts searchable text content from a node
func extractTextContent(node *storage.Node, properties []string) string {
	var content strings.Builder
	
	for _, propName := range properties {
		if val, ok := node.Properties[propName]; ok {
			content.WriteString(fmt.Sprintf("%v ", val))
		}
	}
	
	return strings.TrimSpace(content.String())
}

// calculateBM25Score calculates a BM25-like score for a document
func calculateBM25Score(content string, terms []string, docFreq map[string]int, totalDocs int) float64 {
	if totalDocs == 0 {
		return 0
	}

	// BM25 parameters
	k1 := 1.2
	b := 0.75
	avgDocLen := 100.0 // Assume average document length

	docLen := float64(len(strings.Fields(content)))
	var score float64

	for _, term := range terms {
		tf := float64(strings.Count(content, term))
		if tf == 0 {
			continue
		}

		// IDF calculation using BM25 formula with smoothing
		df := float64(docFreq[term])
		if df == 0 {
			df = 0.5 // Smoothing for unseen terms
		}
		
		// Use IDF+ variant: log((N + 1) / df) to ensure positive IDF
		// This prevents common terms from having zero or negative IDF
		idf := math.Log((float64(totalDocs)+1) / df)
		if idf < 0.1 {
			idf = 0.1 // Minimum IDF floor
		}

		// TF normalization
		tfNorm := (tf * (k1 + 1)) / (tf + k1*(1-b+b*(docLen/avgDocLen)))

		score += idf * tfNorm
	}

	return score
}

// extractFulltextQuery extracts the search query from a fulltext CALL statement (legacy)
func (e *StorageExecutor) extractFulltextQuery(cypher string) string {
	_, query := e.extractFulltextParams(cypher)
	return query
}

// ========================================
// APOC Path Procedures (CRITICAL for Mimir graph traversal)
// ========================================

// callApocPathSubgraphNodes implements apoc.path.subgraphNodes
// Syntax: CALL apoc.path.subgraphNodes(startNode, {maxLevel: n, relationshipFilter: 'TYPE'})
//
// This is the primary graph traversal procedure used by Mimir for:
//   - Knowledge graph exploration
//   - Relationship discovery
//   - Context gathering from connected nodes
//
// Config Parameters:
//   - maxLevel: Maximum traversal depth (default: 3)
//   - relationshipFilter: Filter by relationship types (e.g., "RELATES_TO|CONTAINS")
//   - labelFilter: Filter by node labels (e.g., "+Memory|-Archive")
//   - minLevel: Minimum traversal depth before returning results
//   - limit: Maximum number of nodes to return
//   - bfs: Use breadth-first search (default: true)
//
// Relationship Filter Syntax:
//   - "TYPE" - Match relationships of type TYPE in either direction
//   - ">TYPE" - Match outgoing relationships of type TYPE
//   - "<TYPE" - Match incoming relationships of type TYPE
//   - "TYPE1|TYPE2" - Match multiple types
//
// Label Filter Syntax:
//   - "+Label" - Only include nodes with Label
//   - "-Label" - Exclude nodes with Label
//   - "/Label" - Terminate traversal at nodes with Label (end nodes)
func (e *StorageExecutor) callApocPathSubgraphNodes(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{},
	}

	// Parse configuration and start node
	config := e.parseApocPathConfig(cypher)
	startNodeID := e.extractStartNodeID(cypher)

	// Get starting node(s)
	var startNodes []*storage.Node
	if startNodeID == "*" {
		// Special case: traverse from all nodes (when no specific start node)
		allNodes, err := e.storage.AllNodes()
		if err != nil {
			return nil, err
		}
		startNodes = allNodes
	} else if startNodeID != "" {
		if node, err := e.storage.GetNode(storage.NodeID(startNodeID)); err == nil && node != nil {
			startNodes = append(startNodes, node)
		}
	} else {
		// If no start node at all (parameter reference), return empty
		return result, nil
	}

	if len(startNodes) == 0 {
		return result, nil
	}

	// BFS traversal
	visited := make(map[string]bool)
	var resultNodes []*storage.Node

	for _, startNode := range startNodes {
		nodes := e.bfsTraversal(startNode, config, visited)
		resultNodes = append(resultNodes, nodes...)
	}

	// Apply limit if specified
	if config.limit > 0 && len(resultNodes) > config.limit {
		resultNodes = resultNodes[:config.limit]
	}

	// Convert to result rows
	for _, node := range resultNodes {
		result.Rows = append(result.Rows, []interface{}{e.nodeToMap(node)})
	}

	return result, nil
}

// apocPathConfig holds parsed APOC path configuration
type apocPathConfig struct {
	maxLevel          int
	minLevel          int
	relationshipTypes []string
	direction         string // "both", "outgoing", "incoming"
	includeLabels     []string
	excludeLabels     []string
	terminateLabels   []string
	limit             int
	bfs               bool
}

// parseApocPathConfig extracts configuration from APOC path calls
func (e *StorageExecutor) parseApocPathConfig(cypher string) apocPathConfig {
	config := apocPathConfig{
		maxLevel:  3,
		minLevel:  0,
		direction: "both",
		bfs:       true,
		limit:     0, // No limit
	}

	// Find config object { ... }
	configStart := strings.Index(cypher, "{")
	configEnd := strings.LastIndex(cypher, "}")
	if configStart == -1 || configEnd == -1 || configEnd <= configStart {
		return config
	}

	configStr := cypher[configStart+1 : configEnd]

	// Parse maxLevel
	if match := regexp.MustCompile(`maxLevel\s*:\s*(\d+)`).FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil && level > 0 {
			config.maxLevel = level
		}
	}

	// Parse minLevel
	if match := regexp.MustCompile(`minLevel\s*:\s*(\d+)`).FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil {
			config.minLevel = level
		}
	}

	// Parse limit
	if match := regexp.MustCompile(`limit\s*:\s*(\d+)`).FindStringSubmatch(configStr); len(match) > 1 {
		if limit, err := strconv.Atoi(match[1]); err == nil {
			config.limit = limit
		}
	}

	// Parse relationshipFilter
	if match := regexp.MustCompile(`relationshipFilter\s*:\s*['"]([^'"]+)['"]`).FindStringSubmatch(configStr); len(match) > 1 {
		filterStr := match[1]
		config.relationshipTypes, config.direction = parseRelationshipFilter(filterStr)
	}

	// Parse labelFilter
	if match := regexp.MustCompile(`labelFilter\s*:\s*['"]([^'"]+)['"]`).FindStringSubmatch(configStr); len(match) > 1 {
		filterStr := match[1]
		config.includeLabels, config.excludeLabels, config.terminateLabels = parseLabelFilter(filterStr)
	}

	// Parse bfs
	if strings.Contains(configStr, "bfs: false") || strings.Contains(configStr, "bfs:false") {
		config.bfs = false
	}

	return config
}

// parseRelationshipFilter parses a relationship filter string
func parseRelationshipFilter(filter string) (types []string, direction string) {
	direction = "both"
	
	// Handle direction prefix
	if strings.HasPrefix(filter, ">") {
		direction = "outgoing"
		filter = filter[1:]
	} else if strings.HasPrefix(filter, "<") {
		direction = "incoming"
		filter = filter[1:]
	}

	// Split by | for multiple types
	for _, t := range strings.Split(filter, "|") {
		t = strings.TrimSpace(t)
		if t != "" && t != ">" && t != "<" {
			types = append(types, t)
		}
	}

	return types, direction
}

// parseLabelFilter parses a label filter string
func parseLabelFilter(filter string) (include, exclude, terminate []string) {
	parts := strings.Split(filter, "|")
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		
		if strings.HasPrefix(part, "+") {
			include = append(include, part[1:])
		} else if strings.HasPrefix(part, "-") {
			exclude = append(exclude, part[1:])
		} else if strings.HasPrefix(part, "/") {
			terminate = append(terminate, part[1:])
		}
	}
	
	return include, exclude, terminate
}

// extractStartNodeID extracts the starting node ID from the CALL statement
func (e *StorageExecutor) extractStartNodeID(cypher string) string {
	// Look for node variable in MATCH clause
	// Pattern: MATCH (varName:Label {id: 'value'}) or MATCH (varName) WHERE varName.id = 'value'
	
	// Try to find a MATCH pattern with id property
	if match := regexp.MustCompile(`\{[^}]*id\s*:\s*['"]([^'"]+)['"]`).FindStringSubmatch(cypher); len(match) > 1 {
		return match[1]
	}
	
	// Try to find WHERE clause with id
	if match := regexp.MustCompile(`WHERE\s+\w+\.id\s*=\s*['"]([^'"]+)['"]`).FindStringSubmatch(cypher); len(match) > 1 {
		return match[1]
	}
	
	// Try to find $nodeId parameter (would need to be substituted)
	if strings.Contains(cypher, "$nodeId") || strings.Contains(cypher, "$startNode") {
		return ""
	}
	
	// Return special marker for "traverse all" when no specific ID found
	return "*"
}

// bfsTraversal performs breadth-first traversal from a start node
func (e *StorageExecutor) bfsTraversal(startNode *storage.Node, config apocPathConfig, globalVisited map[string]bool) []*storage.Node {
	var results []*storage.Node
	
	// Queue: (node, level)
	type queueItem struct {
		node  *storage.Node
		level int
	}
	queue := []queueItem{{node: startNode, level: 0}}
	
	// Track visited for this traversal
	visited := make(map[string]bool)
	visited[string(startNode.ID)] = true
	
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		
		node := item.node
		level := item.level
		
		// Check if we should include this node
		if level >= config.minLevel && !globalVisited[string(node.ID)] {
			// Check label filters
			if passesLabelFilter(node, config.includeLabels, config.excludeLabels) {
				results = append(results, node)
				globalVisited[string(node.ID)] = true
			}
		}
		
		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}
		
		// Check if we've reached max level
		if level >= config.maxLevel {
			continue
		}
		
		// Get edges based on direction
		var edges []*storage.Edge
		switch config.direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(node.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(node.ID)
			in, _ := e.storage.GetIncomingEdges(node.ID)
			edges = append(out, in...)
		}
		
		// Process each edge
		for _, edge := range edges {
			// Check relationship type filter
			if len(config.relationshipTypes) > 0 {
				found := false
				for _, t := range config.relationshipTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
			
			// Get the other node
			var nextNodeID storage.NodeID
			if edge.StartNode == node.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}
			
			// Skip if already visited
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true
			
			// Get the node and add to queue
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err == nil && nextNode != nil {
				queue = append(queue, queueItem{node: nextNode, level: level + 1})
			}
		}
	}
	
	return results
}

// passesLabelFilter checks if a node passes the label filter
func passesLabelFilter(node *storage.Node, include, exclude []string) bool {
	// Check exclude labels first
	for _, excLabel := range exclude {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == excLabel {
				return false
			}
		}
	}
	
	// If no include labels specified, pass
	if len(include) == 0 {
		return true
	}
	
	// Check include labels
	for _, incLabel := range include {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == incLabel {
				return true
			}
		}
	}
	
	return false
}

// isTerminateNode checks if traversal should terminate at this node
func isTerminateNode(node *storage.Node, terminateLabels []string) bool {
	for _, termLabel := range terminateLabels {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == termLabel {
				return true
			}
		}
	}
	return false
}

// callApocPathExpand implements apoc.path.expand
// Syntax: CALL apoc.path.expand(startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
//
// Similar to subgraphNodes but returns paths instead of just nodes.
// For now, delegates to subgraphNodes since path tracking is not yet implemented.
func (e *StorageExecutor) callApocPathExpand(cypher string) (*ExecuteResult, error) {
	// For now, delegate to subgraphNodes with the same logic
	// A full implementation would track and return actual paths
	subgraphResult, err := e.callApocPathSubgraphNodes(cypher)
	if err != nil {
		return nil, err
	}

	// Convert to path format
	result := &ExecuteResult{
		Columns: []string{"path"},
		Rows:    make([][]interface{}, 0, len(subgraphResult.Rows)),
	}

	for _, row := range subgraphResult.Rows {
		if len(row) > 0 {
			// Wrap node in a simple path representation
			result.Rows = append(result.Rows, []interface{}{
				map[string]interface{}{
					"nodes":         []interface{}{row[0]},
					"relationships": []interface{}{},
					"length":        0,
				},
			})
		}
	}

	return result, nil
}

// ===== Additional Neo4j Compatibility Procedures =====

// callDbInfo returns database information - Neo4j db.info()
func (e *StorageExecutor) callDbInfo() (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	return &ExecuteResult{
		Columns: []string{"id", "name", "creationDate", "nodeCount", "relationshipCount"},
		Rows: [][]interface{}{
			{"nornicdb-default", "nornicdb", "2024-01-01T00:00:00Z", nodeCount, edgeCount},
		},
	}, nil
}

// callDbPing checks database connectivity - Neo4j db.ping()
func (e *StorageExecutor) callDbPing() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"success"},
		Rows:    [][]interface{}{{true}},
	}, nil
}

// callDbmsInfo returns DBMS information - Neo4j dbms.info()
func (e *StorageExecutor) callDbmsInfo() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"id", "name", "creationDate"},
		Rows: [][]interface{}{
			{"nornicdb-instance", "NornicDB", "2024-01-01T00:00:00Z"},
		},
	}, nil
}

// callDbmsListConfig lists DBMS configuration - Neo4j dbms.listConfig()
func (e *StorageExecutor) callDbmsListConfig() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"name", "description", "value", "dynamic"},
		Rows: [][]interface{}{
			{"nornicdb.version", "NornicDB version", "1.0.0", false},
			{"nornicdb.bolt.enabled", "Bolt protocol enabled", true, false},
			{"nornicdb.http.enabled", "HTTP API enabled", true, false},
		},
	}, nil
}

// callDbmsClientConfig lists client-visible configuration - Neo4j dbms.clientConfig()
func (e *StorageExecutor) callDbmsClientConfig() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"name", "value"},
		Rows: [][]interface{}{
			{"server.bolt.advertised_address", "localhost:7687"},
			{"server.http.advertised_address", "localhost:7474"},
		},
	}, nil
}

// callDbmsListConnections lists active connections - Neo4j dbms.listConnections()
func (e *StorageExecutor) callDbmsListConnections() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"connectionId", "connectTime", "connector", "username", "userAgent", "clientAddress"},
		Rows:    [][]interface{}{},
	}, nil
}

// callDbIndexFulltextListAvailableAnalyzers lists fulltext analyzers - Neo4j db.index.fulltext.listAvailableAnalyzers()
func (e *StorageExecutor) callDbIndexFulltextListAvailableAnalyzers() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"analyzer", "description"},
		Rows: [][]interface{}{
			{"standard-no-stop-words", "Standard analyzer without stop words"},
			{"simple", "Simple analyzer with lowercase tokenizer"},
			{"whitespace", "Whitespace analyzer"},
			{"keyword", "Keyword analyzer - entire string as single token"},
			{"url-or-email", "URL or email analyzer"},
		},
	}, nil
}

// callDbIndexFulltextQueryRelationships searches relationships using fulltext index - Neo4j db.index.fulltext.queryRelationships()
func (e *StorageExecutor) callDbIndexFulltextQueryRelationships(cypher string) (*ExecuteResult, error) {
	// Extract query string from CALL statement
	query := e.extractFulltextQuery(cypher)
	if query == "" {
		return &ExecuteResult{
			Columns: []string{"relationship", "score"},
			Rows:    [][]interface{}{},
		}, nil
	}

	// Get all edges and search them
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	lowerQuery := strings.ToLower(query)
	results := [][]interface{}{}

	for _, edge := range edges {
		// Search in edge properties
		for _, val := range edge.Properties {
			if str, ok := val.(string); ok {
				if strings.Contains(strings.ToLower(str), lowerQuery) {
					results = append(results, []interface{}{
						map[string]interface{}{
							"_id":        string(edge.ID),
							"_type":      edge.Type,
							"_start":     string(edge.StartNode),
							"_end":       string(edge.EndNode),
							"properties": edge.Properties,
						},
						1.0, // score
					})
					break
				}
			}
		}
	}

	return &ExecuteResult{
		Columns: []string{"relationship", "score"},
		Rows:    results,
	}, nil
}

// callDbIndexVectorQueryRelationships searches relationships using vector similarity - Neo4j db.index.vector.queryRelationships()
func (e *StorageExecutor) callDbIndexVectorQueryRelationships(cypher string) (*ExecuteResult, error) {
	// For now, return empty - relationships typically don't have embeddings
	// A full implementation would need relationship embeddings
	return &ExecuteResult{
		Columns: []string{"relationship", "score"},
		Rows:    [][]interface{}{},
	}, nil
}
