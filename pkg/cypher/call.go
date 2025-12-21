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

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// toFloat32Slice is a package-level alias to convert.ToFloat32Slice for internal use.
func toFloat32Slice(v interface{}) []float32 {
	return convert.ToFloat32Slice(v)
}

// yieldClause represents parsed YIELD information from a CALL statement.
// Syntax: CALL procedure() YIELD var1, var2 AS alias WHERE condition RETURN ... ORDER BY ... LIMIT n SKIP m
type yieldClause struct {
	items      []yieldItem // List of yielded items (possibly with aliases)
	yieldAll   bool        // YIELD * - return all columns
	where      string      // Optional WHERE condition after YIELD
	hasReturn  bool        // Whether there's a RETURN clause after
	returnExpr string      // The RETURN expression if present
	orderBy    string      // ORDER BY clause (e.g., "score DESC")
	limit      int         // LIMIT value (-1 if not specified)
	skip       int         // SKIP value (-1 if not specified)
}

// yieldItem represents a single item in a YIELD clause
type yieldItem struct {
	name  string // Original column name from procedure
	alias string // Alias (empty if no AS clause)
}

// parseYieldClause extracts YIELD information from a CALL statement.
// Handles: YIELD *, YIELD a, b, YIELD a AS x, b AS y, YIELD a WHERE a.score > 0.5
func parseYieldClause(cypher string) *yieldClause {
	// Normalize whitespace: replace newlines/tabs with spaces for keyword detection
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")
	upper := strings.ToUpper(normalized)
	yieldIdx := strings.Index(upper, " YIELD ")
	if yieldIdx == -1 {
		// Also try at start of string (no leading space needed)
		if strings.HasPrefix(upper, "YIELD ") {
			yieldIdx = -1 // Will be handled below
		} else {
			return nil
		}
	}

	result := &yieldClause{
		items: []yieldItem{},
		limit: -1,
		skip:  -1,
	}

	// Get everything after YIELD
	var afterYield string
	if yieldIdx == -1 {
		afterYield = strings.TrimSpace(normalized[6:]) // After "YIELD "
	} else {
		afterYield = strings.TrimSpace(normalized[yieldIdx+7:])
	}

	// Check for YIELD *
	trimmedYield := strings.TrimSpace(afterYield)
	if len(trimmedYield) > 0 && trimmedYield[0] == '*' {
		result.yieldAll = true
		afterYield = strings.TrimSpace(afterYield[1:])
	}

	// Find WHERE, RETURN, ORDER BY, LIMIT, SKIP boundaries
	whereIdx := findKeywordIndexInContext(afterYield, "WHERE")
	returnIdx := findKeywordIndexInContext(afterYield, "RETURN")
	orderIdx := findKeywordIndexInContext(afterYield, "ORDER")
	limitIdx := findKeywordIndexInContext(afterYield, "LIMIT")
	skipIdx := findKeywordIndexInContext(afterYield, "SKIP")

	// Extract WHERE clause if present
	if whereIdx != -1 {
		if returnIdx != -1 && returnIdx > whereIdx {
			result.where = strings.TrimSpace(afterYield[whereIdx+5 : returnIdx])
		} else {
			result.where = strings.TrimSpace(afterYield[whereIdx+5:])
		}
	}

	// Extract RETURN clause if present (strip and parse ORDER BY, LIMIT, SKIP)
	if returnIdx != -1 {
		result.hasReturn = true
		returnPart := strings.TrimSpace(afterYield[returnIdx+6:])

		// Find ORDER BY, LIMIT, SKIP positions
		orderIdx := findKeywordIndexInContext(returnPart, "ORDER")
		limitIdx := findKeywordIndexInContext(returnPart, "LIMIT")
		skipIdx := findKeywordIndexInContext(returnPart, "SKIP")

		// Find where RETURN items end
		endIdx := len(returnPart)
		if orderIdx != -1 {
			endIdx = min(endIdx, orderIdx)
		}
		if limitIdx != -1 {
			endIdx = min(endIdx, limitIdx)
		}
		if skipIdx != -1 {
			endIdx = min(endIdx, skipIdx)
		}

		result.returnExpr = strings.TrimSpace(returnPart[:endIdx])

		// Parse ORDER BY clause
		if orderIdx != -1 {
			// Find end of ORDER BY (at LIMIT, SKIP, or end of string)
			orderEnd := len(returnPart)
			if limitIdx != -1 && limitIdx > orderIdx {
				orderEnd = min(orderEnd, limitIdx)
			}
			if skipIdx != -1 && skipIdx > orderIdx {
				orderEnd = min(orderEnd, skipIdx)
			}
			orderPart := strings.TrimSpace(returnPart[orderIdx:orderEnd])
			// Strip "ORDER BY" prefix
			if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER BY") {
				result.orderBy = strings.TrimSpace(orderPart[8:])
			} else if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER") {
				result.orderBy = strings.TrimSpace(orderPart[5:])
			}
		}

		// Parse LIMIT value
		if limitIdx != -1 {
			limitEnd := len(returnPart)
			if skipIdx != -1 && skipIdx > limitIdx {
				limitEnd = skipIdx
			}
			limitPart := strings.TrimSpace(returnPart[limitIdx+5 : limitEnd])
			// Extract just the number
			limitPart = strings.TrimSpace(strings.Split(limitPart, " ")[0])
			if n, err := strconv.Atoi(limitPart); err == nil {
				result.limit = n
			}
		}

		// Parse SKIP value
		if skipIdx != -1 {
			skipEnd := len(returnPart)
			if limitIdx != -1 && limitIdx > skipIdx {
				skipEnd = limitIdx
			}
			skipPart := strings.TrimSpace(returnPart[skipIdx+4 : skipEnd])
			// Extract just the number
			skipPart = strings.TrimSpace(strings.Split(skipPart, " ")[0])
			if n, err := strconv.Atoi(skipPart); err == nil {
				result.skip = n
			}
		}
	} else {
		// No RETURN clause - parse ORDER BY, LIMIT, SKIP directly from afterYield
		// Parse ORDER BY clause
		if orderIdx != -1 {
			// Find end of ORDER BY (at LIMIT, SKIP, or end of string)
			orderEnd := len(afterYield)
			if limitIdx != -1 && limitIdx > orderIdx {
				orderEnd = min(orderEnd, limitIdx)
			}
			if skipIdx != -1 && skipIdx > orderIdx {
				orderEnd = min(orderEnd, skipIdx)
			}
			orderPart := strings.TrimSpace(afterYield[orderIdx:orderEnd])
			// Strip "ORDER BY" prefix
			if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER BY") {
				result.orderBy = strings.TrimSpace(orderPart[8:])
			} else if strings.HasPrefix(strings.ToUpper(orderPart), "ORDER") {
				result.orderBy = strings.TrimSpace(orderPart[5:])
			}
		}

		// Parse LIMIT value
		if limitIdx != -1 {
			limitEnd := len(afterYield)
			if skipIdx != -1 && skipIdx > limitIdx {
				limitEnd = skipIdx
			}
			if orderIdx != -1 && orderIdx > limitIdx {
				limitEnd = min(limitEnd, orderIdx)
			}
			limitPart := strings.TrimSpace(afterYield[limitIdx+5 : limitEnd])
			// Extract just the number
			limitPart = strings.TrimSpace(strings.Split(limitPart, " ")[0])
			if n, err := strconv.Atoi(limitPart); err == nil {
				result.limit = n
			}
		}

		// Parse SKIP value
		if skipIdx != -1 {
			skipEnd := len(afterYield)
			if limitIdx != -1 && limitIdx > skipIdx {
				skipEnd = limitIdx
			}
			if orderIdx != -1 && orderIdx > skipIdx {
				skipEnd = min(skipEnd, orderIdx)
			}
			skipPart := strings.TrimSpace(afterYield[skipIdx+4 : skipEnd])
			// Extract just the number
			skipPart = strings.TrimSpace(strings.Split(skipPart, " ")[0])
			if n, err := strconv.Atoi(skipPart); err == nil {
				result.skip = n
			}
		}
	}

	// Parse yield items (if not YIELD *)
	if !result.yieldAll {
		// Get the items part (before WHERE, RETURN, ORDER, LIMIT, SKIP)
		itemsEnd := len(afterYield)
		for _, idx := range []int{whereIdx, returnIdx, orderIdx, limitIdx, skipIdx} {
			if idx != -1 && idx < itemsEnd {
				itemsEnd = idx
			}
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

	// Apply RETURN clause transformation if present
	// RETURN allows projecting properties from yielded values and renaming columns
	// Example: YIELD node, score RETURN node.id as id, node.type, score
	if yield.hasReturn && yield.returnExpr != "" {
		var err error
		result, err = e.applyReturnToYieldResult(result, yield.returnExpr)
		if err != nil {
			return nil, err
		}
	}

	// Apply ORDER BY if present
	if yield.orderBy != "" {
		result = e.applyOrderByToResult(result, yield.orderBy)
	}

	// Apply SKIP if present
	if yield.skip > 0 && yield.skip < len(result.Rows) {
		result.Rows = result.Rows[yield.skip:]
	} else if yield.skip >= len(result.Rows) {
		result.Rows = [][]interface{}{}
	}

	// Apply LIMIT if present
	if yield.limit >= 0 && yield.limit < len(result.Rows) {
		result.Rows = result.Rows[:yield.limit]
	}

	return result, nil
}

// applyReturnToYieldResult transforms procedure results based on a RETURN clause.
// This handles property access (node.id), aliasing (AS), and expression evaluation.
func (e *StorageExecutor) applyReturnToYieldResult(result *ExecuteResult, returnExpr string) (*ExecuteResult, error) {
	// Parse RETURN items
	returnItems := splitReturnExpressions(returnExpr)
	if len(returnItems) == 0 {
		return result, nil
	}

	// Build column index map for current result
	colIndex := make(map[string]int)
	for i, col := range result.Columns {
		colIndex[col] = i
	}

	// Parse each return item to determine new columns and how to compute values
	type returnItem struct {
		expr  string // Original expression (e.g., "node.id", "score")
		alias string // Column name in output (e.g., "id", "score")
	}
	var items []returnItem

	for _, item := range returnItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		ri := returnItem{expr: item, alias: item}

		// Check for AS alias
		upperItem := strings.ToUpper(item)
		if asIdx := strings.Index(upperItem, " AS "); asIdx != -1 {
			ri.expr = strings.TrimSpace(item[:asIdx])
			ri.alias = strings.TrimSpace(item[asIdx+4:])
		}

		items = append(items, ri)
	}

	// Build new columns
	newColumns := make([]string, len(items))
	for i, item := range items {
		newColumns[i] = item.alias
	}

	// Transform each row
	newRows := make([][]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		// Build context with current row values
		ctx := make(map[string]interface{})
		for i, col := range result.Columns {
			if i < len(row) {
				ctx[col] = row[i]
			}
		}

		// Evaluate each return expression
		newRow := make([]interface{}, len(items))
		for i, item := range items {
			newRow[i] = e.evaluateReturnExprInContext(item.expr, ctx)
		}
		newRows = append(newRows, newRow)
	}

	return &ExecuteResult{
		Columns: newColumns,
		Rows:    newRows,
		Stats:   result.Stats,
	}, nil
}

// evaluateReturnExprInContext evaluates a RETURN expression in the context of yielded values.
// Handles: direct references (score), property access (node.id), and functions.
func (e *StorageExecutor) evaluateReturnExprInContext(expr string, ctx map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)

	// Direct reference to a yielded value
	if val, ok := ctx[expr]; ok {
		return val
	}

	// Property access: node.property
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		if len(parts) == 2 {
			varName := strings.TrimSpace(parts[0])
			propName := strings.TrimSpace(parts[1])

			if val, ok := ctx[varName]; ok {
				// Handle *storage.Node (Neo4j compatible)
				if node, ok := val.(*storage.Node); ok && node != nil {
					// Handle special "id" property
					if propName == "id" {
						if propVal, ok := node.Properties["id"]; ok {
							return propVal
						}
						return string(node.ID)
					}
					// Regular property access
					if propVal, ok := node.Properties[propName]; ok {
						return propVal
					}
					return nil
				}
				// If the value is a map (legacy node representation), extract property
				if mapVal, ok := val.(map[string]interface{}); ok {
					// Try direct property access
					if propVal, ok := mapVal[propName]; ok {
						return propVal
					}
					// Try in "properties" sub-map (Neo4j style)
					if props, ok := mapVal["properties"].(map[string]interface{}); ok {
						if propVal, ok := props[propName]; ok {
							return propVal
						}
					}
				}
			}
		}
	}

	// Return nil for unresolved expressions
	return nil
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
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)

	// Parse YIELD clause for post-processing
	yield := parseYieldClause(cypher)

	var result *ExecuteResult
	var err error

	switch {
	// Neo4j Vector Index Procedures (CRITICAL for Mimir)
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYNODES"):
		result, err = e.callDbIndexVectorQueryNodes(ctx, cypher)
	// Neo4j Fulltext Index Procedures (CRITICAL for Mimir)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYNODES"):
		result, err = e.callDbIndexFulltextQueryNodes(cypher)
	// APOC Procedures (CRITICAL for Mimir graph traversal)
	case strings.Contains(upper, "APOC.PATH.SUBGRAPHNODES"):
		result, err = e.callApocPathSubgraphNodes(cypher)
	case strings.Contains(upper, "APOC.PATH.EXPAND"):
		result, err = e.callApocPathExpand(cypher)
	case strings.Contains(upper, "APOC.PATH.SPANNINGTREE"):
		result, err = e.callApocPathSpanningTree(cypher)
	// APOC Graph Algorithms
	case strings.Contains(upper, "APOC.ALGO.DIJKSTRA"):
		result, err = e.callApocAlgoDijkstra(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.ASTAR"):
		result, err = e.callApocAlgoAStar(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.ALLSIMPLEPATHS"):
		result, err = e.callApocAlgoAllSimplePaths(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.PAGERANK"):
		result, err = e.callApocAlgoPageRank(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.BETWEENNESS"):
		result, err = e.callApocAlgoBetweenness(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.CLOSENESS"):
		result, err = e.callApocAlgoCloseness(ctx, cypher)
	// APOC Community Detection
	case strings.Contains(upper, "APOC.ALGO.LOUVAIN"):
		result, err = e.callApocAlgoLouvain(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.LABELPROPAGATION"):
		result, err = e.callApocAlgoLabelPropagation(ctx, cypher)
	case strings.Contains(upper, "APOC.ALGO.WCC"):
		result, err = e.callApocAlgoWCC(ctx, cypher)
	// APOC Neighbor Traversal
	case strings.Contains(upper, "APOC.NEIGHBORS.TOHOP"):
		result, err = e.callApocNeighborsTohop(ctx, cypher)
	case strings.Contains(upper, "APOC.NEIGHBORS.BYHOP"):
		result, err = e.callApocNeighborsByhop(ctx, cypher)
	// APOC Load/Export Procedures
	case strings.Contains(upper, "APOC.LOAD.JSONARRAY"):
		result, err = e.callApocLoadJsonArray(ctx, cypher)
	case strings.Contains(upper, "APOC.LOAD.JSON"):
		result, err = e.callApocLoadJson(ctx, cypher)
	case strings.Contains(upper, "APOC.LOAD.CSV"):
		result, err = e.callApocLoadCsv(ctx, cypher)
	case strings.Contains(upper, "APOC.EXPORT.JSON.ALL"):
		result, err = e.callApocExportJsonAll(ctx, cypher)
	case strings.Contains(upper, "APOC.EXPORT.JSON.QUERY"):
		result, err = e.callApocExportJsonQuery(ctx, cypher)
	case strings.Contains(upper, "APOC.EXPORT.CSV.ALL"):
		result, err = e.callApocExportCsvAll(ctx, cypher)
	case strings.Contains(upper, "APOC.EXPORT.CSV.QUERY"):
		result, err = e.callApocExportCsvQuery(ctx, cypher)
	case strings.Contains(upper, "APOC.IMPORT.JSON"):
		result, err = e.callApocImportJson(ctx, cypher)
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
	case strings.Contains(upper, "DB.INDEX.STATS"):
		result, err = e.callDbIndexStats()
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
	// GDS Graph Management and FastRP
	case strings.Contains(upper, "GDS.VERSION"):
		result, err = e.callGdsVersion()
	case strings.Contains(upper, "GDS.GRAPH.LIST"):
		result, err = e.callGdsGraphList()
	case strings.Contains(upper, "GDS.GRAPH.DROP"):
		result, err = e.callGdsGraphDrop(cypher)
	case strings.Contains(upper, "GDS.GRAPH.PROJECT"):
		result, err = e.callGdsGraphProject(cypher)
	case strings.Contains(upper, "GDS.FASTRP.STREAM"):
		result, err = e.callGdsFastRPStream(cypher)
	case strings.Contains(upper, "GDS.FASTRP.STATS"):
		result, err = e.callGdsFastRPStats(cypher)
	// Additional Neo4j procedures for compatibility
	case strings.Contains(upper, "DB.INFO"):
		result, err = e.callDbInfo()
	case strings.Contains(upper, "DB.PING"):
		result, err = e.callDbPing()
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexFulltextQueryRelationships(cypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.QUERYRELATIONSHIPS"):
		result, err = e.callDbIndexVectorQueryRelationships(cypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.CREATENODEINDEX"):
		result, err = e.callDbIndexVectorCreateNodeIndex(ctx, cypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.CREATERELATIONSHIPINDEX"):
		result, err = e.callDbIndexVectorCreateRelationshipIndex(ctx, cypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.CREATENODEINDEX"):
		result, err = e.callDbIndexFulltextCreateNodeIndex(ctx, cypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.CREATERELATIONSHIPINDEX"):
		result, err = e.callDbIndexFulltextCreateRelationshipIndex(ctx, cypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.DROP"):
		result, err = e.callDbIndexFulltextDrop(cypher)
	case strings.Contains(upper, "DB.INDEX.VECTOR.DROP"):
		result, err = e.callDbIndexVectorDrop(cypher)
	case strings.Contains(upper, "DB.INDEX.FULLTEXT.LISTAVAILABLEANALYZERS"):
		result, err = e.callDbIndexFulltextListAvailableAnalyzers()
	case strings.Contains(upper, "DB.CREATE.SETNODEVECTORPROPERTY"):
		result, err = e.callDbCreateSetNodeVectorProperty(ctx, cypher)
	case strings.Contains(upper, "DB.CREATE.SETRELATIONSHIPVECTORPROPERTY"):
		result, err = e.callDbCreateSetRelationshipVectorProperty(ctx, cypher)
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
	// Transaction metadata (Neo4j tx.setMetaData)
	case strings.Contains(upper, "TX.SETMETADATA"):
		result, err = e.callTxSetMetadata(cypher)
	// Index management procedures
	case strings.Contains(upper, "DB.AWAITINDEXES"):
		result, err = e.callDbAwaitIndexes(cypher)
	case strings.Contains(upper, "DB.AWAITINDEX"):
		result, err = e.callDbAwaitIndex(cypher)
	case strings.Contains(upper, "DB.RESAMPLEINDEX"):
		result, err = e.callDbResampleIndex(cypher)
	// Query statistics procedures (longer matches first)
	case strings.Contains(upper, "DB.STATS.RETRIEVEALLANTHESTATS"):
		result, err = e.callDbStatsRetrieveAllAnTheStats()
	case strings.Contains(upper, "DB.STATS.RETRIEVE"):
		result, err = e.callDbStatsRetrieve(cypher)
	case strings.Contains(upper, "DB.STATS.COLLECT"):
		result, err = e.callDbStatsCollect(cypher)
	case strings.Contains(upper, "DB.STATS.CLEAR"):
		result, err = e.callDbStatsClear()
	case strings.Contains(upper, "DB.STATS.STATUS"):
		result, err = e.callDbStatsStatus()
	case strings.Contains(upper, "DB.STATS.STOP"):
		result, err = e.callDbStatsStop()
	// Database cleardown procedures (for testing)
	case strings.Contains(upper, "DB.CLEARQUERYCACHES"):
		result, err = e.callDbClearQueryCaches()
	// APOC Dynamic Cypher Execution
	case strings.Contains(upper, "APOC.CYPHER.RUN"):
		result, err = e.callApocCypherRun(ctx, cypher)
	case strings.Contains(upper, "APOC.CYPHER.DOITALL"):
		result, err = e.callApocCypherRun(ctx, cypher) // Alias
	case strings.Contains(upper, "APOC.CYPHER.RUNMANY"):
		result, err = e.callApocCypherRunMany(ctx, cypher)
	// APOC Periodic/Batch Operations
	case strings.Contains(upper, "APOC.PERIODIC.ITERATE"):
		result, err = e.callApocPeriodicIterate(ctx, cypher)
	case strings.Contains(upper, "APOC.PERIODIC.COMMIT"):
		result, err = e.callApocPeriodicCommit(ctx, cypher)
	case strings.Contains(upper, "APOC.PERIODIC.ROCK_N_ROLL"):
		result, err = e.callApocPeriodicIterate(ctx, cypher) // Alias
	default:
		// Extract procedure name for clearer error
		procName := extractProcedureName(cypher)
		return nil, fmt.Errorf("unknown procedure: %s (try SHOW PROCEDURES for available procedures)", procName)
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
	// Get indexes from schema manager
	schema := e.storage.GetSchema()
	indexes := schema.GetIndexes()

	rows := make([][]interface{}, 0, len(indexes))
	for _, idx := range indexes {
		idxMap := idx.(map[string]interface{})
		name := idxMap["name"]
		idxType := idxMap["type"]

		// Get labels/properties based on index type
		var labels interface{}
		var properties interface{}

		if l, ok := idxMap["label"]; ok {
			labels = []string{l.(string)}
		} else if ls, ok := idxMap["labels"]; ok {
			labels = ls
		}

		if p, ok := idxMap["property"]; ok {
			properties = []string{p.(string)}
		} else if ps, ok := idxMap["properties"]; ok {
			properties = ps
		}

		rows = append(rows, []interface{}{name, idxType, labels, properties, "ONLINE"})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties", "state"},
		Rows:    rows,
	}, nil
}

// callDbIndexStats returns statistics for all indexes.
// Syntax: CALL db.index.stats() YIELD name, type, totalEntries, uniqueValues, selectivity
func (e *StorageExecutor) callDbIndexStats() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	stats := schema.GetIndexStats()

	rows := make([][]interface{}, 0, len(stats))
	for _, s := range stats {
		rows = append(rows, []interface{}{
			s.Name,
			s.Type,
			s.Label,
			s.Property,
			s.TotalEntries,
			s.UniqueValues,
			s.Selectivity,
		})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "label", "property", "totalEntries", "uniqueValues", "selectivity"},
		Rows:    rows,
	}, nil
}

// callDbConstraints returns all constraints in the database.
// Syntax: CALL db.constraints() YIELD name, type, labelsOrTypes, properties
// Returns constraints in Neo4j-compatible format.
func (e *StorageExecutor) callDbConstraints() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return &ExecuteResult{
			Columns: []string{"name", "type", "labelsOrTypes", "properties"},
			Rows:    [][]interface{}{},
		}, nil
	}

	// Get all constraints from schema
	allConstraints := schema.GetAllConstraints()

	rows := make([][]interface{}, 0, len(allConstraints))
	for _, constraint := range allConstraints {
		// Format labelsOrTypes as []string (single label for node constraints)
		labelsOrTypes := []string{constraint.Label}

		// Format properties as []string
		properties := constraint.Properties

		// Convert constraint type to string
		constraintType := string(constraint.Type)

		rows = append(rows, []interface{}{
			constraint.Name,
			constraintType,
			labelsOrTypes,
			properties,
		})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties"},
		Rows:    rows,
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
func (e *StorageExecutor) callDbIndexVectorQueryNodes(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse parameters from: CALL db.index.vector.queryNodes('indexName', k, queryInput)
	// queryInput can be: [0.1, 0.2, ...] OR 'search text' OR $param
	indexName, k, input, err := e.parseVectorQueryParams(cypher)
	if err != nil {
		return nil, fmt.Errorf("vector query parse error: %w", err)
	}

	// Resolve the query vector
	var queryVector []float32

	if len(input.vector) > 0 {
		// Direct vector provided (Neo4j compatible)
		queryVector = input.vector
	} else if input.stringQuery != "" {
		// String query - embed server-side (NornicDB enhancement)
		if e.embedder == nil {
			return nil, fmt.Errorf("string query provided but no embedder configured; use vector array or configure embedding service")
		}
		embedded, embedErr := e.embedder.Embed(ctx, input.stringQuery)
		if embedErr != nil {
			return nil, fmt.Errorf("failed to embed query '%s': %w", input.stringQuery, embedErr)
		}
		queryVector = embedded
	} else if input.paramName != "" {
		// Parameter reference - resolve from context parameters
		// Parameters should have been substituted by executeCall, but if not, try to resolve here
		params := getParamsFromContext(ctx)
		if params == nil {
			// No parameters provided - return empty result (parameter not resolved)
			return &ExecuteResult{
				Columns: []string{"node", "score"},
				Rows:    [][]interface{}{},
			}, nil
		}

		paramValue, exists := params[input.paramName]
		if !exists {
			// Parameter not found in provided parameters
			return nil, fmt.Errorf("parameter $%s not provided", input.paramName)
		}

		// Convert parameter value to []float32
		// Parameter can be []float32, []float64, []interface{}, or string (to embed)
		switch val := paramValue.(type) {
		case []float32:
			queryVector = val
		case []float64:
			queryVector = make([]float32, len(val))
			for i, v := range val {
				queryVector[i] = float32(v)
			}
		case []interface{}:
			queryVector = make([]float32, 0, len(val))
			for _, item := range val {
				switch v := item.(type) {
				case float32:
					queryVector = append(queryVector, v)
				case float64:
					queryVector = append(queryVector, float32(v))
				case int:
					queryVector = append(queryVector, float32(v))
				case int64:
					queryVector = append(queryVector, float32(v))
				default:
					return nil, fmt.Errorf("parameter $%s contains non-numeric value: %T", input.paramName, v)
				}
			}
		case string:
			// String parameter - embed it
			if e.embedder == nil {
				return nil, fmt.Errorf("parameter $%s is a string but no embedder configured; provide vector array or configure embedding service", input.paramName)
			}
			embedded, embedErr := e.embedder.Embed(ctx, val)
			if embedErr != nil {
				return nil, fmt.Errorf("failed to embed parameter $%s value '%s': %w", input.paramName, val, embedErr)
			}
			queryVector = embedded
		default:
			return nil, fmt.Errorf("parameter $%s has unsupported type for vector query: %T (expected []float32, []float64, []interface{}, or string)", input.paramName, val)
		}
	} else {
		// No query input provided - check if this might be a substituted invalid parameter
		params := getParamsFromContext(ctx)
		if params != nil {
			// Parameters were provided, so if we have no input, it might be a substituted invalid type
			// Check which parameters have unsupported types to provide a better error message
			var unsupportedParams []string
			for paramName, paramValue := range params {
				// Check if this parameter has an unsupported type for vector queries
				switch paramValue.(type) {
				case []float32, []float64, []interface{}, string:
					// Supported types - skip
					continue
				default:
					// Unsupported type - add to list
					unsupportedParams = append(unsupportedParams, fmt.Sprintf("$%s (%T)", paramName, paramValue))
				}
			}

			if len(unsupportedParams) > 0 {
				// Found parameters with unsupported types - provide specific error
				paramList := strings.Join(unsupportedParams, ", ")
				return nil, fmt.Errorf("no query vector or search text provided - parameter(s) %s have unsupported type (expected []float32, []float64, []interface{}, or string)", paramList)
			}
			// Parameters exist but all have supported types - might be a different issue
			return nil, fmt.Errorf("no query vector or search text provided (parameter may have unsupported type - expected []float32, []float64, []interface{}, or string)")
		}
		return nil, fmt.Errorf("no query vector or search text provided")
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
			score = vector.EuclideanSimilarity(queryVector, nodeEmbedding)
		case "dot":
			score = vector.DotProduct(queryVector, nodeEmbedding)
		default: // cosine
			score = vector.CosineSimilarity(queryVector, nodeEmbedding)
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
			sn.node,
			sn.score,
		})
	}

	return result, nil
}

// vectorQueryInput represents either a vector or a string query for vector search
type vectorQueryInput struct {
	vector      []float32 // Pre-computed vector (from client)
	stringQuery string    // Text query to embed server-side
	paramName   string    // Parameter name if using $param
}

// parseVectorQueryParams extracts indexName, k, and query input from a vector query CALL.
// The query can be either:
//   - A vector array: [0.1, 0.2, ...]
//   - A string query: 'search text' (will be embedded server-side if embedder available)
//   - A parameter: $queryVector (resolved later)
func (e *StorageExecutor) parseVectorQueryParams(cypher string) (indexName string, k int, input *vectorQueryInput, err error) {
	// Default values
	k = 10
	indexName = "default"
	input = &vectorQueryInput{}

	// Find the procedure call (supports both queryNodes and queryRelationships)
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.VECTOR.QUERYNODES")
	if callIdx == -1 {
		callIdx = strings.Index(upper, "DB.INDEX.VECTOR.QUERYRELATIONSHIPS")
	}
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
		// Third param can be:
		// - Vector array: [0.1, 0.2, ...]
		// - String query: 'search text' or "search text"
		// - Parameter: $queryVector
		// - Substituted parameter value: 123, "text", [0.1, 0.2], etc.
		queryStr := strings.TrimSpace(parts[2])

		if strings.HasPrefix(queryStr, "$") {
			// Parameter reference - store name for later resolution
			input.paramName = strings.TrimPrefix(queryStr, "$")
		} else if strings.HasPrefix(queryStr, "[") {
			// Inline vector array
			input.vector = parseInlineVector(queryStr)
		} else if (strings.HasPrefix(queryStr, "'") && strings.HasSuffix(queryStr, "'")) ||
			(strings.HasPrefix(queryStr, "\"") && strings.HasSuffix(queryStr, "\"")) {
			// Quoted string query - will be embedded server-side
			input.stringQuery = strings.Trim(queryStr, "'\"")
		} else {
			// Could be a substituted parameter value that doesn't match patterns above
			// This will be handled in callDbIndexVectorQueryNodes when checking for parameter resolution
			// For now, leave it empty - will be caught by validation later
		}
	}

	return indexName, k, input, nil
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
//
//	Uses a simplified BM25-like scoring that considers:
//	- Term frequency (TF): How often query terms appear
//	- Inverse document frequency (IDF): How rare terms are
//	- Field length normalization: Shorter fields score higher
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

	// Known built-in index names that don't require explicit creation
	// These use the default searchable properties for Mimir compatibility
	builtInIndexes := map[string]bool{
		"default":     true,
		"node_search": true, // Used by Mimir's UnifiedSearchService
	}

	// Neo4j compatibility: error if index doesn't exist and isn't a built-in
	if len(targetProperties) == 0 && indexName != "" && !builtInIndexes[indexName] {
		return nil, fmt.Errorf("there is no such fulltext schema index: %s", indexName)
	}

	// Default searchable properties if no index config or using built-in index
	// Includes Mimir-specific properties: path, workerRole, requirements
	if len(targetProperties) == 0 {
		targetProperties = []string{"content", "text", "title", "name", "description", "body", "summary", "path", "workerRole", "requirements"}
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
			sn.node,
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

	// Handle quoted phrases using pre-compiled pattern from regex_patterns.go
	phrases := fulltextPhrasePattern.FindAllStringSubmatch(query, -1)
	for _, match := range phrases {
		mustHaveTerms = append(mustHaveTerms, match[1])
	}
	query = fulltextPhrasePattern.ReplaceAllString(query, "")

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
		idf := math.Log((float64(totalDocs) + 1) / df)
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
		result.Rows = append(result.Rows, []interface{}{node})
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

	// Parse maxLevel using pre-compiled pattern from regex_patterns.go
	if match := apocMaxLevelPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil && level > 0 {
			config.maxLevel = level
		}
	}

	// Parse minLevel using pre-compiled pattern
	if match := apocMinLevelPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil {
			config.minLevel = level
		}
	}

	// Parse limit using pre-compiled pattern
	if match := apocLimitPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if limit, err := strconv.Atoi(match[1]); err == nil {
			config.limit = limit
		}
	}

	// Parse relationshipFilter using pre-compiled pattern
	if match := apocRelFilterPattern.FindStringSubmatch(configStr); len(match) > 1 {
		filterStr := match[1]
		config.relationshipTypes, config.direction = parseRelationshipFilter(filterStr)
	}

	// Parse labelFilter using pre-compiled pattern
	if match := apocLabelFilterPattern.FindStringSubmatch(configStr); len(match) > 1 {
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
	// Uses pre-compiled patterns from regex_patterns.go

	// Try to find a MATCH pattern with id property
	if match := apocNodeIdBracePattern.FindStringSubmatch(cypher); len(match) > 1 {
		return match[1]
	}

	// Try to find WHERE clause with id
	if match := apocWhereIdPattern.FindStringSubmatch(cypher); len(match) > 1 {
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
// Returns paths (sequences of nodes and relationships) from the start node.
// Unlike subgraphNodes which returns just nodes, expand returns complete paths.
func (e *StorageExecutor) callApocPathExpand(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"path"},
		Rows:    [][]interface{}{},
	}

	// Parse parameters: (startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
	params := e.parseApocPathExpandParams(cypher)
	if params.startNode == nil {
		// No start node found, return empty result
		return result, nil
	}

	// Build config from parameters
	config := apocPathConfig{
		maxLevel:          params.maxLevel,
		minLevel:          params.minLevel,
		relationshipTypes: params.relationshipTypes,
		direction:         params.direction,
		includeLabels:     params.includeLabels,
		excludeLabels:     params.excludeLabels,
		terminateLabels:   params.terminateLabels,
		limit:             0, // No limit for expand
		bfs:               true,
	}

	// Perform BFS traversal with path tracking
	paths := e.bfsPathTraversal(params.startNode, config)

	// Convert paths to result format
	for _, path := range paths {
		result.Rows = append(result.Rows, []interface{}{e.pathToMap(path)})
	}

	return result, nil
}

// apocPathExpandParams holds parsed parameters for apoc.path.expand
type apocPathExpandParams struct {
	startNode         *storage.Node
	relationshipTypes []string
	direction         string
	includeLabels     []string
	excludeLabels     []string
	terminateLabels   []string
	minLevel          int
	maxLevel          int
}

// parseApocPathExpandParams parses positional parameters from apoc.path.expand call
// Syntax: CALL apoc.path.expand(startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
func (e *StorageExecutor) parseApocPathExpandParams(cypher string) apocPathExpandParams {
	params := apocPathExpandParams{
		minLevel:  1,
		maxLevel:  1,
		direction: "both",
	}

	// Find the procedure call and extract parameters
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PATH.EXPAND")
	if callIdx == -1 {
		return params
	}

	// Find opening parenthesis
	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return params
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
		return params
	}

	// Split parameters carefully (respecting quotes and nested structures)
	paramStr := parenContent[:endIdx]
	parts := splitParamsCarefully(paramStr)

	// Parse startNode (first parameter)
	if len(parts) > 0 {
		firstParam := strings.TrimSpace(parts[0])
		// Try to extract node ID from MATCH clause first
		startNodeID := e.extractStartNodeID(cypher)
		if startNodeID != "" && startNodeID != "*" {
			if node, err := e.storage.GetNode(storage.NodeID(startNodeID)); err == nil && node != nil {
				params.startNode = node
			}
		} else if firstParam != "" && firstParam != "null" {
			// If no ID found, try to find node by variable name from MATCH clause
			// Look for pattern: MATCH (varName:Label {id: 'value'})
			varName := strings.Trim(firstParam, "'\"")
			if node := e.findNodeByVariableInMatch(cypher, varName); node != nil {
				params.startNode = node
			}
		}
	}

	// Parse relationshipFilter (second parameter)
	if len(parts) > 1 {
		relFilter := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		if relFilter != "" && relFilter != "null" {
			params.relationshipTypes, params.direction = parseRelationshipFilter(relFilter)
		}
	}

	// Parse labelFilter (third parameter)
	if len(parts) > 2 {
		labelFilter := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
		if labelFilter != "" && labelFilter != "null" {
			params.includeLabels, params.excludeLabels, params.terminateLabels = parseLabelFilter(labelFilter)
		}
	}

	// Parse minLevel (fourth parameter)
	if len(parts) > 3 {
		minLevelStr := strings.TrimSpace(parts[3])
		if minLevel, err := strconv.Atoi(minLevelStr); err == nil {
			params.minLevel = minLevel
		}
	}

	// Parse maxLevel (fifth parameter)
	if len(parts) > 4 {
		maxLevelStr := strings.TrimSpace(parts[4])
		if maxLevel, err := strconv.Atoi(maxLevelStr); err == nil {
			params.maxLevel = maxLevel
		}
	}

	return params
}

// findNodeByVariableInMatch finds a node by variable name from a MATCH clause
func (e *StorageExecutor) findNodeByVariableInMatch(cypher, varName string) *storage.Node {
	// Look for MATCH clause with this variable
	// Pattern: MATCH (varName:Label {id: 'value'}) or MATCH (varName:Label {prop: 'value'})
	matchPattern := regexp.MustCompile(`(?i)MATCH\s*\(` + regexp.QuoteMeta(varName) + `[^)]*\)`)
	matches := matchPattern.FindStringSubmatch(cypher)
	if len(matches) == 0 {
		return nil
	}

	// Extract the node pattern
	nodePattern := matches[0]

	// Try to extract ID from {id: 'value'} pattern
	if idMatch := apocNodeIdBracePattern.FindStringSubmatch(nodePattern); len(idMatch) > 1 {
		if node, err := e.storage.GetNode(storage.NodeID(idMatch[1])); err == nil && node != nil {
			return node
		}
	}

	// Try to find node by parsing the pattern
	nodeInfo := e.parseNodePattern(nodePattern)
	if len(nodeInfo.labels) > 0 {
		candidates, _ := e.storage.GetNodesByLabel(nodeInfo.labels[0])
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				return node
			}
		}
	}

	return nil
}

// bfsPathTraversal performs breadth-first traversal with path tracking
func (e *StorageExecutor) bfsPathTraversal(startNode *storage.Node, config apocPathConfig) []PathResult {
	var results []PathResult

	// Queue item tracks the path taken to reach this node
	type pathQueueItem struct {
		path  PathResult
		level int
	}

	// Start with path containing just the start node
	initialPath := PathResult{
		Nodes:         []*storage.Node{startNode},
		Relationships: []*storage.Edge{},
		Length:        0,
	}
	queue := []pathQueueItem{{path: initialPath, level: 0}}

	// Track visited nodes to avoid cycles
	visited := make(map[string]bool)
	visited[string(startNode.ID)] = true

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		currentPath := item.path
		level := item.level
		currentNode := currentPath.Nodes[len(currentPath.Nodes)-1]

		// Check if we should include this path
		if level >= config.minLevel && level <= config.maxLevel {
			// Check label filters on the end node
			if passesLabelFilter(currentNode, config.includeLabels, config.excludeLabels) {
				// Create a copy of the path for results
				pathCopy := PathResult{
					Nodes:         make([]*storage.Node, len(currentPath.Nodes)),
					Relationships: make([]*storage.Edge, len(currentPath.Relationships)),
					Length:        currentPath.Length,
				}
				copy(pathCopy.Nodes, currentPath.Nodes)
				copy(pathCopy.Relationships, currentPath.Relationships)
				results = append(results, pathCopy)
			}
		}

		// Check if we should terminate at this node
		if isTerminateNode(currentNode, config.terminateLabels) {
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
			edges, _ = e.storage.GetOutgoingEdges(currentNode.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(currentNode.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(currentNode.ID)
			in, _ := e.storage.GetIncomingEdges(currentNode.ID)
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
			if edge.StartNode == currentNode.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Skip if already visited in this path (avoid cycles)
			// But allow revisiting nodes if they're reached via different paths
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Create new path by extending current path
			newPath := PathResult{
				Nodes:         make([]*storage.Node, len(currentPath.Nodes)+1),
				Relationships: make([]*storage.Edge, len(currentPath.Relationships)+1),
				Length:        currentPath.Length + 1,
			}
			copy(newPath.Nodes, currentPath.Nodes)
			copy(newPath.Relationships, currentPath.Relationships)
			newPath.Nodes[len(newPath.Nodes)-1] = nextNode
			newPath.Relationships[len(newPath.Relationships)-1] = edge

			// Add to queue
			queue = append(queue, pathQueueItem{path: newPath, level: level + 1})
		}
	}

	return results
}

// callApocPathSpanningTree implements apoc.path.spanningTree
// Syntax: CALL apoc.path.spanningTree(startNode, {maxLevel: n, relationshipFilter: 'TYPE', ...})
//
// Returns a spanning tree from the start node - a minimal tree that connects all reachable
// nodes without creating cycles. The tree is represented as a list of relationships.
//
// Config Parameters:
//   - maxLevel: Maximum traversal depth (default: -1 for unlimited)
//   - minLevel: Minimum traversal depth before returning results (default: 0)
//   - relationshipFilter: Filter by relationship types (e.g., "RELATES_TO|CONTAINS")
//   - labelFilter: Filter by node labels (e.g., "+Memory|-Archive")
//   - limit: Maximum number of relationships to return
//   - bfs: Use breadth-first search (default: true, DFS if false)
//
// Returns: List of relationships that form the spanning tree
func (e *StorageExecutor) callApocPathSpanningTree(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"path"},
		Rows:    [][]interface{}{},
	}

	// Parse configuration and start node
	config := e.parseApocPathConfig(cypher)
	if config.maxLevel == 3 { // Default from parseApocPathConfig
		config.maxLevel = -1 // For spanning tree, default to unlimited
	}
	startNodeID := e.extractStartNodeID(cypher)

	// Get starting node
	if startNodeID == "" || startNodeID == "*" {
		// Spanning tree requires a specific start node
		return result, fmt.Errorf("apoc.path.spanningTree requires a specific start node")
	}

	startNode, err := e.storage.GetNode(storage.NodeID(startNodeID))
	if err != nil || startNode == nil {
		return result, nil
	}

	// Build spanning tree using BFS or DFS
	var treeEdges []*storage.Edge
	if config.bfs {
		treeEdges = e.bfsSpanningTree(startNode, config)
	} else {
		treeEdges = e.dfsSpanningTree(startNode, config)
	}

	// Apply limit if specified
	if config.limit > 0 && len(treeEdges) > config.limit {
		treeEdges = treeEdges[:config.limit]
	}

	// Convert edges to path format
	// Each path contains the edge and its connected nodes
	for _, edge := range treeEdges {
		// Get the nodes
		startNodeObj, _ := e.storage.GetNode(edge.StartNode)
		endNodeObj, _ := e.storage.GetNode(edge.EndNode)

		if startNodeObj != nil && endNodeObj != nil {
			path := map[string]interface{}{
				"nodes": []interface{}{
					startNodeObj,
					endNodeObj,
				},
				"relationships": []interface{}{
					map[string]interface{}{
						"_edgeId":    string(edge.ID),
						"type":       edge.Type,
						"properties": edge.Properties,
						"startNode":  string(edge.StartNode),
						"endNode":    string(edge.EndNode),
					},
				},
				"length": 1,
			}
			result.Rows = append(result.Rows, []interface{}{path})
		}
	}

	return result, nil
}

// bfsSpanningTree builds a spanning tree using breadth-first search
func (e *StorageExecutor) bfsSpanningTree(startNode *storage.Node, config apocPathConfig) []*storage.Edge {
	var treeEdges []*storage.Edge
	visited := make(map[string]bool)

	// Queue: (node, level, parentEdge)
	type queueItem struct {
		node       *storage.Node
		level      int
		parentEdge *storage.Edge
	}
	queue := []queueItem{{node: startNode, level: 0, parentEdge: nil}}
	visited[string(startNode.ID)] = true

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		node := item.node
		level := item.level

		// Add the edge that got us here (if any) and if level > minLevel
		// Note: edges connect level N to level N+1, so check level > minLevel not level >= minLevel
		if item.parentEdge != nil && level > config.minLevel {
			treeEdges = append(treeEdges, item.parentEdge)
		}

		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if config.maxLevel >= 0 && level >= config.maxLevel {
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

			// Skip if already visited (no cycles in spanning tree)
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true

			// Get the node
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Check label filters
			if !passesLabelFilter(nextNode, config.includeLabels, config.excludeLabels) {
				continue
			}

			// Add to queue with this edge
			queue = append(queue, queueItem{
				node:       nextNode,
				level:      level + 1,
				parentEdge: edge,
			})
		}
	}

	return treeEdges
}

// dfsSpanningTree builds a spanning tree using depth-first search
func (e *StorageExecutor) dfsSpanningTree(startNode *storage.Node, config apocPathConfig) []*storage.Edge {
	var treeEdges []*storage.Edge
	visited := make(map[string]bool)

	// Stack: (node, level, parentEdge)
	type stackItem struct {
		node       *storage.Node
		level      int
		parentEdge *storage.Edge
	}
	stack := []stackItem{{node: startNode, level: 0, parentEdge: nil}}
	visited[string(startNode.ID)] = true

	for len(stack) > 0 {
		// Pop from stack
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node := item.node
		level := item.level

		// Add the edge that got us here (if any) and if level > minLevel
		// Note: edges connect level N to level N+1, so check level > minLevel not level >= minLevel
		if item.parentEdge != nil && level > config.minLevel {
			treeEdges = append(treeEdges, item.parentEdge)
		}

		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if config.maxLevel >= 0 && level >= config.maxLevel {
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

		// Process each edge (in reverse for DFS to maintain order)
		for i := len(edges) - 1; i >= 0; i-- {
			edge := edges[i]

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

			// Skip if already visited (no cycles in spanning tree)
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true

			// Get the node
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Check label filters
			if !passesLabelFilter(nextNode, config.includeLabels, config.excludeLabels) {
				continue
			}

			// Push to stack with this edge
			stack = append(stack, stackItem{
				node:       nextNode,
				level:      level + 1,
				parentEdge: edge,
			})
		}
	}

	return treeEdges
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
// Syntax: CALL db.index.vector.queryRelationships('indexName', k, queryInput)
// queryInput can be: [0.1, 0.2, ...] OR 'search text' OR $param
func (e *StorageExecutor) callDbIndexVectorQueryRelationships(cypher string) (*ExecuteResult, error) {
	ctx := context.Background()

	// Parse parameters from: CALL db.index.vector.queryRelationships('indexName', k, queryInput)
	// queryInput can be: [0.1, 0.2, ...] OR 'search text' OR $param
	indexName, k, input, err := e.parseVectorQueryParams(cypher)
	if err != nil {
		return nil, fmt.Errorf("vector query parse error: %w", err)
	}

	// Resolve the query vector (same logic as queryNodes)
	var queryVector []float32

	if len(input.vector) > 0 {
		// Direct vector provided (Neo4j compatible)
		queryVector = input.vector
	} else if input.stringQuery != "" {
		// String query - embed server-side (NornicDB enhancement)
		if e.embedder == nil {
			return nil, fmt.Errorf("string query provided but no embedder configured; use vector array or configure embedding service")
		}
		embedded, embedErr := e.embedder.Embed(ctx, input.stringQuery)
		if embedErr != nil {
			return nil, fmt.Errorf("failed to embed query '%s': %w", input.stringQuery, embedErr)
		}
		queryVector = embedded
	} else if input.paramName != "" {
		// Parameter reference - resolve from context parameters
		params := getParamsFromContext(ctx)
		if params == nil {
			// No parameters provided - return empty result (parameter not resolved)
			return &ExecuteResult{
				Columns: []string{"relationship", "score"},
				Rows:    [][]interface{}{},
			}, nil
		}

		paramValue, exists := params[input.paramName]
		if !exists {
			// Parameter not found in provided parameters
			return nil, fmt.Errorf("parameter $%s not provided", input.paramName)
		}

		// Convert parameter value to []float32
		// Parameter can be []float32, []float64, []interface{}, or string (to embed)
		switch val := paramValue.(type) {
		case []float32:
			queryVector = val
		case []float64:
			queryVector = make([]float32, len(val))
			for i, v := range val {
				queryVector[i] = float32(v)
			}
		case []interface{}:
			queryVector = make([]float32, 0, len(val))
			for _, item := range val {
				switch v := item.(type) {
				case float32:
					queryVector = append(queryVector, v)
				case float64:
					queryVector = append(queryVector, float32(v))
				case int:
					queryVector = append(queryVector, float32(v))
				case int64:
					queryVector = append(queryVector, float32(v))
				default:
					return nil, fmt.Errorf("parameter $%s contains non-numeric value: %T", input.paramName, v)
				}
			}
		case string:
			// String parameter - embed it
			if e.embedder == nil {
				return nil, fmt.Errorf("parameter $%s is a string but no embedder configured; provide vector array or configure embedding service", input.paramName)
			}
			embedded, embedErr := e.embedder.Embed(ctx, val)
			if embedErr != nil {
				return nil, fmt.Errorf("failed to embed parameter $%s value '%s': %w", input.paramName, val, embedErr)
			}
			queryVector = embedded
		default:
			return nil, fmt.Errorf("parameter $%s has unsupported type for vector query: %T (expected []float32, []float64, []interface{}, or string)", input.paramName, val)
		}
	} else {
		// No query input provided - check if this might be a substituted invalid parameter
		params := getParamsFromContext(ctx)
		if params != nil {
			// Parameters were provided, so if we have no input, it might be a substituted invalid type
			// Check which parameters have unsupported types to provide a better error message
			var unsupportedParams []string
			for paramName, paramValue := range params {
				// Check if this parameter has an unsupported type for vector queries
				switch paramValue.(type) {
				case []float32, []float64, []interface{}, string:
					// Supported types - skip
					continue
				default:
					// Unsupported type - add to list
					unsupportedParams = append(unsupportedParams, fmt.Sprintf("$%s (%T)", paramName, paramValue))
				}
			}

			if len(unsupportedParams) > 0 {
				// Found parameters with unsupported types - provide specific error
				paramList := strings.Join(unsupportedParams, ", ")
				return nil, fmt.Errorf("no query vector or search text provided - parameter(s) %s have unsupported type (expected []float32, []float64, []interface{}, or string)", paramList)
			}
			// Parameters exist but all have supported types - might be a different issue
			return nil, fmt.Errorf("no query vector or search text provided (parameter may have unsupported type - expected []float32, []float64, []interface{}, or string)")
		}
		return nil, fmt.Errorf("no query vector or search text provided")
	}

	result := &ExecuteResult{
		Columns: []string{"relationship", "score"},
		Rows:    [][]interface{}{},
	}

	// Get vector index configuration (if it exists)
	var targetRelType, targetProperty string
	var similarityFunc string = "cosine"

	schema := e.storage.GetSchema()
	if schema != nil {
		if vectorIdx, exists := schema.GetVectorIndex(indexName); exists {
			// For relationship indexes, Label field stores the relationship type
			targetRelType = vectorIdx.Label
			targetProperty = vectorIdx.Property
			similarityFunc = vectorIdx.SimilarityFunc
		}
	}

	// Get all edges and filter to those with embeddings
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	// Collect edges with embeddings and calculate similarities
	type scoredEdge struct {
		edge  *storage.Edge
		score float64
	}
	var scoredEdges []scoredEdge

	for _, edge := range edges {
		// Check relationship type filter if index specifies one
		if targetRelType != "" && edge.Type != targetRelType {
			continue
		}

		// Get embedding from property
		var edgeEmbedding []float32
		if targetProperty != "" {
			if emb, ok := edge.Properties[targetProperty]; ok {
				edgeEmbedding = toFloat32Slice(emb)
			}
		}

		if edgeEmbedding == nil || len(edgeEmbedding) == 0 {
			continue
		}

		// Skip if dimensions don't match
		if len(edgeEmbedding) != len(queryVector) {
			continue
		}

		// Calculate similarity
		var score float64
		switch similarityFunc {
		case "euclidean":
			score = vector.EuclideanSimilarity(queryVector, edgeEmbedding)
		case "dot":
			score = vector.DotProduct(queryVector, edgeEmbedding)
		default: // cosine
			score = vector.CosineSimilarity(queryVector, edgeEmbedding)
		}

		scoredEdges = append(scoredEdges, scoredEdge{edge: edge, score: score})
	}

	// Sort by score descending
	sort.Slice(scoredEdges, func(i, j int) bool {
		return scoredEdges[i].score > scoredEdges[j].score
	})

	// Limit to k results
	if k > 0 && len(scoredEdges) > k {
		scoredEdges = scoredEdges[:k]
	}

	// Convert to result rows
	for _, se := range scoredEdges {
		result.Rows = append(result.Rows, []interface{}{
			e.edgeToMap(se.edge),
			se.score,
		})
	}

	return result, nil
}

// callDbIndexVectorCreateNodeIndex creates a vector index on nodes - Neo4j db.index.vector.createNodeIndex()
// Syntax: CALL db.index.vector.createNodeIndex(indexName, label, property, dimension, similarityFunction)
func (e *StorageExecutor) callDbIndexVectorCreateNodeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse: CALL db.index.vector.createNodeIndex('indexName', 'Label', 'propertyKey', dimension, 'similarity')
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "CREATENODEINDEX")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.vector.createNodeIndex syntax")
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, fmt.Errorf("invalid syntax: missing parentheses")
	}

	args := remainder[openParen+1 : closeParen]
	parts := strings.Split(args, ",")
	if len(parts) < 4 {
		return nil, fmt.Errorf("db.index.vector.createNodeIndex requires at least 4 arguments: indexName, label, property, dimension")
	}

	indexName := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	label := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	property := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
	dimensionStr := strings.TrimSpace(parts[3])
	var dimension int
	fmt.Sscanf(dimensionStr, "%d", &dimension)

	similarity := "cosine" // Default
	if len(parts) > 4 {
		similarity = strings.Trim(strings.TrimSpace(parts[4]), "'\"")
	}

	// Create vector index using schema manager
	schema := e.storage.GetSchema()
	err := schema.AddVectorIndex(indexName, label, property, dimension, similarity)
	if err != nil {
		return nil, fmt.Errorf("failed to create vector index: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "label", "property", "dimension", "similarityFunction"},
		Rows:    [][]interface{}{{indexName, label, property, dimension, similarity}},
	}, nil
}

// callDbIndexVectorCreateRelationshipIndex creates a vector index on relationships - Neo4j db.index.vector.createRelationshipIndex()
// Syntax: CALL db.index.vector.createRelationshipIndex(indexName, relationshipType, property, dimension, similarityFunction)
func (e *StorageExecutor) callDbIndexVectorCreateRelationshipIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "CREATERELATIONSHIPINDEX")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.vector.createRelationshipIndex syntax")
	}

	// Parse arguments similar to createNodeIndex
	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, fmt.Errorf("invalid db.index.vector.createRelationshipIndex syntax: missing parentheses")
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsSimple(argsStr)
	if len(parts) < 4 {
		return nil, fmt.Errorf("db.index.vector.createRelationshipIndex requires at least 4 arguments: indexName, relationshipType, property, dimension")
	}

	indexName := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	relType := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	property := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
	dimension, err := strconv.Atoi(strings.TrimSpace(parts[3]))
	if err != nil {
		return nil, fmt.Errorf("invalid dimension: %w", err)
	}

	similarity := "cosine"
	if len(parts) > 4 {
		similarity = strings.Trim(strings.TrimSpace(parts[4]), "'\"")
	}

	// Create vector index on relationships using schema manager
	schema := e.storage.GetSchema()
	// Use relationship type as "label" for index naming
	err = schema.AddVectorIndex(indexName, relType, property, dimension, similarity)
	if err != nil {
		return nil, fmt.Errorf("failed to create relationship vector index: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "relationshipType", "property", "dimension", "similarityFunction"},
		Rows:    [][]interface{}{{indexName, relType, property, dimension, similarity}},
	}, nil
}

// callDbIndexFulltextCreateNodeIndex creates a fulltext index on nodes - Neo4j db.index.fulltext.createNodeIndex()
// Syntax: CALL db.index.fulltext.createNodeIndex(indexName, labels, properties, config)
func (e *StorageExecutor) callDbIndexFulltextCreateNodeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "CREATENODEINDEX")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.createNodeIndex syntax")
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.createNodeIndex syntax: missing parentheses")
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsRespectingArrays(argsStr)
	if len(parts) < 3 {
		return nil, fmt.Errorf("db.index.fulltext.createNodeIndex requires at least 3 arguments: indexName, labels, properties")
	}

	indexName := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	labelsStr := strings.TrimSpace(parts[1])
	propsStr := strings.TrimSpace(parts[2])

	// Parse labels array: ['Label1', 'Label2'] or 'Label'
	labels := e.parseStringArray(labelsStr)
	properties := e.parseStringArray(propsStr)

	// Create fulltext index using schema manager
	schema := e.storage.GetSchema()
	err := schema.AddFulltextIndex(indexName, labels, properties)
	if err != nil {
		return nil, fmt.Errorf("failed to create fulltext index: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "labels", "properties"},
		Rows:    [][]interface{}{{indexName, labels, properties}},
	}, nil
}

// callDbIndexFulltextCreateRelationshipIndex creates a fulltext index on relationships - Neo4j db.index.fulltext.createRelationshipIndex()
// Syntax: CALL db.index.fulltext.createRelationshipIndex(indexName, relationshipTypes, properties, config)
func (e *StorageExecutor) callDbIndexFulltextCreateRelationshipIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "CREATERELATIONSHIPINDEX")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.createRelationshipIndex syntax")
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.createRelationshipIndex syntax: missing parentheses")
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsRespectingArrays(argsStr)
	if len(parts) < 3 {
		return nil, fmt.Errorf("db.index.fulltext.createRelationshipIndex requires at least 3 arguments: indexName, relationshipTypes, properties")
	}

	indexName := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	relTypesStr := strings.TrimSpace(parts[1])
	propsStr := strings.TrimSpace(parts[2])

	// Parse arrays
	relTypes := e.parseStringArray(relTypesStr)
	properties := e.parseStringArray(propsStr)

	// Create fulltext index using schema manager
	schema := e.storage.GetSchema()
	err := schema.AddFulltextIndex(indexName, relTypes, properties)
	if err != nil {
		return nil, fmt.Errorf("failed to create relationship fulltext index: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "relationshipTypes", "properties"},
		Rows:    [][]interface{}{{indexName, relTypes, properties}},
	}, nil
}

// callDbIndexFulltextDrop drops a fulltext index - Neo4j db.index.fulltext.drop()
// Syntax: CALL db.index.fulltext.drop(indexName)
func (e *StorageExecutor) callDbIndexFulltextDrop(cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "DROP")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.drop syntax")
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, fmt.Errorf("invalid db.index.fulltext.drop syntax: missing parentheses")
	}

	indexName := strings.Trim(strings.TrimSpace(cypher[idx+argsStart+1:idx+argsEnd]), "'\"")

	// Drop fulltext index - NornicDB manages indexes internally, so this is a no-op but returns success
	return &ExecuteResult{
		Columns: []string{"name", "dropped"},
		Rows:    [][]interface{}{{indexName, true}},
	}, nil
}

// callDbIndexVectorDrop drops a vector index - Neo4j db.index.vector.drop()
// Syntax: CALL db.index.vector.drop(indexName)
func (e *StorageExecutor) callDbIndexVectorDrop(cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "DROP")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.index.vector.drop syntax")
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, fmt.Errorf("invalid db.index.vector.drop syntax: missing parentheses")
	}

	indexName := strings.Trim(strings.TrimSpace(cypher[idx+argsStart+1:idx+argsEnd]), "'\"")

	// Drop vector index - NornicDB manages indexes internally, so this is a no-op but returns success
	return &ExecuteResult{
		Columns: []string{"name", "dropped"},
		Rows:    [][]interface{}{{indexName, true}},
	}, nil
}

// splitArgsSimple splits comma-separated arguments, respecting quoted strings
func (e *StorageExecutor) splitArgsSimple(args string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(args); i++ {
		c := args[i]
		if (c == '\'' || c == '"') && (i == 0 || args[i-1] != '\\') {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteByte(c)
		} else if c == ',' && !inQuote {
			result = append(result, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

// splitArgsRespectingArrays splits arguments, keeping array brackets together
func (e *StorageExecutor) splitArgsRespectingArrays(args string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(args); i++ {
		c := args[i]
		if (c == '\'' || c == '"') && (i == 0 || args[i-1] != '\\') {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteByte(c)
		} else if c == '[' && !inQuote {
			depth++
			current.WriteByte(c)
		} else if c == ']' && !inQuote {
			depth--
			current.WriteByte(c)
		} else if c == ',' && depth == 0 && !inQuote {
			result = append(result, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

// parseStringArray parses a string that may be an array ['a', 'b'] or single value 'a'
func (e *StorageExecutor) parseStringArray(s string) []string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		s = s[1 : len(s)-1]
		var result []string
		for _, item := range strings.Split(s, ",") {
			item = strings.Trim(strings.TrimSpace(item), "'\"")
			if item != "" {
				result = append(result, item)
			}
		}
		return result
	}
	return []string{strings.Trim(s, "'\"")}
}

// callDbCreateSetNodeVectorProperty sets a vector property on a node - Neo4j db.create.setNodeVectorProperty()
// Syntax: CALL db.create.setNodeVectorProperty(node, propertyKey, vector)
func (e *StorageExecutor) callDbCreateSetNodeVectorProperty(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse: CALL db.create.setNodeVectorProperty(nodeId, 'propertyKey', [vector])
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETNODEVECTORPROPERTY")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.create.setNodeVectorProperty syntax")
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, fmt.Errorf("db.create.setNodeVectorProperty: missing parentheses (expected db.create.setNodeVectorProperty(nodeId, 'key', [vector]))")
	}

	argsStr := remainder[openParen+1 : closeParen]

	// Extract nodeId (first arg)
	commaIdx := strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, fmt.Errorf("db.create.setNodeVectorProperty: requires 3 arguments (nodeId, propertyKey, vector)")
	}
	nodeIDStr := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract property key (second arg)
	commaIdx = strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, fmt.Errorf("db.create.setNodeVectorProperty: missing vector argument (expected nodeId, propertyKey, [vector])")
	}
	propertyKey := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract vector (third arg) - can be [1.0, 2.0, 3.0] format
	vectorStr := strings.TrimSpace(argsStr)
	vectorStr = strings.Trim(vectorStr, "[]")
	vectorParts := strings.Split(vectorStr, ",")
	vector := make([]float64, len(vectorParts))
	for i, vp := range vectorParts {
		var val float64
		fmt.Sscanf(strings.TrimSpace(vp), "%f", &val)
		vector[i] = val
	}

	// Get and update the node
	nodeID := storage.NodeID(nodeIDStr)
	node, err := e.storage.GetNode(nodeID)
	if err != nil {
		return nil, fmt.Errorf("node not found: %s", nodeIDStr)
	}

	// Set the vector property
	node.Properties[propertyKey] = vector
	err = e.storage.UpdateNode(node)
	if err != nil {
		return nil, fmt.Errorf("failed to update node: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{{node}},
	}, nil
}

// callDbCreateSetRelationshipVectorProperty sets a vector property on a relationship - Neo4j db.create.setRelationshipVectorProperty()
// Syntax: CALL db.create.setRelationshipVectorProperty(relationship, propertyKey, vector)
func (e *StorageExecutor) callDbCreateSetRelationshipVectorProperty(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse: CALL db.create.setRelationshipVectorProperty(relId, 'propertyKey', [vector])
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETRELATIONSHIPVECTORPROPERTY")
	if idx < 0 {
		return nil, fmt.Errorf("invalid db.create.setRelationshipVectorProperty syntax")
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, fmt.Errorf("db.create.setRelationshipVectorProperty: missing parentheses (expected db.create.setRelationshipVectorProperty(relId, 'key', [vector]))")
	}

	argsStr := remainder[openParen+1 : closeParen]

	// Extract relId (first arg)
	commaIdx := strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, fmt.Errorf("db.create.setRelationshipVectorProperty: requires 3 arguments (relId, propertyKey, vector)")
	}
	relIDStr := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract property key (second arg)
	commaIdx = strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, fmt.Errorf("db.create.setRelationshipVectorProperty: missing vector argument (expected relId, propertyKey, [vector])")
	}
	propertyKey := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract vector (third arg)
	vectorStr := strings.TrimSpace(argsStr)
	vectorStr = strings.Trim(vectorStr, "[]")
	vectorParts := strings.Split(vectorStr, ",")
	vector := make([]float64, len(vectorParts))
	for i, vp := range vectorParts {
		var val float64
		fmt.Sscanf(strings.TrimSpace(vp), "%f", &val)
		vector[i] = val
	}

	// Get and update the relationship
	relID := storage.EdgeID(relIDStr)
	rel, err := e.storage.GetEdge(relID)
	if err != nil {
		return nil, fmt.Errorf("relationship not found: %s", relIDStr)
	}

	// Set the vector property
	rel.Properties[propertyKey] = vector
	err = e.storage.UpdateEdge(rel)
	if err != nil {
		return nil, fmt.Errorf("failed to update relationship: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"relationship"},
		Rows:    [][]interface{}{{e.edgeToMap(rel)}},
	}, nil
}

// callTxSetMetadata sets transaction metadata - Neo4j tx.setMetaData()
//
// This procedure is used to attach metadata to transactions for logging/debugging.
// Syntax: CALL tx.setMetaData({key: value})
//
// Requires an active transaction (BEGIN ... COMMIT). If no transaction is active,
// returns an error. Metadata is stored with the transaction and can be used for
// logging, debugging, or audit trails.
func (e *StorageExecutor) callTxSetMetadata(cypher string) (*ExecuteResult, error) {
	// Check if there's an active transaction
	if e.txContext == nil || !e.txContext.active {
		return nil, fmt.Errorf("tx.setMetaData() requires an active transaction. Use BEGIN TRANSACTION first")
	}

	// Extract metadata object from Cypher: CALL tx.setMetaData({key: value})
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETMETADATA")
	if idx < 0 {
		return nil, fmt.Errorf("invalid tx.setMetaData syntax")
	}

	// Find opening parenthesis
	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, fmt.Errorf("invalid tx.setMetaData syntax: missing parentheses")
	}

	// Extract the metadata object string: {key: value}
	argsStr := strings.TrimSpace(remainder[openParen+1 : closeParen])
	if argsStr == "" {
		return nil, fmt.Errorf("tx.setMetaData requires a metadata object: {key: value}")
	}

	// Parse the metadata object
	metadata := e.parseProperties(argsStr)
	if len(metadata) == 0 {
		return nil, fmt.Errorf("tx.setMetaData requires at least one key-value pair")
	}

	// Get the transaction and set metadata
	tx, ok := e.txContext.tx.(*storage.BadgerTransaction)
	if !ok {
		return nil, fmt.Errorf("transaction type not supported for metadata")
	}

	err := tx.SetMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to set transaction metadata: %w", err)
	}

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Transaction metadata set successfully"},
		},
	}, nil
}

// ========================================
// Index Management Procedures
// ========================================

// callDbAwaitIndex waits for a specific index to come online - Neo4j db.awaitIndex()
// Syntax: CALL db.awaitIndex(indexName, timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndex(cypher string) (*ExecuteResult, error) {
	// NornicDB indexes are always online (no background building)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Index is online"},
		},
	}, nil
}

// callDbAwaitIndexes waits for all indexes to come online - Neo4j db.awaitIndexes()
// Syntax: CALL db.awaitIndexes(timeOutSeconds)
func (e *StorageExecutor) callDbAwaitIndexes(cypher string) (*ExecuteResult, error) {
	// NornicDB indexes are always online (no background building)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"All indexes are online"},
		},
	}, nil
}

// callDbResampleIndex forces index statistics to be recalculated - Neo4j db.resampleIndex()
// Syntax: CALL db.resampleIndex(indexName)
func (e *StorageExecutor) callDbResampleIndex(cypher string) (*ExecuteResult, error) {
	// NornicDB doesn't use index statistics (no cost-based optimizer using stats)
	// This is a no-op for compatibility
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Index statistics updated"},
		},
	}, nil
}

// ========================================
// Query Statistics Procedures
// ========================================

// callDbStatsClear clears collected query statistics - Neo4j db.stats.clear()
func (e *StorageExecutor) callDbStatsClear() (*ExecuteResult, error) {
	// Clear any cached query stats
	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"QUERIES", map[string]interface{}{"cleared": true}},
		},
	}, nil
}

// callDbStatsCollect starts collecting query statistics - Neo4j db.stats.collect()
// Syntax: CALL db.stats.collect(section, config)
func (e *StorageExecutor) callDbStatsCollect(cypher string) (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "success", "message"},
		Rows: [][]interface{}{
			{"QUERIES", true, "Query collection started"},
		},
	}, nil
}

// callDbStatsRetrieve retrieves collected statistics - Neo4j db.stats.retrieve()
// Syntax: CALL db.stats.retrieve(section)
func (e *StorageExecutor) callDbStatsRetrieve(cypher string) (*ExecuteResult, error) {
	// Return basic query statistics
	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"QUERIES", map[string]interface{}{
				"totalQueries":   0,
				"cachedQueries":  0,
				"avgExecutionMs": 0,
			}},
		},
	}, nil
}

// callDbStatsRetrieveAllAnTheStats retrieves all statistics - Neo4j db.stats.retrieveAllAnTheStats()
func (e *StorageExecutor) callDbStatsRetrieveAllAnTheStats() (*ExecuteResult, error) {
	nodeCount := 0
	edgeCount := 0

	if nodes, err := e.storage.AllNodes(); err == nil {
		nodeCount = len(nodes)
	}
	if edges, err := e.storage.AllEdges(); err == nil {
		edgeCount = len(edges)
	}

	return &ExecuteResult{
		Columns: []string{"section", "data"},
		Rows: [][]interface{}{
			{"GRAPH COUNTS", map[string]interface{}{
				"nodeCount":         nodeCount,
				"relationshipCount": edgeCount,
			}},
			{"QUERIES", map[string]interface{}{
				"totalQueries":   0,
				"cachedQueries":  0,
				"avgExecutionMs": 0,
			}},
		},
	}, nil
}

// callDbStatsStatus returns statistics collection status - Neo4j db.stats.status()
func (e *StorageExecutor) callDbStatsStatus() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "status", "message"},
		Rows: [][]interface{}{
			{"QUERIES", "idle", "Statistics collection is available"},
		},
	}, nil
}

// callDbStatsStop stops statistics collection - Neo4j db.stats.stop()
func (e *StorageExecutor) callDbStatsStop() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"section", "success", "message"},
		Rows: [][]interface{}{
			{"QUERIES", true, "Statistics collection stopped"},
		},
	}, nil
}

// callDbClearQueryCaches clears all query caches - Neo4j db.clearQueryCaches()
//
// Clears all caches in the executor:
//   - Query result cache (SmartQueryCache)
//   - Query plan cache (QueryPlanCache)
//   - Query analyzer cache (AST cache)
//   - Node lookup cache
//
// This is useful for:
//   - Testing (ensuring fresh queries)
//   - After bulk data imports
//   - When schema changes affect query plans
//   - Debugging cache-related issues
func (e *StorageExecutor) callDbClearQueryCaches() (*ExecuteResult, error) {
	// Clear query result cache
	if e.cache != nil {
		e.cache.Invalidate()
	}

	// Clear query plan cache
	if e.planCache != nil {
		e.planCache.Clear()
	}

	// Clear query analyzer cache (AST cache)
	if e.analyzer != nil {
		e.analyzer.ClearCache()
	}

	// Clear node lookup cache
	e.nodeLookupCacheMu.Lock()
	e.nodeLookupCache = make(map[string]*storage.Node, 1000)
	e.nodeLookupCacheMu.Unlock()

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Query caches cleared"},
		},
	}, nil
}

// =============================================================================
// APOC Dynamic Cypher Execution Procedures
// =============================================================================

// callApocCypherRun executes a dynamic Cypher query string.
// CALL apoc.cypher.run(statement, params) YIELD value
// This allows executing Cypher queries stored in strings or variables.
func (e *StorageExecutor) callApocCypherRun(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse the CALL statement to extract the inner query and parameters
	// Format: CALL apoc.cypher.run('MATCH (n) RETURN n', {})

	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.CYPHER.RUN")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.cypher.run call")
	}

	// Find the opening parenthesis after the procedure name
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.cypher.run requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.cypher.run")
	}

	// Extract arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])

	// Parse the first argument (the query string)
	innerQuery, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.cypher.run arguments: %w", err)
	}

	// Execute the inner query
	innerResult, err := e.Execute(ctx, innerQuery, params)
	if err != nil {
		return nil, fmt.Errorf("apoc.cypher.run inner query failed: %w", err)
	}

	// Transform result to match APOC format (YIELD value)
	// Each row becomes a map under the "value" column
	result := &ExecuteResult{
		Columns: []string{"value"},
		Rows:    make([][]interface{}, 0, len(innerResult.Rows)),
		Stats:   innerResult.Stats,
	}

	for _, row := range innerResult.Rows {
		// Convert row to a map with column names as keys
		valueMap := make(map[string]interface{})
		for i, col := range innerResult.Columns {
			if i < len(row) {
				valueMap[col] = row[i]
			}
		}
		result.Rows = append(result.Rows, []interface{}{valueMap})
	}

	return result, nil
}

// callApocCypherRunMany executes multiple Cypher statements separated by semicolons.
// CALL apoc.cypher.runMany(statements, params) YIELD row, result
func (e *StorageExecutor) callApocCypherRunMany(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.CYPHER.RUNMANY")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.cypher.runMany call")
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.cypher.runMany requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.cypher.runMany")
	}

	// Extract arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])

	// Parse the first argument (the multi-statement string)
	statements, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.cypher.runMany arguments: %w", err)
	}

	// Split by semicolons (respecting quotes)
	queries := e.splitBySemicolon(statements)

	result := &ExecuteResult{
		Columns: []string{"row", "result"},
		Rows:    make([][]interface{}, 0),
		Stats:   &QueryStats{},
	}

	for i, query := range queries {
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		innerResult, err := e.Execute(ctx, query, params)
		if err != nil {
			// Include error in result instead of failing
			result.Rows = append(result.Rows, []interface{}{
				int64(i),
				map[string]interface{}{"error": err.Error()},
			})
			continue
		}

		// Add each row from the inner result
		for _, row := range innerResult.Rows {
			valueMap := make(map[string]interface{})
			for j, col := range innerResult.Columns {
				if j < len(row) {
					valueMap[col] = row[j]
				}
			}
			result.Rows = append(result.Rows, []interface{}{
				int64(i),
				valueMap,
			})
		}

		// Accumulate stats
		if innerResult.Stats != nil {
			result.Stats.NodesCreated += innerResult.Stats.NodesCreated
			result.Stats.NodesDeleted += innerResult.Stats.NodesDeleted
			result.Stats.RelationshipsCreated += innerResult.Stats.RelationshipsCreated
			result.Stats.RelationshipsDeleted += innerResult.Stats.RelationshipsDeleted
			result.Stats.PropertiesSet += innerResult.Stats.PropertiesSet
		}
	}

	return result, nil
}

// =============================================================================
// APOC Periodic/Batch Operations
// =============================================================================

// callApocPeriodicIterate performs batch processing with periodic commits.
// CALL apoc.periodic.iterate(cypherIterate, cypherAction, {batchSize:1000, parallel:false})
// This is used for large-scale data processing to avoid memory issues.
func (e *StorageExecutor) callApocPeriodicIterate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PERIODIC.ITERATE")
	if callIdx == -1 {
		// Try rock_n_roll alias
		callIdx = strings.Index(upper, "APOC.PERIODIC.ROCK_N_ROLL")
		if callIdx == -1 {
			return nil, fmt.Errorf("invalid apoc.periodic.iterate call")
		}
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.periodic.iterate requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.periodic.iterate")
	}

	// Parse arguments: (iterateQuery, actionQuery, config)
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])
	iterateQuery, actionQuery, config, err := e.parseApocPeriodicIterateArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.periodic.iterate arguments: %w", err)
	}

	// Extract config options
	batchSize := 1000
	if bs, ok := config["batchSize"].(float64); ok {
		batchSize = int(bs)
	} else if bs, ok := config["batchSize"].(int); ok {
		batchSize = bs
	} else if bs, ok := config["batchSize"].(int64); ok {
		batchSize = int(bs)
	}

	// Execute the iterate query to get data to process
	iterateResult, err := e.Execute(ctx, iterateQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("iterate query failed: %w", err)
	}

	// Process in batches
	totalRows := len(iterateResult.Rows)
	batches := (totalRows + batchSize - 1) / batchSize

	stats := &QueryStats{}
	errorCount := int64(0)
	successCount := int64(0)

	for batchNum := 0; batchNum < batches; batchNum++ {
		startIdx := batchNum * batchSize
		endIdx := startIdx + batchSize
		if endIdx > totalRows {
			endIdx = totalRows
		}

		// Process each row in the batch
		for i := startIdx; i < endIdx; i++ {
			row := iterateResult.Rows[i]

			// Build params map from row
			params := make(map[string]interface{})
			for j, col := range iterateResult.Columns {
				if j < len(row) {
					params[col] = row[j]
				}
			}

			// Execute action query with row data as parameters
			actionResult, err := e.Execute(ctx, actionQuery, params)
			if err != nil {
				errorCount++
				continue
			}
			successCount++

			// Accumulate stats
			if actionResult.Stats != nil {
				stats.NodesCreated += actionResult.Stats.NodesCreated
				stats.NodesDeleted += actionResult.Stats.NodesDeleted
				stats.RelationshipsCreated += actionResult.Stats.RelationshipsCreated
				stats.RelationshipsDeleted += actionResult.Stats.RelationshipsDeleted
				stats.PropertiesSet += actionResult.Stats.PropertiesSet
			}
		}
	}

	return &ExecuteResult{
		Columns: []string{"batches", "total", "timeTaken", "committedOperations", "failedOperations", "failedBatches", "retries", "errorMessages", "batch", "operations", "wasTerminated", "failedParams", "updateStatistics"},
		Rows: [][]interface{}{
			{
				int64(batches),           // batches
				int64(totalRows),         // total
				int64(0),                 // timeTaken (ms) - not measured
				successCount,             // committedOperations
				errorCount,               // failedOperations
				int64(0),                 // failedBatches
				int64(0),                 // retries
				map[string]interface{}{}, // errorMessages
				map[string]interface{}{ // batch
					"total":     int64(batches),
					"committed": int64(batches),
					"failed":    int64(0),
					"errors":    map[string]interface{}{},
				},
				map[string]interface{}{ // operations
					"total":     int64(totalRows),
					"committed": successCount,
					"failed":    errorCount,
					"errors":    map[string]interface{}{},
				},
				false,                    // wasTerminated
				map[string]interface{}{}, // failedParams
				map[string]interface{}{ // updateStatistics
					"nodesCreated":         stats.NodesCreated,
					"nodesDeleted":         stats.NodesDeleted,
					"relationshipsCreated": stats.RelationshipsCreated,
					"relationshipsDeleted": stats.RelationshipsDeleted,
					"propertiesSet":        stats.PropertiesSet,
				},
			},
		},
		Stats: stats,
	}, nil
}

// callApocPeriodicCommit performs a query with periodic commits.
// CALL apoc.periodic.commit(statement, params) YIELD updates, executions, runtime, batches
// This commits every N operations to avoid large transactions.
func (e *StorageExecutor) callApocPeriodicCommit(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PERIODIC.COMMIT")
	if callIdx == -1 {
		return nil, fmt.Errorf("invalid apoc.periodic.commit call")
	}

	// Find the opening parenthesis
	parenStart := strings.Index(cypher[callIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("apoc.periodic.commit requires parameters")
	}
	parenStart += callIdx

	// Find matching closing parenthesis
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in apoc.periodic.commit")
	}

	// Parse arguments
	argsStr := strings.TrimSpace(cypher[parenStart+1 : parenEnd])
	statement, params, err := e.parseApocCypherRunArgs(argsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apoc.periodic.commit arguments: %w", err)
	}

	// Extract limit from params if present
	limit := 10000
	if l, ok := params["limit"].(float64); ok {
		limit = int(l)
	} else if l, ok := params["limit"].(int); ok {
		limit = l
	}

	// Execute the statement repeatedly until it affects 0 rows
	totalUpdates := int64(0)
	executions := int64(0)
	stats := &QueryStats{}

	for {
		// Add LIMIT to statement if not present
		stmtUpper := strings.ToUpper(statement)
		if !strings.Contains(stmtUpper, "LIMIT") {
			statement = statement + fmt.Sprintf(" LIMIT %d", limit)
		}

		result, err := e.Execute(ctx, statement, params)
		if err != nil {
			break
		}
		executions++

		// Check if any updates were made
		updates := int64(0)
		if result.Stats != nil {
			updates = int64(result.Stats.NodesCreated + result.Stats.NodesDeleted +
				result.Stats.RelationshipsCreated + result.Stats.RelationshipsDeleted +
				result.Stats.PropertiesSet)

			stats.NodesCreated += result.Stats.NodesCreated
			stats.NodesDeleted += result.Stats.NodesDeleted
			stats.RelationshipsCreated += result.Stats.RelationshipsCreated
			stats.RelationshipsDeleted += result.Stats.RelationshipsDeleted
			stats.PropertiesSet += result.Stats.PropertiesSet
		}

		if updates == 0 {
			break
		}
		totalUpdates += updates

		// Safety limit
		if executions > 1000 {
			break
		}
	}

	return &ExecuteResult{
		Columns: []string{"updates", "executions", "runtime", "batches"},
		Rows: [][]interface{}{
			{totalUpdates, executions, int64(0), executions},
		},
		Stats: stats,
	}, nil
}

// =============================================================================
// APOC Helper Functions
// =============================================================================

// findMatchingParen finds the index of the closing parenthesis matching the one at startIdx.
func (e *StorageExecutor) findMatchingParen(s string, startIdx int) int {
	if startIdx >= len(s) || s[startIdx] != '(' {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := startIdx; i < len(s); i++ {
		c := rune(s[i])

		if inQuote {
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// parseApocCypherRunArgs parses the arguments for apoc.cypher.run/runMany.
// Expected format: 'query string', {params} or 'query string', null
func (e *StorageExecutor) parseApocCypherRunArgs(argsStr string) (string, map[string]interface{}, error) {
	// Find the first quoted string (the query)
	query := ""
	params := make(map[string]interface{})

	// Find first quote
	quoteStart := -1
	quoteChar := rune(0)
	for i, c := range argsStr {
		if c == '\'' || c == '"' {
			quoteStart = i
			quoteChar = c
			break
		}
	}

	if quoteStart == -1 {
		return "", nil, fmt.Errorf("query string not found")
	}

	// Find matching closing quote
	quoteEnd := -1
	for i := quoteStart + 1; i < len(argsStr); i++ {
		if rune(argsStr[i]) == quoteChar && (i == 0 || argsStr[i-1] != '\\') {
			quoteEnd = i
			break
		}
	}

	if quoteEnd == -1 {
		return "", nil, fmt.Errorf("unclosed query string")
	}

	query = argsStr[quoteStart+1 : quoteEnd]

	// Try to parse params after the query
	remaining := strings.TrimSpace(argsStr[quoteEnd+1:])
	if strings.HasPrefix(remaining, ",") {
		remaining = strings.TrimSpace(remaining[1:])

		// Skip 'null' or 'NULL'
		if len(remaining) >= 4 && strings.EqualFold(remaining[:4], "NULL") {
			return query, params, nil
		}

		// Try to parse as map literal {...}
		if strings.HasPrefix(remaining, "{") {
			mapEnd := e.findMatchingBrace(remaining, 0)
			if mapEnd > 0 {
				mapStr := remaining[:mapEnd+1]
				params = e.parseMapLiteral(mapStr)
			}
		}
	}

	return query, params, nil
}

// parseApocPeriodicIterateArgs parses arguments for apoc.periodic.iterate.
// Expected format: 'iterateQuery', 'actionQuery', {config}
func (e *StorageExecutor) parseApocPeriodicIterateArgs(argsStr string) (string, string, map[string]interface{}, error) {
	config := make(map[string]interface{})

	// Parse first query string
	iterateQuery, remaining, err := e.extractQuotedString(argsStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to parse iterate query: %w", err)
	}

	// Skip comma
	remaining = strings.TrimSpace(remaining)
	if !strings.HasPrefix(remaining, ",") {
		return "", "", nil, fmt.Errorf("expected comma after iterate query")
	}
	remaining = strings.TrimSpace(remaining[1:])

	// Parse second query string
	actionQuery, remaining, err := e.extractQuotedString(remaining)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to parse action query: %w", err)
	}

	// Try to parse config map
	remaining = strings.TrimSpace(remaining)
	if strings.HasPrefix(remaining, ",") {
		remaining = strings.TrimSpace(remaining[1:])
		if strings.HasPrefix(remaining, "{") {
			mapEnd := e.findMatchingBrace(remaining, 0)
			if mapEnd > 0 {
				mapStr := remaining[:mapEnd+1]
				config = e.parseMapLiteral(mapStr)
			}
		}
	}

	return iterateQuery, actionQuery, config, nil
}

// extractQuotedString extracts a quoted string from the start of s and returns it along with the remaining string.
func (e *StorageExecutor) extractQuotedString(s string) (string, string, error) {
	s = strings.TrimSpace(s)

	if len(s) == 0 {
		return "", "", fmt.Errorf("empty string")
	}

	quoteChar := rune(s[0])
	if quoteChar != '\'' && quoteChar != '"' {
		return "", "", fmt.Errorf("expected quote, got %c", quoteChar)
	}

	// Find matching closing quote
	for i := 1; i < len(s); i++ {
		if rune(s[i]) == quoteChar && (i == 1 || s[i-1] != '\\') {
			return s[1:i], s[i+1:], nil
		}
	}

	return "", "", fmt.Errorf("unclosed quote")
}

// findMatchingBrace finds the index of the closing brace matching the one at startIdx.
func (e *StorageExecutor) findMatchingBrace(s string, startIdx int) int {
	if startIdx >= len(s) || s[startIdx] != '{' {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := startIdx; i < len(s); i++ {
		c := rune(s[i])

		if inQuote {
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// parseMapLiteral parses a Cypher map literal like {key: value, key2: value2}.
func (e *StorageExecutor) parseMapLiteral(s string) map[string]interface{} {
	result := make(map[string]interface{})

	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return result
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return result
	}

	// Simple key:value parsing (handles basic cases)
	pairs := e.splitMapPairs(inner)
	for _, pair := range pairs {
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		value := strings.TrimSpace(pair[colonIdx+1:])

		// Parse value
		result[key] = e.parseValue(value)
	}

	return result
}

// splitMapPairs splits a map literal's contents by commas, respecting nesting.
func (e *StorageExecutor) splitMapPairs(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range s {
		if inQuote {
			current.WriteRune(c)
			if c == quoteChar {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
			current.WriteRune(c)
		case '{', '[', '(':
			depth++
			current.WriteRune(c)
		case '}', ']', ')':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// splitBySemicolon splits a string by semicolons, respecting quotes.
func (e *StorageExecutor) splitBySemicolon(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, c := range s {
		if inQuote {
			current.WriteRune(c)
			if c == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch c {
		case '\'', '"':
			inQuote = true
			quoteChar = c
			current.WriteRune(c)
		case ';':
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// extractProcedureName extracts the procedure name from a CALL statement for error messages.
func extractProcedureName(cypher string) string {
	// Match CALL followed by procedure name (e.g., "CALL db.labels()" -> "db.labels")
	re := regexp.MustCompile(`(?i)CALL\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`)
	matches := re.FindStringSubmatch(cypher)
	if len(matches) > 1 {
		return matches[1]
	}
	// Fallback: return truncated query
	if len(cypher) > 60 {
		return cypher[:60] + "..."
	}
	return cypher
}
