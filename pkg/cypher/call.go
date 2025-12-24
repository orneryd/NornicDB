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
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
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
