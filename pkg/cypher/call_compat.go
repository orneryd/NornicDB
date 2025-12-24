package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
)

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
