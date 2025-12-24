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

		// Get embeddings - check property first, then ChunkEmbeddings
		// For nodes with multiple chunks, we compare against ALL chunks and use the best match
		var embeddingsToCompare [][]float32
		if targetProperty != "" {
			if emb, ok := node.Properties[targetProperty]; ok {
				if floatSlice := toFloat32Slice(emb); len(floatSlice) > 0 {
					embeddingsToCompare = [][]float32{floatSlice}
				}
			}
		}
		// If no property embedding, use ChunkEmbeddings (always stored as array, even single chunk = array of 1)
		if len(embeddingsToCompare) == 0 && len(node.ChunkEmbeddings) > 0 {
			embeddingsToCompare = node.ChunkEmbeddings
		}

		if len(embeddingsToCompare) == 0 {
			continue
		}

		// Calculate similarity against all chunk embeddings and use the best match
		// This ensures we find the node even if query matches a later chunk better
		var bestScore float64 = -1.0
		for _, nodeEmbedding := range embeddingsToCompare {
			// Skip if dimensions don't match
			if len(nodeEmbedding) != len(queryVector) {
				continue
			}

			// Calculate similarity for this chunk
			var chunkScore float64
			switch similarityFunc {
			case "euclidean":
				chunkScore = vector.EuclideanSimilarity(queryVector, nodeEmbedding)
			case "dot":
				chunkScore = vector.DotProduct(queryVector, nodeEmbedding)
			default: // cosine
				chunkScore = vector.CosineSimilarity(queryVector, nodeEmbedding)
			}

			// Keep the best score across all chunks
			if chunkScore > bestScore {
				bestScore = chunkScore
			}
		}

		// Only add if we found a valid match (dimensions matched for at least one chunk)
		if bestScore >= 0.0 {
			scoredNodes = append(scoredNodes, scoredNode{node: node, score: bestScore})
		}
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
