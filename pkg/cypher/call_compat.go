package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
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
			{"nornicdb.version", "NornicDB version", buildinfo.Version(), false},
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

// callDbIndexFulltextQueryRelationships searches relationships using a
// fulltext index — Neo4j db.index.fulltext.queryRelationships(). The
// resolution mirrors the queryNodes path:
//
//   - Look up the named index in the schema.
//   - Scope the edge scan to the index's declared RelationshipTypes
//     (a backwards-compatible nil/empty slice means "every edge",
//     matching pre-PR behavior so legacy databases keep working).
//   - When the query is the Lucene match-all wildcard ("*" or "*:*"),
//     return every in-scope edge with score 1.0.
//   - Otherwise, score indexed relationship text with the same
//     tokenized BM25-like matching used by queryNodes.
//
// Returns one row per matching edge with columns [relationship, score].
func (e *StorageExecutor) callDbIndexFulltextQueryRelationships(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"relationship", "score"},
		Rows:    [][]interface{}{},
	}
	opts, err := e.extractFulltextQueryOptions(cypher)
	if err != nil {
		return nil, err
	}

	indexName, query := e.extractFulltextParams(cypher)
	if query == "" {
		return result, nil
	}

	// Look up the index's declared scope. Missing index, missing schema,
	// or a node-only index (no relationship_types declared) all fall
	// through to the unscoped scan so legacy queries keep working.
	var targetTypes []string
	var targetProperties []string
	if schema := e.storage.GetSchema(); schema != nil {
		if ftIdx, exists := schema.GetFulltextIndex(indexName); exists {
			targetTypes = ftIdx.RelationshipTypes
			targetProperties = ftIdx.Properties
		}
	}

	wildcard := isFulltextWildcard(query)
	presenceProp, isPresenceQuery := fulltextFieldPresenceQuery(query)

	// If the query asks for `<prop>:*` against a field the index didn't
	// declare, mirror Neo4j-Lucene: empty result set (no postings list).
	if isPresenceQuery && len(targetProperties) > 0 && !containsString(targetProperties, presenceProp) {
		return result, nil
	}

	if wildcard || isPresenceQuery {
		if opts.limit == 0 {
			return result, nil
		}
		seen := 0
		err := storage.StreamEdgesWithFallback(context.Background(), e.storage, 1024, func(edge *storage.Edge) error {
			if !matchesRelationshipTypes(edge, targetTypes) {
				return nil
			}
			if wildcard {
				if appendFulltextOptionedRow(result, opts, &seen, []interface{}{edgeToMap(edge), 1.0}) {
					return storage.ErrIterationStopped
				}
				return nil
			}
			if edgeHasNonEmptyProperty(edge, presenceProp) {
				if appendFulltextOptionedRow(result, opts, &seen, []interface{}{edgeToMap(edge), 1.0}) {
					return storage.ErrIterationStopped
				}
			}
			return nil
		})
		if err != nil && err != storage.ErrIterationStopped {
			return nil, err
		}
		return result, nil
	}

	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	parsed, err := ParseFulltextQuery(query)
	if err != nil {
		return nil, err
	}
	if parsed.IsEmpty() {
		return result, nil
	}

	type edgeDoc struct {
		edge *storage.Edge
		doc  *ftDoc
	}
	docs := make([]edgeDoc, 0, len(edges))
	docFreq := make(map[string]int, len(parsed.PrimaryTerms()))
	var totalDocLen int64
	for _, edge := range edges {
		if !matchesRelationshipTypes(edge, targetTypes) {
			continue
		}
		doc := buildEdgeFulltextDoc(edge, targetProperties)
		if doc == nil {
			continue
		}
		docs = append(docs, edgeDoc{edge: edge, doc: doc})
		totalDocLen += int64(doc.contentTokenN)
		for _, term := range parsed.PrimaryTerms() {
			if strings.Contains(doc.contentLower, term) {
				docFreq[term]++
			}
		}
	}
	totalDocs := len(docs)
	avgDocLen := 100.0
	if totalDocs > 0 {
		avgDocLen = float64(totalDocLen) / float64(totalDocs)
	}

	ctx := &ftEvalCtx{
		DefaultFields: targetProperties,
		avgDocLen:     avgDocLen,
		totalDocs:     totalDocs,
		docFreq:       docFreq,
	}

	type scoredEdge struct {
		edge  *storage.Edge
		score float64
	}
	scored := make([]scoredEdge, 0, len(docs))
	for _, d := range docs {
		matched, score := parsed.Match(ctx, d.doc)
		if !matched {
			continue
		}
		if score <= 0 {
			score = 1.0
		}
		scored = append(scored, scoredEdge{edge: d.edge, score: score})
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return scored[i].edge.ID < scored[j].edge.ID
		}
		return scored[i].score > scored[j].score
	})
	result.Rows = make([][]interface{}, 0, len(scored))
	for _, s := range scored {
		result.Rows = append(result.Rows, []interface{}{edgeToMap(s.edge), s.score})
	}
	applyFulltextOptions(result, opts)

	return result, nil
}

// buildEdgeFulltextDoc mirrors buildNodeFulltextDoc for relationships.
func buildEdgeFulltextDoc(edge *storage.Edge, properties []string) *ftDoc {
	if edge == nil {
		return nil
	}
	props := make(map[string]string, len(edge.Properties))
	rawProps := make(map[string]string, len(edge.Properties))
	for k, v := range edge.Properties {
		if v == nil {
			continue
		}
		raw := fmt.Sprintf("%v", v)
		if raw == "" {
			continue
		}
		props[strings.ToLower(k)] = strings.ToLower(raw)
		rawProps[strings.ToLower(k)] = raw
	}
	content := extractEdgeTextContent(edge, properties)
	if content == "" {
		return nil
	}
	contentLower := strings.ToLower(content)
	return &ftDoc{
		properties:    props,
		rawProperties: rawProps,
		contentLower:  contentLower,
		contentTokenN: len(strings.Fields(content)),
	}
}

// edgeHasNonEmptyProperty mirrors nodeHasNonEmptyProperty: the
// `<prop>:*` Lucene field-presence query treats empty strings as
// missing values.
func edgeHasNonEmptyProperty(edge *storage.Edge, propName string) bool {
	if edge == nil {
		return false
	}
	val, ok := edge.Properties[propName]
	if !ok || val == nil {
		return false
	}
	if s, ok := val.(string); ok && s == "" {
		return false
	}
	return true
}

// matchesRelationshipTypes reports whether edge.Type is in the
// target list. An empty target list means "every type" — the legacy
// behavior for indexes that don't carry a RelationshipTypes scope.
func matchesRelationshipTypes(edge *storage.Edge, targetTypes []string) bool {
	if len(targetTypes) == 0 {
		return true
	}
	for _, t := range targetTypes {
		if edge.Type == t {
			return true
		}
	}
	return false
}

// edgePropertiesContain reports whether any of the edge's declared
// properties (or every property, when targetProperties is empty)
// contains lowerQuery as a case-insensitive substring.
func edgePropertiesContain(edge *storage.Edge, targetProperties []string, lowerQuery string) bool {
	if len(targetProperties) > 0 {
		for _, prop := range targetProperties {
			val, ok := edge.Properties[prop]
			if !ok {
				continue
			}
			if str, ok := val.(string); ok {
				if strings.Contains(strings.ToLower(str), lowerQuery) {
					return true
				}
			}
		}
		return false
	}
	for _, val := range edge.Properties {
		if str, ok := val.(string); ok {
			if strings.Contains(strings.ToLower(str), lowerQuery) {
				return true
			}
		}
	}
	return false
}

func extractEdgeTextContent(edge *storage.Edge, properties []string) string {
	if edge == nil {
		return ""
	}
	var content strings.Builder
	if len(properties) > 0 {
		for _, propName := range properties {
			if val, ok := edge.Properties[propName]; ok {
				writeFulltextValue(&content, val)
			}
		}
		return strings.TrimSpace(content.String())
	}
	for _, val := range edge.Properties {
		writeFulltextValue(&content, val)
	}
	return strings.TrimSpace(content.String())
}

func writeFulltextValue(content *strings.Builder, val interface{}) {
	if content == nil || val == nil {
		return
	}
	if s, ok := val.(string); ok {
		content.WriteString(s)
		content.WriteByte(' ')
		return
	}
	content.WriteString(fmt.Sprint(val))
	content.WriteByte(' ')
}

// edgeToMap returns the result-row map representation of an edge. Held
// in one place so the queryRelationships scan and any future relationship
// surface emit identical shapes.
func edgeToMap(edge *storage.Edge) map[string]interface{} {
	result := map[string]interface{}{
		"_id":        string(edge.ID),
		"_type":      edge.Type,
		"_start":     string(edge.StartNode),
		"_end":       string(edge.EndNode),
		"properties": edge.Properties,
	}
	for key, value := range edge.Properties {
		if _, exists := result[key]; !exists {
			result[key] = value
		}
	}
	return result
}

// callDbIndexVectorQueryRelationships searches relationships using vector similarity - Neo4j db.index.vector.queryRelationships()
// Syntax: CALL db.index.vector.queryRelationships('indexName', k, queryInput)
// queryInput can be: [0.1, 0.2, ...] OR 'search text' OR $param
func (e *StorageExecutor) callDbIndexVectorQueryRelationships(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse parameters from: CALL db.index.vector.queryRelationships('indexName', k, queryInput)
	// queryInput can be: [0.1, 0.2, ...] OR 'search text' OR $param
	indexName, k, input, err := e.parseVectorQueryParams(cypher)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresVectorQueryParseFailed(err), err)
	}

	// Resolve the query vector (same logic as queryNodes)
	var queryVector []float32

	if len(input.vector) > 0 {
		// Direct vector provided (Neo4j compatible)
		queryVector = input.vector
	} else if input.stringQuery != "" {
		// String query - embed server-side (NornicDB enhancement)
		if e.embedder == nil {
			return nil, localizedError(localization.CypherProceduresStringQueryEmbedderRequired(), nil)
		}
		embedded, embedErr := e.embedVectorQueryText(ctx, input.stringQuery)
		if embedErr != nil {
			return nil, localizedError(localization.CypherProceduresEmbedQueryFailed(input.stringQuery, embedErr), embedErr)
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
			return nil, localizedError(localization.CypherProceduresParameterNotProvided(input.paramName), nil)
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
					return nil, localizedError(localization.CypherProceduresParameterNonNumeric(input.paramName, fmt.Sprintf("%T", v)), nil)
				}
			}
		case string:
			// String parameter - embed it
			if e.embedder == nil {
				return nil, localizedError(localization.CypherProceduresParameterStringEmbedderRequired(input.paramName), nil)
			}
			embedded, embedErr := e.embedVectorQueryText(ctx, val)
			if embedErr != nil {
				return nil, localizedError(localization.CypherProceduresEmbedParameterFailed(input.paramName, val, embedErr), embedErr)
			}
			queryVector = embedded
		default:
			return nil, localizedError(localization.CypherProceduresParameterUnsupportedType(input.paramName, fmt.Sprintf("%T", val)), nil)
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
				return nil, localizedError(localization.CypherProceduresUnsupportedParameters(paramList), nil)
			}
			// Parameters exist but all have supported types - might be a different issue
			return nil, localizedError(localization.CypherProceduresQueryInputPossiblyUnsupported(), nil)
		}
		return nil, localizedError(localization.CypherProceduresQueryInputRequired(), nil)
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

	if e.searchService != nil && targetProperty != "" && e.searchService.HasRelationshipVectorEntries(targetRelType, targetProperty) {
		hits, err := e.searchService.VectorQueryRelationships(ctx, queryVector, search.RelationshipVectorQuerySpec{
			IndexName:  indexName,
			Type:       targetRelType,
			Property:   targetProperty,
			Similarity: similarityFunc,
			Limit:      k,
		})
		if err != nil {
			return nil, err
		}
		for _, hit := range hits {
			edge, err := e.storage.GetEdge(storage.EdgeID(hit.ID))
			if err != nil {
				continue
			}
			result.Rows = append(result.Rows, []interface{}{edgeToMap(edge), hit.Score})
		}
		return result, nil
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

		if len(edgeEmbedding) == 0 {
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
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeInvalidSyntax(false), nil)
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeInvalidSyntax(true), nil)
	}

	args := remainder[openParen+1 : closeParen]
	parts := strings.Split(args, ",")
	if len(parts) < 4 {
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeArgumentsRequired(), nil)
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
	err := schema.AddVectorIndexForEntity(indexName, label, property, dimension, similarity, storage.ConstraintEntityNode)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresCreateVectorIndexFailed(err), err)
	}

	e.registerVectorSpace(indexName, label, property, dimension, similarity)

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
		return nil, localizedError(localization.CypherProceduresVectorCreateRelationshipInvalidSyntax(false), nil)
	}

	// Parse arguments similar to createNodeIndex
	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, localizedError(localization.CypherProceduresVectorCreateRelationshipInvalidSyntax(true), nil)
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsSimple(argsStr)
	if len(parts) < 4 {
		return nil, localizedError(localization.CypherProceduresVectorCreateRelationshipArguments(), nil)
	}

	indexName := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	relType := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	property := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
	dimension, err := strconv.Atoi(strings.TrimSpace(parts[3]))
	if err != nil {
		return nil, localizedError(localization.CypherProceduresInvalidDimension(err), err)
	}

	similarity := "cosine"
	if len(parts) > 4 {
		similarity = strings.Trim(strings.TrimSpace(parts[4]), "'\"")
	}

	// Create vector index on relationships using schema manager
	schema := e.storage.GetSchema()
	// Use relationship type as "label" for index naming
	err = schema.AddVectorIndexForEntity(indexName, relType, property, dimension, similarity, storage.ConstraintEntityRelationship)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresCreateRelationshipVectorIndexFailed(err), err)
	}

	e.registerVectorSpace(indexName, relType, property, dimension, similarity)

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
		return nil, localizedError(localization.CypherProceduresFulltextCreateNodeInvalidSyntax(false), nil)
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, localizedError(localization.CypherProceduresFulltextCreateNodeInvalidSyntax(true), nil)
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsRespectingArrays(argsStr)
	if len(parts) < 3 {
		return nil, localizedError(localization.CypherProceduresFulltextCreateNodeArgumentsRequired(), nil)
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
		return nil, localizedError(localization.CypherProceduresCreateFulltextIndexFailed(err), err)
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
		return nil, localizedError(localization.CypherProceduresFulltextCreateRelationshipInvalid(false), nil)
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, localizedError(localization.CypherProceduresFulltextCreateRelationshipInvalid(true), nil)
	}

	argsStr := cypher[idx+argsStart+1 : idx+argsEnd]
	parts := e.splitArgsRespectingArrays(argsStr)
	if len(parts) < 3 {
		return nil, localizedError(localization.CypherProceduresFulltextCreateRelationshipArguments(), nil)
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
		return nil, localizedError(localization.CypherProceduresCreateRelationshipFulltextIndexFailed(err), err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "relationshipTypes", "properties"},
		Rows:    [][]interface{}{{indexName, relTypes, properties}},
	}, nil
}

// callDbIndexFulltextDrop drops a fulltext index - Neo4j db.index.fulltext.drop()
// Syntax: CALL db.index.fulltext.drop(indexName)
func (e *StorageExecutor) callDbIndexFulltextDrop(cypher string) (*ExecuteResult, error) {
	idx := findKeywordIndex(cypher, "DROP")
	if idx < 0 {
		return nil, localizedError(localization.CypherProceduresFulltextDropInvalidSyntax(false), nil)
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, localizedError(localization.CypherProceduresFulltextDropInvalidSyntax(true), nil)
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
	idx := findKeywordIndex(cypher, "DROP")
	if idx < 0 {
		return nil, localizedError(localization.CypherProceduresVectorDropInvalidSyntax(false), nil)
	}

	argsStart := strings.Index(cypher[idx:], "(")
	argsEnd := strings.LastIndex(cypher[idx:], ")")
	if argsStart < 0 || argsEnd < 0 {
		return nil, localizedError(localization.CypherProceduresVectorDropInvalidSyntax(true), nil)
	}

	indexName := strings.Trim(strings.TrimSpace(cypher[idx+argsStart+1:idx+argsEnd]), "'\"")

	e.unregisterVectorSpace(indexName)

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
	store := e.getStorage(ctx)
	// Parse: CALL db.create.setNodeVectorProperty(nodeId, 'propertyKey', [vector])
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETNODEVECTORPROPERTY")
	if idx < 0 {
		return nil, localizedError(localization.CypherProceduresSetNodeVectorInvalidSyntax(), nil)
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, localizedError(localization.CypherProceduresSetNodeVectorParenthesesRequired(), nil)
	}

	argsStr := remainder[openParen+1 : closeParen]

	// Extract nodeId (first arg)
	commaIdx := strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, localizedError(localization.CypherProceduresSetNodeVectorArgumentsRequired(), nil)
	}
	nodeIDStr := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract property key (second arg)
	commaIdx = strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, localizedError(localization.CypherProceduresSetNodeVectorArgumentRequired(), nil)
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
	node, err := store.GetNode(nodeID)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresNodeNotFound(nodeIDStr), nil)
	}

	// Set the vector property
	node.Properties[propertyKey] = vector
	err = store.UpdateNode(node)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresUpdateNodeFailed(err), err)
	}
	e.notifyNodeMutated(string(node.ID))

	return &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{{node}},
	}, nil
}

// callDbCreateSetRelationshipVectorProperty sets a vector property on a relationship - Neo4j db.create.setRelationshipVectorProperty()
// Syntax: CALL db.create.setRelationshipVectorProperty(relationship, propertyKey, vector)
func (e *StorageExecutor) callDbCreateSetRelationshipVectorProperty(ctx context.Context, cypher string) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	// Parse: CALL db.create.setRelationshipVectorProperty(relId, 'propertyKey', [vector])
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETRELATIONSHIPVECTORPROPERTY")
	if idx < 0 {
		return nil, localizedError(localization.CypherProceduresSetRelationshipVectorInvalidSyntax(), nil)
	}

	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, localizedError(localization.CypherProceduresSetRelationshipVectorParentheses(), nil)
	}

	argsStr := remainder[openParen+1 : closeParen]

	// Extract relId (first arg)
	commaIdx := strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, localizedError(localization.CypherProceduresSetRelationshipVectorArguments(), nil)
	}
	relIDStr := strings.Trim(strings.TrimSpace(argsStr[:commaIdx]), "'\"")
	argsStr = argsStr[commaIdx+1:]

	// Extract property key (second arg)
	commaIdx = strings.Index(argsStr, ",")
	if commaIdx < 0 {
		return nil, localizedError(localization.CypherProceduresSetRelationshipVectorArgument(), nil)
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
	rel, err := store.GetEdge(relID)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresRelationshipNotFound(relIDStr), nil)
	}

	// Set the vector property
	rel.Properties[propertyKey] = vector
	err = store.UpdateEdge(rel)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresUpdateRelationshipFailed(err), err)
	}
	e.notifyEdgeMutated(string(rel.ID))

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
func (e *StorageExecutor) callTxSetMetadata(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Check if there's an active transaction
	if e.txContext == nil || !e.txContext.active {
		return nil, localizedError(localization.CypherProceduresMetadataActiveTransactionRequired(), nil)
	}

	// Extract metadata object from Cypher: CALL tx.setMetaData({key: value})
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, "SETMETADATA")
	if idx < 0 {
		return nil, localizedError(localization.CypherProceduresMetadataInvalidSyntax(false), nil)
	}

	// Find opening parenthesis
	remainder := cypher[idx:]
	openParen := strings.Index(remainder, "(")
	closeParen := strings.LastIndex(remainder, ")")
	if openParen < 0 || closeParen < 0 {
		return nil, localizedError(localization.CypherProceduresMetadataInvalidSyntax(true), nil)
	}

	// Extract the metadata object string: {key: value}
	argsStr := strings.TrimSpace(remainder[openParen+1 : closeParen])
	if argsStr == "" {
		return nil, localizedError(localization.CypherProceduresMetadataObjectRequired(), nil)
	}

	// Parse the metadata object
	metadata := e.parseProperties(ctx, argsStr)
	if len(metadata) == 0 {
		return nil, localizedError(localization.CypherProceduresMetadataEntryRequired(), nil)
	}

	// Get the transaction and set metadata
	tx, ok := e.txContext.tx.(*storage.BadgerTransaction)
	if !ok {
		return nil, localizedError(localization.CypherProceduresMetadataTransactionUnsupported(), nil)
	}

	err := tx.SetMetadata(metadata)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresSetMetadataFailed(err), err)
	}

	return &ExecuteResult{
		Columns: []string{"status"},
		Rows: [][]interface{}{
			{"Transaction metadata set successfully"},
		},
	}, nil
}
