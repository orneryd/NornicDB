package cypher

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ===== Additional Neo4j Compatibility Procedures =====

// callDbInfo returns database information - Neo4j db.info()
func (e *StorageExecutor) callDbInfo(ctx context.Context) (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()
	name := e.currentDatabaseName()
	if selected := GetUseDatabaseFromContext(ctx); selected != "" {
		name = selected
	}
	var id, creationDate interface{}
	if provider, ok := e.dbManager.(DatabaseIdentityProvider); ok {
		databaseID, createdAt := provider.DatabaseIdentity(name)
		if databaseID != "" {
			id = databaseID
		}
		if !createdAt.IsZero() {
			creationDate = createdAt.UTC().Format(time.RFC3339Nano)
		}
	}

	return &ExecuteResult{
		Columns: []string{"id", "name", "creationDate", "nodeCount", "relationshipCount"},
		Rows: [][]interface{}{
			{id, name, creationDate, nodeCount, edgeCount},
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
	name := "system"
	if manager, ok := e.dbManager.(interface{ SystemDatabaseName() string }); ok {
		name = manager.SystemDatabaseName()
	}
	var id, creationDate interface{}
	if provider, ok := e.dbManager.(DatabaseIdentityProvider); ok {
		databaseID, createdAt := provider.DatabaseIdentity(name)
		if databaseID != "" {
			id = databaseID
		}
		if !createdAt.IsZero() {
			creationDate = createdAt.UTC().Format(time.RFC3339Nano)
		}
	}
	return &ExecuteResult{
		Columns: []string{"id", "name", "creationDate"},
		Rows: [][]interface{}{
			{id, name, creationDate},
		},
	}, nil
}

// callDbmsListConfig lists DBMS configuration - Neo4j dbms.listConfig()
func (e *StorageExecutor) callDbmsListConfig() (*ExecuteResult, error) {
	return e.callDbmsListConfigArguments(context.Background(), nil)
}

func (e *StorageExecutor) callDbmsListConfigArguments(ctx context.Context, arguments []interface{}) (*ExecuteResult, error) {
	searchString := ""
	if len(arguments) > 0 {
		var valid bool
		searchString, valid = arguments[0].(string)
		if !valid {
			return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "dbms.listConfig requires a STRING search filter")
		}
	}
	settings, err := e.executeShowSettings(ctx, "SHOW SETTINGS")
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0, len(settings.Rows)+3)
	for _, setting := range settings.Rows {
		validValues, _ := setting[7].([]string)
		rows = append(rows, []interface{}{setting[0], setting[4], setting[1], setting[2], setting[3], setting[5], setting[6], strings.Join(validValues, ", ")})
	}
	version := buildinfo.Version()
	rows = append(rows,
		[]interface{}{"nornicdb.version", "NornicDB version", version, false, version, version, false, "Build version"},
		[]interface{}{"nornicdb.bolt.enabled", "Bolt protocol enabled", nil, false, nil, nil, nil, "true, false"},
		[]interface{}{"nornicdb.http.enabled", "HTTP API enabled", nil, false, nil, nil, nil, "true, false"},
	)
	sort.Slice(rows, func(first, second int) bool { return rows[first][0].(string) < rows[second][0].(string) })
	result := &ExecuteResult{
		Columns: []string{"name", "description", "value", "dynamic", "defaultValue", "startupValue", "explicitlySet", "validValues"},
		Rows:    [][]interface{}{},
	}
	for _, row := range rows {
		if strings.Contains(row[0].(string), searchString) {
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

// callDbmsClientConfig lists client-visible configuration - Neo4j dbms.clientConfig()
func (e *StorageExecutor) callDbmsClientConfig() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"name", "description", "value", "dynamic", "defaultValue", "startupValue", "explicitlySet", "validValues"},
		Rows: [][]interface{}{
			{"server.bolt.advertised_address", "Bolt connector advertised address", "localhost:7687", false, nil, nil, false, "localhost:7687"},
			{"server.http.advertised_address", "HTTP connector advertised address", "localhost:7474", false, nil, nil, false, "localhost:7474"},
		},
	}, nil
}

// callDbmsListConnections lists active connections - Neo4j dbms.listConnections()
func (e *StorageExecutor) callDbmsListConnections() (*ExecuteResult, error) {
	return e.callDbmsListConnectionsWithContext(context.Background())
}

func (e *StorageExecutor) callDbmsListConnectionsWithContext(ctx context.Context) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress"},
		Rows:    [][]interface{}{},
	}
	identity := requestIdentityFromContext(ctx)
	if identity == nil || identity.Connections == nil {
		return result, nil
	}
	viewAll := identity.User == nil
	if identity.User != nil {
		for _, role := range identity.User.Roles {
			viewAll = viewAll || strings.EqualFold(role, "admin")
		}
	}
	for _, connection := range identity.Connections() {
		if !viewAll && connection.Username != identity.User.Name {
			continue
		}
		result.Rows = append(result.Rows, []interface{}{connection.ConnectionID, connection.ConnectTime, connection.Connector, connection.Username, connection.UserAgent, connection.ServerAddress, connection.ClientAddress})
	}
	sort.Slice(result.Rows, func(first, second int) bool { return result.Rows[first][0].(string) < result.Rows[second][0].(string) })
	return result, nil
}

// callDbIndexFulltextListAvailableAnalyzers lists fulltext analyzers - Neo4j db.index.fulltext.listAvailableAnalyzers()
func (e *StorageExecutor) callDbIndexFulltextListAvailableAnalyzers() (*ExecuteResult, error) {
	rows := [][]interface{}{{"none", "Language-neutral Unicode analyzer", []string{}, "exact", "", "", false, []string{}}}
	for _, registration := range stemmer.Available() {
		digest := registration.Digest
		if len(digest) > 12 {
			digest = digest[:12]
		}
		rows = append(rows, []interface{}{
			registration.ID,
			"Registered BM25 stemmer plugin",
			nil,
			"stemmer",
			registration.Version,
			digest,
			stemmer.DynamicLoadSupported(),
			[]string{},
		})
	}
	return &ExecuteResult{
		Columns: []string{"analyzer", "description", "stopwords", "kind", "version", "digest", "dynamicLoad", "selectedDatabases"},
		Rows:    rows,
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

	// Look up the index's declared scope. A missing index fails as for
	// queryNodes (requireFulltextIndex); a built-in name, or a node-only index
	// (no relationship_types declared), falls through to the unscoped scan.
	var targetTypes []string
	var targetProperties []string
	declared := false
	if schema := e.storage.GetSchema(); schema != nil {
		if ftIdx, exists := schema.GetFulltextIndex(indexName); exists {
			targetTypes = ftIdx.RelationshipTypes
			targetProperties = ftIdx.Properties
			declared = true
		}
	}
	if err := requireFulltextIndex(indexName, declared); err != nil {
		return nil, err
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
				// As for nodes (and in Neo4j), a relationship with none of the
				// indexed properties has no document, so the wildcard skips it
				// (#547).
				if len(targetProperties) > 0 && !edgeHasAnyNonEmptyProperty(edge, targetProperties) {
					return nil
				}
				if appendFulltextOptionedRow(result, opts, &seen, []interface{}{e.procedureRelationship(edge), 1.0}) {
					return storage.ErrIterationStopped
				}
				return nil
			}
			if edgeHasNonEmptyProperty(edge, presenceProp) {
				if appendFulltextOptionedRow(result, opts, &seen, []interface{}{e.procedureRelationship(edge), 1.0}) {
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
		result.Rows = append(result.Rows, []interface{}{e.procedureRelationship(s.edge), s.score})
	}
	applyFulltextOptions(result, opts)

	return result, nil
}

// buildEdgeFulltextDoc mirrors buildNodeFulltextDoc for relationships.
func buildEdgeFulltextDoc(edge *storage.Edge, properties []string) *ftDoc {
	if edge == nil {
		return nil
	}
	return buildFulltextDocFromProperties(edge.Properties, extractEdgeTextContent(edge, properties))
}

// edgeHasNonEmptyProperty mirrors nodeHasNonEmptyProperty: the
// `<prop>:*` Lucene field-presence query treats empty strings as
// missing values.
// edgeHasAnyNonEmptyProperty reports whether edge has a non-empty value for
// any of props (the relationship has a fulltext document for an index on
// them).
func edgeHasAnyNonEmptyProperty(edge *storage.Edge, props []string) bool {
	for _, prop := range props {
		if edgeHasNonEmptyProperty(edge, prop) {
			return true
		}
	}
	return false
}

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
				if strings.Contains(lowerASCII(str), lowerQuery) {
					return true
				}
			}
		}
		return false
	}
	for _, val := range edge.Properties {
		if str, ok := val.(string); ok {
			if strings.Contains(lowerASCII(str), lowerQuery) {
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
	return e.callDbIndexVectorQueryRelationshipsInput(ctx, indexName, k, input)
}

func (e *StorageExecutor) callDbIndexVectorQueryRelationshipsInput(ctx context.Context, indexName string, k int, input *vectorQueryInput) (*ExecuteResult, error) {
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
	} else if input.hasValue || input.paramName != "" {
		// Parameter reference - resolve from context parameters
		paramValue := input.value
		if !input.hasValue {
			params := getParamsFromContext(ctx)
			if params == nil {
				return &ExecuteResult{Columns: []string{"relationship", "score"}, Rows: [][]interface{}{}}, nil
			}
			var exists bool
			paramValue, exists = params[input.paramName]
			if !exists {
				return nil, localizedError(localization.CypherProceduresParameterNotProvided(input.paramName), nil)
			}
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
			return nil, localizedError(localization.CypherProceduresParameterUnsupportedType(input.paramName, cypherTypeName(val)), nil)
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
			result.Rows = append(result.Rows, []interface{}{e.procedureRelationship(edge), hit.Score})
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
			e.procedureRelationship(se.edge),
			se.score,
		})
	}

	return result, nil
}

// callDbIndexVectorCreateNodeIndex creates a vector index on nodes - Neo4j db.index.vector.createNodeIndex()
// Syntax: CALL db.index.vector.createNodeIndex(indexName, label, property, dimension, similarityFunction)
func (e *StorageExecutor) callDbIndexVectorCreateNodeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if !strings.EqualFold(extractProcedureName(cypher), "db.index.vector.createNodeIndex") {
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeInvalidSyntax(false), nil)
	}
	if !strings.Contains(cypher, "(") || !strings.Contains(cypher, ")") {
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeInvalidSyntax(true), nil)
	}
	arguments, err := extractProcedureInvocationArguments(ctx, vectorCreateNodeProcedureSpec(), cypher)
	if err != nil {
		return nil, err
	}
	return e.callDbIndexVectorCreateNodeIndexArguments(ctx, arguments)
}

func (e *StorageExecutor) callDbIndexVectorCreateNodeIndexArguments(ctx context.Context, arguments []interface{}) (*ExecuteResult, error) {
	if len(arguments) < 4 || len(arguments) > 5 {
		return nil, localizedError(localization.CypherProceduresVectorCreateNodeArgumentsRequired(), nil)
	}
	indexName, validName := arguments[0].(string)
	label, validLabel := arguments[1].(string)
	property, validProperty := arguments[2].(string)
	dimension := toInt64(arguments[3])
	validDimension := isIntegerProcedureValue(arguments[3])
	similarity := "cosine"
	validSimilarity := true
	if len(arguments) == 5 {
		similarity, validSimilarity = arguments[4].(string)
	}
	if !validName || !validLabel || !validProperty || !validDimension || !validSimilarity {
		return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "vector index creation requires STRING names, an INTEGER dimension, and a STRING similarity function")
	}
	similarity = strings.ToLower(similarity)
	if dimension <= 0 || (similarity != "cosine" && similarity != "euclidean" && similarity != "dot") {
		return nil, newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "InvalidArgument", "vector index creation requires a positive dimension and a supported similarity function")
	}
	err := e.mutateSchema(ctx, func(schema *storage.SchemaManager) error {
		if _, err := admitSchemaIndexCreation(schema, "", indexName, "VECTOR", []string{label}, []string{property}, storage.ConstraintEntityNode); err != nil {
			return err
		}
		if err := schema.AddVectorIndexForEntity(indexName, label, property, int(dimension), similarity, storage.ConstraintEntityNode); err != nil {
			return err
		}
		e.afterSchemaCommit(ctx, func() {
			e.registerVectorSpace(indexName, label, property, int(dimension), similarity)
		})
		return nil
	})
	if err != nil {
		return nil, &classifiedCypherError{
			cause: localizedError(localization.CypherProceduresCreateVectorIndexFailed(err), err),
			code:  "Neo.ClientError.Procedure.ProcedureCallFailed", detail: "ProcedureCallFailed",
		}
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callDbIndexVectorCreateRelationshipIndex creates a vector index on relationships - Neo4j db.index.vector.createRelationshipIndex()
// Syntax: CALL db.index.vector.createRelationshipIndex(indexName, relationshipType, property, dimension, similarityFunction)
func (e *StorageExecutor) callDbIndexVectorCreateRelationshipIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := upperASCII(cypher)
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
	// Use relationship type as "label" for index naming
	err = e.mutateSchema(ctx, func(schema *storage.SchemaManager) error {
		if _, err := admitSchemaIndexCreation(schema, "", indexName, "VECTOR", []string{relType}, []string{property}, storage.ConstraintEntityRelationship); err != nil {
			return err
		}
		return schema.AddVectorIndexForEntity(indexName, relType, property, dimension, similarity, storage.ConstraintEntityRelationship)
	})
	if err != nil {
		return nil, localizedError(localization.CypherProceduresCreateRelationshipVectorIndexFailed(err), err)
	}

	return &ExecuteResult{
		Columns: []string{"name", "relationshipType", "property", "dimension", "similarityFunction"},
		Rows:    [][]interface{}{{indexName, relType, property, dimension, similarity}},
	}, nil
}

// callDbIndexFulltextCreateNodeIndex creates a fulltext index on nodes - Neo4j db.index.fulltext.createNodeIndex()
// Syntax: CALL db.index.fulltext.createNodeIndex(indexName, labels, properties, config)
func (e *StorageExecutor) callDbIndexFulltextCreateNodeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := upperASCII(cypher)
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
	err := e.mutateSchema(ctx, func(schema *storage.SchemaManager) error {
		if _, err := admitSchemaIndexCreation(schema, "", indexName, "FULLTEXT", labels, properties, storage.ConstraintEntityNode); err != nil {
			return err
		}
		return schema.AddFulltextIndex(indexName, labels, properties)
	})
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
	upper := upperASCII(cypher)
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
	err := e.mutateSchema(ctx, func(schema *storage.SchemaManager) error {
		if _, err := admitSchemaIndexCreation(schema, "", indexName, "FULLTEXT", relTypes, properties, storage.ConstraintEntityRelationship); err != nil {
			return err
		}
		return schema.AddFulltextRelationshipIndex(indexName, relTypes, properties)
	})
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

	if err := e.dropIndexOfKind(indexName, "fulltext", func(schema *storage.SchemaManager) bool {
		_, ok := schema.GetFulltextIndex(indexName)
		return ok
	}); err != nil {
		return nil, err
	}
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

	if err := e.dropIndexOfKind(indexName, "vector", func(schema *storage.SchemaManager) bool {
		_, ok := schema.GetVectorIndex(indexName)
		return ok
	}); err != nil {
		return nil, err
	}
	return &ExecuteResult{
		Columns: []string{"name", "dropped"},
		Rows:    [][]interface{}{{indexName, true}},
	}, nil
}

// dropIndexOfKind backs the db.index.<kind>.drop compatibility procedures: it
// drops the named index through dropIndexByName (the DROP INDEX path) only when
// an index of that kind exists under the name, and otherwise fails with
// Neo.ClientError.Schema.IndexDropFailed, so a procedure never reports a drop
// that did not happen and never drops an index of another kind.
func (e *StorageExecutor) dropIndexOfKind(name, kind string, exists func(*storage.SchemaManager) bool) error {
	if isCompositeRoot(e.storage) {
		return localizedError(localization.CypherSchemaCompositeDDLNotAllowed(), nil)
	}
	return e.mutateSchema(context.Background(), func(schema *storage.SchemaManager) error {
		if schema == nil || !exists(schema) {
			return newSemanticError("Neo.ClientError.Schema.IndexDropFailed", "MissingIndex",
				fmt.Sprintf("there is no %s index named %q", kind, name))
		}
		return e.dropIndexByName(name, false)
	})
}

// splitArgsSimple splits comma-separated arguments, respecting quoted strings
func (e *StorageExecutor) splitArgsSimple(args string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(args); i++ {
		c := args[i]
		if (c == '\'' || c == '"') && !isBackslashEscaped(args, i) {
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
		if (c == '\'' || c == '"') && !isBackslashEscaped(args, i) {
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
	arguments, err := extractProcedureInvocationArguments(ctx, ProcedureSpec{Name: "db.create.setNodeVectorProperty", MinArgs: 3, MaxArgs: 3}, cypher)
	if err != nil {
		return nil, err
	}
	return e.callSetVectorProperty(ctx, arguments, false)
}

// callDbCreateSetRelationshipVectorProperty sets a vector property on a relationship - Neo4j db.create.setRelationshipVectorProperty()
// Syntax: CALL db.create.setRelationshipVectorProperty(relationship, propertyKey, vector)
func (e *StorageExecutor) callDbCreateSetRelationshipVectorProperty(ctx context.Context, cypher string) (*ExecuteResult, error) {
	arguments, err := extractProcedureInvocationArguments(ctx, ProcedureSpec{Name: "db.create.setRelationshipVectorProperty", MinArgs: 3, MaxArgs: 3}, cypher)
	if err != nil {
		return nil, err
	}
	return e.callSetVectorProperty(ctx, arguments, true)
}

func (e *StorageExecutor) callSetVectorProperty(ctx context.Context, arguments []interface{}, relationship bool) (*ExecuteResult, error) {
	if len(arguments) != 3 {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments", "vector property setter requires three arguments")
	}
	identifier, validID := arguments[0].(string)
	switch entity := arguments[0].(type) {
	case *storage.Node:
		if !relationship && entity != nil {
			identifier, validID = string(entity.ID), true
		}
	case *storage.Edge:
		if relationship && entity != nil {
			identifier, validID = string(entity.ID), true
		}
	}
	propertyKey, validKey := arguments[1].(string)
	if expression, raw := arguments[2].(string); raw && strings.HasPrefix(strings.TrimSpace(expression), "[") {
		arguments[2] = e.evaluateExpressionWithContext(ctx, expression, nil, nil)
	}
	vector, validVector := toFloat64Slice(arguments[2])
	if !validID || !validKey || !validVector {
		return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "vector property setter requires an entity, a STRING property key, and a numeric vector")
	}
	store := e.getStorage(ctx)
	if relationship {
		edge, err := store.GetEdge(storage.EdgeID(identifier))
		if err != nil {
			return nil, localizedError(localization.CypherProceduresRelationshipNotFound(identifier), nil)
		}
		if current, ok := toFloat64Slice(edge.Properties[propertyKey]); !ok || !slices.Equal(current, vector) {
			edge.Properties[propertyKey] = vector
			if err := store.UpdateEdge(edge); err != nil {
				return nil, localizedError(localization.CypherProceduresUpdateRelationshipFailed(err), err)
			}
			e.notifyEdgeMutated(string(edge.ID))
		}
		if entity, ok := arguments[0].(*storage.Edge); ok {
			entity.Properties = edge.Properties
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	node, err := store.GetNode(storage.NodeID(identifier))
	if err != nil {
		return nil, localizedError(localization.CypherProceduresNodeNotFound(identifier), nil)
	}
	if current, ok := toFloat64Slice(node.Properties[propertyKey]); !ok || !slices.Equal(current, vector) {
		node.Properties[propertyKey] = vector
		if err := store.UpdateNode(node); err != nil {
			return nil, localizedError(localization.CypherProceduresUpdateNodeFailed(err), err)
		}
		e.notifyNodeMutated(string(node.ID))
	}
	if entity, ok := arguments[0].(*storage.Node); ok {
		entity.Properties = node.Properties
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// callTxSetMetadata sets transaction metadata - Neo4j tx.setMetaData()
//
// This procedure is used to attach metadata to transactions for logging/debugging.
// Syntax: CALL tx.setMetaData({key: value})
//
// Uses the active explicit transaction or the statement's implicit transaction.
// Metadata is stored with the transaction for logging, debugging, or audit trails.
func (e *StorageExecutor) callTxSetMetadata(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if !strings.EqualFold(extractProcedureName(cypher), "tx.setMetaData") {
		return nil, localizedError(localization.CypherProceduresMetadataInvalidSyntax(false), nil)
	}
	arguments, err := extractProcedureInvocationArguments(ctx, ProcedureSpec{Name: "tx.setMetaData", MinArgs: 1, MaxArgs: 1}, cypher)
	if err != nil {
		return nil, err
	}
	return e.callTxSetMetadataArguments(ctx, arguments)
}

func (e *StorageExecutor) callTxSetMetadataArguments(ctx context.Context, arguments []interface{}) (*ExecuteResult, error) {
	var tx *storage.BadgerTransaction
	if e.txContext != nil && e.txContext.active {
		var supported bool
		tx, supported = e.txContext.tx.(*storage.BadgerTransaction)
		if !supported {
			return nil, localizedError(localization.CypherProceduresMetadataTransactionUnsupported(), nil)
		}
	} else if wrapper, ok := e.getStorage(ctx).(*transactionStorageWrapper); ok {
		tx = wrapper.tx
	}
	if tx == nil {
		return nil, localizedError(localization.CypherProceduresMetadataActiveTransactionRequired(), nil)
	}

	if len(arguments) != 1 {
		return nil, localizedError(localization.CypherProceduresMetadataObjectRequired(), nil)
	}

	metadata, valid := toStringAnyMap(arguments[0])
	if expression, raw := arguments[0].(string); raw {
		metadata, valid = toStringAnyMap(e.evaluateExpressionWithContext(ctx, expression, nil, nil))
	}
	if !valid {
		return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "tx.setMetaData requires a MAP")
	}

	err := tx.SetMetadata(metadata)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresSetMetadataFailed(err), err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
