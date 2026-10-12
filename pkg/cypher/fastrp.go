// FastRP (Fast Random Projection) implementation for Neo4j GDS compatibility.
//
// FastRP creates node embeddings based on graph structure using random projection.
// This is useful for downstream ML tasks like node classification, link prediction,
// and similarity search.
//
// Neo4j GDS FastRP API:
//   CALL gds.fastRP.stream(graphName, configuration)
//   CALL gds.fastRP.stats(graphName, configuration)
//   CALL gds.graph.project(graphName, nodeQuery, relationshipQuery, config)
//   CALL gds.graph.list()
//   CALL gds.graph.drop(graphName)
//   CALL gds.version()
//
// Example Usage:
//
//	// Create a graph projection
//	CALL gds.graph.project('myGraph', 'Person', 'KNOWS')
//
//	// Generate embeddings
//	CALL gds.fastRP.stream('myGraph', {embeddingDimension: 64})
//	YIELD nodeId, embedding
//	RETURN nodeId, embedding
//
//	// Clean up
//	CALL gds.graph.drop('myGraph')

package cypher

import (
	"context"
	"crypto/rand"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	mathrand "math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Memory constants for streaming thresholds
const (
	// maxNodesBeforeStreaming triggers streaming mode for large graphs
	maxNodesBeforeStreaming = 10000
	// streamChunkSize is the batch size for streaming operations
	streamChunkSize = 1000
	// embeddingChunkSize is batch size for embedding generation
	embeddingChunkSize = 500
	// maxFastRPEmbeddingDimension prevents pathological embedding requests from
	// forcing oversized exact-length allocations.
	maxFastRPEmbeddingDimension = 8192
)

// ============================================================================
// In-Memory Graph Projections
// ============================================================================

// GraphProjection holds an in-memory graph projection for GDS algorithms
type GraphProjection struct {
	Name              string
	NodeLabels        []string
	RelationshipTypes []string
	NodeCount         int
	RelationshipCount int
	NodeIDs           []string                      // All node IDs in projection
	NodeProperties    map[string]map[string]any     // nodeID -> properties
	Adjacency         map[string][]string           // nodeID -> neighbor IDs
	EdgeWeights       map[string]map[string]float64 // source -> target -> weight
	CreatedAt         time.Time
}

// Global graph projection store (thread-safe)
var (
	graphProjections = make(map[string]*GraphProjection)
	projectionsMu    sync.RWMutex
)

// ============================================================================
// GDS Version
// ============================================================================

// callGdsVersion implements CALL gds.version()
func (e *StorageExecutor) callGdsVersion() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"version"},
		Rows: [][]any{
			{"2.6.0-nornicdb"}, // NornicDB's GDS-compatible version
		},
	}, nil
}

// ============================================================================
// Graph Projection Management
// ============================================================================

// callGdsGraphProject implements CALL gds.graph.project(graphName,
// nodeProjection, relationshipProjection) from the call's evaluated
// arguments. A projection is '*' (everything), a label or type name, a list
// of them, or a map whose keys name them (an entry's label / type overrides
// its key). A name text may also list several, separated by ':' or '|'
// ('Person:User', 'KNOWS|REFERENCES'): NornicDB's kept form.
func (e *StorageExecutor) callGdsGraphProject(args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.graph.project"
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	nodeLabels, err := graphProjectionNames(procedure, args, 1, "nodeProjection", "label")
	if err != nil {
		return nil, err
	}
	relTypes, err := graphProjectionNames(procedure, args, 2, "relationshipProjection", "type")
	if err != nil {
		return nil, err
	}

	// Build the projection from storage
	projection, err := e.buildGraphProjection(graphName, nodeLabels, relTypes)
	if err != nil {
		return nil, err
	}

	// Store the projection
	projectionsMu.Lock()
	graphProjections[graphName] = projection
	projectionsMu.Unlock()

	return &ExecuteResult{
		Columns: []string{"graphName", "nodeCount", "relationshipCount", "projectMillis"},
		Rows: [][]any{
			{graphName, projection.NodeCount, projection.RelationshipCount, int64(10)},
		},
	}, nil
}

// graphProjectionNames reads a node or relationship projection argument:
// the label or type names it selects, ["*"] for every one.
func graphProjectionNames(procedure string, args []interface{}, index int, argument, nameKey string) ([]string, error) {
	var names []string
	add := func(text string) {
		for _, name := range strings.FieldsFunc(text, func(r rune) bool { return r == ':' || r == '|' }) {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, name)
			}
		}
	}
	value := procedureArgument(args, index)
	switch projection := value.(type) {
	case nil:
	case string:
		add(projection)
	case map[string]interface{}:
		keys := make([]string, 0, len(projection))
		for key := range projection {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if entry, isMap := projection[key].(map[string]interface{}); isMap {
				if name, isString := entry[nameKey].(string); isString {
					add(name)
					continue
				}
			}
			add(key)
		}
	default:
		items, isList := cypherListValue(value)
		if !isList {
			return nil, procedureArgumentTypeError(procedure, argument, "STRING, LIST<STRING> or MAP", value)
		}
		for _, item := range items {
			name, isString := item.(string)
			if !isString {
				return nil, procedureArgumentTypeError(procedure, argument, "STRING, LIST<STRING> or MAP", value)
			}
			add(name)
		}
	}
	if len(names) == 0 {
		return []string{"*"}, nil
	}
	return names, nil
}

// buildGraphProjection creates an in-memory graph projection from storage
// Uses streaming to avoid loading all nodes/edges into memory at once
func (e *StorageExecutor) buildGraphProjection(name string, nodeLabels, relTypes []string) (*GraphProjection, error) {
	projection := &GraphProjection{
		Name:              name,
		NodeLabels:        nodeLabels,
		RelationshipTypes: relTypes,
		NodeIDs:           make([]string, 0, 1000), // Pre-allocate reasonable capacity
		NodeProperties:    make(map[string]map[string]any),
		Adjacency:         make(map[string][]string),
		EdgeWeights:       make(map[string]map[string]float64),
		CreatedAt:         time.Now(),
	}

	// Determine if we're matching all labels
	matchAllLabels := len(nodeLabels) == 1 && nodeLabels[0] == "*"

	// Track which nodes are in the projection
	nodeSet := make(map[string]bool)

	// Stream nodes instead of loading all at once
	ctx := context.Background()
	err := storage.StreamNodesWithFallback(ctx, e.storage, streamChunkSize, func(node *storage.Node) error {
		// Check if node matches label filter
		matchesLabel := matchAllLabels
		if !matchesLabel {
			for _, label := range node.Labels {
				for _, wantLabel := range nodeLabels {
					if label == wantLabel {
						matchesLabel = true
						break
					}
				}
				if matchesLabel {
					break
				}
			}
		}

		if matchesLabel {
			nodeID := string(node.ID)
			projection.NodeIDs = append(projection.NodeIDs, nodeID)
			nodeSet[nodeID] = true

			// Only copy essential properties to save memory
			if len(node.Properties) > 0 {
				projection.NodeProperties[nodeID] = make(map[string]any, len(node.Properties))
				for k, v := range node.Properties {
					projection.NodeProperties[nodeID][k] = v
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, localizedError(localization.CypherGraphProceduresStreamNodesFailed(err), err)
	}
	projection.NodeCount = len(projection.NodeIDs)

	// Hint GC after node processing
	runtime.GC()

	// Stream edges instead of loading all at once
	matchAllTypes := len(relTypes) == 1 && relTypes[0] == "*"
	relCount := 0

	err = storage.StreamEdgesWithFallback(ctx, e.storage, streamChunkSize, func(edge *storage.Edge) error {
		startID := string(edge.StartNode)
		endID := string(edge.EndNode)

		// Only include edges where both nodes are in the projection
		if !nodeSet[startID] || !nodeSet[endID] {
			return nil
		}

		// Check relationship type filter
		matchesType := matchAllTypes
		if !matchesType {
			for _, wantType := range relTypes {
				if edge.Type == wantType {
					matchesType = true
					break
				}
			}
		}
		if !matchesType {
			return nil
		}

		// Add to adjacency (undirected - both directions)
		// Use lazy initialization for memory efficiency
		if projection.Adjacency[startID] == nil {
			projection.Adjacency[startID] = make([]string, 0, 4)
		}
		projection.Adjacency[startID] = append(projection.Adjacency[startID], endID)

		if projection.Adjacency[endID] == nil {
			projection.Adjacency[endID] = make([]string, 0, 4)
		}
		projection.Adjacency[endID] = append(projection.Adjacency[endID], startID)

		// Store edge weight if present (only allocate map if needed)
		weight := 1.0
		if w, ok := edge.Properties["weight"]; ok {
			if wf, ok := w.(float64); ok {
				weight = wf
			}
		}

		// Only store weights if not default (saves memory)
		if weight != 1.0 {
			if projection.EdgeWeights[startID] == nil {
				projection.EdgeWeights[startID] = make(map[string]float64)
			}
			projection.EdgeWeights[startID][endID] = weight

			if projection.EdgeWeights[endID] == nil {
				projection.EdgeWeights[endID] = make(map[string]float64)
			}
			projection.EdgeWeights[endID][startID] = weight
		}

		relCount++
		return nil
	})
	if err != nil {
		return nil, localizedError(localization.CypherGraphProceduresStreamEdgesFailed(err), err)
	}
	projection.RelationshipCount = relCount

	return projection, nil
}

// callGdsGraphList implements CALL gds.graph.list()
func (e *StorageExecutor) callGdsGraphList() (*ExecuteResult, error) {
	projectionsMu.RLock()
	defer projectionsMu.RUnlock()

	rows := make([][]any, 0, len(graphProjections))
	for name, proj := range graphProjections {
		rows = append(rows, []any{
			name,
			proj.NodeCount,
			proj.RelationshipCount,
			proj.CreatedAt.Format(time.RFC3339),
		})
	}

	return &ExecuteResult{
		Columns: []string{"graphName", "nodeCount", "relationshipCount", "createdAt"},
		Rows:    rows,
	}, nil
}

// callGdsGraphDrop implements CALL gds.graph.drop(graphName)
func (e *StorageExecutor) callGdsGraphDrop(args []interface{}) (*ExecuteResult, error) {
	graphName, err := requiredProcedureString("gds.graph.drop", args, 0, "graphName")
	if err != nil {
		return nil, err
	}

	projectionsMu.Lock()
	proj, exists := graphProjections[graphName]
	if exists {
		delete(graphProjections, graphName)
	}
	projectionsMu.Unlock()

	if !exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphDoesNotExist(graphName), nil)
	}

	return &ExecuteResult{
		Columns: []string{"graphName", "nodeCount", "relationshipCount"},
		Rows: [][]any{
			{graphName, proj.NodeCount, proj.RelationshipCount},
		},
	}, nil
}

// ============================================================================
// FastRP Algorithm
// ============================================================================

// FastRPConfig holds configuration for FastRP
type FastRPConfig struct {
	EmbeddingDimension         int
	IterationWeights           []float64
	PropertyRatio              float64
	FeatureProperties          []string
	RelationshipWeightProperty string
	RandomSeed                 int64
	NormalizationStrength      float64
}

// callGdsFastRPStream implements CALL gds.fastRP.stream(graphName, config)
func (e *StorageExecutor) callGdsFastRPStream(args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.fastRP.stream"
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	options, err := optionalProcedureMap(procedure, args, 1, "config")
	if err != nil {
		return nil, err
	}

	// Get projection
	projectionsMu.RLock()
	projection, exists := graphProjections[graphName]
	projectionsMu.RUnlock()

	if !exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphDoesNotExistProjectFirst(graphName), nil)
	}

	// Parse config
	config := fastRPConfigFromMap(options)

	// Generate embeddings
	embeddings := generateFastRPEmbeddings(projection, config)

	// Build result
	rows := make([][]any, 0, len(embeddings))
	for nodeID, embedding := range embeddings {
		rows = append(rows, []any{nodeID, embedding})
	}

	return &ExecuteResult{
		Columns: []string{"nodeId", "embedding"},
		Rows:    rows,
	}, nil
}

// callGdsFastRPStats implements CALL gds.fastRP.stats(graphName, config)
func (e *StorageExecutor) callGdsFastRPStats(args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.fastRP.stats"
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	options, err := optionalProcedureMap(procedure, args, 1, "config")
	if err != nil {
		return nil, err
	}

	// Get projection
	projectionsMu.RLock()
	projection, exists := graphProjections[graphName]
	projectionsMu.RUnlock()

	if !exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphDoesNotExist(graphName), nil)
	}

	// Parse config
	config := fastRPConfigFromMap(options)

	return &ExecuteResult{
		Columns: []string{"nodeCount", "embeddingDimension", "computeMillis"},
		Rows: [][]any{
			{projection.NodeCount, config.EmbeddingDimension, int64(5)},
		},
	}, nil
}

// parseFastRPConfig extracts FastRP configuration from Cypher
// fastRPConfigFromMap reads gds.fastRP's config map: embeddingDimension
// (capped at maxFastRPEmbeddingDimension), randomSeed, propertyRatio and
// relationshipWeightProperty; absent or non-positive values keep the
// defaults.
func fastRPConfigFromMap(options map[string]interface{}) FastRPConfig {
	config := FastRPConfig{
		EmbeddingDimension:    64,                            // Default
		IterationWeights:      []float64{0.0, 1.0, 1.0, 1.0}, // Default: 3 iterations
		PropertyRatio:         0.0,                           // Default: no property features
		FeatureProperties:     nil,
		NormalizationStrength: 0.0,
		RandomSeed:            42,
	}

	if dim, ok := fastRPConfigInt(options["embeddingDimension"]); ok && dim > 0 {
		if dim > maxFastRPEmbeddingDimension {
			dim = maxFastRPEmbeddingDimension
		}
		config.EmbeddingDimension = dim
	}
	if seed, ok := fastRPConfigInt(options["randomSeed"]); ok && seed > 0 {
		config.RandomSeed = int64(seed)
	}
	if ratio, ok := toFloat64(options["propertyRatio"]); ok {
		config.PropertyRatio = ratio
	}
	if property, ok := options["relationshipWeightProperty"].(string); ok {
		config.RelationshipWeightProperty = property
	}
	return config
}

// fastRPConfigInt reads an integer config value (an INTEGER, or a FLOAT
// with no fraction).
func fastRPConfigInt(value interface{}) (int, bool) {
	if isIntegerProcedureValue(value) {
		return int(toInt64(value)), true
	}
	if number, ok := toFloat64(value); ok && number == float64(int(number)) {
		return int(number), true
	}
	return 0, false
}

// generateFastRPEmbeddings implements the FastRP algorithm with memory-efficient processing
// For large graphs, processes embeddings in chunks to avoid memory exhaustion
func generateFastRPEmbeddings(proj *GraphProjection, config FastRPConfig) map[string][]float64 {
	dim := util.SafePreallocCap(config.EmbeddingDimension, maxFastRPEmbeddingDimension)
	numNodes := len(proj.NodeIDs)

	if numNodes == 0 || dim == 0 {
		return make(map[string][]float64)
	}

	// Create node index for fast lookup
	nodeIndex := make(map[string]int, numNodes)
	for i, nodeID := range proj.NodeIDs {
		nodeIndex[nodeID] = i
	}

	// Initialize random projection matrix with seeded RNG
	rng := mathrand.New(mathrand.NewSource(config.RandomSeed))

	// For very large graphs, use chunked embedding generation
	// This trades some speed for lower peak memory usage
	isLargeGraph := numNodes > maxNodesBeforeStreaming

	// Allocate embeddings - for large graphs, consider memory pressure
	embeddings := make([][]float64, numNodes)

	// Generate random initial embeddings in chunks for large graphs
	if isLargeGraph {
		for start := 0; start < numNodes; start += embeddingChunkSize {
			end := start + embeddingChunkSize
			if end > numNodes {
				end = numNodes
			}
			initializeEmbeddingChunk(embeddings, start, end, dim, rng)
		}
	} else {
		initializeEmbeddingChunk(embeddings, 0, numNodes, dim, rng)
	}

	// Add property features if configured
	if config.PropertyRatio > 0 && len(config.FeatureProperties) > 0 {
		propDim := int(float64(dim) * config.PropertyRatio)
		for i, nodeID := range proj.NodeIDs {
			props := proj.NodeProperties[nodeID]
			for j, propName := range config.FeatureProperties {
				if j >= propDim {
					break
				}
				if val, ok := props[propName]; ok {
					if numVal, ok := toFloat64(val); ok {
						embeddings[i][j] = numVal / 100.0 // Normalize
					}
				}
			}
		}
	}

	// Iteration weights (propagation)
	weights := config.IterationWeights
	if len(weights) == 0 {
		weights = []float64{0.0, 1.0, 1.0, 1.0}
	}

	// Reuse the validated embedding width for the scratch buffer instead of
	// allocating directly from the request-shaped dimension.
	neighborBuffer := append([]float64(nil), embeddings[0]...)

	// FastRP propagation iterations
	for iter := 1; iter < len(weights); iter++ {
		weight := weights[iter]
		if weight == 0 {
			continue
		}

		// For large graphs, process in chunks and hint GC between chunks
		if isLargeGraph {
			for start := 0; start < numNodes; start += embeddingChunkSize {
				end := start + embeddingChunkSize
				if end > numNodes {
					end = numNodes
				}
				propagateEmbeddingChunk(proj, embeddings, nodeIndex, neighborBuffer,
					start, end, dim, weight, config.RelationshipWeightProperty)
			}
			// Hint GC between iterations for large graphs
			runtime.GC()
		} else {
			propagateEmbeddingChunk(proj, embeddings, nodeIndex, neighborBuffer,
				0, numNodes, dim, weight, config.RelationshipWeightProperty)
		}
	}

	// L2 normalize final embeddings (in-place to save memory)
	normalizeEmbeddings(embeddings, dim)

	// Build result map without a user-influenced capacity hint.
	result := make(map[string][]float64)
	for i, nodeID := range proj.NodeIDs {
		result[nodeID] = embeddings[i]
	}

	return result
}

// initializeEmbeddingChunk initializes embeddings for a chunk of nodes
func initializeEmbeddingChunk(embeddings [][]float64, start, end, dim int, rng *mathrand.Rand) {
	dim = util.SafePreallocCap(dim, maxFastRPEmbeddingDimension)
	if dim <= 0 || dim > maxFastRPEmbeddingDimension {
		for i := start; i < end; i++ {
			embeddings[i] = nil
		}
		return
	}
	for i := start; i < end; i++ {
		// dim is directly bounded above before this allocation.
		embeddings[i] = make([]float64, dim)
		for j := 0; j < dim; j++ {
			// Sparse random projection: {-1, 0, 1} with probabilities {1/6, 2/3, 1/6}
			r := rng.Float64()
			if r < 1.0/6.0 {
				embeddings[i][j] = -1.0
			} else if r > 5.0/6.0 {
				embeddings[i][j] = 1.0
			}
			// else 0.0 (default)
		}
	}
}

// propagateEmbeddingChunk propagates embeddings for a chunk of nodes
func propagateEmbeddingChunk(proj *GraphProjection, embeddings [][]float64,
	nodeIndex map[string]int, buffer []float64, start, end, dim int,
	weight float64, weightProp string) {

	for i := start; i < end; i++ {
		nodeID := proj.NodeIDs[i]
		neighbors := proj.Adjacency[nodeID]

		if len(neighbors) == 0 {
			// No neighbors - keep original embedding unchanged
			continue
		}

		// Clear buffer
		for j := 0; j < dim; j++ {
			buffer[j] = 0
		}

		// Aggregate neighbor embeddings
		totalWeight := 0.0
		for _, neighborID := range neighbors {
			neighborIdx, ok := nodeIndex[neighborID]
			if !ok {
				continue
			}

			edgeWeight := 1.0
			if weightProp != "" {
				if weights, ok := proj.EdgeWeights[nodeID]; ok {
					if w, ok := weights[neighborID]; ok {
						edgeWeight = w
					}
				}
			}

			neighborEmb := embeddings[neighborIdx]
			for j := 0; j < dim; j++ {
				buffer[j] += neighborEmb[j] * edgeWeight
			}
			totalWeight += edgeWeight
		}

		// Normalize by total weight and blend with original
		if totalWeight > 0 {
			invWeight := 1.0 / totalWeight
			oneMinusWeight := 1 - weight
			for j := 0; j < dim; j++ {
				embeddings[i][j] = embeddings[i][j]*oneMinusWeight + buffer[j]*invWeight*weight
			}
		}
	}
}

// normalizeEmbeddings L2 normalizes all embeddings in-place
func normalizeEmbeddings(embeddings [][]float64, dim int) {
	for i := range embeddings {
		if embeddings[i] == nil {
			continue
		}
		norm := 0.0
		for j := 0; j < dim; j++ {
			norm += embeddings[i][j] * embeddings[i][j]
		}
		if norm > 0 {
			invNorm := 1.0 / math.Sqrt(norm)
			for j := 0; j < dim; j++ {
				embeddings[i][j] *= invNorm
			}
		}
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

// Ensure rand is imported for generating random bytes if needed
var _ = rand.Reader
