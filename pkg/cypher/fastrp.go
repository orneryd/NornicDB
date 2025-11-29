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
	"fmt"
	"math"
	mathrand "math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Memory constants for streaming thresholds
const (
	// maxNodesBeforeStreaming triggers streaming mode for large graphs
	maxNodesBeforeStreaming = 10000
	// streamChunkSize is the batch size for streaming operations
	streamChunkSize = 1000
	// embeddingChunkSize is batch size for embedding generation
	embeddingChunkSize = 500
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

// callGdsGraphProject implements CALL gds.graph.project(...)
// Supports multiple syntax variants from Neo4j GDS
func (e *StorageExecutor) callGdsGraphProject(cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Extract graph name from the call
	graphName := extractStringArg(cypher, "gds.graph.project")
	if graphName == "" {
		// Try extracting from RETURN gds.graph.project(...) syntax
		graphName = extractGraphNameFromReturn(cypher)
	}
	if graphName == "" {
		return nil, fmt.Errorf("graph name required for gds.graph.project")
	}

	// Determine node labels and relationship types
	nodeLabels := []string{}
	relTypes := []string{}

	// Parse node label filter (simplified)
	if strings.Contains(upper, ":PERSON") {
		nodeLabels = append(nodeLabels, "Person")
	}
	if strings.Contains(upper, ":USER") {
		nodeLabels = append(nodeLabels, "User")
	}
	if strings.Contains(upper, ":MEMORY") {
		nodeLabels = append(nodeLabels, "Memory")
	}

	// Parse relationship types
	if strings.Contains(upper, ":KNOWS") {
		relTypes = append(relTypes, "KNOWS")
	}
	if strings.Contains(upper, ":RELATES_TO") {
		relTypes = append(relTypes, "RELATES_TO")
	}
	if strings.Contains(upper, ":REFERENCES") {
		relTypes = append(relTypes, "REFERENCES")
	}

	// Default: project all labels and types
	if len(nodeLabels) == 0 {
		nodeLabels = []string{"*"}
	}
	if len(relTypes) == 0 {
		relTypes = []string{"*"}
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
					if strings.EqualFold(label, wantLabel) {
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
		return nil, fmt.Errorf("failed to stream nodes: %w", err)
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
				if strings.EqualFold(edge.Type, wantType) {
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
		return nil, fmt.Errorf("failed to stream edges: %w", err)
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
func (e *StorageExecutor) callGdsGraphDrop(cypher string) (*ExecuteResult, error) {
	graphName := extractStringArg(cypher, "gds.graph.drop")
	if graphName == "" {
		return nil, fmt.Errorf("graph name required for gds.graph.drop")
	}

	projectionsMu.Lock()
	proj, exists := graphProjections[graphName]
	if exists {
		delete(graphProjections, graphName)
	}
	projectionsMu.Unlock()

	if !exists {
		return nil, fmt.Errorf("graph '%s' does not exist", graphName)
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
func (e *StorageExecutor) callGdsFastRPStream(cypher string) (*ExecuteResult, error) {
	// Parse graph name
	graphName := extractStringArg(cypher, "gds.fastrp.stream")
	if graphName == "" {
		return nil, fmt.Errorf("graph name required for gds.fastRP.stream")
	}

	// Get projection
	projectionsMu.RLock()
	projection, exists := graphProjections[graphName]
	projectionsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("graph '%s' does not exist. Create it with gds.graph.project first", graphName)
	}

	// Parse config
	config := parseFastRPConfig(cypher)

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
func (e *StorageExecutor) callGdsFastRPStats(cypher string) (*ExecuteResult, error) {
	// Parse graph name
	graphName := extractStringArg(cypher, "gds.fastrp.stats")
	if graphName == "" {
		return nil, fmt.Errorf("graph name required for gds.fastRP.stats")
	}

	// Get projection
	projectionsMu.RLock()
	projection, exists := graphProjections[graphName]
	projectionsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("graph '%s' does not exist", graphName)
	}

	// Parse config
	config := parseFastRPConfig(cypher)

	return &ExecuteResult{
		Columns: []string{"nodeCount", "embeddingDimension", "computeMillis"},
		Rows: [][]any{
			{projection.NodeCount, config.EmbeddingDimension, int64(5)},
		},
	}, nil
}

// parseFastRPConfig extracts FastRP configuration from Cypher
func parseFastRPConfig(cypher string) FastRPConfig {
	config := FastRPConfig{
		EmbeddingDimension:    64,                            // Default
		IterationWeights:      []float64{0.0, 1.0, 1.0, 1.0}, // Default: 3 iterations
		PropertyRatio:         0.0,                           // Default: no property features
		FeatureProperties:     nil,
		NormalizationStrength: 0.0,
		RandomSeed:            42,
	}

	// Parse embeddingDimension
	if dim := extractIntArg(cypher, "embeddingDimension"); dim > 0 {
		config.EmbeddingDimension = dim
	}

	// Parse randomSeed
	if seed := extractIntArg(cypher, "randomSeed"); seed > 0 {
		config.RandomSeed = int64(seed)
	}

	// Parse propertyRatio
	if strings.Contains(cypher, "propertyRatio") {
		config.PropertyRatio = extractFloatArg(cypher, "propertyRatio")
	}

	// Parse relationshipWeightProperty
	if strings.Contains(cypher, "relationshipWeightProperty") {
		config.RelationshipWeightProperty = extractStringConfigArg(cypher, "relationshipWeightProperty")
	}

	return config
}

// generateFastRPEmbeddings implements the FastRP algorithm with memory-efficient processing
// For large graphs, processes embeddings in chunks to avoid memory exhaustion
func generateFastRPEmbeddings(proj *GraphProjection, config FastRPConfig) map[string][]float64 {
	dim := config.EmbeddingDimension
	numNodes := len(proj.NodeIDs)

	if numNodes == 0 {
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

	// Pre-allocate a single buffer for neighbor aggregation to reduce allocations
	neighborBuffer := make([]float64, dim)

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

	// Build result map
	result := make(map[string][]float64, numNodes)
	for i, nodeID := range proj.NodeIDs {
		result[nodeID] = embeddings[i]
	}

	return result
}

// initializeEmbeddingChunk initializes embeddings for a chunk of nodes
func initializeEmbeddingChunk(embeddings [][]float64, start, end, dim int, rng *mathrand.Rand) {
	for i := start; i < end; i++ {
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

// extractStringArg extracts the first string argument from a procedure call
func extractStringArg(cypher string, procName string) string {
	lower := strings.ToLower(cypher)
	idx := strings.Index(lower, strings.ToLower(procName))
	if idx == -1 {
		return ""
	}

	// Find opening paren
	parenStart := strings.Index(cypher[idx:], "(")
	if parenStart == -1 {
		return ""
	}
	start := idx + parenStart + 1

	// Find first string argument (in quotes)
	quoteStart := strings.Index(cypher[start:], "'")
	if quoteStart == -1 {
		return ""
	}
	quoteStart += start + 1

	quoteEnd := strings.Index(cypher[quoteStart:], "'")
	if quoteEnd == -1 {
		return ""
	}

	return cypher[quoteStart : quoteStart+quoteEnd]
}

// extractGraphNameFromReturn extracts graph name from RETURN gds.graph.project(...) syntax
func extractGraphNameFromReturn(cypher string) string {
	lower := strings.ToLower(cypher)
	idx := strings.Index(lower, "gds.graph.project")
	if idx == -1 {
		return ""
	}

	// Find the opening paren
	parenStart := strings.Index(cypher[idx:], "(")
	if parenStart == -1 {
		return ""
	}

	// Look for first string in quotes
	start := idx + parenStart
	for i := start; i < len(cypher)-1; i++ {
		if cypher[i] == '\'' {
			end := strings.Index(cypher[i+1:], "'")
			if end != -1 {
				return cypher[i+1 : i+1+end]
			}
		}
	}

	return ""
}

// extractIntArg extracts an integer config value
func extractIntArg(cypher string, key string) int {
	lower := strings.ToLower(cypher)
	keyLower := strings.ToLower(key)
	idx := strings.Index(lower, keyLower)
	if idx == -1 {
		return 0
	}

	// Find the colon after the key
	colonIdx := strings.Index(cypher[idx:], ":")
	if colonIdx == -1 {
		return 0
	}
	start := idx + colonIdx + 1

	// Skip whitespace
	for start < len(cypher) && (cypher[start] == ' ' || cypher[start] == '\t') {
		start++
	}

	// Extract number
	end := start
	for end < len(cypher) && (cypher[end] >= '0' && cypher[end] <= '9') {
		end++
	}

	if end > start {
		val, _ := strconv.Atoi(cypher[start:end])
		return val
	}
	return 0
}

// extractFloatArg extracts a float config value
func extractFloatArg(cypher string, key string) float64 {
	lower := strings.ToLower(cypher)
	keyLower := strings.ToLower(key)
	idx := strings.Index(lower, keyLower)
	if idx == -1 {
		return 0
	}

	colonIdx := strings.Index(cypher[idx:], ":")
	if colonIdx == -1 {
		return 0
	}
	start := idx + colonIdx + 1

	for start < len(cypher) && (cypher[start] == ' ' || cypher[start] == '\t') {
		start++
	}

	end := start
	for end < len(cypher) && ((cypher[end] >= '0' && cypher[end] <= '9') || cypher[end] == '.') {
		end++
	}

	if end > start {
		val, _ := strconv.ParseFloat(cypher[start:end], 64)
		return val
	}
	return 0
}

// extractStringConfigArg extracts a string config value
func extractStringConfigArg(cypher string, key string) string {
	lower := strings.ToLower(cypher)
	keyLower := strings.ToLower(key)
	idx := strings.Index(lower, keyLower)
	if idx == -1 {
		return ""
	}

	// Find quote after key
	quoteStart := strings.Index(cypher[idx:], "'")
	if quoteStart == -1 {
		return ""
	}
	start := idx + quoteStart + 1

	quoteEnd := strings.Index(cypher[start:], "'")
	if quoteEnd == -1 {
		return ""
	}

	return cypher[start : start+quoteEnd]
}

// Ensure rand is imported for generating random bytes if needed
var _ = rand.Reader
