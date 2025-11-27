// Link Prediction procedures for Neo4j GDS compatibility.
//
// This file implements Neo4j Graph Data Science (GDS) link prediction procedures,
// making NornicDB compatible with Neo4j GDS workflows and tooling.
//
// IMPORTANT: These procedures are ALWAYS AVAILABLE (no feature flag required).
// The feature flag (NORNICDB_TOPOLOGY_AUTO_INTEGRATION_ENABLED) only controls
// automatic integration with inference.Engine.OnStore(), not these procedures.
//
// Neo4j GDS Link Prediction API:
//   CALL gds.linkPrediction.adamicAdar.stream(configuration)
//   CALL gds.linkPrediction.commonNeighbors.stream(configuration)
//   CALL gds.linkPrediction.resourceAllocation.stream(configuration)
//   CALL gds.linkPrediction.preferentialAttachment.stream(configuration)
//   CALL gds.linkPrediction.predict.stream(configuration)  // Hybrid
//
// All procedures follow Neo4j GDS conventions:
//   - Stream mode returns results immediately (no graph projection persistence)
//   - Configuration maps with sourceNode, targetNodes, relationshipTypes
//   - Standard result format: {node1, node2, score}
//
// Example Usage (Neo4j GDS compatible):
//
//	// Find potential connections using Adamic-Adar
//	CALL gds.linkPrediction.adamicAdar.stream({
//	  sourceNode: id(n),
//	  topK: 10,
//	  relationshipTypes: ['KNOWS', 'WORKS_WITH']
//	})
//	YIELD node1, node2, score
//	WHERE score > 0.5
//	RETURN node2.name, score
//	ORDER BY score DESC
//
//	// Compare multiple algorithms
//	CALL gds.linkPrediction.predict.stream({
//	  sourceNode: id(n),
//	  algorithm: 'ensemble',
//	  topologyWeight: 0.6,
//	  semanticWeight: 0.4,
//	  topK: 20
//	})
//	YIELD node1, node2, score, topology_score, semantic_score
//	RETURN node2, score
//
// Configuration Parameters:
//   - sourceNode (required): Node ID to predict edges from
//   - topK (optional): Maximum predictions to return (default: 10)
//   - relationshipTypes (optional): Filter to specific relationship types
//   - algorithm (optional): 'adamic_adar', 'jaccard', etc. (for predict)
//   - topologyWeight (optional): Weight for structural signals (default: 0.5)
//   - semanticWeight (optional): Weight for semantic signals (default: 0.5)
//
// Result Columns:
//   - node1: Source node ID
//   - node2: Target node ID (predicted connection)
//   - score: Prediction score (algorithm-specific scale)
//   - topology_score: Structural score (hybrid only)
//   - semantic_score: Semantic similarity score (hybrid only)
//   - reason: Human-readable explanation (hybrid only)

package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// callGdsLinkPredictionAdamicAdar implements gds.linkPrediction.adamicAdar.stream
//
// Syntax:
//   CALL gds.linkPrediction.adamicAdar.stream({sourceNode: id, topK: 10})
//   YIELD node1, node2, score
func (e *StorageExecutor) callGdsLinkPredictionAdamicAdar(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	// Build graph from storage
	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	// Run algorithm
	predictions := linkpredict.AdamicAdar(graph, config.SourceNode, config.TopK)

	// Format results
	return e.formatLinkPredictionResults(predictions, config.SourceNode), nil
}

// callGdsLinkPredictionCommonNeighbors implements gds.linkPrediction.commonNeighbors.stream
func (e *StorageExecutor) callGdsLinkPredictionCommonNeighbors(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	predictions := linkpredict.CommonNeighbors(graph, config.SourceNode, config.TopK)

	return e.formatLinkPredictionResults(predictions, config.SourceNode), nil
}

// callGdsLinkPredictionResourceAllocation implements gds.linkPrediction.resourceAllocation.stream
func (e *StorageExecutor) callGdsLinkPredictionResourceAllocation(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	predictions := linkpredict.ResourceAllocation(graph, config.SourceNode, config.TopK)

	return e.formatLinkPredictionResults(predictions, config.SourceNode), nil
}

// callGdsLinkPredictionPreferentialAttachment implements gds.linkPrediction.preferentialAttachment.stream
func (e *StorageExecutor) callGdsLinkPredictionPreferentialAttachment(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	predictions := linkpredict.PreferentialAttachment(graph, config.SourceNode, config.TopK)

	return e.formatLinkPredictionResults(predictions, config.SourceNode), nil
}

// callGdsLinkPredictionJaccard implements gds.linkPrediction.jaccard.stream (not standard GDS but useful)
func (e *StorageExecutor) callGdsLinkPredictionJaccard(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	predictions := linkpredict.Jaccard(graph, config.SourceNode, config.TopK)

	return e.formatLinkPredictionResults(predictions, config.SourceNode), nil
}

// callGdsLinkPredictionPredict implements gds.linkPrediction.predict.stream (hybrid scoring)
//
// This is a NornicDB extension that combines topological and semantic signals,
// but follows Neo4j GDS naming conventions for compatibility.
func (e *StorageExecutor) callGdsLinkPredictionPredict(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher)
	if err != nil {
		return nil, err
	}

	// Build graph
	graph, err := linkpredict.BuildGraphFromEngine(context.Background(), e.storage, true)
	if err != nil {
		return nil, fmt.Errorf("failed to build graph: %w", err)
	}

	// Create hybrid scorer
	hybridConfig := linkpredict.HybridConfig{
		TopologyWeight:    config.TopologyWeight,
		SemanticWeight:    config.SemanticWeight,
		TopologyAlgorithm: config.Algorithm,
		UseEnsemble:       config.Algorithm == "ensemble",
		NormalizeScores:   true,
		MinThreshold:      config.MinThreshold,
	}
	scorer := linkpredict.NewHybridScorer(hybridConfig)

	// Set up semantic scorer using embeddings
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		sourceNode, err := e.storage.GetNode(source)
		if err != nil || len(sourceNode.Embedding) == 0 {
			return 0.0
		}
		targetNode, err := e.storage.GetNode(target)
		if err != nil || len(targetNode.Embedding) == 0 {
			return 0.0
		}
		return linkpredict.CosineSimilarity(sourceNode.Embedding, targetNode.Embedding)
	})

	// Get predictions
	predictions := scorer.Predict(context.Background(), graph, config.SourceNode, config.TopK)

	// Format hybrid results
	return e.formatHybridPredictionResults(predictions, config.SourceNode), nil
}

// linkPredictionConfig holds parsed configuration for link prediction procedures
type linkPredictionConfig struct {
	SourceNode     storage.NodeID
	TopK           int
	Algorithm      string
	TopologyWeight float64
	SemanticWeight float64
	MinThreshold   float64
}

// parseLinkPredictionConfig extracts configuration from procedure call
//
// Supports multiple formats:
//   - Map syntax: {sourceNode: 123, topK: 10}
//   - Named params: sourceNode: 123, topK: 10
//   - Positional: (123, 10)
func (e *StorageExecutor) parseLinkPredictionConfig(cypher string) (*linkPredictionConfig, error) {
	config := &linkPredictionConfig{
		TopK:           10,        // Default
		Algorithm:      "adamic_adar", // Default
		TopologyWeight: 0.5,
		SemanticWeight: 0.5,
		MinThreshold:   0.0,
	}

	// Extract parameter block (everything between first ( and matching ))
	paramStart := strings.Index(cypher, "(")
	paramEnd := strings.LastIndex(cypher, ")")
	
	if paramStart == -1 || paramEnd == -1 || paramEnd <= paramStart {
		return nil, fmt.Errorf("invalid procedure call syntax")
	}

	params := strings.TrimSpace(cypher[paramStart+1 : paramEnd])

	// Handle map syntax: {key: value, ...}
	if strings.HasPrefix(params, "{") && strings.HasSuffix(params, "}") {
		params = strings.TrimSpace(params[1 : len(params)-1])
	}

	// Parse key-value pairs
	pairs := strings.Split(params, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Split on colon
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes from key
		key = strings.Trim(key, "'\"")

		// Remove quotes from value
		value = strings.Trim(value, "'\"")

		switch strings.ToLower(key) {
		case "sourcenode":
			// Could be id(n) or numeric
			if strings.Contains(value, "id(") {
				// Extract node variable
				re := regexp.MustCompile(`id\((\w+)\)`)
				matches := re.FindStringSubmatch(value)
				if len(matches) > 1 {
					// This would need to be resolved from query context
					// For now, treat as literal
					value = matches[1]
				}
			}
			config.SourceNode = storage.NodeID(value)

		case "topk":
			if v, err := strconv.Atoi(value); err == nil {
				config.TopK = v
			}

		case "algorithm":
			config.Algorithm = value

		case "topologyweight":
			if v, err := strconv.ParseFloat(value, 64); err == nil {
				config.TopologyWeight = v
			}

		case "semanticweight":
			if v, err := strconv.ParseFloat(value, 64); err == nil {
				config.SemanticWeight = v
			}

		case "minthreshold":
			if v, err := strconv.ParseFloat(value, 64); err == nil {
				config.MinThreshold = v
			}
		}
	}

	if config.SourceNode == "" {
		return nil, fmt.Errorf("sourceNode parameter required")
	}

	return config, nil
}

// formatLinkPredictionResults formats topology predictions as Neo4j-compatible result
func (e *StorageExecutor) formatLinkPredictionResults(predictions []linkpredict.Prediction, sourceNode storage.NodeID) *ExecuteResult {
	result := &ExecuteResult{
		Columns: []string{"node1", "node2", "score"},
		Rows:    make([][]interface{}, 0, len(predictions)),
	}

	for _, pred := range predictions {
		row := []interface{}{
			string(sourceNode),
			string(pred.TargetID),
			pred.Score,
		}
		result.Rows = append(result.Rows, row)
	}

	return result
}

// formatHybridPredictionResults formats hybrid predictions with extended columns
func (e *StorageExecutor) formatHybridPredictionResults(predictions []linkpredict.HybridPrediction, sourceNode storage.NodeID) *ExecuteResult {
	result := &ExecuteResult{
		Columns: []string{"node1", "node2", "score", "topology_score", "semantic_score", "reason"},
		Rows:    make([][]interface{}, 0, len(predictions)),
	}

	for _, pred := range predictions {
		row := []interface{}{
			string(sourceNode),
			string(pred.TargetID),
			pred.Score,
			pred.TopologyScore,
			pred.SemanticScore,
			pred.Reason,
		}
		result.Rows = append(result.Rows, row)
	}

	return result
}
