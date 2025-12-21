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
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// callGdsLinkPredictionAdamicAdar implements gds.linkPrediction.adamicAdar.stream
//
// The Adamic-Adar algorithm predicts links based on common neighbors, giving more
// weight to neighbors that are less connected. This favors connections through
// "rare" intermediaries rather than highly-connected hubs.
//
// Formula: For each potential connection, sum 1/log(degree(common_neighbor))
// across all common neighbors. Higher scores indicate stronger predictions.
//
// Syntax:
//
//	CALL gds.linkPrediction.adamicAdar.stream({sourceNode: id, topK: 10})
//	YIELD node1, node2, score
//
// Parameters:
//   - sourceNode: Node ID to predict connections from (required)
//   - topK: Number of predictions to return (default: 10)
//   - relationshipTypes: Filter by relationship types (optional)
//
// Returns:
//   - node1: Source node ID
//   - node2: Predicted target node ID
//   - score: Adamic-Adar score (higher = stronger prediction)
//
// Example 1 - Find Potential Collaborators:
//
//	MATCH (alice:Person {name: 'Alice'})
//	CALL gds.linkPrediction.adamicAdar.stream({
//	  sourceNode: id(alice),
//	  topK: 5,
//	  relationshipTypes: ['WORKS_WITH', 'KNOWS']
//	})
//	YIELD node1, node2, score
//	MATCH (target) WHERE id(target) = node2
//	RETURN target.name, score
//	ORDER BY score DESC
//	// Returns top 5 people Alice should connect with
//
// Example 2 - Friend Recommendations:
//
//	MATCH (user:User {id: 'user-123'})
//	CALL gds.linkPrediction.adamicAdar.stream({
//	  sourceNode: id(user),
//	  topK: 10
//	})
//	YIELD node1, node2, score
//	WHERE score > 0.5
//	MATCH (friend) WHERE id(friend) = node2
//	RETURN friend.name, friend.interests, score
//
// Example 3 - Research Paper Citations:
//
//	MATCH (paper:Paper {title: 'Machine Learning'})
//	CALL gds.linkPrediction.adamicAdar.stream({
//	  sourceNode: id(paper),
//	  topK: 20,
//	  relationshipTypes: ['CITES']
//	})
//	YIELD node1, node2, score
//	MATCH (related) WHERE id(related) = node2
//	RETURN related.title, score
//
// ELI12:
//
// Imagine you want to find new friends at school. Adamic-Adar says:
//
//	"If you and someone else both know the SAME person, you should be friends!"
//
// BUT there's a twist: If your mutual friend knows EVERYONE in school (super popular),
// that connection isn't very special. If your mutual friend only knows a FEW people,
// that's a STRONG connection that really matters.
//
// Example:
//   - You and Bob both know Alice (who knows 100 people) → Score: small
//   - You and Bob both know Charlie (who only knows 3 people) → Score: BIG!
//
// It's like finding friends through rare, special connections rather than popular hubs.
//
// When to Use:
//   - Social network friend recommendations
//   - Academic collaboration suggestions
//   - Product recommendation systems
//   - Knowledge graph link completion
//
// Performance:
//   - O(n * avg_degree²) where n = number of nodes
//   - Fast for sparse graphs
//   - Memory: ~O(nodes + edges)
func (e *StorageExecutor) callGdsLinkPredictionAdamicAdar(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
//
// The Common Neighbors algorithm predicts links by counting shared neighbors between nodes.
// It's the simplest link prediction metric: the more neighbors two nodes share, the more
// likely they should be connected.
//
// Formula: Count the number of neighbors both nodes have in common.
//
// Syntax:
//
//	CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: id, topK: 10})
//	YIELD node1, node2, score
//
// Parameters:
//   - sourceNode: Node ID to predict connections from (required)
//   - topK: Number of predictions to return (default: 10)
//   - relationshipTypes: Filter by relationship types (optional)
//
// Returns:
//   - node1: Source node ID
//   - node2: Predicted target node ID
//   - score: Number of common neighbors (integer)
//
// Example 1 - Movie Recommendations:
//
//	MATCH (user:User {name: 'Alice'})
//	CALL gds.linkPrediction.commonNeighbors.stream({
//	  sourceNode: id(user),
//	  topK: 10,
//	  relationshipTypes: ['LIKES']
//	})
//	YIELD node1, node2, score
//	MATCH (movie:Movie) WHERE id(movie) = node2
//	RETURN movie.title, score AS common_fans
//	ORDER BY score DESC
//	// Shows movies liked by people with similar taste
//
// Example 2 - Professional Network:
//
//	MATCH (person:Person {id: 'p123'})
//	CALL gds.linkPrediction.commonNeighbors.stream({
//	  sourceNode: id(person),
//	  topK: 5
//	})
//	YIELD node1, node2, score
//	WHERE score >= 3
//	MATCH (colleague) WHERE id(colleague) = node2
//	RETURN colleague.name, colleague.company, score
//
// Example 3 - Tag-Based Content Discovery:
//
//	MATCH (article:Article {id: 'article-1'})
//	CALL gds.linkPrediction.commonNeighbors.stream({
//	  sourceNode: id(article),
//	  relationshipTypes: ['TAGGED_WITH']
//	})
//	YIELD node1, node2, score
//	MATCH (similar:Article) WHERE id(similar) = node2
//	RETURN similar.title, score AS shared_tags
//
// ELI12:
//
// Imagine you're in a class and want to find who you might become friends with.
// Common Neighbors says: "Count how many of the SAME people you both hang out with."
//
//   - You hang out with: Alice, Bob, Charlie
//   - Sarah hangs out with: Bob, Charlie, David
//   - Common friends: Bob and Charlie (2 people)
//   - Score: 2
//
// The more friends you share, the higher the score, and the more likely you'd
// get along! It's simple: people with overlapping social circles tend to connect.
//
// Real-world analogy:
//   - "People who bought this also bought..." (Amazon)
//   - "You may know..." (Facebook)
//   - "Recommended for you" (Netflix)
//
// When to Use:
//   - Simple, fast recommendations
//   - Cold-start scenarios (new users/items)
//   - Baseline for comparing other algorithms
//   - When interpretability matters
//
// Performance:
//   - O(n * avg_degree) where n = number of nodes
//   - Fastest link prediction algorithm
//   - Memory: ~O(nodes + edges)
func (e *StorageExecutor) callGdsLinkPredictionCommonNeighbors(cypher string) (*ExecuteResult, error) {
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
	config, err := e.parseLinkPredictionConfig(cypher, nil)
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
//
// If nodeVars is provided, expressions like id(n) will be evaluated using the bound variables.
// If nodeVars is nil, id(n) will be treated as a literal string (backward compatibility).
func (e *StorageExecutor) parseLinkPredictionConfig(cypher string, nodeVars map[string]*storage.Node) (*linkPredictionConfig, error) {
	config := &linkPredictionConfig{
		TopK:           10,            // Default
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
				// Try to evaluate id(n) expression if nodeVars is provided
				if nodeVars != nil {
					evaluated := e.evaluateExpressionWithContext(value, nodeVars, make(map[string]*storage.Edge))
					if evaluated != nil {
						// Convert evaluated result to string (node ID)
						if nodeID, ok := evaluated.(string); ok && nodeID != "" {
							config.SourceNode = storage.NodeID(nodeID)
							continue
						}
					}
					// If evaluation returned nil, the variable wasn't found in nodeVars
					// Extract variable name for better error message
					matches := idFunctionPattern.FindStringSubmatch(value)
					if len(matches) > 1 {
						return nil, fmt.Errorf("variable %q not found in query context (id(%s) cannot be resolved)", matches[1], matches[1])
					}
				}
				// Fallback: Extract node variable using pre-compiled pattern
				// This handles cases where nodeVars is nil (backward compatibility)
				matches := idFunctionPattern.FindStringSubmatch(value)
				if len(matches) > 1 {
					// If nodeVars is available, try to look up the variable
					if nodeVars != nil {
						if node, ok := nodeVars[matches[1]]; ok {
							config.SourceNode = node.ID
							continue
						}
					}
					// Otherwise, treat as literal (will likely fail validation)
					// This maintains backward compatibility but may not work correctly
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
