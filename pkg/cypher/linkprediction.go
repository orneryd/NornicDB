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

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/localization"
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
func (e *StorageExecutor) callGdsLinkPredictionAdamicAdar(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.callGdsLinkPredictionTopology(ctx, args, "adamic_adar")
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
func (e *StorageExecutor) callGdsLinkPredictionCommonNeighbors(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.callGdsLinkPredictionTopology(ctx, args, "common_neighbors")
}

// callGdsLinkPredictionResourceAllocation implements gds.linkPrediction.resourceAllocation.stream
func (e *StorageExecutor) callGdsLinkPredictionResourceAllocation(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.callGdsLinkPredictionTopology(ctx, args, "resource_allocation")
}

// callGdsLinkPredictionPreferentialAttachment implements gds.linkPrediction.preferentialAttachment.stream
func (e *StorageExecutor) callGdsLinkPredictionPreferentialAttachment(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.callGdsLinkPredictionTopology(ctx, args, "preferential_attachment")
}

// callGdsLinkPredictionJaccard implements gds.linkPrediction.jaccard.stream (not standard GDS but useful)
func (e *StorageExecutor) callGdsLinkPredictionJaccard(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.callGdsLinkPredictionTopology(ctx, args, "jaccard")
}

// callGdsLinkPredictionPredict implements gds.linkPrediction.predict.stream (hybrid scoring)
//
// This is a NornicDB extension that combines topological and semantic signals,
// but follows Neo4j GDS naming conventions for compatibility.
func (e *StorageExecutor) callGdsLinkPredictionPredict(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	config, err := linkPredictionConfigFromArguments(args)
	if err != nil {
		return nil, err
	}

	graph, degree, projection, err := e.linkPredictionGraph(ctx, config)
	if err != nil {
		return nil, err
	}

	// Create hybrid scorer
	hybridConfig := linkpredict.HybridConfig{
		TopologyWeight:    config.TopologyWeight,
		SemanticWeight:    config.SemanticWeight,
		TopologyAlgorithm: config.Algorithm,
		UseEnsemble:       config.Algorithm == "ensemble",
		// Topology scores keep each algorithm's own [0, 1] scale, so the
		// same pair scores the same whatever the other candidates (#1186).
		NormalizeScores: false,
		MinThreshold:    config.MinThreshold,
	}
	scorer := linkpredict.NewHybridScorer(hybridConfig)
	scorer.SetDegreeFunction(degree)

	// The semantic score: the best cosine similarity between the nodes'
	// embeddings, every chunk, each named embedding with its namesake
	// (#1186). The source's embeddings are read once.
	var sourceEmbeddings map[string][][]float32
	if sourceNode, err := e.storage.GetNode(config.SourceNode); err == nil {
		sourceEmbeddings = nodeEmbeddingGroups(sourceNode)
	}
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		targetNode, err := e.storage.GetNode(target)
		if err != nil || source != config.SourceNode {
			return 0.0
		}
		return linkpredict.BestCosineSimilarity(sourceEmbeddings, nodeEmbeddingGroups(targetNode))
	})

	// Get predictions
	predictions := scorer.Predict(ctx, graph, config.SourceNode, config.candidateLimit())
	predictions = linkPredictionTargets(config, predictions, func(p linkpredict.HybridPrediction) storage.NodeID { return p.TargetID })

	// Format hybrid results
	result, err := e.formatHybridPredictionResults(ctx, predictions, config.SourceNode)
	if err != nil {
		return nil, err
	}
	result.gdsProjection = projection
	return result, nil
}

// callGdsLinkPredictionTopology runs a topology link prediction algorithm
// over the graph the config selects and keeps the config's target nodes. The
// scores are GDS's values (gds.alpha.linkprediction.*), not rescaled: a
// degree counts relationships, a parallel one again and a self-loop once
// (#1192).
func (e *StorageExecutor) callGdsLinkPredictionTopology(ctx context.Context, args []interface{}, algorithm string) (*ExecuteResult, error) {
	config, err := linkPredictionConfigFromArguments(args)
	if err != nil {
		return nil, err
	}
	graph, degree, projection, err := e.linkPredictionGraph(ctx, config)
	if err != nil {
		return nil, err
	}
	scores := linkpredict.TopologyScores(algorithm, graph, degree, config.SourceNode)
	predictions := linkpredict.RankedPredictions(scores, config.candidateLimit(), algorithm)
	predictions = linkPredictionTargets(config, predictions, func(p linkpredict.Prediction) storage.NodeID { return p.TargetID })
	result, err := e.formatLinkPredictionResults(ctx, predictions, config.SourceNode)
	if err != nil {
		return nil, err
	}
	result.gdsProjection = projection
	return result, nil
}

// linkPredictionGraph is the graph a link prediction reads (#1192): the
// named projection's nodes and relationships (a snapshot, as projected), or
// without a graph name every stored node and relationship; of the config's
// relationshipTypes (every one without); undirected. The degree function
// counts a node's relationships in that graph, a self-loop once. The
// sourceNode must be in the graph.
func (e *StorageExecutor) linkPredictionGraph(ctx context.Context, config *linkPredictionConfig) (linkpredict.Graph, linkpredict.DegreeFunc, *GraphProjection, error) {
	var graph linkpredict.Graph
	var degree linkpredict.DegreeFunc
	var read *GraphProjection
	if config.GraphName != "" {
		projection, err := e.graphProjection(config.GraphName)
		read = projection
		if err != nil {
			return nil, nil, nil, err
		}
		graph = make(linkpredict.Graph, len(projection.NodeIDs))
		relationships := make(map[storage.NodeID]map[string]struct{})
		touch := func(node storage.NodeID, relationship string) {
			if relationships[node] == nil {
				relationships[node] = make(map[string]struct{})
			}
			relationships[node][relationship] = struct{}{}
		}
		visit := func(relationship *ProjectedRelationship) {
			source, target := storage.NodeID(relationship.Source), storage.NodeID(relationship.Target)
			sourceNeighbors := graph[source]
			if sourceNeighbors == nil {
				sourceNeighbors = make(linkpredict.NodeSet)
				graph[source] = sourceNeighbors
			}
			targetNeighbors := graph[target]
			if targetNeighbors == nil {
				targetNeighbors = make(linkpredict.NodeSet)
				graph[target] = targetNeighbors
			}
			if source != target {
				sourceNeighbors[target] = struct{}{}
				targetNeighbors[source] = struct{}{}
			}
			// An UNDIRECTED relationship is held both ways, an aggregated
			// one once: each counts once by its id.
			touch(source, relationship.ID+"/"+relationship.Type)
			touch(target, relationship.ID+"/"+relationship.Type)
		}
		selected, err := selectGDSSubgraph(projection, nil, config.RelationshipTypes, "", gdsSubgraphOptions{
			omitAdjacency: true, visitRelationship: visit,
			emptyTypesNone: config.RelationshipTypes != nil,
		})
		if err != nil {
			return nil, nil, nil, err
		}
		for _, id := range selected.nodes {
			node := storage.NodeID(id)
			if graph[node] == nil {
				graph[node] = make(linkpredict.NodeSet)
			}
		}
		degree = func(node storage.NodeID) int { return len(relationships[node]) }
	} else {
		build := linkpredict.DefaultBuildConfig()
		build.RelationshipTypes = config.RelationshipTypes
		built, err := linkpredict.BuildGraphFromEngineOptimized(ctx, e.storage, build)
		if err != nil {
			return nil, nil, nil, localizedError(localization.CypherGraphProceduresBuildGraphFailed(err), err)
		}
		graph = built
		degrees := make(map[storage.NodeID]int)
		degree = func(node storage.NodeID) int {
			if count, ok := degrees[node]; ok {
				return count
			}
			seen := make(map[storage.EdgeID]struct{})
			for _, edge := range e.getNodeEdges(ctx, node) {
				if config.RelationshipTypes == nil || containsString(config.RelationshipTypes, edge.Type) {
					seen[edge.ID] = struct{}{}
				}
			}
			degrees[node] = len(seen)
			return len(seen)
		}
	}
	if _, ok := graph[config.SourceNode]; !ok {
		return nil, nil, nil, localizedError(localization.CypherGraphProceduresSourceNodeNotFound(string(config.SourceNode)), nil)
	}
	return graph, degree, read, nil
}

// candidateLimit is how many predictions the algorithm returns: topK, or
// every candidate (0) when targetNodes picks among them afterwards.
func (c *linkPredictionConfig) candidateLimit() int {
	if c.TargetNodes != nil {
		return 0
	}
	return c.TopK
}

// linkPredictionTargets keeps the predictions for the config's targetNodes,
// in order, at most topK of them; without targetNodes, all of them.
func linkPredictionTargets[P any](config *linkPredictionConfig, predictions []P, target func(P) storage.NodeID) []P {
	if config.TargetNodes == nil {
		return predictions
	}
	kept := predictions[:0]
	for _, prediction := range predictions {
		if config.TargetNodes[target(prediction)] {
			kept = append(kept, prediction)
		}
	}
	if config.TopK > 0 && len(kept) > config.TopK {
		kept = kept[:config.TopK]
	}
	return kept
}

// nodeEmbeddingGroups is a node's embeddings by group: each named embedding
// under its name, and its chunk embeddings under "default", with a named
// "default" embedding, as the node's default embedding reads them
// (storage.Node.GetDefaultEmbedding).
func nodeEmbeddingGroups(node *storage.Node) map[string][][]float32 {
	if node == nil {
		return nil
	}
	groups := make(map[string][][]float32, len(node.NamedEmbeddings)+1)
	for name, vector := range node.NamedEmbeddings {
		groups[name] = append(groups[name], vector)
	}
	if len(node.ChunkEmbeddings) > 0 {
		groups["default"] = append(groups["default"], node.ChunkEmbeddings...)
	}
	return groups
}

// linkPredictionConfig holds parsed configuration for link prediction procedures
type linkPredictionConfig struct {
	GraphName         string // "": the whole database
	SourceNode        storage.NodeID
	TopK              int
	Algorithm         string
	TopologyWeight    float64
	SemanticWeight    float64
	MinThreshold      float64
	RelationshipTypes []string                // nil: every type
	TargetNodes       map[storage.NodeID]bool // nil: every candidate
}

// linkPredictionConfigKeys are the configuration keys link prediction takes.
var linkPredictionConfigKeys = []string{
	"sourceNode", "targetNodes", "relationshipTypes", "topK", "algorithm", "topologyWeight", "semanticWeight", "minThreshold",
}

// linkPredictionConfigFromArguments reads a link prediction procedure's
// arguments: an optional graph name (#1192) and the configuration map,
// stream({sourceNode: id(n), topK: 5}) or stream('g', {...}), with
// sourceNode (a node, its id, or an INTEGER id; required), targetNodes (a
// list of them, or one), relationshipTypes (a list of type names, or one;
// '*' for every type), topK (at least 1), algorithm, topologyWeight,
// semanticWeight and minThreshold (#1185). Keys match whatever their case;
// another key, or a value of the wrong type, is refused as GDS refuses it.
func linkPredictionConfigFromArguments(args []interface{}) (*linkPredictionConfig, error) {
	config := &linkPredictionConfig{
		Algorithm:      "adamic_adar",
		TopologyWeight: 0.5,
		SemanticWeight: 0.5,
	}
	var options map[string]interface{}
	for index, argument := range args {
		switch value := argument.(type) {
		case string:
			if index == 0 {
				config.GraphName = value
			}
		case map[string]interface{}:
			if options == nil {
				options = value
			}
		}
	}
	c, err := newGDSConfig(options, linkPredictionConfigKeys...)
	if err != nil {
		return nil, err
	}
	if value := c.value("sourceNode"); value != nil {
		source, ok := gdsNodeID(value)
		if !ok {
			return nil, c.typeError("sourceNode", "Node")
		}
		config.SourceNode = source
	}
	if value := c.value("targetNodes"); value != nil {
		targets, isList := cypherListValue(value)
		if !isList {
			targets = []interface{}{value}
		}
		config.TargetNodes = make(map[storage.NodeID]bool, len(targets))
		for _, target := range targets {
			id, ok := gdsNodeID(target)
			if !ok {
				return nil, c.typeError("targetNodes", "List")
			}
			config.TargetNodes[id] = true
		}
	}
	if config.RelationshipTypes, err = c.names("relationshipTypes"); err != nil {
		return nil, err
	}
	topK, err := c.integer("topK", "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return nil, err
	}
	config.TopK = int(topK)
	if algorithm, ok, err := c.text("algorithm"); err != nil {
		return nil, err
	} else if ok {
		config.Algorithm = algorithm
	}
	if config.TopologyWeight, err = c.number("topologyWeight", "Double", config.TopologyWeight); err != nil {
		return nil, err
	}
	if config.SemanticWeight, err = c.number("semanticWeight", "Double", config.SemanticWeight); err != nil {
		return nil, err
	}
	if config.MinThreshold, err = c.number("minThreshold", "Double", 0); err != nil {
		return nil, err
	}
	if config.SourceNode == "" {
		return nil, localizedError(localization.CypherGraphProceduresSourceNodeRequired(), nil)
	}
	return config, nil
}

// formatLinkPredictionResults formats topology predictions as Neo4j-compatible result
func (e *StorageExecutor) formatLinkPredictionResults(ctx context.Context, predictions []linkpredict.Prediction, sourceNode storage.NodeID) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node1", "node2", "score"},
		Rows:    make([][]interface{}, 0, len(predictions)),
	}

	for batch, batchErr := range materializedBatches(ctx, len(predictions)) {
		if batchErr != nil {
			return nil, batchErr
		}
		for _, pred := range predictions[batch.start:batch.end] {
			row := []interface{}{
				string(sourceNode),
				string(pred.TargetID),
				pred.Score,
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// formatHybridPredictionResults formats hybrid predictions with extended columns
func (e *StorageExecutor) formatHybridPredictionResults(ctx context.Context, predictions []linkpredict.HybridPrediction, sourceNode storage.NodeID) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node1", "node2", "score", "topology_score", "semantic_score", "reason"},
		Rows:    make([][]interface{}, 0, len(predictions)),
	}

	for batch, batchErr := range materializedBatches(ctx, len(predictions)) {
		if batchErr != nil {
			return nil, batchErr
		}
		for _, pred := range predictions[batch.start:batch.end] {
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
	}

	return result, nil
}
