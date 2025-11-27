// Package linkpredict provides topological link prediction algorithms for NornicDB.
//
// This package implements canonical graph-based link prediction heuristics
// that complement NornicDB's existing semantic/behavioral inference engine.
//
// Algorithms Implemented:
//   - Common Neighbors: |N(u) ∩ N(v)|
//   - Jaccard Coefficient: |N(u) ∩ N(v)| / |N(u) ∪ N(v)|
//   - Adamic-Adar: Σ(1 / log(|N(z)|)) for z in common neighbors
//   - Preferential Attachment: |N(u)| * |N(v)|
//   - Resource Allocation: Σ(1 / |N(z)|) for z in common neighbors
//
// Usage Example:
//
//	// Build graph from storage
//	graph := linkpredict.BuildGraphFromEngine(ctx, storageEngine)
//
//	// Get predictions for a specific node
//	sourceID := storage.NodeID("user-123")
//	predictions := linkpredict.AdamicAdar(graph, sourceID, 10)
//
//	for _, pred := range predictions {
//		fmt.Printf("Suggest edge to %s (score: %.3f)\n", pred.TargetID, pred.Score)
//	}
//
//	// Hybrid scoring: blend topology + semantic
//	topologyScore := predictions[0].Score
//	semanticScore := getEmbeddingSimilarity(sourceID, predictions[0].TargetID)
//	hybridScore := 0.5*topologyScore + 0.5*semanticScore
//
// How Topological vs Semantic Differ:
//
// **Topological** (this package):
//   - Uses only graph structure (neighbors, paths, degrees)
//   - Captures social/organizational/citation patterns
//   - Example: Two people with many mutual friends should connect
//   - Fast (no embedding lookups), deterministic
//
// **Semantic** (existing inference engine):
//   - Uses embeddings, co-access, temporal signals
//   - Captures meaning and behavior
//   - Example: Two documents about similar topics should link
//   - Requires embeddings, captures semantic relatedness
//
// **When to Use Each:**
//   - Social networks → Topology dominates
//   - Knowledge/document graphs → Semantic dominates
//   - Citation networks → Both valuable
//   - AI agent memory → Both valuable (hybrid)
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you're at a new school and want to make friends:
//
// **Common Neighbors**: "You and Sarah both know Alex and Jamie.
// You should probably meet Sarah!"
//
// **Jaccard**: "You and Sarah share 2 friends, but you each know 10 people total.
// That's 2/(10+10-2) = 11% overlap - moderate connection."
//
// **Adamic-Adar**: "Your mutual friend Alex only knows 3 people (rare connection!),
// but Jamie knows 50 people. Alex is a stronger signal you should know Sarah."
//
// **Preferential Attachment**: "You know 20 people, Sarah knows 30.
// Popular people connect to popular people (20*30 = 600 'popularity score')."
//
// **Resource Allocation**: "Each mutual friend 'votes' for your connection,
// weighted by how exclusive that friend is."
package linkpredict

import (
	"context"
	"math"
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Graph represents a graph as an adjacency map for efficient link prediction.
//
// The graph is undirected by default (for most social/knowledge graphs),
// but can handle directed graphs by only populating adjacency in one direction.
//
// Example:
//
//	graph := Graph{
//		"alice": {"bob": {}, "charlie": {}},
//		"bob":   {"alice": {}, "diana": {}},
//	}
//
// This represents:
//   - alice -- bob
//   - alice -- charlie
//   - bob -- diana
type Graph map[storage.NodeID]NodeSet

// NodeSet represents a set of node IDs (adjacent nodes).
type NodeSet map[storage.NodeID]struct{}

// Prediction represents a predicted edge with confidence score.
//
// Scores are algorithm-specific and not directly comparable across algorithms:
//   - Common Neighbors: Integer count (0-N)
//   - Jaccard: Similarity ratio (0.0-1.0)
//   - Adamic-Adar: Weighted sum (0.0-∞)
//   - Preferential Attachment: Product of degrees (0-∞)
//   - Resource Allocation: Weighted sum (0.0-∞)
//
// To compare across algorithms, normalize scores to [0, 1] range.
type Prediction struct {
	TargetID  storage.NodeID
	Score     float64
	Algorithm string
	Reason    string
}

// BuildGraphFromEngine constructs a Graph from a storage engine.
//
// This creates an in-memory adjacency representation optimized for
// link prediction algorithms. For large graphs (>1M nodes), consider
// sampling or using candidate generation to limit memory usage.
//
// Parameters:
//   - ctx: Context for cancellation
//   - engine: Storage engine implementing GetOutgoingEdges
//   - undirected: If true, treats edges as bidirectional
//
// Example:
//
//	engine := storage.NewMemoryEngine()
//	// ... populate with nodes and edges ...
//	
//	// Build undirected graph (social network)
//	graph := linkpredict.BuildGraphFromEngine(ctx, engine, true)
//	
//	// Build directed graph (citation network)
//	graph = linkpredict.BuildGraphFromEngine(ctx, engine, false)
func BuildGraphFromEngine(ctx context.Context, engine storage.Engine, undirected bool) (Graph, error) {
	graph := make(Graph)

	// Get all nodes
	nodes, err := engine.AllNodes()
	if err != nil {
		return nil, err
	}

	// Initialize adjacency sets
	for _, node := range nodes {
		graph[node.ID] = make(NodeSet)
	}

	// Build adjacency lists
	for _, node := range nodes {
		edges, err := engine.GetOutgoingEdges(node.ID)
		if err != nil {
			continue
		}

		for _, edge := range edges {
			graph[edge.StartNode][edge.EndNode] = struct{}{}
			if undirected {
				graph[edge.EndNode][edge.StartNode] = struct{}{}
			}
		}
	}

	return graph, nil
}

// CommonNeighbors computes link predictions based on common neighbor count.
//
// Algorithm: score(u, v) = |N(u) ∩ N(v)|
//
// This is the simplest structural heuristic. Two nodes with many shared
// neighbors are likely to connect.
//
// Pros:
//   - Very fast (O(deg(u) * deg(v)))
//   - Intuitive and explainable
//   - Works well for social networks
//
// Cons:
//   - Biased toward high-degree nodes
//   - Doesn't account for neighbor importance
//
// Example:
//
//	predictions := linkpredict.CommonNeighbors(graph, "user-123", 10)
//	for _, p := range predictions {
//		fmt.Printf("→ %s: %d common neighbors\n", p.TargetID, int(p.Score))
//	}
func CommonNeighbors(graph Graph, source storage.NodeID, topK int) []Prediction {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}

	scores := make(map[storage.NodeID]float64)

	// For each neighbor of source
	for neighbor := range neighbors {
		// For each neighbor of that neighbor
		for candidate := range graph[neighbor] {
			if candidate == source {
				continue // Skip self
			}
			if _, isNeighbor := neighbors[candidate]; isNeighbor {
				continue // Skip existing edges
			}
			scores[candidate]++
		}
	}

	return topKPredictions(scores, topK, "common_neighbors")
}

// Jaccard computes link predictions using Jaccard coefficient.
//
// Algorithm: score(u, v) = |N(u) ∩ N(v)| / |N(u) ∪ N(v)|
//
// Normalizes common neighbors by total neighborhood size, reducing bias
// toward high-degree nodes.
//
// Score Range: [0.0, 1.0]
//   - 0.0: No common neighbors
//   - 1.0: Identical neighborhoods
//
// Pros:
//   - Normalized (comparable across node pairs)
//   - Less biased than raw common neighbors
//   - Standard similarity measure
//
// Cons:
//   - May underweight important connections
//   - Sensitive to degree imbalance
//
// Example:
//
//	predictions := linkpredict.Jaccard(graph, "doc-456", 20)
//	for _, p := range predictions {
//		fmt.Printf("→ %s: %.2f%% similarity\n", p.TargetID, p.Score*100)
//	}
func Jaccard(graph Graph, source storage.NodeID, topK int) []Prediction {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}

	scores := make(map[storage.NodeID]float64)

	// Generate candidates from 2-hop neighborhood
	candidates := make(map[storage.NodeID]struct{})
	for neighbor := range neighbors {
		for candidate := range graph[neighbor] {
			if candidate == source {
				continue
			}
			if _, isNeighbor := neighbors[candidate]; isNeighbor {
				continue
			}
			candidates[candidate] = struct{}{}
		}
	}

	// Compute Jaccard for each candidate
	for candidate := range candidates {
		candidateNeighbors := graph[candidate]
		
		// Count intersection
		intersection := 0
		for n := range neighbors {
			if _, exists := candidateNeighbors[n]; exists {
				intersection++
			}
		}

		if intersection == 0 {
			continue
		}

		// Union = |A| + |B| - |A ∩ B|
		union := len(neighbors) + len(candidateNeighbors) - intersection
		if union > 0 {
			scores[candidate] = float64(intersection) / float64(union)
		}
	}

	return topKPredictions(scores, topK, "jaccard")
}

// AdamicAdar computes link predictions using Adamic-Adar index.
//
// Algorithm: score(u, v) = Σ(1 / log(|N(z)|)) for z in N(u) ∩ N(v)
//
// Weights common neighbors by their rarity. A common neighbor with few
// connections is a stronger signal than a highly-connected one.
//
// Intuition: If you and I both know a person who knows everyone, that's
// weak evidence we should connect. But if we both know someone exclusive,
// that's strong evidence.
//
// Pros:
//   - Weights rare connections higher (less noise)
//   - Often outperforms simpler methods
//   - Well-studied in literature
//
// Cons:
//   - Requires degree calculation
//   - Undefined for degree-1 nodes (uses log)
//
// Example:
//
//	predictions := linkpredict.AdamicAdar(graph, "user-789", 15)
//	for _, p := range predictions {
//		fmt.Printf("→ %s: %.3f weighted score\n", p.TargetID, p.Score)
//	}
//
// Reference: Adamic & Adar (2003), "Friends and neighbors on the Web"
func AdamicAdar(graph Graph, source storage.NodeID, topK int) []Prediction {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}

	scores := make(map[storage.NodeID]float64)

	// Generate candidates from 2-hop neighborhood
	candidates := make(map[storage.NodeID]struct{})
	for neighbor := range neighbors {
		for candidate := range graph[neighbor] {
			if candidate == source {
				continue
			}
			if _, isNeighbor := neighbors[candidate]; isNeighbor {
				continue
			}
			candidates[candidate] = struct{}{}
		}
	}

	// Compute Adamic-Adar score for each candidate
	for candidate := range candidates {
		sum := 0.0
		candidateNeighbors := graph[candidate]

		// For each common neighbor
		for neighbor := range neighbors {
			if _, exists := candidateNeighbors[neighbor]; exists {
				degree := len(graph[neighbor])
				if degree > 1 {
					// Weight by 1/log(degree)
					sum += 1.0 / math.Log(float64(degree))
				}
			}
		}

		if sum > 0 {
			scores[candidate] = sum
		}
	}

	return topKPredictions(scores, topK, "adamic_adar")
}

// PreferentialAttachment computes link predictions based on node degree product.
//
// Algorithm: score(u, v) = |N(u)| * |N(v)|
//
// Models the "rich get richer" phenomenon: high-degree nodes tend to
// acquire more connections. Common in scale-free networks (social media,
// citation networks, the web).
//
// Pros:
//   - Extremely fast (O(1) per candidate)
//   - No neighbor intersection needed
//   - Captures growth dynamics
//
// Cons:
//   - Strongly biased toward hubs
//   - Ignores local structure
//   - Not similarity-based
//
// Example:
//
//	predictions := linkpredict.PreferentialAttachment(graph, "paper-123", 10)
//	for _, p := range predictions {
//		fmt.Printf("→ %s: %.0f degree product\n", p.TargetID, p.Score)
//	}
//
// Reference: Barabási & Albert (1999), "Emergence of scaling in random networks"
func PreferentialAttachment(graph Graph, source storage.NodeID, topK int) []Prediction {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}

	sourceDegree := float64(len(neighbors))
	scores := make(map[storage.NodeID]float64)

	// Score all non-neighbors by degree product
	for candidate, candidateNeighbors := range graph {
		if candidate == source {
			continue
		}
		if _, isNeighbor := neighbors[candidate]; isNeighbor {
			continue
		}

		candidateDegree := float64(len(candidateNeighbors))
		scores[candidate] = sourceDegree * candidateDegree
	}

	return topKPredictions(scores, topK, "preferential_attachment")
}

// ResourceAllocation computes link predictions using resource allocation index.
//
// Algorithm: score(u, v) = Σ(1 / |N(z)|) for z in N(u) ∩ N(v)
//
// Similar to Adamic-Adar but uses linear weighting instead of logarithmic.
// Models resource transmission through common neighbors.
//
// Intuition: Each common neighbor sends a "resource" unit to the target.
// If the neighbor has many connections, the resource is divided among them.
//
// Pros:
//   - Simpler than Adamic-Adar (no log)
//   - Often performs comparably
//   - Intuitive "spreading" interpretation
//
// Cons:
//   - Similar limitations to Adamic-Adar
//   - Undefined for degree-0 nodes
//
// Example:
//
//	predictions := linkpredict.ResourceAllocation(graph, "concept-456", 20)
//	for _, p := range predictions {
//		fmt.Printf("→ %s: %.3f resource score\n", p.TargetID, p.Score)
//	}
//
// Reference: Zhou et al. (2009), "Predicting missing links via local information"
func ResourceAllocation(graph Graph, source storage.NodeID, topK int) []Prediction {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}

	scores := make(map[storage.NodeID]float64)

	// Generate candidates from 2-hop neighborhood
	candidates := make(map[storage.NodeID]struct{})
	for neighbor := range neighbors {
		for candidate := range graph[neighbor] {
			if candidate == source {
				continue
			}
			if _, isNeighbor := neighbors[candidate]; isNeighbor {
				continue
			}
			candidates[candidate] = struct{}{}
		}
	}

	// Compute resource allocation score
	for candidate := range candidates {
		sum := 0.0
		candidateNeighbors := graph[candidate]

		// For each common neighbor
		for neighbor := range neighbors {
			if _, exists := candidateNeighbors[neighbor]; exists {
				degree := len(graph[neighbor])
				if degree > 0 {
					sum += 1.0 / float64(degree)
				}
			}
		}

		if sum > 0 {
			scores[candidate] = sum
		}
	}

	return topKPredictions(scores, topK, "resource_allocation")
}

// topKPredictions converts score map to sorted prediction list.
func topKPredictions(scores map[storage.NodeID]float64, k int, algorithm string) []Prediction {
	predictions := make([]Prediction, 0, len(scores))

	for nodeID, score := range scores {
		predictions = append(predictions, Prediction{
			TargetID:  nodeID,
			Score:     score,
			Algorithm: algorithm,
			Reason:    "Topological similarity",
		})
	}

	// Sort by score descending
	sort.Slice(predictions, func(i, j int) bool {
		return predictions[i].Score > predictions[j].Score
	})

	// Return top K
	if k > 0 && len(predictions) > k {
		predictions = predictions[:k]
	}

	return predictions
}

// Contains checks if a node exists in a set.
func (ns NodeSet) Contains(id storage.NodeID) bool {
	_, exists := ns[id]
	return exists
}

// Size returns the number of nodes in the set.
func (ns NodeSet) Size() int {
	return len(ns)
}

// Degree returns the degree (number of neighbors) for a node.
func (g Graph) Degree(node storage.NodeID) int {
	if neighbors, exists := g[node]; exists {
		return len(neighbors)
	}
	return 0
}

// Neighbors returns the neighbor set for a node.
func (g Graph) Neighbors(node storage.NodeID) NodeSet {
	if neighbors, exists := g[node]; exists {
		return neighbors
	}
	return make(NodeSet)
}
