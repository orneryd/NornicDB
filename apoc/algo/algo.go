// Package algo provides APOC graph algorithm functions.
//
// This package implements all apoc.algo.* functions for graph
// algorithms and analysis in Cypher queries.
package algo

import (
	"container/heap"
	"math"
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

// PageRank calculates PageRank scores for nodes.
//
// Example:
//   apoc.algo.pageRank(nodes) => [{node: n, score: 0.15}, ...]
func PageRank(nodes []*Node, iterations int, dampingFactor float64) []map[string]interface{} {
	if len(nodes) == 0 {
		return []map[string]interface{}{}
	}
	
	// Initialize scores
	scores := make(map[int64]float64)
	for _, node := range nodes {
		scores[node.ID] = 1.0 / float64(len(nodes))
	}
	
	// Iterate
	for iter := 0; iter < iterations; iter++ {
		newScores := make(map[int64]float64)
		
		for _, node := range nodes {
			// Get incoming links (placeholder)
			incomingLinks := getIncomingLinks(node)
			
			sum := 0.0
			for _, link := range incomingLinks {
				outDegree := getOutDegree(link)
				if outDegree > 0 {
					sum += scores[link.ID] / float64(outDegree)
				}
			}
			
			newScores[node.ID] = (1-dampingFactor)/float64(len(nodes)) + dampingFactor*sum
		}
		
		scores = newScores
	}
	
	// Convert to result format
	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"node":  node,
			"score": scores[node.ID],
		}
	}
	
	return result
}

// BetweennessCentrality calculates betweenness centrality.
//
// Example:
//   apoc.algo.betweenness(nodes) => [{node: n, score: 0.5}, ...]
func BetweennessCentrality(nodes []*Node) []map[string]interface{} {
	betweenness := make(map[int64]float64)
	
	for _, node := range nodes {
		betweenness[node.ID] = 0.0
	}
	
	// For each node as source
	for _, source := range nodes {
		// BFS to find shortest paths
		stack := make([]*Node, 0)
		pred := make(map[int64][]*Node)
		sigma := make(map[int64]float64)
		dist := make(map[int64]int)
		
		for _, node := range nodes {
			pred[node.ID] = make([]*Node, 0)
			sigma[node.ID] = 0.0
			dist[node.ID] = -1
		}
		
		sigma[source.ID] = 1.0
		dist[source.ID] = 0
		
		queue := []*Node{source}
		
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			stack = append(stack, current)
			
			neighbors := getNeighbors(current)
			for _, neighbor := range neighbors {
				if dist[neighbor.ID] < 0 {
					queue = append(queue, neighbor)
					dist[neighbor.ID] = dist[current.ID] + 1
				}
				
				if dist[neighbor.ID] == dist[current.ID]+1 {
					sigma[neighbor.ID] += sigma[current.ID]
					pred[neighbor.ID] = append(pred[neighbor.ID], current)
				}
			}
		}
		
		// Accumulate betweenness
		delta := make(map[int64]float64)
		for _, node := range nodes {
			delta[node.ID] = 0.0
		}
		
		for i := len(stack) - 1; i >= 0; i-- {
			w := stack[i]
			for _, v := range pred[w.ID] {
				delta[v.ID] += (sigma[v.ID] / sigma[w.ID]) * (1.0 + delta[w.ID])
			}
			if w.ID != source.ID {
				betweenness[w.ID] += delta[w.ID]
			}
		}
	}
	
	// Normalize
	n := float64(len(nodes))
	normFactor := 1.0 / ((n - 1) * (n - 2))
	
	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"node":  node,
			"score": betweenness[node.ID] * normFactor,
		}
	}
	
	return result
}

// ClosenessCentrality calculates closeness centrality.
//
// Example:
//   apoc.algo.closeness(nodes) => [{node: n, score: 0.8}, ...]
func ClosenessCentrality(nodes []*Node) []map[string]interface{} {
	closeness := make(map[int64]float64)
	
	for _, source := range nodes {
		// BFS to find distances
		dist := make(map[int64]int)
		for _, node := range nodes {
			dist[node.ID] = -1
		}
		dist[source.ID] = 0
		
		queue := []*Node{source}
		
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			
			neighbors := getNeighbors(current)
			for _, neighbor := range neighbors {
				if dist[neighbor.ID] < 0 {
					dist[neighbor.ID] = dist[current.ID] + 1
					queue = append(queue, neighbor)
				}
			}
		}
		
		// Calculate closeness
		sum := 0.0
		reachable := 0
		for _, node := range nodes {
			if node.ID != source.ID && dist[node.ID] > 0 {
				sum += float64(dist[node.ID])
				reachable++
			}
		}
		
		if reachable > 0 {
			closeness[source.ID] = float64(reachable) / sum
		} else {
			closeness[source.ID] = 0.0
		}
	}
	
	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"node":  node,
			"score": closeness[node.ID],
		}
	}
	
	return result
}

// DegreeCentrality calculates degree centrality.
//
// Example:
//   apoc.algo.degree(nodes) => [{node: n, score: 5}, ...]
func DegreeCentrality(nodes []*Node) []map[string]interface{} {
	result := make([]map[string]interface{}, len(nodes))
	
	for i, node := range nodes {
		degree := getDegree(node)
		result[i] = map[string]interface{}{
			"node":  node,
			"score": degree,
		}
	}
	
	return result
}

// Community detects communities using label propagation.
//
// Example:
//   apoc.algo.community(nodes, iterations) 
//   => [{node: n, community: 1}, ...]
func Community(nodes []*Node, iterations int) []map[string]interface{} {
	// Initialize each node to its own community
	community := make(map[int64]int64)
	for _, node := range nodes {
		community[node.ID] = node.ID
	}
	
	// Label propagation
	for iter := 0; iter < iterations; iter++ {
		changed := false
		
		for _, node := range nodes {
			// Count neighbor communities
			neighborCommunities := make(map[int64]int)
			neighbors := getNeighbors(node)
			
			for _, neighbor := range neighbors {
				neighborCommunities[community[neighbor.ID]]++
			}
			
			// Find most common community
			maxCount := 0
			var maxCommunity int64
			for comm, count := range neighborCommunities {
				if count > maxCount {
					maxCount = count
					maxCommunity = comm
				}
			}
			
			if maxCount > 0 && community[node.ID] != maxCommunity {
				community[node.ID] = maxCommunity
				changed = true
			}
		}
		
		if !changed {
			break
		}
	}
	
	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"node":      node,
			"community": community[node.ID],
		}
	}
	
	return result
}

// AStar finds shortest path using A* algorithm.
//
// Example:
//   apoc.algo.aStar(start, end, 'ROAD', 'distance', 'latitude', 'longitude')
func AStar(start, end *Node, relType, weightProperty, latProperty, lonProperty string) []map[string]interface{} {
	// Priority queue for A*
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	
	// Track distances and paths
	gScore := make(map[int64]float64)
	fScore := make(map[int64]float64)
	cameFrom := make(map[int64]*Node)
	
	gScore[start.ID] = 0
	fScore[start.ID] = heuristic(start, end, latProperty, lonProperty)
	
	heap.Push(&pq, &Item{
		node:     start,
		priority: fScore[start.ID],
	})
	
	for pq.Len() > 0 {
		current := heap.Pop(&pq).(*Item).node
		
		if current.ID == end.ID {
			return reconstructAStarPath(cameFrom, current)
		}
		
		neighbors := getNeighborsWithRel(current, relType)
		for _, neighbor := range neighbors {
			weight := getWeight(current, neighbor, weightProperty)
			tentativeGScore := gScore[current.ID] + weight
			
			if prevG, ok := gScore[neighbor.ID]; !ok || tentativeGScore < prevG {
				cameFrom[neighbor.ID] = current
				gScore[neighbor.ID] = tentativeGScore
				fScore[neighbor.ID] = tentativeGScore + heuristic(neighbor, end, latProperty, lonProperty)
				
				heap.Push(&pq, &Item{
					node:     neighbor,
					priority: fScore[neighbor.ID],
				})
			}
		}
	}
	
	return []map[string]interface{}{} // No path found
}

// Dijkstra finds shortest path using Dijkstra's algorithm.
//
// Example:
//   apoc.algo.dijkstra(start, end, 'ROAD', 'distance')
func Dijkstra(start, end *Node, relType, weightProperty string) []map[string]interface{} {
	dist := make(map[int64]float64)
	prev := make(map[int64]*Node)
	visited := make(map[int64]bool)
	
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	
	dist[start.ID] = 0
	heap.Push(&pq, &Item{node: start, priority: 0})
	
	for pq.Len() > 0 {
		current := heap.Pop(&pq).(*Item).node
		
		if visited[current.ID] {
			continue
		}
		visited[current.ID] = true
		
		if current.ID == end.ID {
			return reconstructDijkstraPath(prev, start, end, dist)
		}
		
		neighbors := getNeighborsWithRel(current, relType)
		for _, neighbor := range neighbors {
			if visited[neighbor.ID] {
				continue
			}
			
			weight := getWeight(current, neighbor, weightProperty)
			alt := dist[current.ID] + weight
			
			if prevDist, ok := dist[neighbor.ID]; !ok || alt < prevDist {
				dist[neighbor.ID] = alt
				prev[neighbor.ID] = current
				heap.Push(&pq, &Item{node: neighbor, priority: alt})
			}
		}
	}
	
	return []map[string]interface{}{} // No path found
}

// AllPairs finds shortest paths between all pairs of nodes.
//
// Example:
//   apoc.algo.allPairs(nodes, 'distance')
func AllPairs(nodes []*Node, weightProperty string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	
	for _, source := range nodes {
		for _, target := range nodes {
			if source.ID != target.ID {
				path := Dijkstra(source, target, "", weightProperty)
				if len(path) > 0 {
					result = append(result, map[string]interface{}{
						"source": source,
						"target": target,
						"path":   path,
					})
				}
			}
		}
	}
	
	return result
}

// Cover finds a minimum vertex cover.
//
// Example:
//   apoc.algo.cover(nodes) => [node1, node3, node5]
func Cover(nodes []*Node) []*Node {
	// Greedy approximation algorithm
	cover := make([]*Node, 0)
	covered := make(map[int64]bool)
	edges := getAllEdges(nodes)
	
	for len(edges) > 0 {
		// Find node with highest degree
		maxDegree := 0
		var maxNode *Node
		
		for _, node := range nodes {
			if covered[node.ID] {
				continue
			}
			
			degree := countUncoveredEdges(node, edges, covered)
			if degree > maxDegree {
				maxDegree = degree
				maxNode = node
			}
		}
		
		if maxNode == nil {
			break
		}
		
		cover = append(cover, maxNode)
		covered[maxNode.ID] = true
		
		// Remove covered edges
		edges = removeEdgesIncident(edges, maxNode.ID)
	}
	
	return cover
}

// Helper types and functions

type Item struct {
	node     *Node
	priority float64
	index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func getIncomingLinks(node *Node) []*Node {
	// Placeholder - would query database
	return []*Node{}
}

func getOutDegree(node *Node) int {
	// Placeholder - would query database
	return 0
}

func getNeighbors(node *Node) []*Node {
	// Placeholder - would query database
	return []*Node{}
}

func getNeighborsWithRel(node *Node, relType string) []*Node {
	// Placeholder - would query database
	return []*Node{}
}

func getDegree(node *Node) int {
	// Placeholder - would query database
	return 0
}

func getWeight(from, to *Node, property string) float64 {
	// Placeholder - would query relationship weight
	return 1.0
}

func heuristic(from, to *Node, latProp, lonProp string) float64 {
	// Haversine distance for geographic coordinates
	lat1, ok1 := from.Properties[latProp].(float64)
	lon1, ok2 := from.Properties[lonProp].(float64)
	lat2, ok3 := to.Properties[latProp].(float64)
	lon2, ok4 := to.Properties[lonProp].(float64)
	
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0
	}
	
	// Haversine formula
	const R = 6371 // Earth radius in km
	
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	
	return R * c
}

func reconstructAStarPath(cameFrom map[int64]*Node, current *Node) []map[string]interface{} {
	path := make([]map[string]interface{}, 0)
	path = append(path, map[string]interface{}{"node": current})
	
	for {
		if prev, ok := cameFrom[current.ID]; ok {
			path = append([]map[string]interface{}{{"node": prev}}, path...)
			current = prev
		} else {
			break
		}
	}
	
	return path
}

func reconstructDijkstraPath(prev map[int64]*Node, start, end *Node, dist map[int64]float64) []map[string]interface{} {
	path := make([]map[string]interface{}, 0)
	current := end
	
	for current.ID != start.ID {
		path = append([]map[string]interface{}{{"node": current}}, path...)
		if p, ok := prev[current.ID]; ok {
			current = p
		} else {
			break
		}
	}
	
	path = append([]map[string]interface{}{{"node": start}}, path...)
	
	return path
}

func getAllEdges(nodes []*Node) []*Relationship {
	// Placeholder - would query database
	return []*Relationship{}
}

func countUncoveredEdges(node *Node, edges []*Relationship, covered map[int64]bool) int {
	count := 0
	for _, edge := range edges {
		if edge.StartNode == node.ID || edge.EndNode == node.ID {
			otherID := edge.EndNode
			if edge.StartNode != node.ID {
				otherID = edge.StartNode
			}
			if !covered[otherID] {
				count++
			}
		}
	}
	return count
}

func removeEdgesIncident(edges []*Relationship, nodeID int64) []*Relationship {
	result := make([]*Relationship, 0)
	for _, edge := range edges {
		if edge.StartNode != nodeID && edge.EndNode != nodeID {
			result = append(result, edge)
		}
	}
	return result
}
