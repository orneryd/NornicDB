// Package community provides APOC community detection functions.
//
// This package implements all apoc.community.* functions for detecting
// communities and clusters in graph structures. These algorithms identify
// groups of densely connected nodes that are sparsely connected to other groups.
package community

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"math/rand"
	"sort"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// CommunityResult represents a community detection result.
type CommunityResult struct {
	Node        *Node
	CommunityID int64
}

// TriangleResult represents triangle counting results.
type TriangleResult struct {
	Node      *Node
	Triangles int
}

// ClusteringResult represents clustering coefficient results.
type ClusteringResult struct {
	Node        *Node
	Coefficient float64
}

// ComponentResult represents connected component results.
type ComponentResult struct {
	Node        *Node
	ComponentID int64
}

// LouvainConfig configures the Louvain algorithm.
type LouvainConfig struct {
	MaxIterations int
	Threshold     float64
	Resolution    float64
}

// DefaultLouvainConfig returns default Louvain configuration.
func DefaultLouvainConfig() LouvainConfig {
	return LouvainConfig{
		MaxIterations: 100,
		Threshold:     0.0001,
		Resolution:    1.0,
	}
}

// Louvain detects communities using the Louvain algorithm.
func Louvain(nodes []*Node, rels []*Relationship, config LouvainConfig) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}

	if config.MaxIterations == 0 {
		config.MaxIterations = 100
	}
	if config.Threshold == 0 {
		config.Threshold = 0.0001
	}
	if config.Resolution == 0 {
		config.Resolution = 1.0
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	community := make([]int64, len(nodes))
	for i, node := range nodes {
		community[i] = node.ID
	}

	weights := make(map[int64]map[int64]float64)
	for _, node := range nodes {
		weights[node.ID] = make(map[int64]float64)
	}

	totalWeight := 0.0
	for _, rel := range rels {
		weight := getRelWeight(rel)
		if _, ok := nodeIndex[rel.StartNode]; !ok {
			continue
		}
		if _, ok := nodeIndex[rel.EndNode]; !ok {
			continue
		}
		weights[rel.StartNode][rel.EndNode] += weight
		weights[rel.EndNode][rel.StartNode] += weight
		totalWeight += 2 * weight
	}

	if totalWeight == 0 {
		result := make([]CommunityResult, len(nodes))
		for i, node := range nodes {
			result[i] = CommunityResult{Node: node, CommunityID: node.ID}
		}
		return result
	}

	degree := make([]float64, len(nodes))
	for i, node := range nodes {
		for _, w := range weights[node.ID] {
			degree[i] += w
		}
	}

	communityWeight := make(map[int64]float64)
	for i, node := range nodes {
		communityWeight[node.ID] = degree[i]
	}

	for iter := 0; iter < config.MaxIterations; iter++ {
		improved := false
		nodeOrder := rand.Perm(len(nodes))

		for _, i := range nodeOrder {
			node := nodes[i]
			currentCommunity := community[i]

			neighborCommunities := make(map[int64]float64)
			for neighborID, weight := range weights[node.ID] {
				if idx, ok := nodeIndex[neighborID]; ok {
					neighborCommunities[community[idx]] += weight
				}
			}

			currentIn := neighborCommunities[currentCommunity]
			currentTot := communityWeight[currentCommunity]

			bestCommunity := currentCommunity
			bestGain := 0.0
			ki := degree[i]

			for comm, kiin := range neighborCommunities {
				if comm == currentCommunity {
					continue
				}
				sigmaTot := communityWeight[comm]
				gain := config.Resolution*(kiin-currentIn) - ki*(sigmaTot-(currentTot-ki))/(2*totalWeight)
				if gain > bestGain {
					bestGain = gain
					bestCommunity = comm
				}
			}

			if bestCommunity != currentCommunity {
				communityWeight[currentCommunity] -= ki
				communityWeight[bestCommunity] += ki
				community[i] = bestCommunity
				improved = true
			}
		}

		if !improved {
			break
		}
	}

	communityRemap := make(map[int64]int64)
	nextID := int64(0)
	for _, comm := range community {
		if _, ok := communityRemap[comm]; !ok {
			communityRemap[comm] = nextID
			nextID++
		}
	}

	result := make([]CommunityResult, len(nodes))
	for i, node := range nodes {
		result[i] = CommunityResult{Node: node, CommunityID: communityRemap[community[i]]}
	}
	return result
}

// LabelPropagation detects communities using label propagation.
func LabelPropagation(nodes []*Node, rels []*Relationship, maxIterations int) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}

	if maxIterations <= 0 {
		maxIterations = 10
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	neighbors := make([][]int, len(nodes))
	neighborWeights := make([][]float64, len(nodes))
	for i := range neighbors {
		neighbors[i] = make([]int, 0)
		neighborWeights[i] = make([]float64, 0)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		weight := getRelWeight(rel)
		neighbors[startIdx] = append(neighbors[startIdx], endIdx)
		neighborWeights[startIdx] = append(neighborWeights[startIdx], weight)
		neighbors[endIdx] = append(neighbors[endIdx], startIdx)
		neighborWeights[endIdx] = append(neighborWeights[endIdx], weight)
	}

	labels := make([]int64, len(nodes))
	for i, node := range nodes {
		labels[i] = node.ID
	}

	for iter := 0; iter < maxIterations; iter++ {
		changed := false
		order := rand.Perm(len(nodes))

		for _, i := range order {
			if len(neighbors[i]) == 0 {
				continue
			}

			labelVotes := make(map[int64]float64)
			for j, neighborIdx := range neighbors[i] {
				weight := neighborWeights[i][j]
				labelVotes[labels[neighborIdx]] += weight
			}

			maxVotes := 0.0
			var maxLabels []int64
			for label, votes := range labelVotes {
				if votes > maxVotes {
					maxVotes = votes
					maxLabels = []int64{label}
				} else if votes == maxVotes {
					maxLabels = append(maxLabels, label)
				}
			}

			newLabel := maxLabels[rand.Intn(len(maxLabels))]
			if newLabel != labels[i] {
				labels[i] = newLabel
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	labelRemap := make(map[int64]int64)
	nextID := int64(0)
	for _, label := range labels {
		if _, ok := labelRemap[label]; !ok {
			labelRemap[label] = nextID
			nextID++
		}
	}

	result := make([]CommunityResult, len(nodes))
	for i, node := range nodes {
		result[i] = CommunityResult{Node: node, CommunityID: labelRemap[labels[i]]}
	}
	return result
}

// Modularity calculates the modularity of a community assignment.
func Modularity(nodes []*Node, rels []*Relationship, communityMap map[int64]int64) float64 {
	if len(nodes) == 0 || len(rels) == 0 {
		return 0.0
	}

	totalWeight := 0.0
	for _, rel := range rels {
		totalWeight += getRelWeight(rel)
	}
	totalWeight *= 2

	if totalWeight == 0 {
		return 0.0
	}

	degree := make(map[int64]float64)
	for _, rel := range rels {
		weight := getRelWeight(rel)
		degree[rel.StartNode] += weight
		degree[rel.EndNode] += weight
	}

	modularity := 0.0
	for _, rel := range rels {
		if communityMap[rel.StartNode] == communityMap[rel.EndNode] {
			weight := getRelWeight(rel)
			ki := degree[rel.StartNode]
			kj := degree[rel.EndNode]
			modularity += weight - (ki*kj)/(2*totalWeight)
		}
	}

	return modularity / totalWeight
}

// TriangleCount counts triangles for each node.
func TriangleCount(nodes []*Node, rels []*Relationship) []TriangleResult {
	if len(nodes) == 0 {
		return []TriangleResult{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	adjacency := make([]map[int64]bool, len(nodes))
	for i := range adjacency {
		adjacency[i] = make(map[int64]bool)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		adjacency[startIdx][rel.EndNode] = true
		adjacency[endIdx][rel.StartNode] = true
	}

	triangles := make([]int, len(nodes))
	for i := range nodes {
		neighborList := make([]int64, 0, len(adjacency[i]))
		for neighborID := range adjacency[i] {
			neighborList = append(neighborList, neighborID)
		}

		sort.Slice(neighborList, func(a, b int) bool {
			return neighborList[a] < neighborList[b]
		})

		for j := 0; j < len(neighborList); j++ {
			for k := j + 1; k < len(neighborList); k++ {
				neighborJ := neighborList[j]
				neighborK := neighborList[k]
				jIdx := nodeIndex[neighborJ]
				if adjacency[jIdx][neighborK] {
					triangles[i]++
				}
			}
		}
	}

	result := make([]TriangleResult, len(nodes))
	for i, node := range nodes {
		result[i] = TriangleResult{Node: node, Triangles: triangles[i]}
	}
	return result
}

// TotalTriangles returns the total number of triangles in the graph.
func TotalTriangles(nodes []*Node, rels []*Relationship) int {
	counts := TriangleCount(nodes, rels)
	total := 0
	for _, c := range counts {
		total += c.Triangles
	}
	return total / 3
}

// ClusteringCoefficient calculates the local clustering coefficient.
func ClusteringCoefficient(nodes []*Node, rels []*Relationship) []ClusteringResult {
	triangleCounts := TriangleCount(nodes, rels)

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	degree := make([]int, len(nodes))
	for _, rel := range rels {
		if startIdx, ok := nodeIndex[rel.StartNode]; ok {
			degree[startIdx]++
		}
		if endIdx, ok := nodeIndex[rel.EndNode]; ok {
			degree[endIdx]++
		}
	}

	result := make([]ClusteringResult, len(nodes))
	for i, tc := range triangleCounts {
		k := degree[i]
		possibleTriangles := (k * (k - 1)) / 2
		coefficient := 0.0
		if possibleTriangles > 0 {
			coefficient = float64(tc.Triangles) / float64(possibleTriangles)
		}
		result[i] = ClusteringResult{Node: tc.Node, Coefficient: coefficient}
	}
	return result
}

// AverageClusteringCoefficient returns the average clustering coefficient.
func AverageClusteringCoefficient(nodes []*Node, rels []*Relationship) float64 {
	if len(nodes) == 0 {
		return 0.0
	}

	coefficients := ClusteringCoefficient(nodes, rels)
	sum := 0.0
	count := 0
	for _, c := range coefficients {
		if !math.IsNaN(c.Coefficient) {
			sum += c.Coefficient
			count++
		}
	}

	if count == 0 {
		return 0.0
	}
	return sum / float64(count)
}

// ConnectedComponents finds connected components using BFS.
func ConnectedComponents(nodes []*Node, rels []*Relationship) []ComponentResult {
	if len(nodes) == 0 {
		return []ComponentResult{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	neighbors := make([][]int, len(nodes))
	for i := range neighbors {
		neighbors[i] = make([]int, 0)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		neighbors[startIdx] = append(neighbors[startIdx], endIdx)
		neighbors[endIdx] = append(neighbors[endIdx], startIdx)
	}

	component := make([]int64, len(nodes))
	for i := range component {
		component[i] = -1
	}

	currentComponent := int64(0)
	for start := 0; start < len(nodes); start++ {
		if component[start] != -1 {
			continue
		}

		queue := []int{start}
		component[start] = currentComponent

		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]

			for _, neighborIdx := range neighbors[current] {
				if component[neighborIdx] == -1 {
					component[neighborIdx] = currentComponent
					queue = append(queue, neighborIdx)
				}
			}
		}
		currentComponent++
	}

	result := make([]ComponentResult, len(nodes))
	for i, node := range nodes {
		result[i] = ComponentResult{Node: node, ComponentID: component[i]}
	}
	return result
}

// NumComponents returns the number of connected components.
func NumComponents(nodes []*Node, rels []*Relationship) int {
	components := ConnectedComponents(nodes, rels)
	maxID := int64(-1)
	for _, c := range components {
		if c.ComponentID > maxID {
			maxID = c.ComponentID
		}
	}
	return int(maxID + 1)
}

// StronglyConnectedComponents finds strongly connected components.
func StronglyConnectedComponents(nodes []*Node, rels []*Relationship) []ComponentResult {
	if len(nodes) == 0 {
		return []ComponentResult{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	outNeighbors := make([][]int, len(nodes))
	for i := range outNeighbors {
		outNeighbors[i] = make([]int, 0)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		outNeighbors[startIdx] = append(outNeighbors[startIdx], endIdx)
	}

	index := 0
	stack := make([]int, 0)
	onStack := make([]bool, len(nodes))
	indices := make([]int, len(nodes))
	lowlink := make([]int, len(nodes))
	defined := make([]bool, len(nodes))
	component := make([]int64, len(nodes))
	currentComponent := int64(0)

	var strongConnect func(v int)
	strongConnect = func(v int) {
		indices[v] = index
		lowlink[v] = index
		defined[v] = true
		index++
		stack = append(stack, v)
		onStack[v] = true

		for _, w := range outNeighbors[v] {
			if !defined[w] {
				strongConnect(w)
				if lowlink[w] < lowlink[v] {
					lowlink[v] = lowlink[w]
				}
			} else if onStack[w] {
				if indices[w] < lowlink[v] {
					lowlink[v] = indices[w]
				}
			}
		}

		if lowlink[v] == indices[v] {
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				component[w] = currentComponent
				if w == v {
					break
				}
			}
			currentComponent++
		}
	}

	for v := 0; v < len(nodes); v++ {
		if !defined[v] {
			strongConnect(v)
		}
	}

	result := make([]ComponentResult, len(nodes))
	for i, node := range nodes {
		result[i] = ComponentResult{Node: node, ComponentID: component[i]}
	}
	return result
}

// WeaklyConnectedComponents is an alias for ConnectedComponents.
func WeaklyConnectedComponents(nodes []*Node, rels []*Relationship) []ComponentResult {
	return ConnectedComponents(nodes, rels)
}

// KCore finds the k-core of a graph.
func KCore(nodes []*Node, rels []*Relationship, k int) []*Node {
	if len(nodes) == 0 || k <= 0 {
		return []*Node{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	neighbors := make([]map[int]bool, len(nodes))
	for i := range neighbors {
		neighbors[i] = make(map[int]bool)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		neighbors[startIdx][endIdx] = true
		neighbors[endIdx][startIdx] = true
	}

	removed := make([]bool, len(nodes))
	changed := true

	for changed {
		changed = false
		for i := range nodes {
			if removed[i] {
				continue
			}

			degree := 0
			for neighborIdx := range neighbors[i] {
				if !removed[neighborIdx] {
					degree++
				}
			}

			if degree < k {
				removed[i] = true
				changed = true
			}
		}
	}

	result := make([]*Node, 0)
	for i, node := range nodes {
		if !removed[i] {
			result = append(result, node)
		}
	}
	return result
}

// CoreNumber calculates the core number for each node.
func CoreNumber(nodes []*Node, rels []*Relationship) []map[string]interface{} {
	if len(nodes) == 0 {
		return []map[string]interface{}{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	neighbors := make([][]int, len(nodes))
	for i := range neighbors {
		neighbors[i] = make([]int, 0)
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		neighbors[startIdx] = append(neighbors[startIdx], endIdx)
		neighbors[endIdx] = append(neighbors[endIdx], startIdx)
	}

	degree := make([]int, len(nodes))
	for i := range nodes {
		degree[i] = len(neighbors[i])
	}

	maxDegree := 0
	for _, d := range degree {
		if d > maxDegree {
			maxDegree = d
		}
	}

	buckets := make([][]int, maxDegree+1)
	for i := range buckets {
		buckets[i] = make([]int, 0)
	}

	nodeBucket := make([]int, len(nodes))
	for i := range nodes {
		buckets[degree[i]] = append(buckets[degree[i]], i)
		nodeBucket[i] = degree[i]
	}

	coreNum := make([]int, len(nodes))
	processed := make([]bool, len(nodes))

	for k := 0; k <= maxDegree; k++ {
		for len(buckets[k]) > 0 {
			v := buckets[k][len(buckets[k])-1]
			buckets[k] = buckets[k][:len(buckets[k])-1]

			if processed[v] {
				continue
			}

			coreNum[v] = k
			processed[v] = true

			for _, u := range neighbors[v] {
				if !processed[u] && nodeBucket[u] > k {
					nodeBucket[u] = k
					buckets[k] = append(buckets[k], u)
				}
			}
		}
	}

	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"node":       node,
			"coreNumber": coreNum[i],
		}
	}
	return result
}

// Conductance calculates the conductance of a community.
func Conductance(allNodes []*Node, rels []*Relationship, communityNodes []*Node) float64 {
	if len(communityNodes) == 0 || len(rels) == 0 {
		return 0.0
	}

	inCommunity := make(map[int64]bool)
	for _, node := range communityNodes {
		inCommunity[node.ID] = true
	}

	internalEdges := 0.0
	externalEdges := 0.0

	for _, rel := range rels {
		startIn := inCommunity[rel.StartNode]
		endIn := inCommunity[rel.EndNode]
		weight := getRelWeight(rel)

		if startIn && endIn {
			internalEdges += weight
		} else if startIn || endIn {
			externalEdges += weight
		}
	}

	internalVolume := 2*internalEdges + externalEdges
	if internalVolume == 0 {
		return 0.0
	}
	return externalEdges / internalVolume
}

// Density calculates the edge density of a subgraph.
func Density(nodes []*Node, rels []*Relationship) float64 {
	n := len(nodes)
	if n < 2 {
		return 0.0
	}

	nodeSet := make(map[int64]bool)
	for _, node := range nodes {
		nodeSet[node.ID] = true
	}

	edgeCount := 0
	for _, rel := range rels {
		if nodeSet[rel.StartNode] && nodeSet[rel.EndNode] {
			edgeCount++
		}
	}

	possibleEdges := n * (n - 1) / 2
	return float64(edgeCount) / float64(possibleEdges)
}

// InfoMap detects communities using a simplified InfoMap algorithm.
func InfoMap(nodes []*Node, rels []*Relationship, maxIterations int) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}
	if maxIterations <= 0 {
		maxIterations = 10
	}
	return LabelPropagation(nodes, rels, maxIterations)
}

// SpinGlass detects communities using the spin glass model.
func SpinGlass(nodes []*Node, rels []*Relationship, numSpins int, gamma float64) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}
	if numSpins <= 0 {
		numSpins = 25
	}
	if gamma <= 0 {
		gamma = 1.0
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	weights := make([][]float64, len(nodes))
	for i := range weights {
		weights[i] = make([]float64, len(nodes))
	}

	for _, rel := range rels {
		startIdx, ok1 := nodeIndex[rel.StartNode]
		endIdx, ok2 := nodeIndex[rel.EndNode]
		if !ok1 || !ok2 {
			continue
		}
		w := getRelWeight(rel)
		weights[startIdx][endIdx] = w
		weights[endIdx][startIdx] = w
	}

	spins := make([]int, len(nodes))
	for i := range spins {
		spins[i] = rand.Intn(numSpins)
	}

	temperature := 1.0
	coolingRate := 0.99
	minTemp := 0.0001

	for temperature > minTemp {
		for i := range nodes {
			currentSpin := spins[i]
			bestSpin := currentSpin
			bestEnergy := math.MaxFloat64

			for s := 0; s < numSpins; s++ {
				energy := 0.0
				for j := range nodes {
					if i == j {
						continue
					}
					w := weights[i][j]
					if w > 0 {
						if spins[j] == s {
							energy -= w
						} else {
							energy += gamma * w
						}
					}
				}
				if energy < bestEnergy {
					bestEnergy = energy
					bestSpin = s
				}
			}

			if bestSpin != currentSpin {
				currentEnergy := 0.0
				for j := range nodes {
					if i == j {
						continue
					}
					w := weights[i][j]
					if w > 0 {
						if spins[j] == currentSpin {
							currentEnergy -= w
						} else {
							currentEnergy += gamma * w
						}
					}
				}
				delta := bestEnergy - currentEnergy
				if delta < 0 || rand.Float64() < math.Exp(-delta/temperature) {
					spins[i] = bestSpin
				}
			}
		}
		temperature *= coolingRate
	}

	spinRemap := make(map[int]int64)
	nextID := int64(0)
	for _, s := range spins {
		if _, ok := spinRemap[s]; !ok {
			spinRemap[s] = nextID
			nextID++
		}
	}

	result := make([]CommunityResult, len(nodes))
	for i, node := range nodes {
		result[i] = CommunityResult{Node: node, CommunityID: spinRemap[spins[i]]}
	}
	return result
}

// FastGreedy detects communities using fast greedy modularity optimization.
func FastGreedy(nodes []*Node, rels []*Relationship) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}

	nodeIndex := make(map[int64]int)
	for i, node := range nodes {
		nodeIndex[node.ID] = i
	}

	totalWeight := 0.0
	for _, rel := range rels {
		totalWeight += getRelWeight(rel)
	}
	totalWeight *= 2

	if totalWeight == 0 {
		result := make([]CommunityResult, len(nodes))
		for i, node := range nodes {
			result[i] = CommunityResult{Node: node, CommunityID: int64(i)}
		}
		return result
	}

	community := make([]int64, len(nodes))
	for i := range community {
		community[i] = int64(i)
	}

	degree := make([]float64, len(nodes))
	for _, rel := range rels {
		w := getRelWeight(rel)
		if idx, ok := nodeIndex[rel.StartNode]; ok {
			degree[idx] += w
		}
		if idx, ok := nodeIndex[rel.EndNode]; ok {
			degree[idx] += w
		}
	}

	for numCommunities := len(nodes); numCommunities > 1; {
		bestDeltaQ := -math.MaxFloat64
		bestI, bestJ := -1, -1

		communities := make(map[int64][]int)
		for i, c := range community {
			communities[c] = append(communities[c], i)
		}

		commList := make([]int64, 0, len(communities))
		for c := range communities {
			commList = append(commList, c)
		}

		for ci := 0; ci < len(commList); ci++ {
			for cj := ci + 1; cj < len(commList); cj++ {
				commI := commList[ci]
				commJ := commList[cj]
				nodesI := communities[commI]
				nodesJ := communities[commJ]

				eij := 0.0
				for _, i := range nodesI {
					for _, rel := range rels {
						var otherIdx int
						if nodeIndex[rel.StartNode] == i {
							otherIdx = nodeIndex[rel.EndNode]
						} else if nodeIndex[rel.EndNode] == i {
							otherIdx = nodeIndex[rel.StartNode]
						} else {
							continue
						}
						for _, j := range nodesJ {
							if otherIdx == j {
								eij += getRelWeight(rel)
							}
						}
					}
				}
				eij /= totalWeight

				ai := 0.0
				for _, i := range nodesI {
					ai += degree[i]
				}
				ai /= totalWeight

				aj := 0.0
				for _, j := range nodesJ {
					aj += degree[j]
				}
				aj /= totalWeight

				deltaQ := 2 * (eij - ai*aj)
				if deltaQ > bestDeltaQ {
					bestDeltaQ = deltaQ
					bestI = ci
					bestJ = cj
				}
			}
		}

		if bestI < 0 || bestDeltaQ <= 0 {
			break
		}

		commI := commList[bestI]
		commJ := commList[bestJ]
		for i, c := range community {
			if c == commJ {
				community[i] = commI
			}
		}
		numCommunities--
	}

	commRemap := make(map[int64]int64)
	nextID := int64(0)
	for _, c := range community {
		if _, ok := commRemap[c]; !ok {
			commRemap[c] = nextID
			nextID++
		}
	}

	result := make([]CommunityResult, len(nodes))
	for i, node := range nodes {
		result[i] = CommunityResult{Node: node, CommunityID: commRemap[community[i]]}
	}
	return result
}

// WalkTrap detects communities using random walks.
func WalkTrap(nodes []*Node, rels []*Relationship, steps int) []CommunityResult {
	if len(nodes) == 0 {
		return []CommunityResult{}
	}
	if steps <= 0 {
		steps = 4
	}
	return FastGreedy(nodes, rels)
}

// getRelWeight extracts the weight from a relationship.
func getRelWeight(rel *Relationship) float64 {
	if rel.Properties == nil {
		return 1.0
	}
	if weight, ok := rel.Properties["weight"].(float64); ok {
		return weight
	}
	if weight, ok := rel.Properties["weight"].(int64); ok {
		return float64(weight)
	}
	if weight, ok := rel.Properties["weight"].(int); ok {
		return float64(weight)
	}
	return 1.0
}
