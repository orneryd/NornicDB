// Package search provides HNSW vector indexing for fast approximate nearest neighbor search.
package search

import (
	"container/heap"
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// HNSWConfig contains configuration parameters for the HNSW index.
type HNSWConfig struct {
	M               int     // Max connections per node per layer (default: 16)
	EfConstruction  int     // Candidate list size during construction (default: 200)
	EfSearch        int     // Candidate list size during search (default: 100)
	LevelMultiplier float64 // Level multiplier = 1/ln(M)
}

// DefaultHNSWConfig returns sensible defaults for HNSW index.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{
		M:               16,
		EfConstruction:  200,
		EfSearch:        100,
		LevelMultiplier: 1.0 / math.Log(16.0),
	}
}

// hnswNode represents a node in the HNSW graph.
type hnswNode struct {
	id        string
	vector    []float32
	level     int
	neighbors [][]string
	mu        sync.RWMutex
}

// HNSWIndex provides fast approximate nearest neighbor search using HNSW algorithm.
type HNSWIndex struct {
	config     HNSWConfig
	dimensions int
	mu         sync.RWMutex
	nodes      map[string]*hnswNode
	entryPoint string
	maxLevel   int
}

// NewHNSWIndex creates a new HNSW index with the given dimensions and config.
func NewHNSWIndex(dimensions int, config HNSWConfig) *HNSWIndex {
	if config.M == 0 {
		config = DefaultHNSWConfig()
	}
	return &HNSWIndex{
		config:     config,
		dimensions: dimensions,
		nodes:      make(map[string]*hnswNode),
		maxLevel:   0,
	}
}

// Add inserts a vector into the index.
func (h *HNSWIndex) Add(id string, vec []float32) error {
	if len(vec) != h.dimensions {
		return ErrDimensionMismatch
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	normalized := vector.Normalize(vec)
	level := h.randomLevel()

	node := &hnswNode{
		id:        id,
		vector:    normalized,
		level:     level,
		neighbors: make([][]string, level+1),
	}
	for i := range node.neighbors {
		node.neighbors[i] = make([]string, 0, h.config.M)
	}

	h.nodes[id] = node

	if h.entryPoint == "" {
		h.entryPoint = id
		h.maxLevel = level
		return nil
	}

	ep := h.entryPoint
	epLevel := h.nodes[ep].level

	for l := epLevel; l > level; l-- {
		ep = h.searchLayerSingle(normalized, ep, l)
	}

	for l := min(level, epLevel); l >= 0; l-- {
		candidates := h.searchLayer(normalized, ep, h.config.EfConstruction, l)
		neighbors := h.selectNeighbors(normalized, candidates, h.config.M)
		node.neighbors[l] = neighbors

		for _, neighborID := range neighbors {
			neighbor := h.nodes[neighborID]
			neighbor.mu.Lock()
			if len(neighbor.neighbors) > l {
				if len(neighbor.neighbors[l]) < h.config.M {
					neighbor.neighbors[l] = append(neighbor.neighbors[l], id)
				} else {
					allNeighbors := append(neighbor.neighbors[l], id)
					neighbor.neighbors[l] = h.selectNeighbors(neighbor.vector, allNeighbors, h.config.M)
				}
			}
			neighbor.mu.Unlock()
		}

		if len(candidates) > 0 {
			ep = candidates[0]
		}
	}

	if level > h.maxLevel {
		h.entryPoint = id
		h.maxLevel = level
	}

	return nil
}

// Remove removes a vector from the index by ID.
func (h *HNSWIndex) Remove(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, exists := h.nodes[id]
	if !exists {
		return
	}

	for l := 0; l <= node.level; l++ {
		for _, neighborID := range node.neighbors[l] {
			if neighbor, ok := h.nodes[neighborID]; ok {
				neighbor.mu.Lock()
				if len(neighbor.neighbors) > l {
					newNeighbors := make([]string, 0, len(neighbor.neighbors[l]))
					for _, nid := range neighbor.neighbors[l] {
						if nid != id {
							newNeighbors = append(newNeighbors, nid)
						}
					}
					neighbor.neighbors[l] = newNeighbors
				}
				neighbor.mu.Unlock()
			}
		}
	}

	delete(h.nodes, id)

	if h.entryPoint == id {
		h.entryPoint = ""
		h.maxLevel = -1
		for nid, n := range h.nodes {
			if n.level > h.maxLevel {
				h.maxLevel = n.level
				h.entryPoint = nid
			}
		}
		// Reset maxLevel to 0 if no nodes found
		if h.maxLevel == -1 {
			h.maxLevel = 0
		}
	}
}

// Search finds the k nearest neighbors to the query vector.
func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, minSimilarity float64) ([]SearchResult, error) {
	if len(query) != h.dimensions {
		return nil, ErrDimensionMismatch
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.nodes) == 0 {
		return []SearchResult{}, nil
	}

	normalized := vector.Normalize(query)
	ep := h.entryPoint

	for l := h.maxLevel; l > 0; l-- {
		ep = h.searchLayerSingle(normalized, ep, l)
	}

	candidates := h.searchLayer(normalized, ep, h.config.EfSearch, 0)

	results := make([]SearchResult, 0, k)
	for _, candidateID := range candidates {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}

		node := h.nodes[candidateID]
		similarity := vector.DotProduct(normalized, node.vector)

		if similarity >= minSimilarity {
			results = append(results, SearchResult{
				ID:    candidateID,
				Score: similarity,
			})
		}

		if len(results) >= k {
			break
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// Size returns the number of vectors in the index.
func (h *HNSWIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

func (h *HNSWIndex) searchLayerSingle(query []float32, entryID string, level int) string {
	current := entryID
	currentDist := 1.0 - vector.DotProduct(query, h.nodes[current].vector)

	for {
		changed := false
		node := h.nodes[current]
		node.mu.RLock()
		neighbors := node.neighbors[level]
		node.mu.RUnlock()

		for _, neighborID := range neighbors {
			neighbor := h.nodes[neighborID]
			dist := 1.0 - vector.DotProduct(query, neighbor.vector)
			if dist < currentDist {
				current = neighborID
				currentDist = dist
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	return current
}

func (h *HNSWIndex) searchLayer(query []float32, entryID string, ef int, level int) []string {
	visited := make(map[string]bool)
	visited[entryID] = true

	candidates := &hnswDistHeap{}
	heap.Init(candidates)

	results := &hnswDistHeap{}
	heap.Init(results)

	entryDist := 1.0 - vector.DotProduct(query, h.nodes[entryID].vector)
	heap.Push(candidates, hnswDistItem{id: entryID, dist: entryDist, isMax: false})
	heap.Push(results, hnswDistItem{id: entryID, dist: entryDist, isMax: true})

	for candidates.Len() > 0 {
		closest := heap.Pop(candidates).(hnswDistItem)

		if results.Len() >= ef {
			furthest := (*results)[0]
			if closest.dist > furthest.dist {
				break
			}
		}

		node := h.nodes[closest.id]
		node.mu.RLock()
		neighbors := node.neighbors[level]
		node.mu.RUnlock()

		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighbor := h.nodes[neighborID]
			dist := 1.0 - vector.DotProduct(query, neighbor.vector)

			if results.Len() < ef || dist < (*results)[0].dist {
				heap.Push(candidates, hnswDistItem{id: neighborID, dist: dist, isMax: false})
				heap.Push(results, hnswDistItem{id: neighborID, dist: dist, isMax: true})

				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	resultList := make([]string, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		item := heap.Pop(results).(hnswDistItem)
		resultList[i] = item.id
	}

	return resultList
}

func (h *HNSWIndex) selectNeighbors(query []float32, candidates []string, m int) []string {
	if len(candidates) <= m {
		return candidates
	}

	type distNode struct {
		id   string
		dist float64
	}
	dists := make([]distNode, len(candidates))
	for i, cid := range candidates {
		dists[i] = distNode{
			id:   cid,
			dist: 1.0 - vector.DotProduct(query, h.nodes[cid].vector),
		}
	}

	sort.Slice(dists, func(i, j int) bool {
		return dists[i].dist < dists[j].dist
	})

	result := make([]string, m)
	for i := 0; i < m; i++ {
		result[i] = dists[i].id
	}
	return result
}

func (h *HNSWIndex) randomLevel() int {
	r := rand.Float64()
	return int(-math.Log(r) * h.config.LevelMultiplier)
}

// Heap types for HNSW search
type hnswDistItem struct {
	id    string
	dist  float64
	isMax bool
}

type hnswDistHeap []hnswDistItem

func (dh hnswDistHeap) Len() int { return len(dh) }
func (dh hnswDistHeap) Less(i, j int) bool {
	if dh[i].isMax {
		return dh[i].dist > dh[j].dist
	}
	return dh[i].dist < dh[j].dist
}
func (dh hnswDistHeap) Swap(i, j int) { dh[i], dh[j] = dh[j], dh[i] }

func (dh *hnswDistHeap) Push(x interface{}) {
	*dh = append(*dh, x.(hnswDistItem))
}

func (dh *hnswDistHeap) Pop() interface{} {
	old := *dh
	n := len(old)
	x := old[n-1]
	*dh = old[0 : n-1]
	return x
}
