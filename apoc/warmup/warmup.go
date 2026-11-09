// Package warmup provides APOC database warmup functions.
//
// This package implements all apoc.warmup.* functions for preloading
// data into memory and optimizing database performance.
package warmup

import "errors"

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/apoc/storage"
	nativestorage "github.com/orneryd/nornicdb/pkg/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Functions binds the warmup helpers and their warmup cache to one storage
// backend. Both the backend and the cache are fixed when the instance is
// created, so a backend swap cannot read another backend's cached graph.
type Functions struct {
	backend storage.Storage
	cache   *WarmupCache
}

// New returns the warmup helpers bound to backend, with an empty cache of its
// own. One instance serves one backend.
func New(backend storage.Storage) *Functions {
	return &Functions{
		backend: backend,
		cache: &WarmupCache{
			nodes:         make(map[int64]*Node),
			relationships: make(map[int64]*Relationship),
			nodesByLabel:  make(map[string][]*Node),
			relsByType:    make(map[string][]*Relationship),
			properties:    make(map[string][]interface{}),
			queries:       make(map[string]interface{}),
		},
	}
}

// WarmupCache holds cached data for quick access. Published records and existing
// index-slice elements are immutable: writers replace records/maps or append
// new elements. Getters capture a snapshot under mu and copy after unlocking,
// so caller copy callbacks cannot hold the cache mutex.
type WarmupCache struct {
	mu            sync.RWMutex
	nodes         map[int64]*Node
	relationships map[int64]*Relationship
	nodesByLabel  map[string][]*Node
	relsByType    map[string][]*Relationship
	properties    map[string][]interface{}
	queries       map[string]interface{}
	stats         CacheStats
	lastRun       *time.Time
	nextRun       *time.Time
	running       bool
	scheduledCron string
}

// CacheStats tracks cache statistics.
type CacheStats struct {
	Hits       int64
	Misses     int64
	MemoryUsed int64
}

// Run performs a full database warmup.
//
// Example:
//
//	apoc.warmup.run() => {nodesLoaded: 1000, relsLoaded: 5000, time: 150}
func (f *Functions) Run(ctx context.Context) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()

	f.cache.mu.Lock()
	f.cache.running = true
	f.cache.mu.Unlock()

	defer func() {
		f.cache.mu.Lock()
		f.cache.running = false
		now := time.Now()
		f.cache.lastRun = &now
		f.cache.mu.Unlock()
	}()

	nodesLoaded := 0
	relsLoaded := 0
	propertiesLoaded := 0

	// Load all nodes
	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		for _, node := range nodes {
			if ctx.Err() != nil {
				break
			}

			f.cache.nodes[node.ID] = node
			nodesLoaded++
			propertiesLoaded += len(node.Properties)

			// Index by label
			for _, label := range node.Labels {
				if ctx.Err() != nil {
					break
				}

				f.cache.nodesByLabel[label] = append(f.cache.nodesByLabel[label], node)
			}
		}
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	// Load all relationships
	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		for _, rel := range rels {
			if ctx.Err() != nil {
				break
			}

			f.cache.relationships[rel.ID] = rel
			relsLoaded++
			propertiesLoaded += len(rel.Properties)

			// Index by type
			f.cache.relsByType[rel.Type] = append(f.cache.relsByType[rel.Type], rel)
		}
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"propertiesLoaded":    propertiesLoaded,
		"indexesLoaded":       len(f.cache.nodesByLabel) + len(f.cache.relsByType),
		"timeTaken":           elapsed,
	}, nil
}

// RunWithParams performs warmup with parameters.
//
// Example:
//
//	apoc.warmup.run({labels: ['Person'], loadIndexes: true})
func (f *Functions) RunWithParams(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()

	labels := []string{}
	if l, ok := params["labels"].([]string); ok {
		labels = l
	}

	types := []string{}
	if t, ok := params["types"].([]string); ok {
		types = t
	}

	loadIndexes := true
	if li, ok := params["loadIndexes"].(bool); ok {
		loadIndexes = li
	}

	nodesLoaded := 0
	relsLoaded := 0
	indexesLoaded := 0

	// Load nodes by specific labels
	if len(labels) > 0 {
		helperValue1, helperError1 := f.Nodes(ctx, labels)
		if helperError1 != nil {
			return nil, helperError1
		}
		result := helperValue1
		nodesLoaded = result["nodesLoaded"].(int)
		if loadIndexes {
			indexesLoaded += len(labels)
		}
	}

	// Load relationships by specific types
	if len(types) > 0 {
		helperValue2, helperError2 := f.Relationships(ctx, types)
		if helperError2 != nil {
			return nil, helperError2
		}
		result := helperValue2
		relsLoaded = result["relationshipsLoaded"].(int)
		if loadIndexes {
			indexesLoaded += len(types)
		}
	}

	// If no specific labels/types, load all
	if len(labels) == 0 && len(types) == 0 {
		helperValue3, helperError3 := f.Run(ctx)
		if helperError3 != nil {
			return nil, helperError3
		}
		fullResult := helperValue3
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return fullResult, nil
	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"indexesLoaded":       indexesLoaded,
		"timeTaken":           elapsed,
	}, nil
}

// Nodes warms up specific nodes by label.
//
// Example:
//
//	apoc.warmup.nodes(['Person', 'Company']) => nodes loaded
func (f *Functions) Nodes(ctx context.Context, labels []string) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()
	nodesLoaded := 0

	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return map[string]interface{}{
			"nodesLoaded": 0,
			"labels":      labels,
			"timeTaken":   time.Since(start).Milliseconds(),
			"error":       err.Error(),
		}, nil
	}

	labelSet := make(map[string]bool)
	for _, label := range labels {
		if ctx.Err() != nil {
			break
		}

		labelSet[label] = true
	}

	f.cache.mu.Lock()
	for _, node := range nodes {
		if ctx.Err() != nil {
			break
		}

		for _, label := range node.Labels {
			if ctx.Err() != nil {
				break
			}

			if labelSet[label] {
				f.cache.nodes[node.ID] = node
				f.cache.nodesByLabel[label] = append(f.cache.nodesByLabel[label], node)
				nodesLoaded++
				break
			}
		}
	}
	f.cache.mu.Unlock()
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"nodesLoaded": nodesLoaded,
		"labels":      labels,
		"timeTaken":   elapsed,
	}, nil
}

// Relationships warms up specific relationships by type.
//
// Example:
//
//	apoc.warmup.relationships(['KNOWS', 'WORKS_AT']) => rels loaded
func (f *Functions) Relationships(ctx context.Context, types []string) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()
	relsLoaded := 0

	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return map[string]interface{}{
			"relationshipsLoaded": 0,
			"types":               types,
			"timeTaken":           time.Since(start).Milliseconds(),
			"error":               err.Error(),
		}, nil
	}

	typeSet := make(map[string]bool)
	for _, t := range types {
		if ctx.Err() != nil {
			break
		}

		typeSet[t] = true
	}

	f.cache.mu.Lock()
	for _, rel := range rels {
		if ctx.Err() != nil {
			break
		}

		if typeSet[rel.Type] {
			f.cache.relationships[rel.ID] = rel
			f.cache.relsByType[rel.Type] = append(f.cache.relsByType[rel.Type], rel)
			relsLoaded++
		}
	}
	f.cache.mu.Unlock()
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"relationshipsLoaded": relsLoaded,
		"types":               types,
		"timeTaken":           elapsed,
	}, nil
}

// Indexes warms up indexes (pre-builds label and type indexes).
//
// Example:
//
//	apoc.warmup.indexes() => indexes loaded
func (f *Functions) Indexes(ctx context.Context) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()
	indexesLoaded := 0

	// Build node label index
	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		// Clear existing indexes
		f.cache.nodesByLabel = make(map[string][]*Node)
		for _, node := range nodes {
			if ctx.Err() != nil {
				break
			}

			for _, label := range node.Labels {
				if ctx.Err() != nil {
					break
				}

				f.cache.nodesByLabel[label] = append(f.cache.nodesByLabel[label], node)
			}
		}
		indexesLoaded += len(f.cache.nodesByLabel)
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	// Build relationship type index
	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		f.cache.relsByType = make(map[string][]*Relationship)
		for _, rel := range rels {
			if ctx.Err() != nil {
				break
			}

			f.cache.relsByType[rel.Type] = append(f.cache.relsByType[rel.Type], rel)
		}
		indexesLoaded += len(f.cache.relsByType)
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"indexesLoaded": indexesLoaded,
		"labelIndexes":  len(f.cache.nodesByLabel),
		"typeIndexes":   len(f.cache.relsByType),
		"timeTaken":     elapsed,
	}, nil
}

// Properties warms up property data by collecting unique values.
//
// Example:
//
//	apoc.warmup.properties(['name', 'email']) => properties loaded
func (f *Functions) Properties(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	start := time.Now()
	propertiesLoaded := 0

	keySet := make(map[string]bool)
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}

		keySet[key] = true
	}

	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		for _, node := range nodes {
			if ctx.Err() != nil {
				break
			}

			for key, value := range node.Properties {
				if ctx.Err() != nil {
					break
				}

				if keySet[key] {
					f.cache.properties[key] = append(f.cache.properties[key], value)
					propertiesLoaded++
				}
			}
		}
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err == nil {
		f.cache.mu.Lock()
		for _, rel := range rels {
			if ctx.Err() != nil {
				break
			}

			for key, value := range rel.Properties {
				if ctx.Err() != nil {
					break
				}

				if keySet[key] {
					f.cache.properties[key] = append(f.cache.properties[key], value)
					propertiesLoaded++
				}
			}
		}
		f.cache.mu.Unlock()
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	elapsed := time.Since(start).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"propertiesLoaded": propertiesLoaded,
		"keys":             keys,
		"timeTaken":        elapsed,
	}, nil
}

// Subgraph warms up a subgraph starting from a node.
//
// Example:
//
//	apoc.warmup.subgraph(startNode, 3) => subgraph loaded
func (f *Functions) Subgraph(ctx context.Context, startNode *Node, depth int) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	startTime := time.Now()

	nodesLoaded := 0
	relsLoaded := 0

	if startNode == nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return map[string]interface{}{
			"nodesLoaded":         0,
			"relationshipsLoaded": 0,
			"depth":               depth,
			"timeTaken":           time.Since(startTime).Milliseconds(),
			"error":               "start node is nil",
		}, nil
	}

	// BFS to load subgraph
	visited := make(map[int64]bool)
	queue := []*Node{startNode}
	visited[startNode.ID] = true
	currentDepth := 0

	// The supplied start node stays the caller's: the cache retains its own copy.
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()
	f.cache.nodes[startNode.ID] = nativestorage.CopyValue(startNode, nil).(*Node)
	nodesLoaded++

	for len(queue) > 0 && currentDepth < depth {
		if ctx.Err() != nil {
			break
		}

		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			if ctx.Err() != nil {
				break
			}

			current := queue[0]
			queue = queue[1:]

			neighbors, err := f.backend.GetNodeNeighbors(ctx, current.ID, "", storage.DirectionBoth)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				for _, neighbor := range neighbors {
					if ctx.Err() != nil {
						break
					}

					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						queue = append(queue, neighbor)
						f.cache.nodes[neighbor.ID] = neighbor
						nodesLoaded++
					}
				}
			}

			// Also cache the relationships
			rels, err := f.backend.GetNodeRelationships(ctx, current.ID, "", storage.DirectionBoth)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				for _, rel := range rels {
					if ctx.Err() != nil {
						break
					}

					if _, exists := f.cache.relationships[rel.ID]; !exists {
						f.cache.relationships[rel.ID] = rel
						relsLoaded++
					}
				}
			}
		}
		currentDepth++
	}

	elapsed := time.Since(startTime).Milliseconds()

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"depth":               depth,
		"timeTaken":           elapsed,
	}, nil
}

// Path warms up a specific path.
//
// Example:
//
//	apoc.warmup.path(nodes, rels) => path loaded
func (f *Functions) Path(nodes []*Node, rels []*Relationship) map[string]interface{} {
	start := time.Now()

	// The supplied values stay the caller's: the cache retains its own copy.
	f.cache.mu.Lock()
	for _, node := range nodes {
		f.cache.nodes[node.ID] = nativestorage.CopyValue(node, nil).(*Node)
	}
	for _, rel := range rels {
		f.cache.relationships[rel.ID] = nativestorage.CopyValue(rel, nil).(*Relationship)
	}
	f.cache.mu.Unlock()

	nodesLoaded := len(nodes)
	relsLoaded := len(rels)

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"timeTaken":           elapsed,
	}
}

// Cache warms up cache for specific queries (stores query results).
//
// Example:
//
//	apoc.warmup.cache(['MATCH (n:Person) RETURN n']) => cache warmed
func (f *Functions) Cache(queries []string) map[string]interface{} {
	start := time.Now()
	queriesWarmed := 0

	f.cache.mu.Lock()
	for _, query := range queries {
		// Store a placeholder for the query result
		// In a real implementation, this would execute the query and cache results
		f.cache.queries[query] = map[string]interface{}{
			"cached":   true,
			"cachedAt": time.Now(),
		}
		queriesWarmed++
	}
	f.cache.mu.Unlock()

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"queriesWarmed": queriesWarmed,
		"timeTaken":     elapsed,
	}
}

// Stats returns warmup statistics.
//
// Example:
//
//	apoc.warmup.stats() => {cacheHitRate: 0.95, ...}
func (f *Functions) Stats() map[string]interface{} {
	f.cache.mu.RLock()
	defer f.cache.mu.RUnlock()

	totalRequests := f.cache.stats.Hits + f.cache.stats.Misses
	hitRate := 0.0
	missRate := 0.0
	if totalRequests > 0 {
		hitRate = float64(f.cache.stats.Hits) / float64(totalRequests)
		missRate = float64(f.cache.stats.Misses) / float64(totalRequests)
	}

	itemsCached := len(f.cache.nodes) + len(f.cache.relationships) + len(f.cache.queries)

	return map[string]interface{}{
		"cacheHitRate":        hitRate,
		"cacheMissRate":       missRate,
		"cacheHits":           f.cache.stats.Hits,
		"cacheMisses":         f.cache.stats.Misses,
		"memoryUsed":          f.cache.stats.MemoryUsed,
		"itemsCached":         itemsCached,
		"nodesCached":         len(f.cache.nodes),
		"relationshipsCached": len(f.cache.relationships),
		"queriesCached":       len(f.cache.queries),
		"labelIndexes":        len(f.cache.nodesByLabel),
		"typeIndexes":         len(f.cache.relsByType),
	}
}

// Clear clears warmed up data.
//
// Example:
//
//	apoc.warmup.clear() => cache cleared
func (f *Functions) Clear() map[string]interface{} {
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()

	itemsCleared := len(f.cache.nodes) + len(f.cache.relationships) + len(f.cache.queries)

	f.cache.nodes = make(map[int64]*Node)
	f.cache.relationships = make(map[int64]*Relationship)
	f.cache.nodesByLabel = make(map[string][]*Node)
	f.cache.relsByType = make(map[string][]*Relationship)
	f.cache.properties = make(map[string][]interface{})
	f.cache.queries = make(map[string]interface{})
	f.cache.stats = CacheStats{}

	return map[string]interface{}{
		"cleared":      true,
		"itemsCleared": itemsCleared,
	}
}

// Optimize analyzes cache usage and provides recommendations.
//
// Example:
//
//	apoc.warmup.optimize() => optimization results
func (f *Functions) Optimize() map[string]interface{} {
	start := time.Now()

	f.cache.mu.RLock()
	recommendations := []string{}

	// Analyze cache and provide recommendations
	if len(f.cache.nodes) == 0 {
		recommendations = append(recommendations, "Consider running warmup.Run() to f.cache nodes")
	}

	if len(f.cache.nodesByLabel) == 0 {
		recommendations = append(recommendations, "Consider running warmup.Indexes() to build label indexes")
	}

	totalRequests := f.cache.stats.Hits + f.cache.stats.Misses
	if totalRequests > 0 {
		hitRate := float64(f.cache.stats.Hits) / float64(totalRequests)
		if hitRate < 0.5 {
			recommendations = append(recommendations, "Low f.cache hit rate - consider warming up more frequently accessed data")
		}
	}

	// Find most used labels
	topLabels := []string{}
	for label, nodes := range f.cache.nodesByLabel {
		if len(nodes) > 10 {
			topLabels = append(topLabels, label)
		}
	}
	if len(topLabels) > 0 {
		recommendations = append(recommendations, "Frequently used labels detected - consider prioritizing: "+joinStrings(topLabels, ", "))
	}

	f.cache.mu.RUnlock()

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "Cache is well optimized")
	}

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"optimized":       true,
		"timeTaken":       elapsed,
		"recommendations": recommendations,
	}
}

// Schedule schedules periodic warmup (stores cron expression).
//
// Example:
//
//	apoc.warmup.schedule('0 0 * * *') => scheduled
func (f *Functions) Schedule(cron string) map[string]interface{} {
	f.cache.mu.Lock()
	f.cache.scheduledCron = cron
	// In a real implementation, this would set up a cron job
	f.cache.mu.Unlock()

	return map[string]interface{}{
		"scheduled": true,
		"cron":      cron,
	}
}

// Status returns warmup status.
//
// Example:
//
//	apoc.warmup.status() => {running: false, lastRun: ...}
func (f *Functions) Status() map[string]interface{} {
	f.cache.mu.RLock()
	defer f.cache.mu.RUnlock()

	var lastRunStr, nextRunStr interface{}
	if f.cache.lastRun != nil {
		lastRunStr = f.cache.lastRun.Format(time.RFC3339)
	}
	if f.cache.nextRun != nil {
		nextRunStr = f.cache.nextRun.Format(time.RFC3339)
	}

	return map[string]interface{}{
		"running":       f.cache.running,
		"lastRun":       lastRunStr,
		"nextRun":       nextRunStr,
		"scheduledCron": f.cache.scheduledCron,
		"itemsCached":   len(f.cache.nodes) + len(f.cache.relationships) + len(f.cache.queries),
	}
}

// Progress returns warmup progress.
//
// Example:
//
//	apoc.warmup.progress() => {percentage: 75, ...}
func (f *Functions) Progress(ctx context.Context) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	f.cache.mu.RLock()
	defer f.cache.mu.RUnlock()

	// Get totals from storage
	allNodes, storageError11 := f.backend.AllNodes(ctx)
	if storageError11 != nil {
		return nil, storageError11
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	allRels, storageError12 := f.backend.AllRelationships(ctx)
	if storageError12 != nil {
		return nil, storageError12
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	totalNodes := len(allNodes)
	totalRels := len(allRels)

	cachedNodes := len(f.cache.nodes)
	cachedRels := len(f.cache.relationships)

	percentage := 0.0
	if totalNodes+totalRels > 0 {
		percentage = float64(cachedNodes+cachedRels) / float64(totalNodes+totalRels) * 100
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"percentage":  percentage,
		"nodesLoaded": cachedNodes,
		"totalNodes":  totalNodes,
		"relsLoaded":  cachedRels,
		"totalRels":   totalRels,
	}, nil
}

// GetCachedNode retrieves a node from cache (increments hit/miss stats).
func (f *Functions) GetCachedNode(id int64) (*Node, bool) {
	f.cache.mu.Lock()
	node, exists := f.cache.nodes[id]
	if exists {
		f.cache.stats.Hits++
	} else {
		f.cache.stats.Misses++
	}
	f.cache.mu.Unlock()
	if !exists {
		return nil, false
	}
	return nativestorage.CopyValue(node, nil).(*Node), true
}

// GetCachedRelationship retrieves a relationship from cache.
func (f *Functions) GetCachedRelationship(id int64) (*Relationship, bool) {
	f.cache.mu.Lock()
	rel, exists := f.cache.relationships[id]
	if exists {
		f.cache.stats.Hits++
	} else {
		f.cache.stats.Misses++
	}
	f.cache.mu.Unlock()
	if !exists {
		return nil, false
	}
	return nativestorage.CopyValue(rel, nil).(*Relationship), true
}

// GetNodesByLabel retrieves nodes by label from cache.
func (f *Functions) GetNodesByLabel(label string) ([]*Node, bool) {
	f.cache.mu.RLock()
	nodes, exists := f.cache.nodesByLabel[label]
	f.cache.mu.RUnlock()
	if !exists {
		return nil, false
	}
	return nativestorage.CopyValue(nodes, nil).([]*Node), true
}

// GetRelationshipsByType retrieves relationships by type from cache.
func (f *Functions) GetRelationshipsByType(relType string) ([]*Relationship, bool) {
	f.cache.mu.RLock()
	rels, exists := f.cache.relsByType[relType]
	f.cache.mu.RUnlock()
	if !exists {
		return nil, false
	}
	return nativestorage.CopyValue(rels, nil).([]*Relationship), true
}

// Helper function to join strings
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
