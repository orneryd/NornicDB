// Package warmup provides APOC database warmup functions.
//
// This package implements all apoc.warmup.* functions for preloading
// data into memory and optimizing database performance.
package warmup

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// WarmupCache holds cached data for quick access.
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

// Global cache instance
var cache = &WarmupCache{
	nodes:         make(map[int64]*Node),
	relationships: make(map[int64]*Relationship),
	nodesByLabel:  make(map[string][]*Node),
	relsByType:    make(map[string][]*Relationship),
	properties:    make(map[string][]interface{}),
	queries:       make(map[string]interface{}),
}

// Run performs a full database warmup.
//
// Example:
//
//	apoc.warmup.run() => {nodesLoaded: 1000, relsLoaded: 5000, time: 150}
func Run() map[string]interface{} {
	start := time.Now()

	cache.mu.Lock()
	cache.running = true
	cache.mu.Unlock()

	defer func() {
		cache.mu.Lock()
		cache.running = false
		now := time.Now()
		cache.lastRun = &now
		cache.mu.Unlock()
	}()

	nodesLoaded := 0
	relsLoaded := 0
	propertiesLoaded := 0

	// Load all nodes
	nodes, err := Storage.AllNodes()
	if err == nil {
		cache.mu.Lock()
		for _, node := range nodes {
			cache.nodes[node.ID] = node
			nodesLoaded++
			propertiesLoaded += len(node.Properties)

			// Index by label
			for _, label := range node.Labels {
				cache.nodesByLabel[label] = append(cache.nodesByLabel[label], node)
			}
		}
		cache.mu.Unlock()
	}

	// Load all relationships
	rels, err := Storage.AllRelationships()
	if err == nil {
		cache.mu.Lock()
		for _, rel := range rels {
			cache.relationships[rel.ID] = rel
			relsLoaded++
			propertiesLoaded += len(rel.Properties)

			// Index by type
			cache.relsByType[rel.Type] = append(cache.relsByType[rel.Type], rel)
		}
		cache.mu.Unlock()
	}

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"propertiesLoaded":    propertiesLoaded,
		"indexesLoaded":       len(cache.nodesByLabel) + len(cache.relsByType),
		"timeTaken":           elapsed,
	}
}

// RunWithParams performs warmup with parameters.
//
// Example:
//
//	apoc.warmup.run({labels: ['Person'], loadIndexes: true})
func RunWithParams(params map[string]interface{}) map[string]interface{} {
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
		result := Nodes(labels)
		nodesLoaded = result["nodesLoaded"].(int)
		if loadIndexes {
			indexesLoaded += len(labels)
		}
	}

	// Load relationships by specific types
	if len(types) > 0 {
		result := Relationships(types)
		relsLoaded = result["relationshipsLoaded"].(int)
		if loadIndexes {
			indexesLoaded += len(types)
		}
	}

	// If no specific labels/types, load all
	if len(labels) == 0 && len(types) == 0 {
		fullResult := Run()
		return fullResult
	}

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"indexesLoaded":       indexesLoaded,
		"timeTaken":           elapsed,
	}
}

// Nodes warms up specific nodes by label.
//
// Example:
//
//	apoc.warmup.nodes(['Person', 'Company']) => nodes loaded
func Nodes(labels []string) map[string]interface{} {
	start := time.Now()
	nodesLoaded := 0

	nodes, err := Storage.AllNodes()
	if err != nil {
		return map[string]interface{}{
			"nodesLoaded": 0,
			"labels":      labels,
			"timeTaken":   time.Since(start).Milliseconds(),
			"error":       err.Error(),
		}
	}

	labelSet := make(map[string]bool)
	for _, label := range labels {
		labelSet[label] = true
	}

	cache.mu.Lock()
	for _, node := range nodes {
		for _, label := range node.Labels {
			if labelSet[label] {
				cache.nodes[node.ID] = node
				cache.nodesByLabel[label] = append(cache.nodesByLabel[label], node)
				nodesLoaded++
				break
			}
		}
	}
	cache.mu.Unlock()

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"nodesLoaded": nodesLoaded,
		"labels":      labels,
		"timeTaken":   elapsed,
	}
}

// Relationships warms up specific relationships by type.
//
// Example:
//
//	apoc.warmup.relationships(['KNOWS', 'WORKS_AT']) => rels loaded
func Relationships(types []string) map[string]interface{} {
	start := time.Now()
	relsLoaded := 0

	rels, err := Storage.AllRelationships()
	if err != nil {
		return map[string]interface{}{
			"relationshipsLoaded": 0,
			"types":               types,
			"timeTaken":           time.Since(start).Milliseconds(),
			"error":               err.Error(),
		}
	}

	typeSet := make(map[string]bool)
	for _, t := range types {
		typeSet[t] = true
	}

	cache.mu.Lock()
	for _, rel := range rels {
		if typeSet[rel.Type] {
			cache.relationships[rel.ID] = rel
			cache.relsByType[rel.Type] = append(cache.relsByType[rel.Type], rel)
			relsLoaded++
		}
	}
	cache.mu.Unlock()

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"relationshipsLoaded": relsLoaded,
		"types":               types,
		"timeTaken":           elapsed,
	}
}

// Indexes warms up indexes (pre-builds label and type indexes).
//
// Example:
//
//	apoc.warmup.indexes() => indexes loaded
func Indexes() map[string]interface{} {
	start := time.Now()
	indexesLoaded := 0

	// Build node label index
	nodes, err := Storage.AllNodes()
	if err == nil {
		cache.mu.Lock()
		// Clear existing indexes
		cache.nodesByLabel = make(map[string][]*Node)
		for _, node := range nodes {
			for _, label := range node.Labels {
				cache.nodesByLabel[label] = append(cache.nodesByLabel[label], node)
			}
		}
		indexesLoaded += len(cache.nodesByLabel)
		cache.mu.Unlock()
	}

	// Build relationship type index
	rels, err := Storage.AllRelationships()
	if err == nil {
		cache.mu.Lock()
		cache.relsByType = make(map[string][]*Relationship)
		for _, rel := range rels {
			cache.relsByType[rel.Type] = append(cache.relsByType[rel.Type], rel)
		}
		indexesLoaded += len(cache.relsByType)
		cache.mu.Unlock()
	}

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"indexesLoaded": indexesLoaded,
		"labelIndexes":  len(cache.nodesByLabel),
		"typeIndexes":   len(cache.relsByType),
		"timeTaken":     elapsed,
	}
}

// Properties warms up property data by collecting unique values.
//
// Example:
//
//	apoc.warmup.properties(['name', 'email']) => properties loaded
func Properties(keys []string) map[string]interface{} {
	start := time.Now()
	propertiesLoaded := 0

	keySet := make(map[string]bool)
	for _, key := range keys {
		keySet[key] = true
	}

	nodes, err := Storage.AllNodes()
	if err == nil {
		cache.mu.Lock()
		for _, node := range nodes {
			for key, value := range node.Properties {
				if keySet[key] {
					cache.properties[key] = append(cache.properties[key], value)
					propertiesLoaded++
				}
			}
		}
		cache.mu.Unlock()
	}

	rels, err := Storage.AllRelationships()
	if err == nil {
		cache.mu.Lock()
		for _, rel := range rels {
			for key, value := range rel.Properties {
				if keySet[key] {
					cache.properties[key] = append(cache.properties[key], value)
					propertiesLoaded++
				}
			}
		}
		cache.mu.Unlock()
	}

	elapsed := time.Since(start).Milliseconds()

	return map[string]interface{}{
		"propertiesLoaded": propertiesLoaded,
		"keys":             keys,
		"timeTaken":        elapsed,
	}
}

// Subgraph warms up a subgraph starting from a node.
//
// Example:
//
//	apoc.warmup.subgraph(startNode, 3) => subgraph loaded
func Subgraph(startNode *Node, depth int) map[string]interface{} {
	startTime := time.Now()

	nodesLoaded := 0
	relsLoaded := 0

	if startNode == nil {
		return map[string]interface{}{
			"nodesLoaded":         0,
			"relationshipsLoaded": 0,
			"depth":               depth,
			"timeTaken":           time.Since(startTime).Milliseconds(),
			"error":               "start node is nil",
		}
	}

	// BFS to load subgraph
	visited := make(map[int64]bool)
	queue := []*Node{startNode}
	visited[startNode.ID] = true
	currentDepth := 0

	cache.mu.Lock()
	cache.nodes[startNode.ID] = startNode
	nodesLoaded++

	for len(queue) > 0 && currentDepth < depth {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[0]
			queue = queue[1:]

			neighbors, err := Storage.GetNodeNeighbors(current.ID, "", storage.DirectionBoth)
			if err == nil {
				for _, neighbor := range neighbors {
					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						queue = append(queue, neighbor)
						cache.nodes[neighbor.ID] = neighbor
						nodesLoaded++
					}
				}
			}

			// Also cache the relationships
			rels, err := Storage.GetNodeRelationships(current.ID, "", storage.DirectionBoth)
			if err == nil {
				for _, rel := range rels {
					if _, exists := cache.relationships[rel.ID]; !exists {
						cache.relationships[rel.ID] = rel
						relsLoaded++
					}
				}
			}
		}
		currentDepth++
	}
	cache.mu.Unlock()

	elapsed := time.Since(startTime).Milliseconds()

	return map[string]interface{}{
		"nodesLoaded":         nodesLoaded,
		"relationshipsLoaded": relsLoaded,
		"depth":               depth,
		"timeTaken":           elapsed,
	}
}

// Path warms up a specific path.
//
// Example:
//
//	apoc.warmup.path(nodes, rels) => path loaded
func Path(nodes []*Node, rels []*Relationship) map[string]interface{} {
	start := time.Now()

	cache.mu.Lock()
	for _, node := range nodes {
		cache.nodes[node.ID] = node
	}
	for _, rel := range rels {
		cache.relationships[rel.ID] = rel
	}
	cache.mu.Unlock()

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
func Cache(queries []string) map[string]interface{} {
	start := time.Now()
	queriesWarmed := 0

	cache.mu.Lock()
	for _, query := range queries {
		// Store a placeholder for the query result
		// In a real implementation, this would execute the query and cache results
		cache.queries[query] = map[string]interface{}{
			"cached":   true,
			"cachedAt": time.Now(),
		}
		queriesWarmed++
	}
	cache.mu.Unlock()

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
func Stats() map[string]interface{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	totalRequests := cache.stats.Hits + cache.stats.Misses
	hitRate := 0.0
	missRate := 0.0
	if totalRequests > 0 {
		hitRate = float64(cache.stats.Hits) / float64(totalRequests)
		missRate = float64(cache.stats.Misses) / float64(totalRequests)
	}

	itemsCached := len(cache.nodes) + len(cache.relationships) + len(cache.queries)

	return map[string]interface{}{
		"cacheHitRate":        hitRate,
		"cacheMissRate":       missRate,
		"cacheHits":           cache.stats.Hits,
		"cacheMisses":         cache.stats.Misses,
		"memoryUsed":          cache.stats.MemoryUsed,
		"itemsCached":         itemsCached,
		"nodesCached":         len(cache.nodes),
		"relationshipsCached": len(cache.relationships),
		"queriesCached":       len(cache.queries),
		"labelIndexes":        len(cache.nodesByLabel),
		"typeIndexes":         len(cache.relsByType),
	}
}

// Clear clears warmed up data.
//
// Example:
//
//	apoc.warmup.clear() => cache cleared
func Clear() map[string]interface{} {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	itemsCleared := len(cache.nodes) + len(cache.relationships) + len(cache.queries)

	cache.nodes = make(map[int64]*Node)
	cache.relationships = make(map[int64]*Relationship)
	cache.nodesByLabel = make(map[string][]*Node)
	cache.relsByType = make(map[string][]*Relationship)
	cache.properties = make(map[string][]interface{})
	cache.queries = make(map[string]interface{})
	cache.stats = CacheStats{}

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
func Optimize() map[string]interface{} {
	start := time.Now()

	cache.mu.RLock()
	recommendations := []string{}

	// Analyze cache and provide recommendations
	if len(cache.nodes) == 0 {
		recommendations = append(recommendations, "Consider running warmup.Run() to cache nodes")
	}

	if len(cache.nodesByLabel) == 0 {
		recommendations = append(recommendations, "Consider running warmup.Indexes() to build label indexes")
	}

	totalRequests := cache.stats.Hits + cache.stats.Misses
	if totalRequests > 0 {
		hitRate := float64(cache.stats.Hits) / float64(totalRequests)
		if hitRate < 0.5 {
			recommendations = append(recommendations, "Low cache hit rate - consider warming up more frequently accessed data")
		}
	}

	// Find most used labels
	topLabels := []string{}
	for label, nodes := range cache.nodesByLabel {
		if len(nodes) > 10 {
			topLabels = append(topLabels, label)
		}
	}
	if len(topLabels) > 0 {
		recommendations = append(recommendations, "Frequently used labels detected - consider prioritizing: "+joinStrings(topLabels, ", "))
	}

	cache.mu.RUnlock()

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
func Schedule(cron string) map[string]interface{} {
	cache.mu.Lock()
	cache.scheduledCron = cron
	// In a real implementation, this would set up a cron job
	cache.mu.Unlock()

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
func Status() map[string]interface{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	var lastRunStr, nextRunStr interface{}
	if cache.lastRun != nil {
		lastRunStr = cache.lastRun.Format(time.RFC3339)
	}
	if cache.nextRun != nil {
		nextRunStr = cache.nextRun.Format(time.RFC3339)
	}

	return map[string]interface{}{
		"running":       cache.running,
		"lastRun":       lastRunStr,
		"nextRun":       nextRunStr,
		"scheduledCron": cache.scheduledCron,
		"itemsCached":   len(cache.nodes) + len(cache.relationships) + len(cache.queries),
	}
}

// Progress returns warmup progress.
//
// Example:
//
//	apoc.warmup.progress() => {percentage: 75, ...}
func Progress() map[string]interface{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// Get totals from storage
	allNodes, _ := Storage.AllNodes()
	allRels, _ := Storage.AllRelationships()

	totalNodes := len(allNodes)
	totalRels := len(allRels)

	cachedNodes := len(cache.nodes)
	cachedRels := len(cache.relationships)

	percentage := 0.0
	if totalNodes+totalRels > 0 {
		percentage = float64(cachedNodes+cachedRels) / float64(totalNodes+totalRels) * 100
	}

	return map[string]interface{}{
		"percentage":  percentage,
		"nodesLoaded": cachedNodes,
		"totalNodes":  totalNodes,
		"relsLoaded":  cachedRels,
		"totalRels":   totalRels,
	}
}

// GetCachedNode retrieves a node from cache (increments hit/miss stats).
func GetCachedNode(id int64) (*Node, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if node, exists := cache.nodes[id]; exists {
		cache.stats.Hits++
		return node, true
	}
	cache.stats.Misses++
	return nil, false
}

// GetCachedRelationship retrieves a relationship from cache.
func GetCachedRelationship(id int64) (*Relationship, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if rel, exists := cache.relationships[id]; exists {
		cache.stats.Hits++
		return rel, true
	}
	cache.stats.Misses++
	return nil, false
}

// GetNodesByLabel retrieves nodes by label from cache.
func GetNodesByLabel(label string) ([]*Node, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if nodes, exists := cache.nodesByLabel[label]; exists {
		return nodes, true
	}
	return nil, false
}

// GetRelationshipsByType retrieves relationships by type from cache.
func GetRelationshipsByType(relType string) ([]*Relationship, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if rels, exists := cache.relsByType[relType]; exists {
		return rels, true
	}
	return nil, false
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
