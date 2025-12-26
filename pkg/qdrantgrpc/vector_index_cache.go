// Package qdrantgrpc - In-memory vector index cache for Qdrant gRPC searches.
//
// DEPRECATED: This endpoint-specific indexing cache is being phased out in favor
// of the unified IndexRegistry and NamedEmbeddings data model. The cache is kept
// as a fallback during migration but will be removed once IndexRegistry integration
// is stable.
//
// This cache maintains per-collection, per-vector-name indexes to avoid
// falling back to storage scans when Qdrant collection dimensions differ
// from the DB's default embedding dimensions.
package qdrantgrpc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
	qpb "github.com/qdrant/go-client/qdrant"
)

type vectorIndexCache struct {
	mu      sync.RWMutex
	indexes map[indexKey]vectorIndex
}

type indexKey struct {
	collection string
	vectorName string
}

func newVectorIndexCache() *vectorIndexCache {
	return &vectorIndexCache{
		indexes: make(map[indexKey]vectorIndex),
	}
}

func (c *vectorIndexCache) getOrCreate(collection, vectorName string, dim int, dist qpb.Distance) vectorIndex {
	c.mu.RLock()
	idx, ok := c.indexes[indexKey{collection: collection, vectorName: vectorName}]
	c.mu.RUnlock()
	if ok && idx != nil && idx.dimensions() == dim && idx.distance() == dist {
		return idx
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := indexKey{collection: collection, vectorName: vectorName}
	idx, ok = c.indexes[key]
	if ok && idx != nil && idx.dimensions() == dim && idx.distance() == dist {
		return idx
	}

	idx = newVectorIndex(dim, dist)
	c.indexes[key] = idx
	return idx
}

func (c *vectorIndexCache) deleteCollection(collection string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.indexes {
		if k.collection == collection {
			delete(c.indexes, k)
		}
	}
}

func (c *vectorIndexCache) deletePoint(collection, pointID string, vectorNames []string) {
	if c == nil {
		return
	}
	pointID = compactPointID(collection, pointID)
	if len(vectorNames) == 0 {
		vectorNames = []string{""}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, name := range vectorNames {
		if idx := c.indexes[indexKey{collection: collection, vectorName: name}]; idx != nil {
			idx.remove(pointID)
		}
	}
}

func (c *vectorIndexCache) replacePoint(collection string, dim int, dist qpb.Distance, pointID string, oldNames []string, newNames []string, newVecs [][]float32) error {
	if c == nil {
		return nil
	}
	pointID = compactPointID(collection, pointID)
	if len(newNames) != len(newVecs) {
		return fmt.Errorf("invalid vectors: names=%d vecs=%d", len(newNames), len(newVecs))
	}
	if len(oldNames) == 0 {
		oldNames = []string{""}
	}
	if len(newNames) == 0 {
		newNames = []string{""}
	}

	c.deletePoint(collection, pointID, oldNames)

	for i, name := range newNames {
		vec := newVecs[i]
		if len(vec) != dim {
			return fmt.Errorf("vector dim mismatch for %q: got %d expected %d", name, len(vec), dim)
		}
		c.getOrCreate(collection, name, dim, dist).upsert(pointID, vec)
	}
	return nil
}

func (c *vectorIndexCache) search(ctx context.Context, collection string, dim int, dist qpb.Distance, vectorName string, query []float32, want int, minScore float64) []searchResult {
	if c == nil || want <= 0 {
		return nil
	}
	if len(query) != dim {
		return nil
	}
	idx := c.getOrCreate(collection, vectorName, dim, dist)
	if idx == nil {
		return nil
	}
	return idx.search(ctx, query, want, minScore)
}

type vectorIndex interface {
	dimensions() int
	distance() qpb.Distance
	remove(id string)
	upsert(id string, vec []float32)
	search(ctx context.Context, query []float32, limit int, minScore float64) []searchResult
}

func newVectorIndex(dim int, dist qpb.Distance) vectorIndex {
	// Best default: use HNSW for cosine distance (scales much better than brute-force).
	// For other distances, fall back to exact brute-force.
	if dist == qpb.Distance_Cosine || dist == qpb.Distance_UnknownDistance {
		return newHNSWVectorIndex(dim, dist)
	}
	return newBruteVectorIndex(dim, dist)
}

type bruteVectorIndex struct {
	dim  int
	dist qpb.Distance

	mu      sync.RWMutex
	vectors map[string][]float32
}

func newBruteVectorIndex(dim int, dist qpb.Distance) *bruteVectorIndex {
	return &bruteVectorIndex{
		dim:     dim,
		dist:    dist,
		vectors: make(map[string][]float32),
	}
}

func (v *bruteVectorIndex) dimensions() int { return v.dim }
func (v *bruteVectorIndex) distance() qpb.Distance {
	return v.dist
}

func (v *bruteVectorIndex) remove(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.vectors, id)
}

func (v *bruteVectorIndex) upsert(id string, vec []float32) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cpy := make([]float32, len(vec))
	copy(cpy, vec)
	if v.dist == qpb.Distance_Cosine || v.dist == qpb.Distance_UnknownDistance {
		cpy = vector.Normalize(cpy)
	}
	v.vectors[id] = cpy
}

func (v *bruteVectorIndex) search(ctx context.Context, query []float32, limit int, minScore float64) []searchResult {
	if v == nil || limit <= 0 || len(query) != v.dim {
		return nil
	}

	q := query
	if v.dist == qpb.Distance_Cosine || v.dist == qpb.Distance_UnknownDistance {
		q = vector.Normalize(query)
	}

	type topItem struct {
		id    string
		score float64
	}
	top := make([]topItem, 0, limit)
	minIdx := 0
	minVal := float64(0)

	v.mu.RLock()
	defer v.mu.RUnlock()

	for id, cand := range v.vectors {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		score := scoreVector(v.dist, q, cand)
		if minScore >= 0 && score < minScore {
			continue
		}

		if len(top) < limit {
			top = append(top, topItem{id: id, score: score})
			if len(top) == 1 || score < minVal {
				minVal = score
				minIdx = len(top) - 1
			}
			continue
		}

		if score <= minVal {
			continue
		}

		top[minIdx] = topItem{id: id, score: score}
		minIdx = 0
		minVal = top[0].score
		for i := 1; i < len(top); i++ {
			if top[i].score < minVal {
				minVal = top[i].score
				minIdx = i
			}
		}
	}

	sort.Slice(top, func(i, j int) bool { return top[i].score > top[j].score })

	out := make([]searchResult, 0, len(top))
	for _, item := range top {
		if strings.Contains(item.id, "-chunk-") {
			continue
		}
		out = append(out, searchResult{ID: item.id, Score: item.score})
	}
	return out
}

func compactPointID(collection, pointID string) string {
	// Point nodes are stored as "qdrant:<collection>:<id>".
	prefix := "qdrant:" + collection + ":"
	if strings.HasPrefix(pointID, prefix) {
		return pointID[len(prefix):]
	}
	return pointID
}

func expandPointID(collection, compact string) string {
	// Avoid double-prefixing in case some callers use non-standard IDs.
	if strings.HasPrefix(compact, "qdrant:") {
		return compact
	}
	return "qdrant:" + collection + ":" + compact
}

type hnswVectorIndex struct {
	dim  int
	dist qpb.Distance

	hnsw *search.HNSWIndex
}

func newHNSWVectorIndex(dim int, dist qpb.Distance) *hnswVectorIndex {
	// Load HNSW configuration from environment variables (respects quality presets)
	cfg := search.HNSWConfigFromEnv()
	// Slightly higher search ef improves recall while still being fast.
	// Only override if environment didn't set a higher value.
	if cfg.EfSearch < 128 {
		cfg.EfSearch = 128
	}
	return &hnswVectorIndex{
		dim:  dim,
		dist: dist,
		hnsw: search.NewHNSWIndex(dim, cfg),
	}
}

func (v *hnswVectorIndex) dimensions() int { return v.dim }
func (v *hnswVectorIndex) distance() qpb.Distance {
	return v.dist
}

func (v *hnswVectorIndex) remove(id string) {
	if v == nil || v.hnsw == nil {
		return
	}
	v.hnsw.Remove(id)
}

func (v *hnswVectorIndex) upsert(id string, vec []float32) {
	if v == nil || v.hnsw == nil {
		return
	}
	// HNSW Add() doesn't properly "update" existing nodes (it replaces the node
	// entry but leaves stale neighbor links). Remove first to keep the graph sane.
	v.hnsw.Remove(id)
	_ = v.hnsw.Add(id, vec)
}

func (v *hnswVectorIndex) search(ctx context.Context, query []float32, limit int, minScore float64) []searchResult {
	if v == nil || v.hnsw == nil || limit <= 0 || len(query) != v.dim {
		return nil
	}
	results, err := v.hnsw.Search(ctx, query, limit, minScore)
	if err != nil {
		return nil
	}
	out := make([]searchResult, 0, len(results))
	for _, r := range results {
		if strings.Contains(r.ID, "-chunk-") {
			continue
		}
		out = append(out, searchResult{ID: r.ID, Score: r.Score})
	}
	return out
}
