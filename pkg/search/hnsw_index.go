// Package search provides HNSW vector indexing for fast approximate nearest neighbor search.
//
// HNSW Delete/Update Policy:
//
// Delete:
//   - Remove() tombstones a vector via a dense `deleted []bool` flag
//   - Neighbor lists are not eagerly rewired (tombstones keep deletes cheap)
//   - Entry point is re-selected if the removed node was the entry point
//
// Update:
//   - Current policy: Remove() + Add() pattern
//   - Call Remove(id) then Add(id, newVector) to update a vector
//   - This ensures the graph structure is correctly maintained
//   - Future: A dedicated Update() method may be added for efficiency
//
// Graph Quality:
//   - High-churn workloads (many updates/deletes) can degrade graph quality
//   - Periodic rebuilds are recommended (see NORNICDB_VECTOR_ANN_REBUILD_INTERVAL)
//   - Rebuilds restore optimal graph structure and improve recall
package search

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
)

var errHNSWIndexFull = errors.New("hnsw index full")

const hnswLevelSeed int64 = 1

func validHNSWIndex(index uint32, length int) bool {
	return uint64(index) < uint64(length)
}

// HNSWConfig contains configuration parameters for the HNSW index.
type HNSWConfig struct {
	M                         int     // Max connections per node per layer (default: 16)
	EfConstruction            int     // Candidate list size during construction (default: 200)
	EfSearch                  int     // Candidate list size during search (default: 100)
	SearchBeamFactor          int     // Search beam multiplier relative to requested candidates (default: 4)
	LevelMultiplier           float64 // Level multiplier = 1/ln(M)
	UseGPUBuild               bool    // Attempt GPU-assisted construction when available
	GPUBuildBatchSize         int     // Number of vectors per GPU construction batch
	GPUBuildCandidateK        int     // Number of GPU nearest-neighbor candidates per vector
	GPUBuildDistancePrecision string  // Distance precision for GPU build kernels (currently fp32)
}

// DefaultHNSWConfig returns sensible defaults for HNSW index.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{
		M:                         16,
		EfConstruction:            200,
		EfSearch:                  100,
		SearchBeamFactor:          defaultHNSWSearchBeamFactor,
		LevelMultiplier:           1.0 / math.Log(16.0),
		UseGPUBuild:               true,
		GPUBuildBatchSize:         2048,
		GPUBuildCandidateK:        128,
		GPUBuildDistancePrecision: "fp32",
	}
}

// NOTE: We intentionally avoid a per-node struct/slices in favor of a
// struct-of-arrays layout in HNSWIndex to reduce pointer chasing and improve
// cache locality in the hot search loop.

// ANNResult is a minimal search result from the ANN index (HNSW).
//
// This intentionally stays small (ID + float32 score) to keep per-request
// allocations and copy costs low. Higher-level layers can enrich results as
// needed (labels, properties, etc.).
type ANNResult struct {
	ID    string
	Score float32
}

// HNSWIndex provides fast approximate nearest neighbor search using HNSW algorithm.
type HNSWIndex struct {
	config     HNSWConfig
	dimensions int
	mu         sync.RWMutex

	// Per-node metadata, indexed by internal ID.
	nodeLevel []uint16
	vecOff    []int32

	// Neighbor links stored in one arena to keep iteration cache-friendly.
	// For node i:
	//   - neighborsOff[i] points to 2*M base-layer slots followed by M slots
	//     for every upper layer
	//   - neighborCountsOff[i] points to (level+1) counts in neighborCountsArena
	neighborsArena      []uint32
	neighborsOff        []int32
	neighborCountsArena []uint16
	neighborCountsOff   []int32

	idToInternal map[string]uint32
	internalToID []string
	deleted      []bool
	liveCount    int
	vectors      []float32

	// When set, vectors are resolved by ID at search/build time instead of from h.vectors.
	// vecOff will be -1 for nodes that use the lookup (saves one full vector copy in RAM).
	vectorLookup VectorLookup

	entryPoint    uint32
	hasEntryPoint bool
	maxLevel      int
	levelRNG      *rand.Rand

	queryBufPool sync.Pool
	visitedPool  sync.Pool
	heapPool     sync.Pool
	idsPool      sync.Pool
	itemsPool    sync.Pool

	// Construction scratch is protected by mu: HNSW mutations are serialized,
	// so reusing these buffers avoids sync.Pool boxing and per-neighbor pruning
	// allocations on the write path.
	selectDistScratch []hnswDistNode
	selectVecScratch  [][]float32
	addBestScratch    []uint32
	insertAllScratch  []uint32
	insertBestScratch []uint32
	buildLexicalHints map[string]LexicalSeedHint
}

type visitedGenState struct {
	gen []uint16
	cur uint16
}

type hnswDistNode struct {
	id   uint32
	dist float32
	vec  []float32
}

// NewHNSWIndex creates a new HNSW index with the given dimensions and config.
func NewHNSWIndex(dimensions int, config HNSWConfig) *HNSWIndex {
	if config.M == 0 {
		config = DefaultHNSWConfig()
	}
	h := &HNSWIndex{
		config:              config,
		dimensions:          dimensions,
		nodeLevel:           make([]uint16, 0, 1024),
		vecOff:              make([]int32, 0, 1024),
		neighborsArena:      make([]uint32, 0, util.SafePreallocProduct(2048, config.M)),
		neighborsOff:        make([]int32, 0, 1024),
		neighborCountsArena: make([]uint16, 0, 1024),
		neighborCountsOff:   make([]int32, 0, 1024),
		idToInternal:        make(map[string]uint32, 1024),
		internalToID:        make([]string, 0, 1024),
		deleted:             make([]bool, 0, 1024),
		liveCount:           0,
		vectors:             make([]float32, 0, util.SafePreallocProduct(1024, dimensions)),
		maxLevel:            0,
		levelRNG:            rand.New(rand.NewSource(hnswLevelSeed)),
	}
	h.queryBufPool.New = func() any {
		return make([]float32, dimensions)
	}
	h.visitedPool.New = func() any {
		return &visitedGenState{}
	}
	h.heapPool.New = func() any {
		return &distHeap{items: make([]hnswDistItem, 0, util.SafePreallocProduct(config.EfSearch, 2))}
	}
	h.idsPool.New = func() any {
		return make([]uint32, 0, util.SafePreallocProduct(config.EfSearch, 2))
	}
	h.itemsPool.New = func() any {
		return make([]hnswDistItem, 0, util.SafePreallocProduct(config.EfSearch, 2))
	}
	h.selectDistScratch = make([]hnswDistNode, 0, util.SafePreallocProduct(config.M, 2))
	h.selectVecScratch = make([][]float32, 0, util.SafePreallocProduct(config.M, 2))
	h.addBestScratch = make([]uint32, 0, config.M)
	h.insertAllScratch = make([]uint32, 0, util.SafePreallocSum(util.SafePreallocProduct(config.M, 2), 1))
	h.insertBestScratch = make([]uint32, 0, util.SafePreallocProduct(config.M, 2))
	return h
}

// SetVectorLookup sets an optional lookup so vectors are resolved by ID at search time
// instead of from the in-memory slice. When set, Add does not store vectors (vecOff = -1);
// Load can leave vectors empty and use the lookup. Saves one full vector copy in RAM.
func (h *HNSWIndex) SetVectorLookup(lookup VectorLookup) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectorLookup = lookup
}

// SetBuildLexicalHints installs compact lexical metadata used to make
// deterministic diversity choices between vector-distance ties. The hints do
// not affect distance ordering. They remain compact enough to guide later live
// insertions and maintenance rebuilds without retaining BM25 postings here.
func (h *HNSWIndex) SetBuildLexicalHints(hints []LexicalSeedHint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(hints) == 0 {
		h.buildLexicalHints = nil
		return
	}
	h.buildLexicalHints = make(map[string]LexicalSeedHint, len(hints))
	for _, hint := range hints {
		if hint.ID != "" {
			h.buildLexicalHints[hint.ID] = hint
		}
	}
}

func (h *HNSWIndex) setBuildLexicalHint(hint LexicalSeedHint) {
	if hint.ID == "" {
		return
	}
	h.mu.Lock()
	if h.buildLexicalHints == nil {
		h.buildLexicalHints = make(map[string]LexicalSeedHint)
	}
	h.buildLexicalHints[hint.ID] = hint
	h.mu.Unlock()
}

// Config returns a copy of the index configuration.
func (h *HNSWIndex) Config() HNSWConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.config
}

// setSearchBeamFactor applies query-only tuning to a loaded graph. It does not
// change graph construction, so callers can reuse a persisted index safely.
func (h *HNSWIndex) setSearchBeamFactor(factor int) {
	if factor <= 0 {
		factor = defaultHNSWSearchBeamFactor
	}
	h.mu.Lock()
	h.config.SearchBeamFactor = factor
	h.mu.Unlock()
}

// SupportsGPUBuild reports whether this index can be constructed through the
// GPU-assisted builder. The persisted index format is unchanged either way.
func (h *HNSWIndex) SupportsGPUBuild() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.dimensions > 0 && h.config.M > 0 && h.config.EfConstruction > 0
}

// Add inserts a vector into the index.
func (h *HNSWIndex) Add(id string, vec []float32) error {
	if len(vec) != h.dimensions {
		return ErrDimensionMismatch
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if id == "" {
		return nil
	}
	if internalID, ok := h.idToInternal[id]; ok && validHNSWIndex(internalID, len(h.deleted)) && !h.deleted[internalID] {
		// In-place update: overwrite the stored vector without changing the graph
		// topology. This avoids tombstone growth from hot upsert workloads.
		// When vectorLookup is set we don't store vectors; fall back to remove+add.
		off := int(h.vecOff[internalID])
		if h.vectorLookup == nil && off >= 0 && off+h.dimensions <= len(h.vectors) {
			dst := h.vectors[off : off+h.dimensions]
			copy(dst, vec)
			vector.NormalizeInPlace(dst)
			return nil
		}

		// Fallback: inconsistent internal state or lookup mode; degrade to remove+add.
		h.removeLocked(internalID)
	}

	level := h.randomLevel()

	if len(h.nodeLevel) >= int(^uint32(0)) {
		return errHNSWIndexFull
	}
	internalID := uint32(len(h.nodeLevel))
	m := h.config.M
	if m <= 0 {
		return nil
	}

	var normalized []float32
	if h.vectorLookup != nil {
		normalized = make([]float32, h.dimensions)
		copy(normalized, vec)
		vector.NormalizeInPlace(normalized)
		h.vecOff = append(h.vecOff, -1) // resolve via vectorLookup at search time
	} else {
		vecOff := len(h.vectors)
		h.vectors = append(h.vectors, vec...)
		normalized = h.vectors[vecOff : vecOff+h.dimensions]
		vector.NormalizeInPlace(normalized)
		vecOff32, ok := util.SafeIntToInt32(vecOff)
		if !ok {
			return errHNSWIndexFull
		}
		h.vecOff = append(h.vecOff, vecOff32)
	}

	level16, ok := util.SafeIntToUint16(level)
	if !ok {
		return errHNSWIndexFull
	}
	h.nodeLevel = append(h.nodeLevel, level16)

	neighborsOff := len(h.neighborsArena)
	neighborSlots, ok := h.neighborSlotsForNode(level)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborsArena = append(h.neighborsArena, make([]uint32, neighborSlots)...)
	neighborsOff32, ok := util.SafeIntToInt32(neighborsOff)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborsOff = append(h.neighborsOff, neighborsOff32)

	countsOff := len(h.neighborCountsArena)
	h.neighborCountsArena = append(h.neighborCountsArena, make([]uint16, level+1)...)
	countsOff32, ok := util.SafeIntToInt32(countsOff)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborCountsOff = append(h.neighborCountsOff, countsOff32)
	h.internalToID = append(h.internalToID, id)
	h.idToInternal[id] = internalID
	h.deleted = append(h.deleted, false)
	h.liveCount++

	if !h.hasEntryPoint {
		h.entryPoint = internalID
		h.hasEntryPoint = true
		h.maxLevel = level
		return nil
	}

	ep := h.entryPoint
	epLevel := int(h.nodeLevel[ep])
	neighbors := h.addBestScratch[:0]

	for l := epLevel; l > level; l-- {
		ep = h.searchLayerSingle(normalized, ep, l)
	}

	for l := min(level, epLevel); l >= 0; l-- {
		candidates := h.searchLayer(normalized, ep, h.config.EfConstruction, l)
		neighbors = h.selectNeighborsInto(normalized, candidates, h.config.M, neighbors[:0])
		h.setNeighborsAtLevelLocked(internalID, l, neighbors)

		for _, neighborID := range neighbors {
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) || h.deleted[neighborID] {
				continue
			}
			h.insertNeighborAtLevelLocked(neighborID, l, internalID)
		}

		if len(candidates) > 0 {
			ep = candidates[0]
		}
		h.releaseCandidateIDs(candidates)
	}

	if level > h.maxLevel {
		h.entryPoint = internalID
		h.hasEntryPoint = true
		h.maxLevel = level
	}
	h.addBestScratch = neighbors[:0]

	return nil
}

func (h *HNSWIndex) addWithLevel0Candidates(id string, vec []float32, level0Candidates []uint32) error {
	if len(level0Candidates) == 0 {
		return h.Add(id, vec)
	}
	if len(vec) != h.dimensions {
		return ErrDimensionMismatch
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if id == "" {
		return nil
	}
	if internalID, ok := h.idToInternal[id]; ok && validHNSWIndex(internalID, len(h.deleted)) && !h.deleted[internalID] {
		off := int(h.vecOff[internalID])
		if h.vectorLookup == nil && off >= 0 && off+h.dimensions <= len(h.vectors) {
			dst := h.vectors[off : off+h.dimensions]
			copy(dst, vec)
			vector.NormalizeInPlace(dst)
			return nil
		}
		h.removeLocked(internalID)
	}

	level := h.randomLevel()
	internalID, ok := util.SafeIntToUint32(len(h.nodeLevel))
	if !ok {
		return errHNSWIndexFull
	}
	m := h.config.M
	if m <= 0 {
		return nil
	}

	var normalized []float32
	if h.vectorLookup != nil {
		normalized = make([]float32, h.dimensions)
		copy(normalized, vec)
		vector.NormalizeInPlace(normalized)
		h.vecOff = append(h.vecOff, -1)
	} else {
		vecOff := len(h.vectors)
		h.vectors = append(h.vectors, vec...)
		normalized = h.vectors[vecOff : vecOff+h.dimensions]
		vector.NormalizeInPlace(normalized)
		vecOff32, ok := util.SafeIntToInt32(vecOff)
		if !ok {
			return errHNSWIndexFull
		}
		h.vecOff = append(h.vecOff, vecOff32)
	}

	level16, ok := util.SafeIntToUint16(level)
	if !ok {
		return errHNSWIndexFull
	}
	h.nodeLevel = append(h.nodeLevel, level16)
	neighborsOff := len(h.neighborsArena)
	neighborSlots, ok := h.neighborSlotsForNode(level)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborsArena = append(h.neighborsArena, make([]uint32, neighborSlots)...)
	neighborsOff32, ok := util.SafeIntToInt32(neighborsOff)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborsOff = append(h.neighborsOff, neighborsOff32)
	countsOff := len(h.neighborCountsArena)
	h.neighborCountsArena = append(h.neighborCountsArena, make([]uint16, level+1)...)
	countsOff32, ok := util.SafeIntToInt32(countsOff)
	if !ok {
		return errHNSWIndexFull
	}
	h.neighborCountsOff = append(h.neighborCountsOff, countsOff32)
	h.internalToID = append(h.internalToID, id)
	h.idToInternal[id] = internalID
	h.deleted = append(h.deleted, false)
	h.liveCount++

	if !h.hasEntryPoint {
		h.entryPoint = internalID
		h.hasEntryPoint = true
		h.maxLevel = level
		return nil
	}

	ep := h.entryPoint
	epLevel := int(h.nodeLevel[ep])
	neighbors := h.addBestScratch[:0]
	for l := epLevel; l > level; l-- {
		ep = h.searchLayerSingle(normalized, ep, l)
	}
	for l := min(level, epLevel); l > 0; l-- {
		candidates := h.searchLayer(normalized, ep, h.config.EfConstruction, l)
		neighbors = h.selectNeighborsInto(normalized, candidates, h.config.M, neighbors[:0])
		h.setNeighborsAtLevelLocked(internalID, l, neighbors)
		for _, neighborID := range neighbors {
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) || h.deleted[neighborID] {
				continue
			}
			h.insertNeighborAtLevelLocked(neighborID, l, internalID)
		}
		if len(candidates) > 0 {
			ep = candidates[0]
		}
		h.releaseCandidateIDs(candidates)
	}

	neighbors = h.selectNeighborsInto(normalized, level0Candidates, h.config.M, neighbors[:0])
	h.setNeighborsAtLevelLocked(internalID, 0, neighbors)
	for _, neighborID := range neighbors {
		if !validHNSWIndex(neighborID, len(h.nodeLevel)) || h.deleted[neighborID] {
			continue
		}
		h.insertNeighborAtLevelLocked(neighborID, 0, internalID)
	}

	if level > h.maxLevel {
		h.entryPoint = internalID
		h.hasEntryPoint = true
		h.maxLevel = level
	}
	h.addBestScratch = neighbors[:0]

	return nil
}

func (h *HNSWIndex) internalID(id string) (uint32, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	internalID, ok := h.idToInternal[id]
	return internalID, ok
}

// Update updates an existing vector in the index.
//
// Update policy: Remove + Add pattern
//   - Removes the old vector and all its connections
//   - Adds the new vector with fresh connections
//   - This ensures graph structure is correctly maintained
//
// If the vector doesn't exist, this is equivalent to Add().
//
// Performance: O(M * log(N)) where M is max connections, N is dataset size
// For high-churn workloads, consider periodic rebuilds to restore graph quality.
func (h *HNSWIndex) Update(id string, vec []float32) error {
	// New vectors dominate managed-embedding ingestion. Avoid taking the
	// exclusive removal lock when the ID is not present; Add performs the
	// authoritative check again while holding its mutation lock.
	h.mu.RLock()
	internalID, exists := h.idToInternal[id]
	exists = exists && validHNSWIndex(internalID, len(h.deleted)) && !h.deleted[internalID]
	h.mu.RUnlock()
	if !exists {
		return h.Add(id, vec)
	}
	h.Remove(id)
	return h.Add(id, vec)
}

// Remove removes a vector from the index by ID.
func (h *HNSWIndex) Remove(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	internalID, ok := h.idToInternal[id]
	if !ok || !validHNSWIndex(internalID, len(h.nodeLevel)) || h.deleted[internalID] {
		return
	}
	h.removeLocked(internalID)
}

// Clear removes all vectors from the index and resets it to an empty state.
// This frees memory by clearing all internal arrays and maps.
// Use this when you need to completely reset the index (e.g., after deleting a collection).
func (h *HNSWIndex) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Reset all internal state
	h.nodeLevel = make([]uint16, 0, 1024)
	h.vecOff = make([]int32, 0, 1024)
	h.neighborsArena = make([]uint32, 0, util.SafePreallocProduct(2048, h.config.M))
	h.neighborsOff = make([]int32, 0, 1024)
	h.neighborCountsArena = make([]uint16, 0, 1024)
	h.neighborCountsOff = make([]int32, 0, 1024)
	h.idToInternal = make(map[string]uint32, 1024)
	h.internalToID = make([]string, 0, 1024)
	h.deleted = make([]bool, 0, 1024)
	h.liveCount = 0
	h.vectors = make([]float32, 0, util.SafePreallocProduct(1024, h.dimensions))
	h.entryPoint = 0
	h.hasEntryPoint = false
	h.maxLevel = 0
	h.levelRNG = rand.New(rand.NewSource(hnswLevelSeed))
}

// Search finds the k nearest neighbors to the query vector.
func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, minSimilarity float64) ([]ANNResult, error) {
	return h.searchWithEf(ctx, query, k, minSimilarity, h.config.EfSearch)
}

// SearchWithEf finds the k nearest neighbors using a caller-provided `ef`.
//
// In Qdrant terms, `ef` is the beam size for HNSW search: larger values improve
// recall and usually increase latency. If `ef <= 0`, this falls back to the
// index's configured `EfSearch`.
func (h *HNSWIndex) SearchWithEf(ctx context.Context, query []float32, k int, minSimilarity float64, ef int) ([]ANNResult, error) {
	if ef <= 0 {
		ef = h.config.EfSearch
	}
	return h.searchWithEf(ctx, query, k, minSimilarity, ef)
}

// SearchWithEfFromEntries searches with the normal hierarchical entry point
// plus known relevant vector IDs as additional layer-zero entry points. Missing
// and duplicate IDs are ignored. Hybrid retrieval uses this to enter semantic
// regions already identified by its lexical ranking without changing cosine
// scoring or final result ordering.
func (h *HNSWIndex) SearchWithEfFromEntries(ctx context.Context, query []float32, k int, minSimilarity float64, ef int, entryIDs []string) ([]ANNResult, error) {
	if ef <= 0 {
		ef = h.config.EfSearch
	}
	results, _, err := h.searchWithEfExhaustionFromEntries(ctx, query, k, minSimilarity, ef, entryIDs)
	return results, err
}

func (h *HNSWIndex) searchWithEf(ctx context.Context, query []float32, k int, minSimilarity float64, ef int) ([]ANNResult, error) {
	results, _, err := h.searchWithEfExhaustion(ctx, query, k, minSimilarity, ef)
	return results, err
}

func (h *HNSWIndex) searchWithEfExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64, ef int) ([]ANNResult, bool, error) {
	return h.searchWithEfExhaustionFromEntries(ctx, query, k, minSimilarity, ef, nil)
}

func (h *HNSWIndex) searchWithEfExhaustionFromEntries(ctx context.Context, query []float32, k int, minSimilarity float64, ef int, entryIDs []string) ([]ANNResult, bool, error) {
	if len(query) != h.dimensions {
		return nil, false, ErrDimensionMismatch
	}
	if ef <= 0 {
		ef = h.config.EfSearch
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.hasEntryPoint || len(h.nodeLevel) == 0 {
		return []ANNResult{}, true, nil
	}
	if k > h.liveCount {
		k = h.liveCount
	}
	if ef > h.liveCount {
		ef = h.liveCount
	}

	var (
		normalized []float32
		pooledBuf  []float32
	)
	if h.dimensions <= 256 {
		var qbuf [256]float32
		copy(qbuf[:h.dimensions], query)
		normalized = qbuf[:h.dimensions]
		vector.NormalizeInPlace(normalized)
	} else {
		bufAny := h.queryBufPool.Get()
		buf := bufAny.([]float32)
		if cap(buf) < h.dimensions {
			buf = make([]float32, h.dimensions)
		}
		pooledBuf = buf
		normalized = buf[:h.dimensions]
		copy(normalized, query)
		vector.NormalizeInPlace(normalized)
		defer h.queryBufPool.Put(pooledBuf)
	}

	minSim32 := float32(minSimilarity)
	ep := h.entryPoint

	for l := h.maxLevel; l > 0; l-- {
		var err error
		ep, err = h.searchLayerSingleWithContext(ctx, normalized, ep, l)
		if err != nil {
			return nil, false, err
		}
	}

	entryBufAny := h.idsPool.Get()
	entries := entryBufAny.([]uint32)[:0]
	defer h.idsPool.Put(entries[:0])
	entries = append(entries, ep)
	for _, id := range entryIDs {
		internalID, ok := h.idToInternal[id]
		if !ok || !validHNSWIndex(internalID, len(h.deleted)) || h.deleted[internalID] {
			continue
		}
		entries = append(entries, internalID)
	}
	candidates, err := h.searchLayerHeapPooledFromEntriesWithContext(ctx, normalized, entries, ef, 0)
	if err != nil {
		return nil, false, err
	}
	defer h.itemsPool.Put(candidates[:0])

	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Distances were computed during graph traversal; reuse them to avoid a second
	// scoring pass. Candidate list is already ordered by increasing distance.
	//
	// dist = 1 - cosine_similarity (for normalized vectors)
	// score = 1 - dist
	limit := k
	if limit > len(candidates) {
		limit = len(candidates)
	}
	results := make([]ANNResult, 0, util.SafePreallocCap(limit, len(candidates)))
	for i := 0; i < len(candidates) && len(results) < k; i++ {
		item := candidates[i]
		score := float32(1.0) - item.dist
		if score < minSim32 {
			break // remaining candidates have lower scores
		}
		if !validHNSWIndex(item.id, len(h.deleted)) || h.deleted[item.id] {
			continue
		}
		if !validHNSWIndex(item.id, len(h.internalToID)) {
			continue
		}
		results = append(results, ANNResult{
			ID:    h.internalToID[item.id],
			Score: score,
		})
	}
	// The pre-filter heap must cover the live index while this read lock is
	// held, and top-k must not have hidden further candidates. Merely filling
	// the beam or getting a short post-threshold result proves neither fact.
	// Allocated node slots include tombstones left by normal updates/deletes.
	exhausted := false
	if len(candidates) >= h.liveCount {
		liveCandidates := 0
		for _, item := range candidates {
			if validHNSWIndex(item.id, len(h.deleted)) && !h.deleted[item.id] {
				liveCandidates++
			}
		}
		exhausted = liveCandidates == h.liveCount && (len(results) < k || liveCandidates <= k)
	}
	return results, exhausted, nil
}

// Size returns the number of vectors in the index.
func (h *HNSWIndex) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.liveCount
}

// GetDimensions returns the vector dimension of the index.
func (h *HNSWIndex) GetDimensions() int {
	return h.dimensions
}

// TombstoneRatio returns the ratio of deleted vectors to total vectors.
// Returns 0.0 if there are no vectors. A high ratio (>0.5) indicates
// the index should be rebuilt to free memory.
func (h *HNSWIndex) TombstoneRatio() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := len(h.nodeLevel)
	if total == 0 {
		return 0.0
	}
	deleted := total - h.liveCount
	return float64(deleted) / float64(total)
}

// ShouldRebuild returns true if the index has accumulated too many tombstones
// and should be rebuilt to free memory. Threshold is 50% deleted vectors.
func (h *HNSWIndex) ShouldRebuild() bool {
	return h.TombstoneRatio() > 0.5
}

const (
	hnswIndexFormatVersionGraphOnly = "1.2.0" // diverse graph, 2*M base layer, vectors resolved by ID
)

// hnswIndexSnapshot is the serializable form of the HNSW index for persistence.
// Vectors and VecOff are omitted; they are reconstructed on load from the vector index.
type hnswIndexSnapshot struct {
	Version           string
	Config            HNSWConfig
	Dimensions        int
	NodeLevel         []uint16
	VecOff            []int32
	NeighborsArena    []uint32
	NeighborsOff      []int32
	NeighborCountsAr  []uint16
	NeighborCountsOff []int32
	IDToInternal      map[string]uint32
	InternalToID      []string
	Deleted           []bool
	LiveCount         int
	Vectors           []float32
	EntryPoint        uint32
	HasEntryPoint     bool
	MaxLevel          int
}

// Save writes the HNSW index to path (msgpack format) as graph-only: graph structure and IDs only, no vector data.
// Vectors are always loaded from the vector index (vectors) on load, so the file stays small.
// Dir is created if needed. Copies index data under a short read lock so I/O does not block Search/Add/Remove.
func (h *HNSWIndex) Save(path string) error {
	h.mu.RLock()
	config := h.config
	dimensions := h.dimensions
	nodeLevel := append([]uint16(nil), h.nodeLevel...)
	neighborsArena := append([]uint32(nil), h.neighborsArena...)
	neighborsOff := append([]int32(nil), h.neighborsOff...)
	neighborCountsArena := append([]uint16(nil), h.neighborCountsArena...)
	neighborCountsOff := append([]int32(nil), h.neighborCountsOff...)
	idToInternal := make(map[string]uint32, len(h.idToInternal))
	for k, v := range h.idToInternal {
		idToInternal[k] = v
	}
	internalToID := append([]string(nil), h.internalToID...)
	deleted := append([]bool(nil), h.deleted...)
	liveCount := h.liveCount
	entryPoint := h.entryPoint
	hasEntryPoint := h.hasEntryPoint
	maxLevel := h.maxLevel
	h.mu.RUnlock()

	if err := security.EnsureRootedParent(path, 0o755); err != nil {
		return err
	}
	// Write atomically so interruptions do not leave a truncated/corrupt visible file.
	// Use a stable temp filename so we overwrite the same tmp path each save.
	tmpPath := path + ".tmp"
	tmpFile, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = security.RemoveRootedPath(tmpPath)
	}()

	snap := hnswIndexSnapshot{
		Version:           hnswIndexFormatVersionGraphOnly,
		Config:            config,
		Dimensions:        dimensions,
		NodeLevel:         nodeLevel,
		VecOff:            nil,
		NeighborsArena:    neighborsArena,
		NeighborsOff:      neighborsOff,
		NeighborCountsAr:  neighborCountsArena,
		NeighborCountsOff: neighborCountsOff,
		IDToInternal:      idToInternal,
		InternalToID:      internalToID,
		Deleted:           deleted,
		LiveCount:         liveCount,
		Vectors:           nil,
		EntryPoint:        entryPoint,
		HasEntryPoint:     hasEntryPoint,
		MaxLevel:          maxLevel,
	}
	if err := encodeMsgpackBuffered(tmpFile, &snap); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := security.RenameRootedFile(tmpPath, path); err != nil {
		return err
	}
	return nil
}

// VectorLookup returns a vector by ID (e.g. from the vector index). Used when loading a graph-only HNSW file.
type VectorLookup func(id string) ([]float32, bool)

// LoadHNSWIndex loads an HNSW index from path (msgpack format) and returns it.
// For the graph-only format, vectorLookup must be non-nil and vectors are
// resolved by ID at search time (no in-memory vector copy in HNSW).
// Older graph topologies are rejected so callers rebuild them. If the file
// does not exist or decode fails, returns (nil, nil) so the caller can rebuild.
// Returns an error only for unexpected I/O (e.g. permission denied).
func LoadHNSWIndex(path string, vectorLookup VectorLookup) (*HNSWIndex, error) {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var snap hnswIndexSnapshot
	if err := util.DecodeMsgpackFile(file.File, &snap); err != nil {
		return nil, nil
	}
	if snap.Dimensions <= 0 || snap.InternalToID == nil {
		return nil, nil
	}
	if snap.Version != hnswIndexFormatVersionGraphOnly {
		return nil, nil
	}

	config := snap.Config
	if config.M == 0 {
		config = DefaultHNSWConfig()
	}
	h := NewHNSWIndex(snap.Dimensions, config)
	h.mu.Lock()
	h.nodeLevel = snap.NodeLevel
	h.neighborsArena = snap.NeighborsArena
	h.neighborsOff = snap.NeighborsOff
	h.neighborCountsArena = snap.NeighborCountsAr
	h.neighborCountsOff = snap.NeighborCountsOff
	h.idToInternal = snap.IDToInternal
	h.internalToID = snap.InternalToID
	h.deleted = snap.Deleted
	h.liveCount = snap.LiveCount
	h.entryPoint = snap.EntryPoint
	h.hasEntryPoint = snap.HasEntryPoint
	h.maxLevel = snap.MaxLevel
	if h.idToInternal == nil {
		h.idToInternal = make(map[string]uint32)
	}

	// Keep graph-only in lookup mode to avoid duplicating vector storage in RAM.
	if vectorLookup == nil {
		h.mu.Unlock()
		return nil, nil
	}
	vecOff := make([]int32, len(snap.InternalToID))
	for i := range vecOff {
		vecOff[i] = -1
	}
	h.vectorLookup = vectorLookup
	h.vecOff = vecOff
	h.mu.Unlock()
	return h, nil
}

// SaveIVFHNSW persists per-cluster HNSW indexes to disk under hnsw_ivf/ alongside hnsw.
// hnswPath is the full path to the single HNSW file (e.g. data/search/dbname/hnsw); per-cluster
// files are written to hnsw_ivf/0, 1, 2, ... (no extension). Each cluster is saved as graph-only.
func SaveIVFHNSW(hnswPath string, clusterHNSW map[int]*HNSWIndex) error {
	return SaveIVFHNSWWithContext(context.Background(), hnswPath, clusterHNSW)
}

func SaveIVFHNSWWithContext(ctx context.Context, hnswPath string, clusterHNSW map[int]*HNSWIndex) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if hnswPath == "" || len(clusterHNSW) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	baseDir := filepath.Dir(hnswPath)
	ivfDir := filepath.Join(baseDir, "hnsw_ivf")
	if err := security.EnsureRootedParent(filepath.Join(ivfDir, ".cluster"), 0o755); err != nil {
		return err
	}
	for cid, idx := range clusterHNSW {
		if err := ctx.Err(); err != nil {
			return err
		}
		if idx == nil {
			continue
		}
		path := filepath.Join(ivfDir, fmt.Sprintf("%d", cid))
		if err := idx.Save(path); err != nil {
			return fmt.Errorf("cluster %d: %w", cid, err)
		}
	}
	return nil
}

// LoadIVFHNSWCluster loads one cluster's HNSW index from hnsw_ivf/cid in lookup mode
// (no vector copy in HNSW RAM). Returns (nil, nil) if the file is missing or invalid (caller can build).
// hnswPath is the full path to the single HNSW file (e.g. data/search/dbname/hnsw).
func LoadIVFHNSWCluster(hnswPath string, clusterID int, vectorLookup VectorLookup) (*HNSWIndex, error) {
	if hnswPath == "" || vectorLookup == nil {
		return nil, nil
	}
	baseDir := filepath.Dir(hnswPath)
	path := filepath.Join(baseDir, "hnsw_ivf", fmt.Sprintf("%d", clusterID))
	return LoadHNSWIndex(path, vectorLookup)
}

// loadIVFClusterMemberIDs decodes a cluster's msgpack file and returns the member IDs (InternalToID) without loading vectors.
// ivfDir is the hnsw_ivf directory (prefer absolute path so cwd does not affect resolution).
func loadIVFClusterMemberIDs(ivfDir string, clusterID int) ([]string, error) {
	if ivfDir == "" {
		return nil, nil
	}
	path := filepath.Join(ivfDir, fmt.Sprintf("%d", clusterID))
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var snap hnswIndexSnapshot
	if err := util.DecodeMsgpackFile(file.File, &snap); err != nil {
		return nil, err
	}
	if snap.InternalToID == nil {
		return nil, nil
	}
	return snap.InternalToID, nil
}

// DeriveIVFCentroidsFromClusters builds centroids and idToCluster from existing hnsw_ivf/ cluster files
// (numeric names 0, 1, 2, ...) and vectors from the vector index. No separate centroid file.
// Returns (nil, nil, nil) if no cluster files exist or derivation fails.
func DeriveIVFCentroidsFromClusters(hnswPath string, vectorLookup VectorLookup) (centroids [][]float32, idToCluster map[string]int, err error) {
	if hnswPath == "" || vectorLookup == nil {
		return nil, nil, nil
	}
	baseDir := filepath.Dir(hnswPath)
	ivfDir := filepath.Join(baseDir, "hnsw_ivf")
	entries, err := security.ReadRootedDir(ivfDir)
	if err != nil {
		logSearchPrintf("[IVF-HNSW] ⚠️ DeriveIVFCentroidsFromClusters: ReadDir %q: %v (k-means will run)", ivfDir, err)
		return nil, nil, nil
	}
	var clusterIDs []int
	var seenNames []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		seenNames = append(seenNames, name)
		// Cluster files are numeric names (0, 1, 2, ...); skip e.g. centroids.gob
		if cid, sErr := strconv.Atoi(name); sErr == nil && cid >= 0 {
			clusterIDs = append(clusterIDs, cid)
		}
	}
	if len(clusterIDs) == 0 {
		logSearchPrintf("[IVF-HNSW] ⚠️ DeriveIVFCentroidsFromClusters: no cluster files in %q (saw %d entries: %v); k-means will run", ivfDir, len(seenNames), seenNames)
		return nil, nil, nil
	}
	sort.Ints(clusterIDs)
	maxCID := clusterIDs[len(clusterIDs)-1]

	idToCluster = make(map[string]int)
	dims := 0
	centroidSums := make([][]float64, maxCID+1)
	centroidCounts := make([]int, maxCID+1)

	for _, cid := range clusterIDs {
		memberIDs, err := loadIVFClusterMemberIDs(ivfDir, cid)
		if err != nil || len(memberIDs) == 0 {
			continue
		}
		for _, id := range memberIDs {
			idToCluster[id] = cid
		}
		vecs := make([][]float32, 0, len(memberIDs))
		for _, id := range memberIDs {
			vec, ok := vectorLookup(id)
			if !ok || len(vec) == 0 {
				continue
			}
			if dims == 0 {
				dims = len(vec)
			}
			if len(vec) != dims {
				continue
			}
			vecs = append(vecs, vec)
		}
		if len(vecs) == 0 {
			continue
		}
		if dims == 0 {
			dims = len(vecs[0])
		}
		centroidSums[cid] = make([]float64, dims)
		for _, v := range vecs {
			for d := 0; d < dims; d++ {
				centroidSums[cid][d] += float64(v[d])
			}
		}
		centroidCounts[cid] = len(vecs)
	}

	if dims == 0 {
		logSearchPrintf("[IVF-HNSW] ⚠️ DeriveIVFCentroidsFromClusters: no vectors found for any cluster in %q (vectorLookup returned nothing for cluster member IDs); k-means will run", ivfDir)
		return nil, nil, nil
	}
	// Dense slice so centroid index matches cluster IDs (0, 1, 2, ...); RestoreClusteringState expects cid < len(centroids).
	centroids = make([][]float32, maxCID+1)
	for cid := 0; cid <= maxCID; cid++ {
		centroids[cid] = make([]float32, dims)
		if centroidCounts[cid] > 0 {
			for d := 0; d < dims; d++ {
				centroids[cid][d] = float32(centroidSums[cid][d] / float64(centroidCounts[cid]))
			}
		}
	}
	return centroids, idToCluster, nil
}

func (h *HNSWIndex) removeLocked(internalID uint32) {
	if !validHNSWIndex(internalID, len(h.nodeLevel)) || h.deleted[internalID] {
		return
	}

	h.deleted[internalID] = true
	h.liveCount--

	if validHNSWIndex(internalID, len(h.internalToID)) {
		delete(h.idToInternal, h.internalToID[internalID])
	}

	// If index becomes empty, clear entry point.
	if h.liveCount <= 0 {
		h.entryPoint = 0
		h.hasEntryPoint = false
		h.maxLevel = 0
		return
	}

	// Re-select entry point if we deleted it, or if it may have carried maxLevel.
	if h.hasEntryPoint && (internalID == h.entryPoint || int(h.nodeLevel[internalID]) == h.maxLevel) {
		h.reselectEntryPointLocked()
	}
}

func (h *HNSWIndex) reselectEntryPointLocked() {
	var (
		bestID    uint32
		bestLevel = -1
		found     = false
	)

	for id := range h.nodeLevel {
		internalID, ok := util.SafeIntToUint32(id)
		if !ok {
			continue
		}
		if validHNSWIndex(internalID, len(h.deleted)) && h.deleted[internalID] {
			continue
		}
		lvl := int(h.nodeLevel[internalID])
		if !found || lvl > bestLevel {
			bestID = internalID
			bestLevel = lvl
			found = true
		}
	}

	if !found {
		h.entryPoint = 0
		h.hasEntryPoint = false
		h.maxLevel = 0
		return
	}

	h.entryPoint = bestID
	h.hasEntryPoint = true
	h.maxLevel = bestLevel
}

func (h *HNSWIndex) searchLayerSingle(query []float32, entryID uint32, level int) uint32 {
	out, _ := h.searchLayerSingleWithContext(context.Background(), query, entryID, level)
	return out
}

func (h *HNSWIndex) searchLayerSingleWithContext(ctx context.Context, query []float32, entryID uint32, level int) (uint32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	current := entryID
	currentDist := float32(1.0) - vector.DotProductSIMD(query, h.vectorAtLocked(current))

	for {
		if err := ctx.Err(); err != nil {
			return current, err
		}
		changed := false
		neighbors, ok := h.neighborsAtLevelLocked(current, level)
		if !ok {
			break
		}

		// Reverse iteration: order doesn't matter when finding closest neighbor
		for i := len(neighbors) - 1; i >= 0; i-- {
			if i&31 == 0 {
				if err := ctx.Err(); err != nil {
					return current, err
				}
			}
			neighborID := neighbors[i]
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) {
				continue
			}
			dist := float32(1.0) - vector.DotProductSIMD(query, h.vectorAtLocked(neighborID))
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

	return current, nil
}

func (h *HNSWIndex) searchLayer(query []float32, entryID uint32, ef int, level int) []uint32 {
	if ef <= 0 {
		return nil
	}
	return h.searchLayerHeap(query, entryID, ef, level)
}

func (h *HNSWIndex) searchLayerHeap(query []float32, entryID uint32, ef int, level int) []uint32 {
	visited := h.visitedPool.Get().(*visitedGenState)
	defer h.visitedPool.Put(visited)
	if len(visited.gen) < len(h.nodeLevel) {
		oldLen := len(visited.gen)
		if cap(visited.gen) < len(h.nodeLevel) {
			next := make([]uint16, len(h.nodeLevel))
			copy(next, visited.gen)
			visited.gen = next
		} else {
			visited.gen = visited.gen[:len(h.nodeLevel)]
			clear(visited.gen[oldLen:])
		}
	}
	visited.cur++
	if visited.cur == 0 {
		clear(visited.gen)
		visited.cur = 1
	}
	curGen := visited.cur
	visited.gen[entryID] = curGen

	candidates := h.heapPool.Get().(*distHeap)
	candidates.Reset(false, ef*2)
	defer h.heapPool.Put(candidates)

	results := h.heapPool.Get().(*distHeap)
	results.Reset(true, ef*2)
	defer h.heapPool.Put(results)

	entryDist := float32(1.0) - vector.DotProductSIMD(query, h.vectorAtLocked(entryID))
	candidates.Push(hnswDistItem{id: entryID, dist: entryDist})
	results.Push(hnswDistItem{id: entryID, dist: entryDist})

	for candidates.Len() > 0 {
		closest := candidates.Pop()

		if results.Len() >= ef {
			furthest := results.Peek()
			if closest.dist > furthest.dist {
				break
			}
		}

		nodeID := closest.id
		if !validHNSWIndex(nodeID, len(h.nodeLevel)) || h.deleted[nodeID] {
			continue
		}
		neighbors, ok := h.neighborsAtLevelLocked(nodeID, level)
		if !ok {
			continue
		}

		// Reverse iteration: order doesn't matter when checking all neighbors
		for i := len(neighbors) - 1; i >= 0; i-- {
			neighborID := neighbors[i]
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) || h.deleted[neighborID] {
				continue
			}
			if visited.gen[neighborID] == curGen {
				continue
			}
			visited.gen[neighborID] = curGen

			dist := float32(1.0) - vector.DotProductSIMD(query, h.vectorAtLocked(neighborID))

			if results.Len() < ef || dist < results.Peek().dist {
				candidates.Push(hnswDistItem{id: neighborID, dist: dist})
				results.Push(hnswDistItem{id: neighborID, dist: dist})

				if results.Len() > ef {
					_ = results.Pop()
				}
			}
		}
	}

	resultList := h.idsPool.Get().([]uint32)
	if cap(resultList) < results.Len() {
		resultList = make([]uint32, results.Len())
	} else {
		resultList = resultList[:results.Len()]
	}
	for i := results.Len() - 1; i >= 0; i-- {
		item := results.Pop()
		resultList[i] = item.id
	}

	return resultList
}

func (h *HNSWIndex) releaseCandidateIDs(ids []uint32) {
	if ids != nil {
		h.idsPool.Put(ids[:0])
	}
}

func (h *HNSWIndex) searchLayerHeapPooledFromEntriesWithContext(ctx context.Context, query []float32, entryIDs []uint32, ef int, level int) ([]hnswDistItem, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	visited := h.visitedPool.Get().(*visitedGenState)
	defer h.visitedPool.Put(visited)
	if len(visited.gen) < len(h.nodeLevel) {
		oldLen := len(visited.gen)
		if cap(visited.gen) < len(h.nodeLevel) {
			next := make([]uint16, len(h.nodeLevel))
			copy(next, visited.gen)
			visited.gen = next
		} else {
			visited.gen = visited.gen[:len(h.nodeLevel)]
			clear(visited.gen[oldLen:])
		}
	}
	visited.cur++
	if visited.cur == 0 {
		clear(visited.gen)
		visited.cur = 1
	}
	curGen := visited.cur
	candidates := h.heapPool.Get().(*distHeap)
	candidates.Reset(false, ef*2)
	defer h.heapPool.Put(candidates)

	results := h.heapPool.Get().(*distHeap)
	results.Reset(true, ef*2)
	defer h.heapPool.Put(results)

	for _, entryID := range entryIDs {
		if !validHNSWIndex(entryID, len(h.nodeLevel)) || h.deleted[entryID] || visited.gen[entryID] == curGen {
			continue
		}
		entryVector := h.vectorAtLocked(entryID)
		if len(entryVector) != h.dimensions {
			continue
		}
		visited.gen[entryID] = curGen
		entryDist := float32(1.0) - vector.DotProductSIMD(query, entryVector)
		item := hnswDistItem{id: entryID, dist: entryDist}
		candidates.Push(item)
		results.Push(item)
		if results.Len() > ef {
			_ = results.Pop()
		}
	}

	for candidates.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		closest := candidates.Pop()

		if results.Len() >= ef {
			furthest := results.Peek()
			if closest.dist > furthest.dist {
				break
			}
		}

		nodeID := closest.id
		if !validHNSWIndex(nodeID, len(h.nodeLevel)) || h.deleted[nodeID] {
			continue
		}
		neighbors, ok := h.neighborsAtLevelLocked(nodeID, level)
		if !ok {
			continue
		}

		// Reverse iteration: order doesn't matter when checking all neighbors
		for i := len(neighbors) - 1; i >= 0; i-- {
			if i&31 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			neighborID := neighbors[i]
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) || h.deleted[neighborID] {
				continue
			}
			if visited.gen[neighborID] == curGen {
				continue
			}
			visited.gen[neighborID] = curGen

			dist := float32(1.0) - vector.DotProductSIMD(query, h.vectorAtLocked(neighborID))

			if results.Len() < ef || dist < results.Peek().dist {
				candidates.Push(hnswDistItem{id: neighborID, dist: dist})
				results.Push(hnswDistItem{id: neighborID, dist: dist})

				if results.Len() > ef {
					_ = results.Pop()
				}
			}
		}
	}

	n := results.Len()
	bufAny := h.itemsPool.Get()
	buf := bufAny.([]hnswDistItem)
	if cap(buf) < n {
		buf = make([]hnswDistItem, n)
	} else {
		buf = buf[:n]
	}
	for i := n - 1; i >= 0; i-- {
		item := results.Pop() // furthest first
		buf[i] = item         // closest ends up at index 0
	}
	return buf, nil
}

func (h *HNSWIndex) selectNeighborsInto(query []float32, candidates []uint32, m int, out []uint32) []uint32 {
	if m <= 0 || len(candidates) == 0 {
		return out[:0]
	}

	dists := h.selectDistScratch[:0]
	for _, cid := range candidates {
		if !validHNSWIndex(cid, len(h.nodeLevel)) || h.deleted[cid] {
			continue
		}
		candidateVector := h.vectorAtLocked(cid)
		if len(candidateVector) != h.dimensions {
			continue
		}
		distance := float32(1.0) - vector.DotProductSIMD(query, candidateVector)
		dists = append(dists, hnswDistNode{
			id:   cid,
			dist: distance,
			vec:  candidateVector,
		})
	}
	slices.SortFunc(dists, func(left, right hnswDistNode) int {
		if order := cmp.Compare(left.dist, right.dist); order != 0 {
			return order
		}
		leftHint, leftOK := h.buildLexicalHintLocked(left.id)
		rightHint, rightOK := h.buildLexicalHintLocked(right.id)
		if leftOK != rightOK {
			if leftOK {
				return -1
			}
			return 1
		}
		if leftOK {
			if order := cmp.Compare(leftHint.Rank, rightHint.Rank); order != 0 {
				return order
			}
		}
		return cmp.Compare(left.id, right.id)
	})

	out = out[:0]
	selectedVectors := h.selectVecScratch[:0]
	for i := range dists {
		if len(out) >= m {
			break
		}
		h.preferLexicallyDiverseTieLocked(dists, i, out)
		candidate := &dists[i]
		diverse := true
		for _, selectedVector := range selectedVectors {
			interNeighborDistance := float32(1.0) - vector.DotProductSIMD(candidate.vec, selectedVector)
			if interNeighborDistance < candidate.dist {
				diverse = false
				break
			}
		}
		if diverse {
			out = append(out, candidate.id)
			selectedVectors = append(selectedVectors, candidate.vec)
		}
	}
	clear(dists)
	h.selectDistScratch = dists[:0]
	clear(selectedVectors)
	h.selectVecScratch = selectedVectors[:0]
	return out
}

func (h *HNSWIndex) buildLexicalHintLocked(internalID uint32) (LexicalSeedHint, bool) {
	if len(h.buildLexicalHints) == 0 || !validHNSWIndex(internalID, len(h.internalToID)) {
		return LexicalSeedHint{}, false
	}
	id := normalizeVectorResultIDToNodeID(h.internalToID[internalID])
	hint, ok := h.buildLexicalHints[id]
	return hint, ok
}

func (h *HNSWIndex) preferLexicallyDiverseTieLocked(candidates []hnswDistNode, start int, selected []uint32) {
	if len(h.buildLexicalHints) == 0 || len(selected) == 0 || start >= len(candidates) {
		return
	}
	end := start + 1
	for end < len(candidates) && candidates[end].dist == candidates[start].dist {
		end++
	}
	if end-start < 2 {
		return
	}
	best := start
	bestDiversity := -1
	bestRank := ^uint32(0)
	for i := start; i < end; i++ {
		hint, ok := h.buildLexicalHintLocked(candidates[i].id)
		if !ok {
			continue
		}
		minDiversity := 64
		compared := false
		for _, selectedID := range selected {
			selectedHint, selectedOK := h.buildLexicalHintLocked(selectedID)
			if !selectedOK {
				continue
			}
			compared = true
			minDiversity = min(minDiversity, bits.OnesCount64(hint.Signature^selectedHint.Signature))
		}
		if !compared {
			continue
		}
		if minDiversity > bestDiversity || (minDiversity == bestDiversity && hint.Rank < bestRank) {
			best = i
			bestDiversity = minDiversity
			bestRank = hint.Rank
		}
	}
	if best != start {
		candidates[start], candidates[best] = candidates[best], candidates[start]
	}
}

func (h *HNSWIndex) randomLevel() int {
	r := h.levelRNG.Float64()
	return int(-math.Log(r) * h.config.LevelMultiplier)
}

func (h *HNSWIndex) vectorAtLocked(internalID uint32) []float32 {
	if !validHNSWIndex(internalID, len(h.vecOff)) || !validHNSWIndex(internalID, len(h.internalToID)) {
		return nil
	}
	off := int(h.vecOff[internalID])
	if h.vectorLookup != nil && off < 0 {
		vec, ok := h.vectorLookup(h.internalToID[internalID])
		if !ok || len(vec) != h.dimensions {
			return nil
		}
		return vec
	}
	if off < 0 || off+h.dimensions > len(h.vectors) {
		return nil
	}
	return h.vectors[off : off+h.dimensions]
}

func (h *HNSWIndex) neighborCapacity(level int) int {
	if level == 0 {
		capacity, ok := util.SafeIntProduct(h.config.M, 2)
		if !ok {
			return 0
		}
		return capacity
	}
	return h.config.M
}

func (h *HNSWIndex) neighborLevelOffset(level int) (int, bool) {
	if level == 0 {
		return 0, true
	}
	return util.SafeIntProduct(level+1, h.config.M)
}

func (h *HNSWIndex) neighborSlotsForNode(maxLevel int) (int, bool) {
	return util.SafeIntProduct(maxLevel+2, h.config.M)
}

func (h *HNSWIndex) neighborsAtLevelLocked(nodeID uint32, level int) ([]uint32, bool) {
	if !validHNSWIndex(nodeID, len(h.neighborsOff)) || !validHNSWIndex(nodeID, len(h.neighborCountsOff)) {
		return nil, false
	}
	if level < 0 || level > int(h.nodeLevel[nodeID]) {
		return nil, false
	}
	capacity := h.neighborCapacity(level)
	if capacity <= 0 {
		return nil, false
	}

	neighborsBase, ok := util.SafeInt32ToInt(h.neighborsOff[nodeID])
	if !ok {
		return nil, false
	}
	levelOffset, ok := h.neighborLevelOffset(level)
	if !ok {
		return nil, false
	}
	neighborsBase += levelOffset
	countsBase, ok := util.SafeInt32ToInt(h.neighborCountsOff[nodeID])
	if !ok {
		return nil, false
	}
	countsBase += level
	if countsBase < 0 || countsBase >= len(h.neighborCountsArena) {
		return nil, false
	}
	cnt := int(h.neighborCountsArena[countsBase])
	if cnt == 0 {
		return nil, true
	}
	end := neighborsBase + cnt
	if neighborsBase < 0 || end > len(h.neighborsArena) {
		return nil, false
	}
	return h.neighborsArena[neighborsBase:end], true
}

func (h *HNSWIndex) setNeighborsAtLevelLocked(nodeID uint32, level int, neighbors []uint32) {
	if !validHNSWIndex(nodeID, len(h.neighborsOff)) || !validHNSWIndex(nodeID, len(h.neighborCountsOff)) {
		return
	}
	if level < 0 || level > int(h.nodeLevel[nodeID]) {
		return
	}

	capacity := h.neighborCapacity(level)
	if capacity <= 0 {
		return
	}
	if len(neighbors) > capacity {
		neighbors = neighbors[:capacity]
	}

	neighborsBase, ok := util.SafeInt32ToInt(h.neighborsOff[nodeID])
	if !ok {
		return
	}
	levelOffset, ok := h.neighborLevelOffset(level)
	if !ok {
		return
	}
	neighborsBase += levelOffset
	countsBase, ok := util.SafeInt32ToInt(h.neighborCountsOff[nodeID])
	if !ok {
		return
	}
	countsBase += level
	if neighborsBase < 0 || neighborsBase+capacity > len(h.neighborsArena) {
		return
	}
	if countsBase < 0 || countsBase >= len(h.neighborCountsArena) {
		return
	}

	copy(h.neighborsArena[neighborsBase:neighborsBase+len(neighbors)], neighbors)
	if neighborCount, ok := util.SafeIntToUint16(len(neighbors)); ok {
		h.neighborCountsArena[countsBase] = neighborCount
	}
}

func (h *HNSWIndex) insertNeighborAtLevelLocked(neighborID uint32, level int, newNeighborID uint32) {
	if h.deleted[neighborID] {
		return
	}
	if level < 0 || level > int(h.nodeLevel[neighborID]) {
		return
	}

	capacity := h.neighborCapacity(level)
	if capacity <= 0 {
		return
	}

	neighborsBase, ok := util.SafeInt32ToInt(h.neighborsOff[neighborID])
	if !ok {
		return
	}
	levelOffset, ok := h.neighborLevelOffset(level)
	if !ok {
		return
	}
	neighborsBase += levelOffset
	countsBase, ok := util.SafeInt32ToInt(h.neighborCountsOff[neighborID])
	if !ok {
		return
	}
	countsBase += level
	if neighborsBase < 0 || neighborsBase+capacity > len(h.neighborsArena) {
		return
	}
	if countsBase < 0 || countsBase >= len(h.neighborCountsArena) {
		return
	}

	cnt := int(h.neighborCountsArena[countsBase])
	if cnt < capacity {
		h.neighborsArena[neighborsBase+cnt] = newNeighborID
		if nextCount, ok := util.SafeIntToUint16(cnt + 1); ok {
			h.neighborCountsArena[countsBase] = nextCount
		}
		return
	}

	// Full: apply the same diversity heuristic used for the new node's links.
	all := h.insertAllScratch[:0]
	all = append(all, h.neighborsArena[neighborsBase:neighborsBase+capacity]...)
	all = append(all, newNeighborID)
	best := h.selectNeighborsInto(h.vectorAtLocked(neighborID), all, capacity, h.insertBestScratch[:0])
	copy(h.neighborsArena[neighborsBase:neighborsBase+capacity], best)
	clear(h.neighborsArena[neighborsBase+len(best) : neighborsBase+capacity])
	if bestCount, ok := util.SafeIntToUint16(min(len(best), capacity)); ok {
		h.neighborCountsArena[countsBase] = bestCount
	}
	h.insertAllScratch = all[:0]
	h.insertBestScratch = best[:0]
}

// Heap types for HNSW search
type hnswDistItem struct {
	id   uint32
	dist float32
}

type distHeap struct {
	max   bool
	items []hnswDistItem
}

func newDistHeap(max bool, capHint int) *distHeap {
	if capHint < 0 {
		capHint = 0
	}
	return &distHeap{
		max:   max,
		items: make([]hnswDistItem, 0, capHint),
	}
}

func (h *distHeap) Reset(max bool, capHint int) {
	h.max = max
	h.items = h.items[:0]
	if capHint > cap(h.items) {
		h.items = make([]hnswDistItem, 0, capHint)
	}
}

func (h *distHeap) Len() int { return len(h.items) }

func (h *distHeap) Peek() hnswDistItem {
	return h.items[0]
}

func (h *distHeap) Push(item hnswDistItem) {
	h.items = append(h.items, item)
	h.siftUp(len(h.items) - 1)
}

func (h *distHeap) Pop() hnswDistItem {
	n := len(h.items)
	out := h.items[0]
	last := h.items[n-1]
	h.items = h.items[:n-1]
	if len(h.items) > 0 {
		h.items[0] = last
		h.siftDown(0)
	}
	return out
}

func (h *distHeap) less(i, j int) bool {
	if h.max {
		return h.items[i].dist > h.items[j].dist
	}
	return h.items[i].dist < h.items[j].dist
}

func (h *distHeap) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			return
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

func (h *distHeap) siftDown(i int) {
	n := len(h.items)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		best := l
		r := l + 1
		if r < n && h.less(r, l) {
			best = r
		}
		if !h.less(best, i) {
			return
		}
		h.items[i], h.items[best] = h.items[best], h.items[i]
		i = best
	}
}
