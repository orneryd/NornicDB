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
	"encoding/json"
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
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
	// Quantization controls coordinate compression independently of graph quality.
	Quantization HNSWQuantizationConfig

	M                         int     // Max connections per node per layer (default: 16)
	EfConstruction            int     // Candidate list size during construction (default: 200)
	EfSearch                  int     // Candidate list size during search (default: 100)
	SearchBeamFactor          int     // Search beam multiplier relative to requested candidates (default: 4)
	LevelMultiplier           float64 // Level multiplier = 1/ln(M)
	UseGPUBuild               bool    // Attempt GPU-assisted construction when available
	GPUBuildBatchSize         int     // Number of vectors per GPU construction batch
	GPUBuildCandidateK        int     // Minimum GPU candidate depth (at least M and EfConstruction)
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
	// liveOrder holds every live internal ID in ascending (storage) order,
	// plus removed ones not yet compacted away, never more than the live
	// ones (liveOrderDead counts them). The population-wide scan ranges it,
	// so its work follows the live population while it still reads the
	// vectors in storage order.
	liveOrder     []uint32
	liveOrderDead int
	// fingerprints, when non-nil (cluster graphs), holds the coordinate
	// fingerprint each node was inserted with, by internal ID (hnsw_fingerprint.go).
	fingerprints []uint64
	vectors      []float32
	quantized    *hnswQuantizedVectors

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
	return h.addLocked(id, vec, nil)
}

// addLocked is the shared insertion core for Add and addWithLevel0Candidates:
// the in-place upsert fast path, vector storage, arena/level bookkeeping and
// the entry-point walk are identical. When level0Candidates is non-nil, level 0
// is linked straight from the supplied candidates instead of a layer-0 search
// (the GPU build path); the supplied candidates are caller-owned and never
// released back to the candidate pool.
func (h *HNSWIndex) addLocked(id string, vec []float32, level0Candidates []uint32) error {
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
			if h.quantized != nil {
				h.quantized.set(internalID, dst)
			}
			h.recordFingerprintLocked(internalID, vec)
			return nil
		}

		// Fallback: inconsistent internal state or lookup mode; degrade to remove+add.
		h.removeLocked(internalID)
	}

	level := h.randomLevel()

	// Allocation-free overflow guard for the uint32 internal-ID domain.
	// util.SafeIntToUint32 would also work here but costs strconv/fmt
	// allocations on the per-node hot path (~4 allocs/node measured).
	if uint64(len(h.nodeLevel)) >= uint64(^uint32(0)) {
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
	h.liveOrder = append(h.liveOrder, internalID)
	h.recordFingerprintLocked(internalID, vec)
	h.liveCount++
	if h.quantized != nil {
		h.quantized.set(internalID, normalized)
	}

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
		var candidates []uint32
		searched := true
		if l == 0 && level0Candidates != nil {
			candidates = level0Candidates
			searched = false
		} else {
			candidates = h.searchLayer(normalized, ep, h.config.EfConstruction, l)
		}
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
		if searched {
			h.releaseCandidateIDs(candidates)
		}
	}

	if level > h.maxLevel {
		h.entryPoint = internalID
		h.hasEntryPoint = true
		h.maxLevel = level
	}
	h.addBestScratch = neighbors[:0]

	return nil
}

// levelZeroSearchLocked is the level-0 beam search a query or an insert of
// normalized makes: the greedy descent through the upper layers from the entry
// point, then a beam of width ef at level 0 from the reached node and the live
// nodes of entryIDs (lexical seeds). It returns the candidates closest first in
// a slice from h.itemsPool, which the caller returns. Caller holds h.mu for
// reading.
func (h *HNSWIndex) levelZeroSearchLocked(ctx context.Context, normalized []float32, ef int, entryIDs []string) ([]hnswDistItem, error) {
	ep, err := h.descendLocked(ctx, normalized)
	if err != nil {
		return nil, err
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
	return h.searchLayerHeapPooledFromEntriesWithContext(ctx, normalized, entries, ef, 0)
}

// rebuildLiveOrderLocked sets liveOrder to the live internal IDs of a
// restored graph, ascending. Caller holds h.mu for writing.
func (h *HNSWIndex) rebuildLiveOrderLocked() {
	live := make([]uint32, 0, h.liveCount)
	for id, deleted := range h.deleted {
		if !deleted && uint64(id) <= uint64(^uint32(0)) {
			live = append(live, uint32(id))
		}
	}
	h.liveOrder, h.liveOrderDead = live, 0
}

// compactLiveOrderLocked drops removed IDs from liveOrder in place, keeping
// its order. Removal calls it once removed entries outnumber live ones, so
// each removal costs amortized O(1) and liveOrder never holds more than
// twice the live population. Caller holds h.mu for writing.
func (h *HNSWIndex) compactLiveOrderLocked() {
	live := h.liveOrder[:0]
	for _, id := range h.liveOrder {
		if validHNSWIndex(id, len(h.deleted)) && !h.deleted[id] {
			live = append(live, id)
		}
	}
	h.liveOrder, h.liveOrderDead = live, 0
}

// scanLiveLocked scores every live vector, closest first, in a slice from
// itemsPool, with the same distance as the layer-0 search (the quantized
// scorer when the query carries one). It ranges liveOrder, so its work
// follows the live population (tombstone slots left by removals are never
// visited) and reads vectors in storage order. Caller holds h.mu for reading.
func (h *HNSWIndex) scanLiveLocked(ctx context.Context, normalized []float32) ([]hnswDistItem, error) {
	scorer, _ := ctx.Value(hnswQuantizedQueryKey{}).(*hnswQuantizedQuery)
	items := h.itemsPool.Get().([]hnswDistItem)[:0]
	for i, internalID := range h.liveOrder {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				h.itemsPool.Put(items[:0])
				return nil, err
			}
		}
		if !validHNSWIndex(internalID, len(h.deleted)) || h.deleted[internalID] {
			continue
		}
		var dist float32
		if scorer != nil {
			dist = 1 - scorer.dot(internalID)
		} else {
			// Resolve once: a file-backed store copies the vector per lookup.
			vec := h.vectorAtLocked(internalID)
			if len(vec) != h.dimensions {
				continue
			}
			dist = 1 - vector.DotProductSIMD(normalized, vec)
		}
		items = append(items, hnswDistItem{id: internalID, dist: dist})
	}
	slices.SortFunc(items, func(left, right hnswDistItem) int {
		if order := cmp.Compare(left.dist, right.dist); order != 0 {
			return order
		}
		return cmp.Compare(left.id, right.id)
	})
	return items, nil
}

// descendLocked is the greedy descent through the upper layers from the entry
// point that a query or an insert of normalized makes before its level-0
// search. Caller holds h.mu for reading and checked h.hasEntryPoint.
func (h *HNSWIndex) descendLocked(ctx context.Context, normalized []float32) (uint32, error) {
	ep := h.entryPoint
	for l := h.maxLevel; l > 0; l-- {
		var err error
		if ep, err = h.searchLayerSingleWithContext(ctx, normalized, ep, l); err != nil {
			return 0, err
		}
	}
	return ep, nil
}

// levelZeroEntry is where an insert of normalized starts its level-0 search
// (descendLocked): a GPU build seeds the vector's graph search there.
func (h *HNSWIndex) levelZeroEntry(ctx context.Context, normalized []float32) (uint32, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.hasEntryPoint || len(normalized) != h.dimensions {
		return 0, false
	}
	ep, err := h.descendLocked(ctx, normalized)
	return ep, err == nil
}

// buildLevelZeroCandidates is an insert's level-0 candidate search for a
// vector not in the index yet (levelZeroSearchLocked at width ef), run under
// the read lock so a build batch's searches run in parallel. It returns the
// live candidates' internal IDs and distances, closest first.
func (h *HNSWIndex) buildLevelZeroCandidates(ctx context.Context, normalized []float32, ef int, entryIDs []string) ([]uint32, []float32, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.hasEntryPoint || len(normalized) != h.dimensions {
		return nil, nil, nil
	}
	items, err := h.levelZeroSearchLocked(ctx, normalized, ef, entryIDs)
	if err != nil {
		return nil, nil, err
	}
	defer h.itemsPool.Put(items[:0])
	ids := make([]uint32, 0, len(items))
	dists := make([]float32, 0, len(items))
	for _, item := range items {
		if !validHNSWIndex(item.id, len(h.deleted)) || h.deleted[item.id] {
			continue
		}
		ids = append(ids, item.id)
		dists = append(dists, item.dist)
	}
	return ids, dists, nil
}

func (h *HNSWIndex) addWithLevel0Candidates(id string, vec []float32, level0Candidates []uint32) error {
	if len(level0Candidates) == 0 {
		return h.Add(id, vec)
	}
	if len(vec) != h.dimensions {
		return ErrDimensionMismatch
	}
	return h.addLocked(id, vec, level0Candidates)
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

	h.quantized = nil
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
	h.liveOrder, h.liveOrderDead = nil, 0
	if h.fingerprints != nil {
		h.fingerprints = h.fingerprints[:0]
	}
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
	if err := h.prepareQuantizationReadLocked(ctx); err != nil {
		return nil, false, err
	}

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

	// Cosine similarity cannot be below -1, so a threshold of -1 or lower means
	// "no threshold". Comparing against exactly -1 would let float rounding of
	// the distance drop a vector that is exactly opposite to the query.
	minSim32 := float32(minSimilarity)
	if minSimilarity <= -1 {
		minSim32 = float32(math.Inf(-1))
	}
	if h.quantized != nil && !quantizationOptions(ctx).ignore {
		scorer := h.quantized.query(normalized)
		defer scorer.release()
		ctx = context.WithValue(ctx, hnswQuantizedQueryKey{}, scorer)
		minSim32 = float32(math.Inf(-1))
	}
	var candidates []hnswDistItem
	var err error
	if ef >= h.liveCount {
		// A beam as wide as the live population must return all of it. The
		// graph can't promise that (insert-time pruning can drop a node's last
		// incoming link), so score every live vector instead: the same O(n)
		// work the beam would do.
		candidates, err = h.scanLiveLocked(ctx, normalized)
	} else {
		candidates, err = h.levelZeroSearchLocked(ctx, normalized, ef, entryIDs)
	}
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
	Quantization      *hnswQuantizationSnapshot
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
	Fingerprints      []uint64 `msgpack:",omitempty"`
	LiveCount         int
	Vectors           []float32
	EntryPoint        uint32
	HasEntryPoint     bool
	MaxLevel          int
}

// Save writes graph structure, IDs and any compressed model to path (msgpack).
// Original vectors remain in their canonical vector store and are resolved on load.
// Dir is created if needed. Copies index data under a short read lock so I/O does not block Search/Add/Remove.
func (h *HNSWIndex) Save(path string) error {
	return saveHNSWSnapshot(path, h.snapshot())
}

// snapshot copies the graph's persisted form under a short read lock.
func (h *HNSWIndex) snapshot() *hnswIndexSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	idToInternal := make(map[string]uint32, len(h.idToInternal))
	for k, v := range h.idToInternal {
		idToInternal[k] = v
	}
	return &hnswIndexSnapshot{
		Quantization:      h.quantized.snapshot(),
		Version:           hnswIndexFormatVersionGraphOnly,
		Config:            h.config,
		Dimensions:        h.dimensions,
		NodeLevel:         append([]uint16(nil), h.nodeLevel...),
		NeighborsArena:    append([]uint32(nil), h.neighborsArena...),
		NeighborsOff:      append([]int32(nil), h.neighborsOff...),
		NeighborCountsAr:  append([]uint16(nil), h.neighborCountsArena...),
		NeighborCountsOff: append([]int32(nil), h.neighborCountsOff...),
		IDToInternal:      idToInternal,
		InternalToID:      append([]string(nil), h.internalToID...),
		Deleted:           append([]bool(nil), h.deleted...),
		Fingerprints:      append([]uint64(nil), h.fingerprints...),
		LiveCount:         h.liveCount,
		EntryPoint:        h.entryPoint,
		HasEntryPoint:     h.hasEntryPoint,
		MaxLevel:          h.maxLevel,
	}
}

// liveIDs are the IDs of the snapshot's live (not deleted) entries.
func (snap *hnswIndexSnapshot) liveIDs() []string {
	ids := make([]string, 0, snap.LiveCount)
	for internal, id := range snap.InternalToID {
		if internal < len(snap.Deleted) && snap.Deleted[internal] {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// saveHNSWSnapshot writes snap to path (msgpack) atomically.
func saveHNSWSnapshot(path string, snap *hnswIndexSnapshot) error {
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

	if err := encodeMsgpackBuffered(tmpFile, snap); err != nil {
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

	quantized, validQuantization := restoreHNSWQuantization(snap.Quantization, snap.Dimensions, len(snap.InternalToID))
	if !validQuantization {
		return nil, nil
	}
	config := snap.Config
	if config.M == 0 {
		config = DefaultHNSWConfig()
	}
	h := NewHNSWIndex(snap.Dimensions, config)
	h.mu.Lock()
	h.quantized = quantized
	h.nodeLevel = snap.NodeLevel
	h.neighborsArena = snap.NeighborsArena
	h.neighborsOff = snap.NeighborsOff
	h.neighborCountsArena = snap.NeighborCountsAr
	h.neighborCountsOff = snap.NeighborCountsOff
	h.idToInternal = snap.IDToInternal
	h.internalToID = snap.InternalToID
	h.deleted = snap.Deleted
	if len(snap.Fingerprints) == len(snap.InternalToID) {
		h.fingerprints = snap.Fingerprints
	}
	h.liveCount = snap.LiveCount
	h.rebuildLiveOrderLocked()
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

// ivfHNSWManifestName names the file that makes a saved IVF-HNSW generation
// current. Cluster files are written first under names unique to the
// generation (g<seq>-<cid>); the manifest is written last (temp file,
// rename), so a restore reads either the previous complete generation or the
// new one, never a mix. Files the manifest does not name are deleted after it
// is written (older generations, clusters beyond K, legacy numeric files).
const ivfHNSWManifestName = "MANIFEST"

// ivfHNSWManifest describes one persisted IVF-HNSW generation.
type ivfHNSWManifest struct {
	Seq        uint64   `json:"seq"`
	Dimensions int      `json:"dimensions"`
	Files      []string `json:"files"` // Files[cid] is cluster cid's graph file; "" when not saved
	// Settings are the build settings the generation was built with
	// (ivfHNSWGenerationSettings); restore requires the current ones.
	Settings string `json:"settings"`
	dir      string
}

func ivfHNSWDir(hnswPath string) string {
	return filepath.Join(filepath.Dir(hnswPath), "hnsw_ivf")
}

// readIVFHNSWManifest reads the current generation's manifest; (nil, nil)
// when none was written.
func readIVFHNSWManifest(ivfDir string) (*ivfHNSWManifest, error) {
	file, err := security.OpenRootedFile(filepath.Join(ivfDir, ivfHNSWManifestName), os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	var m ivfHNSWManifest
	if err := json.NewDecoder(file.File).Decode(&m); err != nil {
		return nil, fmt.Errorf("IVF-HNSW manifest: %w", err)
	}
	for _, name := range m.Files {
		if name != "" && (filepath.Base(name) != name || name == ivfHNSWManifestName) {
			return nil, fmt.Errorf("IVF-HNSW manifest: invalid cluster file %q", name)
		}
	}
	m.dir = ivfDir
	return &m, nil
}

// clusterPath returns the file holding cluster cid's graph, or "".
func (m *ivfHNSWManifest) clusterPath(cid int) string {
	if m == nil || cid < 0 || cid >= len(m.Files) || m.Files[cid] == "" {
		return ""
	}
	return filepath.Join(m.dir, m.Files[cid])
}

// loadCluster loads cluster cid's graph of this generation in lookup mode;
// (nil, nil) when the generation has no file for it.
func (m *ivfHNSWManifest) loadCluster(cid int, vectorLookup VectorLookup) (*HNSWIndex, error) {
	path := m.clusterPath(cid)
	if path == "" || vectorLookup == nil {
		return nil, nil
	}
	return LoadHNSWIndex(path, vectorLookup)
}

// SaveIVFHNSW persists one generation of per-cluster HNSW indexes to disk
// under hnsw_ivf/ alongside hnsw, graph-only, described by a manifest written
// last. hnswPath is the full path to the single HNSW file (e.g.
// data/search/dbname/hnsw).
func SaveIVFHNSW(hnswPath string, graphs map[int]*HNSWIndex) error {
	return SaveIVFHNSWWithContext(context.Background(), hnswPath, graphs)
}

// SaveIVFHNSWWithContext captures every graph before writing any file, so
// the generation saved is the graphs as they were at the call; callers that
// change the graphs concurrently capture them under their own exclusion
// (Service persistence: snapshotClusterGenerationLocked).
func SaveIVFHNSWWithContext(ctx context.Context, hnswPath string, graphs map[int]*HNSWIndex) error {
	snaps := make(map[int]*hnswIndexSnapshot, len(graphs))
	for cid, graph := range graphs {
		if graph != nil {
			snaps[cid] = graph.snapshot()
		}
	}
	return saveIVFHNSWSnapshots(ctx, hnswPath, snaps)
}

// saveIVFHNSWSnapshots writes captured cluster graphs as one generation:
// cluster files under names unique to the generation, then the manifest
// (with the current build settings), then the prune.
func saveIVFHNSWSnapshots(ctx context.Context, hnswPath string, graphs map[int]*hnswIndexSnapshot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if hnswPath == "" || len(graphs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ivfDir := ivfHNSWDir(hnswPath)
	if err := security.EnsureRootedParent(filepath.Join(ivfDir, ivfHNSWManifestName), 0o755); err != nil {
		return err
	}
	m := ivfHNSWManifest{Seq: 1, Settings: ivfHNSWGenerationSettings(), dir: ivfDir}
	if prev, _ := readIVFHNSWManifest(ivfDir); prev != nil {
		m.Seq = prev.Seq + 1
	}
	numClusters := 0
	for cid := range graphs {
		if cid >= numClusters {
			numClusters = cid + 1
		}
	}
	m.Files = make([]string, numClusters)
	for cid := 0; cid < numClusters; cid++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		snap := graphs[cid]
		if snap == nil {
			continue
		}
		name := fmt.Sprintf("g%d-%d", m.Seq, cid)
		if err := saveHNSWSnapshot(filepath.Join(ivfDir, name), snap); err != nil {
			return fmt.Errorf("cluster %d: %w", cid, err)
		}
		m.Files[cid] = name
		if m.Dimensions == 0 {
			m.Dimensions = snap.Dimensions
		}
	}
	if m.Dimensions == 0 {
		return nil
	}
	if err := writeIVFHNSWManifest(&m); err != nil {
		return err
	}
	pruneIVFHNSWFiles(&m)
	return nil
}

func writeIVFHNSWManifest(m *ivfHNSWManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := filepath.Join(m.dir, ivfHNSWManifestName)
	tmpPath := path + ".tmp"
	tmpFile, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = security.RemoveRootedPath(tmpPath)
	}()
	if _, err := tmpFile.Write(data); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return security.RenameRootedFile(tmpPath, path)
}

// pruneIVFHNSWFiles deletes every file in the generation's directory that
// its manifest does not name.
func pruneIVFHNSWFiles(m *ivfHNSWManifest) {
	keep := map[string]bool{ivfHNSWManifestName: true}
	for _, name := range m.Files {
		keep[name] = true
	}
	entries, err := security.ReadRootedDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || keep[e.Name()] {
			continue
		}
		_ = security.RemoveRootedPath(filepath.Join(m.dir, e.Name()))
	}
}

// LoadIVFHNSWCluster loads one cluster's HNSW index of the persisted
// generation in lookup mode (no vector copy in HNSW RAM). Returns (nil, nil)
// if there is no generation, no file for the cluster, or it is invalid
// (caller can build). hnswPath is the full path to the single HNSW file.
func LoadIVFHNSWCluster(hnswPath string, clusterID int, vectorLookup VectorLookup) (*HNSWIndex, error) {
	if hnswPath == "" || vectorLookup == nil {
		return nil, nil
	}
	m, err := readIVFHNSWManifest(ivfHNSWDir(hnswPath))
	if err != nil || m == nil {
		return nil, nil
	}
	return m.loadCluster(clusterID, vectorLookup)
}

// loadIVFClusterMemberIDs decodes a cluster file and returns its live member
// IDs (deleted slots excluded) without loading vectors.
func loadIVFClusterMemberIDs(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
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
	return snap.liveIDs(), nil
}

// DeriveIVFCentroidsFromClusters builds centroids and idToCluster from the
// persisted generation's cluster files and vectors from the vector index. No
// separate centroid file. Returns (nil, nil, nil) if no generation was
// persisted or derivation fails.
func DeriveIVFCentroidsFromClusters(hnswPath string, vectorLookup VectorLookup) (centroids [][]float32, idToCluster map[string]int, err error) {
	if hnswPath == "" || vectorLookup == nil {
		return nil, nil, nil
	}
	ivfDir := ivfHNSWDir(hnswPath)
	m, mErr := readIVFHNSWManifest(ivfDir)
	if mErr != nil || m == nil {
		logSearchPrintf("[IVF-HNSW] ⚠️ DeriveIVFCentroidsFromClusters: no persisted generation in %q (%v); k-means will run", ivfDir, mErr)
		return nil, nil, nil
	}
	return deriveIVFCentroidsFromManifest(m, vectorLookup)
}

// deriveIVFCentroidsFromManifest derives the centroids (indexed by cluster ID,
// one per manifest entry) and membership of one persisted generation, from
// its graphs' live members. A generation whose named files cannot be read,
// or that holds a live member in more than one cluster, is not one
// population and is rejected.
func deriveIVFCentroidsFromManifest(m *ivfHNSWManifest, vectorLookup VectorLookup) (centroids [][]float32, idToCluster map[string]int, err error) {
	numClusters := len(m.Files)
	if numClusters == 0 {
		return nil, nil, nil
	}
	idToCluster = make(map[string]int)
	dims := 0
	centroidSums := make([][]float64, numClusters)
	centroidCounts := make([]int, numClusters)

	for cid := 0; cid < numClusters; cid++ {
		path := m.clusterPath(cid)
		if path == "" {
			continue
		}
		memberIDs, err := loadIVFClusterMemberIDs(path)
		if err != nil {
			return nil, nil, fmt.Errorf("IVF-HNSW generation %d: cluster %d: %w", m.Seq, cid, err)
		}
		for _, id := range memberIDs {
			if prev, dup := idToCluster[id]; dup {
				return nil, nil, fmt.Errorf("IVF-HNSW generation %d: %q is a live member of clusters %d and %d", m.Seq, id, prev, cid)
			}
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
		centroidSums[cid] = make([]float64, dims)
		for _, v := range vecs {
			for d := 0; d < dims; d++ {
				centroidSums[cid][d] += float64(v[d])
			}
		}
		centroidCounts[cid] = len(vecs)
	}

	if dims == 0 {
		logSearchPrintf("[IVF-HNSW] ⚠️ DeriveIVFCentroidsFromClusters: no vectors found for any cluster in %q (vectorLookup returned nothing for cluster member IDs); k-means will run", m.dir)
		return nil, nil, nil
	}
	// Dense slice so centroid index matches cluster IDs (0, 1, 2, ...); RestoreClusteringState expects cid < len(centroids).
	centroids = make([][]float32, numClusters)
	for cid := 0; cid < numClusters; cid++ {
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
	if h.liveOrderDead++; h.liveOrderDead > h.liveCount {
		h.compactLiveOrderLocked()
	}

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
		// Allocation-free bounds check: this loop visits every node on each
		// removal of the entry point or of a top-level node.
		if uint64(id) > uint64(^uint32(0)) {
			break
		}
		internalID := uint32(id)
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
	scorer, _ := ctx.Value(hnswQuantizedQueryKey{}).(*hnswQuantizedQuery)
	currentDist := h.graphDistanceLocked(query, current, scorer)

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
			dist := h.graphDistanceLocked(query, neighborID, scorer)
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
	ids, _, _ := searchLayerHeapKernel[layerNoCtx](h, nil, query, entryID, nil, ef, level, true)
	return ids
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
	if len(entryIDs) == 0 {
		return nil, nil
	}
	_, items, err := searchLayerHeapKernel[layerWithCtx](h, ctx, query, entryIDs[0], entryIDs[1:], ef, level, false)
	return items, err
}

// layerCtxMode selects the context-probe policy of searchLayerHeapKernel at
// compile time: layerNoCtx compiles without any probe (the build hot path),
// layerWithCtx probes once per candidate pop and once per 32 neighbors (the
// query path).
type layerCtxMode interface {
	popProbe(ctx context.Context) error
	neighborProbe(ctx context.Context) error
}

type layerNoCtx struct{}

func (layerNoCtx) popProbe(ctx context.Context) error      { return nil }
func (layerNoCtx) neighborProbe(ctx context.Context) error { return nil }

type layerWithCtx struct{}

func (layerWithCtx) popProbe(ctx context.Context) error      { return ctx.Err() }
func (layerWithCtx) neighborProbe(ctx context.Context) error { return ctx.Err() }

// searchLayerHeapKernel expands one graph layer from the given entry points
// with the shared visited-state and candidate/result heap machinery. It is the
// single implementation behind searchLayerHeap (build path) and
// searchLayerHeapPooledFromEntriesWithContext (query path); a recall fix such
// as #433 can no longer land in only one copy.
//
// The type argument M selects the context-probe policy at compile time (see
// layerCtxMode): layerNoCtx has every probe dead-code-eliminated so the build
// hot path stays branch-free, layerWithCtx probes once per candidate pop and
// once per 32 neighbors. wantIDs selects the output shape once per call:
// caller IDs via idsPool, or distance items via itemsPool (both
// closest-first; the returned slice belongs to its pool until the caller
// releases it).
//
// Entry seeding validates validity, prior visits and vector dimensions. The
// build path always passes a live entry point, so those probes never fire
// there; on the query path they guard the caller-supplied entry IDs.
// Tombstones, entries included, are routed through and never scored or
// returned (see relaxLayerNeighbors).
func searchLayerHeapKernel[M layerCtxMode](h *HNSWIndex, ctx context.Context, query []float32, entryID uint32, extraEntries []uint32, ef int, level int, wantIDs bool) (ids []uint32, items []hnswDistItem, err error) {
	var mode M
	var scorer *hnswQuantizedQuery
	if ctx != nil {
		scorer, _ = ctx.Value(hnswQuantizedQueryKey{}).(*hnswQuantizedQuery)
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

	// Tombstoned entries (an upper-layer descent may end on one) are routed
	// through like any other tombstone.
	var bridgeBuf [8]uint32
	bridges := bridgeBuf[:0]
	if h.seedLayerEntry(candidates, results, visited, curGen, query, ef, entryID, scorer) {
		bridges = append(bridges, entryID)
	}
	for _, id := range extraEntries {
		if h.seedLayerEntry(candidates, results, visited, curGen, query, ef, id, scorer) {
			bridges = append(bridges, id)
		}
	}
	if len(bridges) > 0 {
		if bridges, err = relaxLayerNeighbors[M](h, ctx, query, bridges, ef, level, candidates, results, visited, curGen, scorer); err != nil {
			return nil, nil, err
		}
	}

	for candidates.Len() > 0 {
		if err := mode.popProbe(ctx); err != nil {
			return nil, nil, err
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
		if bridges, err = relaxLayerNeighbors[M](h, ctx, query, append(bridges[:0], nodeID), ef, level, candidates, results, visited, curGen, scorer); err != nil {
			return nil, nil, err
		}
	}

	if wantIDs {
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
		return resultList, nil, nil
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
	return nil, buf, nil
}

// relaxLayerNeighbors pushes the unvisited live neighbours of the nodes on
// stack that improve the result set. A tombstone keeps its links until a
// rebuild, and live nodes may be connected only through it, so a tombstoned
// neighbour is never scored or returned (its vector may no longer resolve)
// but routed through: its own neighbours are relaxed as the current node's.
// Otherwise a removal silently cuts those live nodes off. The stack is
// returned for reuse.
func relaxLayerNeighbors[M layerCtxMode](h *HNSWIndex, ctx context.Context, query []float32, stack []uint32, ef int, level int, candidates, results *distHeap, visited *visitedGenState, curGen uint16, scorer *hnswQuantizedQuery) ([]uint32, error) {
	var mode M
	for len(stack) > 0 {
		nodeID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		neighbors, ok := h.neighborsAtLevelLocked(nodeID, level)
		if !ok {
			continue
		}

		// Reverse iteration: order doesn't matter when checking all neighbors
		for i := len(neighbors) - 1; i >= 0; i-- {
			if i&31 == 0 {
				if err := mode.neighborProbe(ctx); err != nil {
					return stack, err
				}
			}
			neighborID := neighbors[i]
			if !validHNSWIndex(neighborID, len(h.nodeLevel)) {
				continue
			}
			if visited.gen[neighborID] == curGen {
				continue
			}
			visited.gen[neighborID] = curGen
			if h.deleted[neighborID] {
				stack = append(stack, neighborID)
				continue
			}

			dist := h.graphDistanceLocked(query, neighborID, scorer)

			if results.Len() < ef || dist < results.Peek().dist {
				candidates.Push(hnswDistItem{id: neighborID, dist: dist})
				if results.Len() < ef {
					results.Push(hnswDistItem{id: neighborID, dist: dist})
				} else {
					// A full beam admits only a closer item. Replace its worst
					// member with one downward sift instead of push then pop.
					results.items[0] = hnswDistItem{id: neighborID, dist: dist}
					results.siftDown(0)
				}
			}
		}
	}
	return stack, nil
}

// seedLayerEntry validates and seeds one entry point into the candidate and
// result heaps. It is a small helper (one call per entry point, not per
// neighbor) shared by the single- and multi-entry instantiations. It reports
// a tombstoned entry, which the caller routes through instead of seeding.
func (h *HNSWIndex) seedLayerEntry(candidates, results *distHeap, visited *visitedGenState, curGen uint16, query []float32, ef int, entryID uint32, scorer *hnswQuantizedQuery) bool {
	if !validHNSWIndex(entryID, len(h.nodeLevel)) || visited.gen[entryID] == curGen {
		return false
	}
	if h.deleted[entryID] {
		visited.gen[entryID] = curGen
		return true
	}
	var entryDist float32
	if scorer != nil {
		entryDist = 1 - scorer.dot(entryID)
	} else {
		entryVector := h.vectorAtLocked(entryID)
		if len(entryVector) != h.dimensions {
			return false
		}
		entryDist = 1 - vector.DotProductSIMD(query, entryVector)
	}
	visited.gen[entryID] = curGen
	item := hnswDistItem{id: entryID, dist: entryDist}
	candidates.Push(item)
	results.Push(item)
	if results.Len() > ef {
		_ = results.Pop()
	}
	return false
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
