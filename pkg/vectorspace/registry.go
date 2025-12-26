// Package vectorspace defines the per-database vector space identity and registry.
//
// A vector space is uniquely identified by:
//   - database name
//   - type/collection name
//   - vectorName (named vectors or the special "chunks" space)
//   - dimensions
//   - distance metric
//
// The registry is intentionally lightweight; it tracks which vector spaces
// exist and which backend each space should use. Future PRs will attach the
// actual indexing implementations and counters.
package vectorspace

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DistanceMetric enumerates supported vector distance metrics.
type DistanceMetric string

// Supported distance metrics for vector search.
const (
	DistanceCosine    DistanceMetric = "cosine"
	DistanceDot       DistanceMetric = "dot"
	DistanceEuclidean DistanceMetric = "euclidean"
)

var supportedDistances = map[DistanceMetric]struct{}{
	DistanceCosine:    {},
	DistanceDot:       {},
	DistanceEuclidean: {},
}

// DefaultVectorName is the canonical name for unnamed vector spaces.
const DefaultVectorName = "default"

// ChunkVectorName is reserved for chunk-level vector search.
const ChunkVectorName = "chunks"

// BackendKind identifies the indexing backend assigned to a vector space.
type BackendKind string

// Known backend identifiers. The registry does not enforce the set strictly to
// allow experimentation while keeping canonical casing.
const (
	BackendAuto       BackendKind = "auto"
	BackendBruteForce BackendKind = "brute-force"
	BackendHNSW       BackendKind = "hnsw"
)

// VectorSpaceKey uniquely identifies a vector space.
type VectorSpaceKey struct {
	DB         string
	Type       string
	VectorName string
	Dims       int
	Distance   DistanceMetric
}

// Canonical returns a normalized copy of the key:
//   - trims whitespace
//   - lowercases identifiers
//   - defaults vectorName to "default" when empty
//   - defaults distance to cosine when empty
//   - validates dimensions > 0 and distance support
func (k VectorSpaceKey) Canonical() (VectorSpaceKey, error) {
	db := normalizeIdentifier(k.DB)
	typ := normalizeIdentifier(k.Type)
	if db == "" || typ == "" {
		return VectorSpaceKey{}, fmt.Errorf("vector space key requires db and type")
	}
	if k.Dims <= 0 {
		return VectorSpaceKey{}, fmt.Errorf("vector space dimensions must be > 0")
	}

	name := normalizeIdentifier(k.VectorName)
	if name == "" {
		name = DefaultVectorName
	}

	dist, err := normalizeDistance(k.Distance)
	if err != nil {
		return VectorSpaceKey{}, err
	}

	return VectorSpaceKey{
		DB:         db,
		Type:       typ,
		VectorName: name,
		Dims:       k.Dims,
		Distance:   dist,
	}, nil
}

// Hash returns the canonical string representation used for registry lookups.
func (k VectorSpaceKey) Hash() (string, error) {
	canonical, err := k.Canonical()
	if err != nil {
		return "", err
	}
	return buildKeyString(canonical), nil
}

// VectorSpace represents a registered vector space and its chosen backend.
type VectorSpace struct {
	Key     VectorSpaceKey
	Backend BackendKind

	vectorCount int64
}

// SetVectorCount updates the tracked vector count for this space.
func (vs *VectorSpace) SetVectorCount(count int64) {
	atomic.StoreInt64(&vs.vectorCount, count)
}

// IncrementVectorCount adjusts the tracked vector count and returns the new value.
func (vs *VectorSpace) IncrementVectorCount(delta int64) int64 {
	return atomic.AddInt64(&vs.vectorCount, delta)
}

func (vs *VectorSpace) stats(hash string) VectorSpaceStats {
	return VectorSpaceStats{
		Key:         vs.Key,
		KeyHash:     hash,
		Dimensions:  vs.Key.Dims,
		Distance:    vs.Key.Distance,
		Backend:     vs.Backend,
		VectorCount: atomic.LoadInt64(&vs.vectorCount),
	}
}

// VectorSpaceStats captures introspection data for a vector space.
type VectorSpaceStats struct {
	Key         VectorSpaceKey
	KeyHash     string
	Dimensions  int
	Distance    DistanceMetric
	Backend     BackendKind
	VectorCount int64
}

// IndexRegistry tracks vector spaces per database.
type IndexRegistry struct {
	mu     sync.RWMutex
	spaces map[string]*VectorSpace
}

// NewIndexRegistry creates an empty registry.
func NewIndexRegistry() *IndexRegistry {
	return &IndexRegistry{
		spaces: make(map[string]*VectorSpace),
	}
}

// CreateSpace registers a vector space or returns the existing one.
func (r *IndexRegistry) CreateSpace(key VectorSpaceKey, backend BackendKind) (*VectorSpace, error) {
	canonical, err := key.Canonical()
	if err != nil {
		return nil, err
	}
	hash := buildKeyString(canonical)
	backend = normalizeBackend(backend)

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.spaces[hash]; ok {
		return existing, nil
	}

	space := &VectorSpace{
		Key:     canonical,
		Backend: backend,
	}
	r.spaces[hash] = space
	return space, nil
}

// GetSpace returns the space for the given key if it exists.
func (r *IndexRegistry) GetSpace(key VectorSpaceKey) (*VectorSpace, bool) {
	canonical, err := key.Canonical()
	if err != nil {
		return nil, false
	}
	hash := buildKeyString(canonical)

	r.mu.RLock()
	defer r.mu.RUnlock()

	space, ok := r.spaces[hash]
	return space, ok
}

// DeleteSpace removes the space for the given key.
func (r *IndexRegistry) DeleteSpace(key VectorSpaceKey) bool {
	canonical, err := key.Canonical()
	if err != nil {
		return false
	}
	hash := buildKeyString(canonical)

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.spaces[hash]; ok {
		delete(r.spaces, hash)
		return true
	}
	return false
}

// Stats returns a snapshot of all registered spaces.
func (r *IndexRegistry) Stats() []VectorSpaceStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make([]VectorSpaceStats, 0, len(r.spaces))
	for hash, space := range r.spaces {
		stats = append(stats, space.stats(hash))
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].KeyHash < stats[j].KeyHash
	})
	return stats
}

func normalizeIdentifier(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func normalizeDistance(dist DistanceMetric) (DistanceMetric, error) {
	d := DistanceMetric(strings.ToLower(strings.TrimSpace(string(dist))))
	if d == "" {
		d = DistanceCosine
	}
	if _, ok := supportedDistances[d]; !ok {
		return "", fmt.Errorf("unsupported distance metric %q", dist)
	}
	return d, nil
}

func normalizeBackend(kind BackendKind) BackendKind {
	k := BackendKind(strings.ToLower(strings.TrimSpace(string(kind))))
	if k == "" {
		return BackendAuto
	}
	return k
}

func buildKeyString(key VectorSpaceKey) string {
	return fmt.Sprintf("%s|%s|%s|%d|%s", key.DB, key.Type, key.VectorName, key.Dims, key.Distance)
}
