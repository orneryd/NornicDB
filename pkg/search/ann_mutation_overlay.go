package search

import (
	"context"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// annMutationOverlay keeps compressed ANN results correct between full index
// rebuilds. New and updated vectors are searched exactly; tombstones suppress
// stale entries that still exist in the immutable compressed index.
type annMutationOverlay struct {
	mu         sync.RWMutex
	vectors    map[string][]float32
	tombstones map[string]struct{}
}

type annMutationOverlaySnapshot struct {
	Vectors          map[string][]float32 `msgpack:"vectors,omitempty"`
	Tombstones       []string             `msgpack:"tombstones,omitempty"`
	VectorStoreCount int                  `msgpack:"vector_store_count,omitempty"`
	VectorStoreSlots int64                `msgpack:"vector_store_slots,omitempty"`
}

func newANNMutationOverlay() *annMutationOverlay {
	return &annMutationOverlay{
		vectors:    make(map[string][]float32),
		tombstones: make(map[string]struct{}),
	}
}

func (o *annMutationOverlay) Add(id string, value []float32) {
	if o == nil || id == "" || len(value) == 0 {
		return
	}
	o.addNormalized(id, vector.Normalize(value))
}

// addNormalized stores an immutable normalized vector reference. The caller
// owns the immutability contract; this avoids duplicating the canonical
// in-memory vector for HNSW's short-lived rebuild delta log.
func (o *annMutationOverlay) addNormalized(id string, value []float32) {
	if o == nil || id == "" || len(value) == 0 {
		return
	}
	o.mu.Lock()
	o.vectors[id] = value
	delete(o.tombstones, id)
	o.mu.Unlock()
}

func (o *annMutationOverlay) Remove(id string) {
	if o == nil || id == "" {
		return
	}
	o.mu.Lock()
	delete(o.vectors, id)
	o.tombstones[id] = struct{}{}
	o.mu.Unlock()
}

func (o *annMutationOverlay) Reset() {
	if o == nil {
		return
	}
	o.mu.Lock()
	clear(o.vectors)
	clear(o.tombstones)
	o.mu.Unlock()
}

func (o *annMutationOverlay) Search(ctx context.Context, query []float32, limit int, minSimilarity float64) []Candidate {
	if o == nil || limit <= 0 {
		return nil
	}
	normalizedQuery := vector.Normalize(query)
	o.mu.RLock()
	candidates := make([]Candidate, 0, min(limit, len(o.vectors)))
	for id, value := range o.vectors {
		if ctx != nil {
			select {
			case <-ctx.Done():
				o.mu.RUnlock()
				return nil
			default:
			}
		}
		score := float64(vector.DotProduct(normalizedQuery, value))
		if score >= minSimilarity {
			candidates = append(candidates, Candidate{ID: id, Score: score})
		}
	}
	o.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func (o *annMutationOverlay) IsRemoved(id string) bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	_, removed := o.tombstones[id]
	o.mu.RUnlock()
	return removed
}

func (o *annMutationOverlay) snapshot() annMutationOverlaySnapshot {
	if o == nil {
		return annMutationOverlaySnapshot{}
	}
	o.mu.RLock()
	snapshot := annMutationOverlaySnapshot{
		Vectors:    make(map[string][]float32, len(o.vectors)),
		Tombstones: make([]string, 0, len(o.tombstones)),
	}
	for id, value := range o.vectors {
		snapshot.Vectors[id] = append([]float32(nil), value...)
	}
	for id := range o.tombstones {
		snapshot.Tombstones = append(snapshot.Tombstones, id)
	}
	o.mu.RUnlock()
	sort.Strings(snapshot.Tombstones)
	return snapshot
}

func (o *annMutationOverlay) restore(snapshot annMutationOverlaySnapshot) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.vectors = make(map[string][]float32, len(snapshot.Vectors))
	o.tombstones = make(map[string]struct{}, len(snapshot.Tombstones))
	for id, value := range snapshot.Vectors {
		o.vectors[id] = append([]float32(nil), value...)
	}
	for _, id := range snapshot.Tombstones {
		o.tombstones[id] = struct{}{}
	}
	o.mu.Unlock()
}

func (s annMutationOverlaySnapshot) matchesVectorStore(store *VectorFileStore) bool {
	if store == nil {
		return true
	}
	if s.VectorStoreSlots <= 0 {
		return false
	}
	count, slots := store.stateVersion()
	return count == s.VectorStoreCount && slots == s.VectorStoreSlots
}

// size is the number of vectors the overlay holds.
func (o *annMutationOverlay) size() int {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.vectors)
}
