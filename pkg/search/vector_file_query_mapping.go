package search

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// lockQueryRead returns with mu read-locked. Mapped views stay inside that
// lock; append publication, reload, compaction and close cannot retire them.
// Platforms without mappings, and files the platform would not map, retain
// the existing ReadAt implementation.
//
// The mapping is refreshed only when the write lock is free at once: callers
// may already hold the read lock (HNSW construction looks vectors up inside
// IterateChunked), so waiting for it would deadlock. Rows the current mapping
// does not cover are read with ReadAt until a later query refreshes it.
func (v *VectorFileStore) lockQueryRead() error {
	v.mu.RLock()
	if !vectorFileMappingSupported || v.closed || v.file == nil || v.nextOrdinal == 0 ||
		v.queryReadSlots == v.nextOrdinal {
		return nil
	}
	v.mu.RUnlock()
	if v.mu.TryLock() {
		err := v.refreshQueryReadMapLocked()
		v.mu.Unlock()
		if err != nil {
			return err
		}
	}
	v.mu.RLock()
	return nil
}

// refreshQueryReadMapLocked establishes the query read state for the current
// slot count: a mapping, or copied reads when the readable file cannot be
// mapped. Mapping is retried once the file grows. File errors are returned.
func (v *VectorFileStore) refreshQueryReadMapLocked() error {
	if v.closed || v.file == nil || v.nextOrdinal == 0 || v.queryReadSlots == v.nextOrdinal {
		return nil
	}
	copiedBefore := v.queryReadMap == nil && v.queryReadSlots != 0
	if err := v.releaseQueryReadMapLocked(); err != nil {
		return err
	}
	mapped, err := v.newMappedReadLeaseLocked()
	if errors.Is(err, errVectorFileUnmapped) {
		if !copiedBefore {
			logSearchPrintf("[VectorFileStore] query mapping of %s unavailable; using copied reads: %v", v.vecPath, err)
		}
		err = nil
	}
	if err != nil {
		return err
	}
	v.queryReadMap = mapped
	v.queryReadSlots = v.nextOrdinal
	return nil
}

func (v *VectorFileStore) releaseQueryReadMapLocked() error {
	if v.queryReadMap == nil {
		v.queryReadSlots = 0
		return nil
	}
	err := v.queryReadMap.Close()
	v.queryReadMap = nil
	v.queryReadSlots = 0
	return err
}

// scoreVectorDot borrows a mapped row only for the duration of this call.
// GetVector keeps owning-copy semantics for callers that retain vector values.
func (v *VectorFileStore) scoreVectorDot(query []float32, id string) (float64, bool) {
	if err := v.lockQueryRead(); err != nil {
		return 0, false
	}
	defer v.mu.RUnlock()
	stored, _, ok := v.vectorLocked(id)
	if !ok {
		return 0, false
	}
	return float64(vector.DotProductSIMD(query, stored)), true
}
