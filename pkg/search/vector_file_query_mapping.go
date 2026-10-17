package search

import "github.com/orneryd/nornicdb/pkg/math/vector"

// lockQueryRead returns with mu read-locked. Mapped views stay inside that
// lock; append publication, reload, compaction and close cannot retire them.
// Platforms without mappings retain the existing ReadAt implementation.
func (v *VectorFileStore) lockQueryRead() error {
	for {
		v.mu.RLock()
		if !vectorFileMappingSupported || v.closed || v.file == nil || v.nextOrdinal == 0 ||
			(v.queryReadMap != nil && v.queryReadSlots == v.nextOrdinal) {
			return nil
		}
		v.mu.RUnlock()
		v.mu.Lock()
		err := v.refreshQueryReadMapLocked()
		v.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (v *VectorFileStore) refreshQueryReadMapLocked() error {
	if v.closed || v.file == nil || v.nextOrdinal == 0 ||
		(v.queryReadMap != nil && v.queryReadSlots == v.nextOrdinal) {
		return nil
	}
	if err := v.releaseQueryReadMapLocked(); err != nil {
		return err
	}
	mapped, err := v.newMappedReadLeaseLocked()
	if err != nil {
		return err
	}
	v.queryReadMap = mapped
	v.queryReadSlots = v.nextOrdinal
	return nil
}

func (v *VectorFileStore) releaseQueryReadMapLocked() error {
	if v.queryReadMap == nil {
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
	if !vectorFileMappingSupported {
		stored, ok := v.GetVector(id)
		if !ok {
			return 0, false
		}
		return float64(vector.DotProductSIMD(query, stored)), true
	}
	if err := v.lockQueryRead(); err != nil {
		return 0, false
	}
	defer v.mu.RUnlock()
	if v.closed || v.queryReadMap == nil {
		return 0, false
	}
	ordinal, ok := v.idToOrdinal[id]
	if !ok || ordinal < 0 || ordinal >= v.nextOrdinal {
		return 0, false
	}
	stored, ok := v.queryReadMap.Lookup(id)
	if !ok {
		return 0, false
	}
	return float64(vector.DotProductSIMD(query, stored)), true
}
