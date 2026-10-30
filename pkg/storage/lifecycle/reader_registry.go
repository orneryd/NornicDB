package lifecycle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type activeReader struct {
	info       storage.SnapshotReaderInfo
	registered time.Time
}

// ReaderRegistry tracks active MVCC snapshot readers.
type ReaderRegistry struct {
	mu      sync.RWMutex
	readers map[string]*activeReader
	seq     atomic.Uint64
}

// NewReaderRegistry creates an empty reader registry.
func NewReaderRegistry() *ReaderRegistry {
	return &ReaderRegistry{readers: make(map[string]*activeReader)}
}

// Register adds a reader and returns its ID and deregistration callback.
func (r *ReaderRegistry) Register(info storage.SnapshotReaderInfo) (string, func()) {
	id := info.ReaderID
	if id == "" {
		id = fmt.Sprintf("reader-%d", r.seq.Add(1))
	}
	registered := time.Now()
	copyInfo := info
	copyInfo.ReaderID = id
	r.mu.Lock()
	r.readers[id] = &activeReader{info: copyInfo, registered: registered}
	r.mu.Unlock()
	return id, func() {
		r.mu.Lock()
		delete(r.readers, id)
		r.mu.Unlock()
	}
}

// ActiveCount reports the number of active readers.
func (r *ReaderRegistry) ActiveCount() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return int64(len(r.readers))
}

// OldestReaderVersion returns the smallest active snapshot version across
// all namespaces. Used for global metrics; for prune-floor decisions, prefer
// OldestReaderVersionByNamespace because version sequences are per-database
// and not comparable across namespaces.
func (r *ReaderRegistry) OldestReaderVersion() (storage.MVCCVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var oldest storage.MVCCVersion
	have := false
	for _, reader := range r.readers {
		if !have || reader.info.SnapshotVersion.Compare(oldest) < 0 {
			oldest = reader.info.SnapshotVersion
			have = true
		}
	}
	return oldest, have
}

// OldestReaderVersionsByNamespace returns the smallest active snapshot
// version per namespace. A namespace appears in the result only if it has
// at least one registered reader; callers should treat absence as "no
// active reader in this namespace" (effectively no floor).
//
// Per-database MVCC counters mean a reader's CommitSequence in namespace A
// has no ordering relationship to a head's CommitSequence in namespace B —
// so the prune planner must use this map to compute a per-namespace safe
// floor, not the global oldest reader.
func (r *ReaderRegistry) OldestReaderVersionsByNamespace() map[string]storage.MVCCVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]storage.MVCCVersion)
	for _, reader := range r.readers {
		ns := reader.info.Namespace
		existing, ok := out[ns]
		if !ok || reader.info.SnapshotVersion.Compare(existing) < 0 {
			out[ns] = reader.info.SnapshotVersion
		}
	}
	return out
}

// OldestReaderAge returns the maximum age of active readers.
func (r *ReaderRegistry) OldestReaderAge() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	var maxAge time.Duration
	for _, reader := range r.readers {
		age := now.Sub(reader.info.StartTime)
		if age > maxAge {
			maxAge = age
		}
	}
	return maxAge
}

// Snapshot returns a copy of active reader metadata.
func (r *ReaderRegistry) Snapshot() []storage.SnapshotReaderInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	readers := make([]storage.SnapshotReaderInfo, 0, len(r.readers))
	for _, reader := range r.readers {
		readers = append(readers, reader.info)
	}
	return readers
}

// ReadersOlderThan returns readers older than the supplied age.
func (r *ReaderRegistry) ReadersOlderThan(age time.Duration) []storage.SnapshotReaderInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	readers := make([]storage.SnapshotReaderInfo, 0)
	for _, reader := range r.readers {
		if now.Sub(reader.info.StartTime) > age {
			readers = append(readers, reader.info)
		}
	}
	return readers
}
