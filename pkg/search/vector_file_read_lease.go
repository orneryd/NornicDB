package search

import (
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"sync"
	"unsafe"
)

// vectorFileReadLease is an immutable snapshot used by HNSW construction.
// The mapped bytes and ordinal table remain stable until Close. Returned
// vectors are borrowed read-only views and must not escape the callback that
// requested them.
type vectorFileReadLease struct {
	dimensions  int
	idToOrdinal map[string]int64
	data        []byte
	unmap       func() error
	closeOnce   sync.Once
	closeErr    error
}

func (v *VectorFileStore) beginReadLease() (*vectorFileReadLease, error) {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed || v.file == nil {
		return nil, errVecFileClosed
	}
	stat, err := v.file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < 0 || uint64(stat.Size()) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("vector file size %d cannot be mapped on this platform", stat.Size())
	}
	data, unmap, err := mapVectorFileReadOnly(v.file.File, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	ordinals := make(map[string]int64, len(v.idToOrdinal))
	for id, ordinal := range v.idToOrdinal {
		ordinals[id] = ordinal
	}
	return &vectorFileReadLease{
		dimensions:  v.dimensions,
		idToOrdinal: ordinals,
		data:        data,
		unmap:       unmap,
	}, nil
}

func (l *vectorFileReadLease) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		if l.unmap != nil {
			l.closeErr = l.unmap()
		}
		l.data = nil
	})
	return l.closeErr
}

func (l *vectorFileReadLease) Lookup(id string) ([]float32, bool) {
	ordinal, ok := l.idToOrdinal[id]
	if !ok || ordinal < 0 || l.dimensions <= 0 {
		return nil, false
	}
	vectorBytes := l.dimensions * 4
	offset := vecHeaderSize + int(ordinal)*vectorBytes
	if offset < vecHeaderSize || offset+vectorBytes > len(l.data) {
		return nil, false
	}
	bytes := l.data[offset : offset+vectorBytes]
	if nativeLittleEndian {
		return unsafe.Slice((*float32)(unsafe.Pointer(&bytes[0])), l.dimensions), true
	}
	// Mapped platforms supported by NornicDB are little-endian today. Keep a
	// correct fallback for future ports without changing the on-disk format.
	vector := make([]float32, l.dimensions)
	for index := range vector {
		vector[index] = math.Float32frombits(binary.LittleEndian.Uint32(bytes[index*4:]))
	}
	return vector, true
}

func (l *vectorFileReadLease) IterateChunked(chunkSize int, fn func(ids []string, vectors [][]float32) error) error {
	if chunkSize <= 0 {
		chunkSize = 10000
	}
	type ordinalID struct {
		ordinal int64
		id      string
	}
	entries := make([]ordinalID, 0, len(l.idToOrdinal))
	for id, ordinal := range l.idToOrdinal {
		entries = append(entries, ordinalID{ordinal: ordinal, id: id})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ordinal < entries[j].ordinal })
	ids := make([]string, 0, chunkSize)
	vectors := make([][]float32, 0, chunkSize)
	for _, entry := range entries {
		vector, ok := l.Lookup(entry.id)
		if !ok {
			return fmt.Errorf("mapped vector ordinal %d for %q is unavailable", entry.ordinal, entry.id)
		}
		ids = append(ids, entry.id)
		vectors = append(vectors, vector)
		if len(ids) == chunkSize {
			if err := fn(ids, vectors); err != nil {
				return err
			}
			ids = ids[:0]
			vectors = vectors[:0]
		}
	}
	if len(ids) > 0 {
		return fn(ids, vectors)
	}
	return nil
}

var nativeLittleEndian = func() bool {
	value := uint16(1)
	return *(*byte)(unsafe.Pointer(&value)) == 1
}()
