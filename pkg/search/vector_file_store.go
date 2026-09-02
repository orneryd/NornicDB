// Package search provides file-backed vector storage for memory-efficient indexing.
//
// VectorFileStore implements append-only vector storage: vectors are written to a
// fixed-stride .vec file and only ID-to-ordinal metadata is kept in RAM. This allows BuildIndexes
// to index large datasets without holding 2–3× vector data in memory. Vectors are
// stored normalized (one copy per id, cosine-only) per the indexing-memory plan.
package search

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	vecFileMagic             = "NVF\n"
	vecFileVersion           = 2
	vecHeaderSize            = 64
	vectorScoreReadBatchSize = 1024 * 1024
)

var (
	errVecFileClosed = errors.New("vector file store is closed")
)

// VectorFileStore is an append-only vector store backed by a file.
// Only ID-to-ordinal metadata is kept in RAM; vector data lives on disk.
// All vectors are stored normalized (one copy per id).
type VectorFileStore struct {
	dimensions int
	vecPath    string
	metaPath   string

	mu                sync.RWMutex
	appendMu          sync.Mutex
	file              *security.RootedFile
	syncFile          func(*os.File) error
	writeRecord       func(*os.File, string, []float32) error
	idToOrdinal       map[string]int64
	nextOrdinal       int64
	buildIndexedCount int64 // last checkpoint count; persisted in .meta for resume
	obsoleteCount     int64 // approximate number of stale slots in .vec from updates/deletes
	scoreScratchPool  sync.Pool
	closed            bool
}

type vfsCandidateOffset struct {
	id     string
	vecOff int64
}

type vfsScoreScratch struct {
	offsets []vfsCandidateOffset
	batch   []byte
}

// Has reports whether id is present in the id-to-ordinal map.
func (v *VectorFileStore) Has(id string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.idToOrdinal[id]
	return ok
}

// VectorFileStoreMeta is persisted to the .meta file (msgpack).
type VectorFileStoreMeta struct {
	Version           int              `msgpack:"v"`
	Dimensions        int              `msgpack:"dim"`
	IDToOrdinal       map[string]int64 `msgpack:"id2ord"`
	DataSlots         int64            `msgpack:"slots"`
	BuildIndexedCount int64            `msgpack:"build_count,omitempty"` // last checkpoint count during BuildIndexes; used for resume
}

// NewVectorFileStore creates a new file-backed store and opens the vector file for append.
// vecBasePath is the path prefix: .vec and .meta will be appended.
// If the .vec file exists it is opened for append; otherwise it is created with a header.
func NewVectorFileStore(vecBasePath string, dimensions int) (*VectorFileStore, error) {
	if dimensions <= 0 {
		return nil, localizedError(localization.SearchDimensionsMustBePositive(dimensions), nil)
	}
	vecPath := vecBasePath + ".vec"
	metaPath := vecBasePath + ".meta"

	v := &VectorFileStore{
		dimensions:  dimensions,
		vecPath:     vecPath,
		metaPath:    metaPath,
		idToOrdinal: make(map[string]int64),
		syncFile: func(f *os.File) error {
			return f.Sync()
		},
		writeRecord: writeVectorRecord,
	}
	v.scoreScratchPool = sync.Pool{
		New: func() any {
			return &vfsScoreScratch{
				offsets: make([]vfsCandidateOffset, 0, 256),
				batch:   make([]byte, 0, 64*1024),
			}
		},
	}

	// Open or create .vec file
	exists := false
	if _, err := security.RootedStat(vecPath); err == nil {
		exists = true
	}
	if err := security.EnsureRootedParent(vecPath, 0o755); err != nil {
		return nil, err
	}

	flags := os.O_RDWR | os.O_CREATE | os.O_APPEND
	var err error
	v.file, err = security.OpenRootedFile(vecPath, flags, 0o644)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := v.writeHeader(); err != nil {
			v.file.Close()
			return nil, err
		}
	} else {
		// Verify header
		if err := v.readHeader(); err != nil {
			v.file.Close()
			return nil, err
		}
	}
	stat, err := v.file.Stat()
	if err != nil {
		v.file.Close()
		return nil, err
	}
	dataBytes := stat.Size() - vecHeaderSize
	stride, ok := v.vectorStride()
	if !ok || dataBytes < 0 || dataBytes%stride != 0 {
		v.file.Close()
		return nil, fmt.Errorf("invalid fixed-stride vector file size: %d", stat.Size())
	}
	v.nextOrdinal = dataBytes / stride

	return v, nil
}

func (v *VectorFileStore) writeHeader() error {
	buf := make([]byte, vecHeaderSize)
	copy(buf, vecFileMagic)
	buf[4] = vecFileVersion
	binary.LittleEndian.PutUint32(buf[5:9], uint32(v.dimensions))
	_, err := v.file.Write(buf)
	return err
}

func (v *VectorFileStore) readHeader() error {
	buf := make([]byte, vecHeaderSize)
	_, err := io.ReadFull(v.file, buf)
	if err != nil {
		return err
	}
	if string(buf[:4]) != vecFileMagic {
		return localizedError(localization.SearchVectorFileMagicInvalid(), nil)
	}
	if buf[4] != vecFileVersion {
		return localizedError(localization.SearchVectorFileVersionUnsupported(buf[4]), nil)
	}
	dim := int(binary.LittleEndian.Uint32(buf[5:9]))
	if dim != v.dimensions {
		return localizedError(localization.SearchVectorFileDimensionsMismatch(dim, v.dimensions), nil)
	}
	return nil
}

// Add appends a normalized vector to the store. vec is normalized in place/copied; only one copy is stored.
func (v *VectorFileStore) Add(id string, vec []float32) error {
	if len(vec) != v.dimensions {
		return ErrDimensionMismatch
	}
	normalized := vector.Normalize(vec)
	v.appendMu.Lock()
	defer v.appendMu.Unlock()

	v.mu.RLock()
	if v.closed || v.file == nil {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	file := v.file
	writeFn := v.writeRecord
	v.mu.RUnlock()

	ordinal := v.nextOrdinal
	if writeFn == nil {
		writeFn = writeVectorRecord
	}
	if err := writeFn(file.File, id, normalized); err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return errVecFileClosed
	}
	v.nextOrdinal++
	_, existed := v.idToOrdinal[id]
	v.idToOrdinal[id] = ordinal
	if existed {
		v.obsoleteCount++
	}
	return nil
}

// Remove deletes id from the live id-to-ordinal map.
// The old .vec slot is left in-place and reclaimed by compaction.
func (v *VectorFileStore) Remove(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return false
	}
	if _, ok := v.idToOrdinal[id]; !ok {
		return false
	}
	delete(v.idToOrdinal, id)
	v.obsoleteCount++
	return true
}

// GetVector returns a copy of the stored (normalized) vector for id, or (nil, false) if not found.
func (v *VectorFileStore) GetVector(id string) ([]float32, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ordinal, ok := v.idToOrdinal[id]
	if !ok || ordinal < 0 || ordinal >= v.nextOrdinal || v.closed || v.file == nil {
		return nil, false
	}
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return nil, false
	}
	buf := make([]byte, vectorBytes)
	if _, err := v.file.ReadAt(buf, v.vectorOffset(ordinal)); err != nil {
		return nil, false
	}
	vec := make([]float32, v.dimensions)
	for i := 0; i < v.dimensions; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}
	return vec, true
}

// scoreCandidatesDot scores candidate IDs directly from the vector file without
// allocating a []float32 per candidate. This reduces query-path allocation pressure
// for large rerank windows.
func (v *VectorFileStore) scoreCandidatesDot(ctx context.Context, normalizedQuery []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed || v.file == nil {
		return nil, nil
	}
	dims := v.dimensions
	if dims <= 0 {
		return nil, nil
	}
	if len(normalizedQuery) < dims {
		dims = len(normalizedQuery)
	}
	if dims <= 0 {
		return nil, nil
	}

	scratch := v.getScoreScratch(len(candidates), vectorScoreReadBatchSize)
	defer v.putScoreScratch(scratch)
	offsets := scratch.offsets[:0]
	offsetsSorted := true
	for _, cand := range candidates {
		ordinal, ok := v.idToOrdinal[cand.ID]
		if !ok || ordinal < 0 || ordinal >= v.nextOrdinal {
			continue
		}
		candidateOffset := vfsCandidateOffset{
			id:     cand.ID,
			vecOff: v.vectorOffset(ordinal),
		}
		if len(offsets) > 0 && candidateOffset.vecOff < offsets[len(offsets)-1].vecOff {
			offsetsSorted = false
		}
		offsets = append(offsets, candidateOffset)
	}
	if len(offsets) == 0 {
		return nil, nil
	}
	if !offsetsSorted {
		slices.SortFunc(offsets, func(a, b vfsCandidateOffset) int {
			if a.vecOff < b.vecOff {
				return -1
			}
			if a.vecOff > b.vecOff {
				return 1
			}
			return 0
		})
	}

	scored := make([]ScoredCandidate, 0, len(candidates))
	vecBytes := dims * 4
	maxBatchBytes := vectorScoreReadBatchSize
	if vecBytes > maxBatchBytes {
		maxBatchBytes = vecBytes
	}
	batch := scratch.batch[:maxBatchBytes]
	for i := 0; i < len(offsets); {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		start := offsets[i].vecOff
		end := start + int64(vecBytes)
		j := i + 1
		for j < len(offsets) {
			nextEnd := offsets[j].vecOff + int64(vecBytes)
			if nextEnd-start > int64(maxBatchBytes) {
				break
			}
			end = nextEnd
			j++
		}
		batchLen := int(end - start)
		n, err := v.file.ReadAt(batch[:batchLen], start)
		if err != nil && err != io.EOF {
			i = j
			continue
		}
		limit := n
		for k := i; k < j; k++ {
			localOff := int(offsets[k].vecOff - start)
			if localOff < 0 || localOff+vecBytes > limit {
				continue
			}
			var score float32
			vecBuf := batch[localOff : localOff+vecBytes]
			for d := 0; d < dims; d++ {
				value := math.Float32frombits(binary.LittleEndian.Uint32(vecBuf[d*4 : d*4+4]))
				score += normalizedQuery[d] * value
			}
			scored = append(scored, ScoredCandidate{ID: offsets[k].id, Score: float64(score)})
		}
		i = j
	}
	slices.SortFunc(scored, func(a, b ScoredCandidate) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		return 0
	})
	return scored, nil
}

func (v *VectorFileStore) getScoreScratch(offsetCap, batchCap int) *vfsScoreScratch {
	if v.scoreScratchPool.New == nil {
		v.scoreScratchPool = sync.Pool{
			New: func() any { return &vfsScoreScratch{} },
		}
	}
	s, _ := v.scoreScratchPool.Get().(*vfsScoreScratch)
	if s == nil {
		s = &vfsScoreScratch{}
	}
	if cap(s.offsets) < offsetCap {
		s.offsets = make([]vfsCandidateOffset, 0, offsetCap)
	} else {
		s.offsets = s.offsets[:0]
	}
	if cap(s.batch) < batchCap {
		s.batch = make([]byte, batchCap)
	} else {
		s.batch = s.batch[:batchCap]
	}
	return s
}

func (v *VectorFileStore) putScoreScratch(s *vfsScoreScratch) {
	if s == nil || v.scoreScratchPool.New == nil {
		return
	}
	s.offsets = s.offsets[:0]
	v.scoreScratchPool.Put(s)
}

// Count returns the number of vectors in the store.
func (v *VectorFileStore) Count() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.idToOrdinal)
}

func (v *VectorFileStore) vectorStride() (int64, bool) {
	bytes, ok := util.SafeIntProduct(v.dimensions, 4)
	return int64(bytes), ok
}

func (v *VectorFileStore) vectorOffset(ordinal int64) int64 {
	stride, _ := v.vectorStride()
	return vecHeaderSize + ordinal*stride
}

// GetDimensions returns the vector dimension.
func (v *VectorFileStore) GetDimensions() int {
	return v.dimensions
}

// IterateChunked reads the vector file in chunks and calls fn(ids, vecs) for each chunk.
// Used to build HNSW without loading all vectors into memory. fn may be called with
// fewer than chunkSize vectors on the last chunk.
func (v *VectorFileStore) IterateChunked(chunkSize int, fn func(ids []string, vecs [][]float32) error) error {
	if chunkSize <= 0 {
		chunkSize = 10000
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return errVecFileClosed
	}
	file := v.file
	if file == nil {
		return errVecFileClosed
	}

	ids := make([]string, 0, chunkSize)
	vecs := make([][]float32, 0, chunkSize)
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return fmt.Errorf("vector dimensions overflow iteration buffer size: %d", v.dimensions)
	}
	type ordinalID struct {
		ordinal int64
		id      string
	}
	entries := make([]ordinalID, 0, len(v.idToOrdinal))
	for id, ordinal := range v.idToOrdinal {
		entries = append(entries, ordinalID{ordinal: ordinal, id: id})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ordinal < entries[j].ordinal })
	buf := make([]byte, vectorBytes)
	for _, entry := range entries {
		if entry.ordinal < 0 || entry.ordinal >= v.nextOrdinal {
			return fmt.Errorf("vector ordinal %d for %q is outside %d slots", entry.ordinal, entry.id, v.nextOrdinal)
		}
		if _, err := file.ReadAt(buf, v.vectorOffset(entry.ordinal)); err != nil {
			return err
		}
		vec := make([]float32, v.dimensions)
		for i := 0; i < v.dimensions; i++ {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
		}
		ids = append(ids, entry.id)
		vecs = append(vecs, vec)
		if len(ids) >= chunkSize {
			if err := fn(ids, vecs); err != nil {
				return err
			}
			ids = ids[:0]
			vecs = vecs[:0]
		}
	}
	if len(ids) > 0 {
		return fn(ids, vecs)
	}
	return nil
}

// Save atomically commits the ID-to-ordinal map after syncing vector payloads.
func (v *VectorFileStore) Save() error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	dim := v.dimensions
	buildCount := v.buildIndexedCount
	dataSlots := v.nextOrdinal
	idToOrdinalCopy := make(map[string]int64, len(v.idToOrdinal))
	for id, ordinal := range v.idToOrdinal {
		idToOrdinalCopy[id] = ordinal
	}
	file := v.file
	v.mu.RUnlock()
	if err := file.Sync(); err != nil {
		return err
	}

	if err := security.EnsureRootedParent(v.metaPath, 0o755); err != nil {
		return err
	}
	tmpPath := v.metaPath + ".tmp"
	f, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	enc := msgpack.NewEncoder(f)
	if err := enc.Encode(&VectorFileStoreMeta{
		Version:           vecFileVersion,
		Dimensions:        dim,
		IDToOrdinal:       idToOrdinalCopy,
		DataSlots:         dataSlots,
		BuildIndexedCount: buildCount,
	}); err != nil {
		_ = f.Close()
		_ = security.RemoveRootedPath(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = security.RemoveRootedPath(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = security.RemoveRootedPath(tmpPath)
		return err
	}
	return security.RenameRootedFile(tmpPath, v.metaPath)
}

// Load populates the store from an existing .vec + .meta. The store must be created with
// NewVectorFileStore(vecBasePath, dimensions); Load then reads the committed ID-to-ordinal
// metadata and discards any uncommitted vector tail.
func (v *VectorFileStore) Load() error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return errVecFileClosed
	}
	f, err := security.OpenRootedFile(v.metaPath, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) && v.nextOrdinal == 0 {
			v.idToOrdinal = make(map[string]int64)
			return nil
		}
		return err
	}
	defer f.Close()
	var meta VectorFileStoreMeta
	if err := util.DecodeMsgpackFile(f.File, &meta); err != nil {
		return err
	}
	if meta.Version != vecFileVersion {
		return localizedError(localization.SearchVectorFileVersionUnsupported(byte(meta.Version)), nil)
	}
	if meta.Dimensions != v.dimensions {
		return localizedError(localization.SearchVectorMetaDimensionsMismatch(meta.Dimensions, v.dimensions), nil)
	}
	if meta.DataSlots < 0 || meta.DataSlots > v.nextOrdinal {
		return fmt.Errorf("vector metadata slot count %d exceeds data slots %d", meta.DataSlots, v.nextOrdinal)
	}
	for id, ordinal := range meta.IDToOrdinal {
		if ordinal < 0 || ordinal >= meta.DataSlots {
			return fmt.Errorf("vector metadata ordinal %d for %q is outside %d slots", ordinal, id, meta.DataSlots)
		}
	}
	stride, _ := v.vectorStride()
	if err := v.file.Truncate(vecHeaderSize + meta.DataSlots*stride); err != nil {
		return err
	}
	v.idToOrdinal = meta.IDToOrdinal
	if v.idToOrdinal == nil {
		v.idToOrdinal = make(map[string]int64)
	}
	v.nextOrdinal = meta.DataSlots
	v.buildIndexedCount = meta.BuildIndexedCount
	v.obsoleteCount = meta.DataSlots - int64(len(v.idToOrdinal))
	return nil
}

// SetBuildIndexedCount sets the last checkpoint count from BuildIndexes (for resume).
// Call before Save() when persisting after a checkpoint so the next run can skip already-indexed nodes.
func (v *VectorFileStore) SetBuildIndexedCount(n int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.buildIndexedCount = n
}

// GetBuildIndexedCount returns the last persisted checkpoint count (0 if none).
// Used at start of BuildIndexes to skip the first N nodes when resuming.
func (v *VectorFileStore) GetBuildIndexedCount() int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.buildIndexedCount
}

// Sync flushes the .vec file to disk so progress is visible and durable.
func (v *VectorFileStore) Sync() error {
	v.mu.RLock()
	closed := v.closed
	file := v.file
	syncFn := v.syncFile
	v.mu.RUnlock()
	if closed || file == nil {
		return nil
	}
	if syncFn == nil {
		return file.Sync()
	}
	return syncFn(file.File)
}

// CompactIfNeeded rewrites .vec with only live slots when stale entries accumulate.
// The rewrite is atomic: write temp file, fsync, rename.
// Returns true when compaction actually ran.
func (v *VectorFileStore) CompactIfNeeded() (bool, error) {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.compactIfNeededLocked()
}

func (v *VectorFileStore) compactIfNeededLocked() (bool, error) {
	if v.closed || v.file == nil {
		return false, nil
	}
	minObsolete := int64(envutil.GetInt("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", 50000))
	minSizeMB := int64(envutil.GetInt("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", 256))
	deadRatioThreshold := envFloat("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", 0.30)
	if minObsolete < 1 {
		minObsolete = 1
	}
	if minSizeMB < 0 {
		minSizeMB = 0
	}
	if deadRatioThreshold < 0 {
		deadRatioThreshold = 0
	}

	live := int64(len(v.idToOrdinal))
	if live == 0 {
		// If everything was deleted, shrink back to a header-only file.
		if v.obsoleteCount == 0 {
			return false, nil
		}
		if err := v.rewriteVecLocked(nil); err != nil {
			return false, err
		}
		v.obsoleteCount = 0
		v.buildIndexedCount = 0
		return true, nil
	}
	if v.obsoleteCount < minObsolete {
		return false, nil
	}
	stat, err := v.file.Stat()
	if err != nil {
		return false, err
	}
	if stat.Size() < minSizeMB*1024*1024 {
		return false, nil
	}
	deadRatio := float64(v.obsoleteCount) / float64(live+v.obsoleteCount)
	if deadRatio < deadRatioThreshold {
		return false, nil
	}

	ids := make([]string, 0, len(v.idToOrdinal))
	for id := range v.idToOrdinal {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if err := v.rewriteVecLocked(ids); err != nil {
		return false, err
	}
	v.obsoleteCount = 0
	v.buildIndexedCount = int64(len(v.idToOrdinal))
	return true, nil
}

func (v *VectorFileStore) rewriteVecLocked(ids []string) error {
	tmpPath := v.vecPath + ".tmp-compact"
	tmp, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = tmp.Close()
		_ = security.RemoveRootedPath(tmpPath)
	}
	defer cleanup()

	header := make([]byte, vecHeaderSize)
	copy(header, vecFileMagic)
	header[4] = vecFileVersion
	binary.LittleEndian.PutUint32(header[5:9], uint32(v.dimensions))
	if _, err := tmp.Write(header); err != nil {
		return err
	}

	newOrdinals := make(map[string]int64, len(ids))
	for _, id := range ids {
		oldOrdinal, ok := v.idToOrdinal[id]
		if !ok {
			continue
		}
		vec, err := v.readVectorAtLocked(oldOrdinal)
		if err != nil {
			return fmt.Errorf("compact read id %q at ordinal %d: %w", id, oldOrdinal, err)
		}
		if err := writeVectorRecord(tmp.File, id, vec); err != nil {
			return err
		}
		newOrdinals[id] = int64(len(newOrdinals))
	}

	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := v.file.Close(); err != nil {
		return err
	}
	if err := security.RenameRootedFile(tmpPath, v.vecPath); err != nil {
		return err
	}
	reopened, err := security.OpenRootedFile(v.vecPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	v.file = reopened
	v.idToOrdinal = newOrdinals
	v.nextOrdinal = int64(len(newOrdinals))
	return nil
}

func (v *VectorFileStore) readVectorAtLocked(ordinal int64) ([]float32, error) {
	if v.file == nil {
		return nil, errVecFileClosed
	}
	if ordinal < 0 || ordinal >= v.nextOrdinal {
		return nil, fmt.Errorf("vector ordinal %d is outside %d slots", ordinal, v.nextOrdinal)
	}
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return nil, fmt.Errorf("vector dimensions overflow payload length: %d", v.dimensions)
	}
	buf := make([]byte, vectorBytes)
	if _, err := v.file.ReadAt(buf, v.vectorOffset(ordinal)); err != nil {
		return nil, err
	}
	vec := make([]float32, v.dimensions)
	for i := 0; i < v.dimensions; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}
	return vec, nil
}

func writeVectorRecord(f *os.File, id string, vec []float32) error {
	_ = id
	vectorBytes, ok := util.SafeIntProduct(len(vec), 4)
	if !ok {
		return fmt.Errorf("vector size overflow for %q", id)
	}
	buf := make([]byte, vectorBytes)
	for i := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(vec[i]))
	}
	_, err := f.Write(buf)
	return err
}

// Close closes the underlying file. The store must not be used after Close.
func (v *VectorFileStore) Close() error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	if v.file != nil {
		err := v.file.Close()
		v.file = nil
		return err
	}
	return nil
}
