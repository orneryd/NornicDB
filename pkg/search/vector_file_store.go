// Package search provides file-backed vector storage for memory-efficient indexing.
//
// VectorFileStore implements append-only vector storage: vectors are written to a
// .vec file and only id→offset metadata is kept in RAM. This allows BuildIndexes
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
	vecFileMagic   = "NVF\n"
	vecFileVersion = 1
	vecHeaderSize  = 64
)

var (
	errVecFileClosed = errors.New("vector file store is closed")
)

// VectorFileStore is an append-only vector store backed by a file.
// Only id→offset is kept in RAM; vector data lives on disk.
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
	idToOff           map[string]int64
	buildIndexedCount int64 // last checkpoint count; persisted in .meta for resume
	obsoleteCount     int64 // approximate number of stale records in .vec from updates/deletes
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

// Has reports whether id is present in the id→offset map.
func (v *VectorFileStore) Has(id string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.idToOff[id]
	return ok
}

// VectorFileStoreMeta is persisted to the .meta file (msgpack).
type VectorFileStoreMeta struct {
	Version           int              `msgpack:"v"`
	Dimensions        int              `msgpack:"dim"`
	IDToOffset        map[string]int64 `msgpack:"id2off"`
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
		dimensions: dimensions,
		vecPath:    vecPath,
		metaPath:   metaPath,
		idToOff:    make(map[string]int64),
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

	flags := os.O_RDWR | os.O_CREATE
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
		return fmt.Errorf("invalid vector file magic")
	}
	if buf[4] != vecFileVersion {
		return fmt.Errorf("unsupported vector file version %d", buf[4])
	}
	dim := int(binary.LittleEndian.Uint32(buf[5:9]))
	if dim != v.dimensions {
		return fmt.Errorf("vector file dimensions %d != store dimensions %d", dim, v.dimensions)
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

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
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
	_, existed := v.idToOff[id]
	v.idToOff[id] = offset
	if existed {
		v.obsoleteCount++
	}
	return nil
}

// Remove deletes id from the live id→offset map.
// The old .vec record is left in-place and reclaimed by compaction.
func (v *VectorFileStore) Remove(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return false
	}
	if _, ok := v.idToOff[id]; !ok {
		return false
	}
	delete(v.idToOff, id)
	v.obsoleteCount++
	return true
}

// GetVector returns a copy of the stored (normalized) vector for id, or (nil, false) if not found.
func (v *VectorFileStore) GetVector(id string) ([]float32, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	off, ok := v.idToOff[id]
	if !ok || v.closed || v.file == nil {
		return nil, false
	}
	// Read record at offset with a one-read fast path:
	// [idLen(4)][id][vector(dim*4)].
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return nil, false
	}
	bufLen, ok := util.SafeIntSum(4, vectorBytes, 256)
	if !ok {
		return nil, false
	}
	buf := make([]byte, bufLen)
	if _, err := v.file.ReadAt(buf, off); err != nil && err != io.EOF {
		return nil, false
	}
	idLen := binary.LittleEndian.Uint32(buf[:4])
	idLenInt := int(idLen)
	if uint32(idLenInt) != idLen {
		return nil, false
	}
	recSize, ok := util.SafeIntSum(4, idLenInt, vectorBytes)
	if !ok {
		return nil, false
	}
	if recSize > len(buf) {
		buf = make([]byte, recSize)
		if _, err := v.file.ReadAt(buf, off); err != nil {
			return nil, false
		}
	}
	vec := make([]float32, v.dimensions)
	for i := 0; i < v.dimensions; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4+int(idLen)+i*4:]))
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

	scratch := v.getScoreScratch(len(candidates), 64*1024)
	defer v.putScoreScratch(scratch)
	offsets := scratch.offsets[:0]
	for _, cand := range candidates {
		off, ok := v.idToOff[cand.ID]
		if !ok {
			continue
		}
		offsets = append(offsets, vfsCandidateOffset{
			id:     cand.ID,
			vecOff: off + int64(4+len(cand.ID)),
		})
	}
	if len(offsets) == 0 {
		return nil, nil
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i].vecOff < offsets[j].vecOff })

	scored := make([]ScoredCandidate, 0, len(candidates))
	vecBytes := dims * 4
	maxBatchBytes := 64 * 1024
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
	sort.Slice(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
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
	return len(v.idToOff)
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
	if v.closed {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	file := v.file
	v.mu.RUnlock()
	if file == nil {
		return errVecFileClosed
	}

	// Seek to first record (after header)
	if _, err := file.Seek(vecHeaderSize, io.SeekStart); err != nil {
		return err
	}

	ids := make([]string, 0, chunkSize)
	vecs := make([][]float32, 0, chunkSize)
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return fmt.Errorf("vector dimensions overflow rebuild buffer size: %d", v.dimensions)
	}
	bufLen, ok := util.SafeIntSum(4, 256, vectorBytes)
	if !ok {
		return fmt.Errorf("vector rebuild buffer size overflow")
	}
	buf := make([]byte, bufLen)
	for {
		// Read id length
		if _, err := io.ReadFull(file, buf[:4]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		idLen := binary.LittleEndian.Uint32(buf[:4])
		recLen, ok := util.SafeIntSum(4, int(idLen), vectorBytes)
		if !ok {
			return fmt.Errorf("vector record length overflow for id length %d", idLen)
		}
		if recLen > len(buf) {
			newBuf := make([]byte, recLen)
			binary.LittleEndian.PutUint32(newBuf[0:4], idLen)
			buf = newBuf
		}
		if _, err := io.ReadFull(file, buf[4:recLen]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		id := string(buf[4 : 4+idLen])
		vec := make([]float32, v.dimensions)
		for i := 0; i < v.dimensions; i++ {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4+int(idLen)+i*4:]))
		}
		ids = append(ids, id)
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

// Save writes the id→offset map to the .meta file so the store can be loaded later.
// Copies idToOff under a short lock so the (potentially slow) encode doesn't block Add().
func (v *VectorFileStore) Save() error {
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	dim := v.dimensions
	buildCount := v.buildIndexedCount
	idToOffCopy := make(map[string]int64, len(v.idToOff))
	for k, o := range v.idToOff {
		idToOffCopy[k] = o
	}
	v.mu.RUnlock()

	if err := security.EnsureRootedParent(v.metaPath, 0o755); err != nil {
		return err
	}
	f, err := security.CreateRootedFile(v.metaPath, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := msgpack.NewEncoder(f)
	return enc.Encode(&VectorFileStoreMeta{
		Version:           vecFileVersion,
		Dimensions:        dim,
		IDToOffset:        idToOffCopy,
		BuildIndexedCount: buildCount,
	})
}

// Load populates the store from an existing .vec + .meta. The store must be created with
// NewVectorFileStore(vecBasePath, dimensions); Load then reads the .meta file to populate
// idToOff. The .vec file is already open from NewVectorFileStore.
func (v *VectorFileStore) Load() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return errVecFileClosed
	}
	f, err := security.OpenRootedFile(v.metaPath, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			// No meta; rebuild idToOff from .vec if present.
			return v.rebuildIndexFromVecLocked()
		}
		return err
	}
	defer f.Close()
	var meta VectorFileStoreMeta
	if err := util.DecodeMsgpackFile(f.File, &meta); err != nil {
		// Corrupt meta; rebuild idToOff from .vec.
		return v.rebuildIndexFromVecLocked()
	}
	if meta.Dimensions != v.dimensions {
		return fmt.Errorf("meta dimensions %d != store dimensions %d", meta.Dimensions, v.dimensions)
	}
	if meta.IDToOffset != nil {
		v.idToOff = meta.IDToOffset
	}
	if meta.BuildIndexedCount > 0 {
		v.buildIndexedCount = meta.BuildIndexedCount
	}
	// Rebuild id→offset from .vec so resume is accurate even if meta is stale.
	return v.rebuildIndexFromVecLocked()
}

// rebuildIndexFromVecLocked rebuilds id→offset by scanning the .vec file.
// Caller must hold v.mu.
func (v *VectorFileStore) rebuildIndexFromVecLocked() error {
	if v.closed || v.file == nil {
		return errVecFileClosed
	}
	if _, err := v.file.Seek(vecHeaderSize, io.SeekStart); err != nil {
		return err
	}
	idToOff := make(map[string]int64)
	totalRecords := int64(0)
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return fmt.Errorf("vector dimensions overflow rebuild buffer size: %d", v.dimensions)
	}
	bufLen, ok := util.SafeIntSum(4, 256, vectorBytes)
	if !ok {
		return fmt.Errorf("vector rebuild buffer size overflow")
	}
	buf := make([]byte, bufLen)
	for {
		offset, err := v.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(v.file, buf[:4]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		idLen := binary.LittleEndian.Uint32(buf[:4])
		idLenInt := int(idLen)
		if uint32(idLenInt) != idLen {
			return fmt.Errorf("vector record id length overflow: %d", idLen)
		}
		recLen, ok := util.SafeIntSum(4, idLenInt, vectorBytes)
		if !ok {
			return fmt.Errorf("vector record length overflow")
		}
		if recLen > len(buf) {
			newBuf := make([]byte, recLen)
			binary.LittleEndian.PutUint32(newBuf[0:4], idLen)
			buf = newBuf
		}
		if _, err := io.ReadFull(v.file, buf[4:recLen]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		id := string(buf[4 : 4+idLen])
		idToOff[id] = offset
		totalRecords++
	}
	v.idToOff = idToOff
	v.buildIndexedCount = int64(len(idToOff))
	v.obsoleteCount = totalRecords - int64(len(idToOff))
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

// CompactIfNeeded rewrites .vec with only live records when stale entries accumulate.
// The rewrite is atomic: write temp file, fsync, rename.
// Returns true when compaction actually ran.
func (v *VectorFileStore) CompactIfNeeded() (bool, error) {
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

	live := int64(len(v.idToOff))
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

	ids := make([]string, 0, len(v.idToOff))
	for id := range v.idToOff {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if err := v.rewriteVecLocked(ids); err != nil {
		return false, err
	}
	v.obsoleteCount = 0
	v.buildIndexedCount = int64(len(v.idToOff))
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

	newOffsets := make(map[string]int64, len(ids))
	for _, id := range ids {
		oldOffset, ok := v.idToOff[id]
		if !ok {
			continue
		}
		vec, err := v.readVectorAtLocked(oldOffset)
		if err != nil {
			return fmt.Errorf("compact read id %q at offset %d: %w", id, oldOffset, err)
		}
		newOffset, err := tmp.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if err := writeVectorRecord(tmp.File, id, vec); err != nil {
			return err
		}
		newOffsets[id] = newOffset
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
	reopened, err := security.OpenRootedFile(v.vecPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	v.file = reopened
	v.idToOff = newOffsets
	return nil
}

func (v *VectorFileStore) readVectorAtLocked(offset int64) ([]float32, error) {
	if v.file == nil {
		return nil, errVecFileClosed
	}
	head := make([]byte, 4)
	if _, err := v.file.ReadAt(head, offset); err != nil {
		return nil, err
	}
	idLen := int(binary.LittleEndian.Uint32(head))
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		return nil, fmt.Errorf("vector dimensions overflow record length: %d", v.dimensions)
	}
	recLen, ok := util.SafeIntSum(4, idLen, vectorBytes)
	if !ok {
		return nil, fmt.Errorf("vector record length overflow")
	}
	buf := make([]byte, recLen)
	if _, err := v.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	vec := make([]float32, v.dimensions)
	base := 4 + idLen
	for i := 0; i < v.dimensions; i++ {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[base+i*4:]))
	}
	return vec, nil
}

func writeVectorRecord(f *os.File, id string, vec []float32) error {
	idBytes := []byte(id)
	idLen := uint32(len(idBytes))
	if int(idLen) != len(idBytes) {
		return fmt.Errorf("id too long")
	}
	vectorBytes, ok := util.SafeIntProduct(len(vec), 4)
	if !ok {
		return fmt.Errorf("vector size overflow for record %q", id)
	}
	bufLen, ok := util.SafeIntSum(4, len(idBytes), vectorBytes)
	if !ok {
		return fmt.Errorf("record buffer size overflow for %q", id)
	}
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf[0:4], idLen)
	copy(buf[4:4+idLen], idBytes)
	for i := range vec {
		binary.LittleEndian.PutUint32(buf[4+int(idLen)+i*4:], math.Float32bits(vec[i]))
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
