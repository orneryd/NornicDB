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
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"io"
	"maps"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
)

const (
	vecFileMagic             = "NVF\n"
	vecFileVersion           = 3 // 3: vector-entry keys (vectorEntryKeyFormat); older stores are rebuilt
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

	mu                    sync.RWMutex
	appendMu              sync.Mutex
	file                  *security.RootedFile
	syncFile              func(*os.File) error
	writeRecord           func(*os.File, string, []float32) error
	mapFile               func(*os.File, int) ([]byte, func() error, error)
	idToOrdinal           map[string]int64
	nodeLabels            map[string][]string
	nodeTypes             map[string]string
	nodeNamedVectors      map[string]map[string]string
	nodePropertyVectors   map[string]map[string]string
	nodeChunkVectors      map[string][]string
	nodeMetadataPersisted bool
	nodeTypesPersisted    bool
	nextOrdinal           int64
	buildIndexedCount     int64 // last checkpoint count; persisted in .meta for resume
	obsoleteCount         int64 // approximate number of stale slots in .vec from updates/deletes
	scoreScratchPool      sync.Pool
	queryReadMap          *vectorFileReadLease // borrowed only while mu is read-locked; nil reads with ReadAt
	queryReadSlots        int64                // slot count queryReadMap was established for, mapped or not
	closed                bool
	// generation changes on every committed content change; checkpointLoaded,
	// checkpointDigest and publicationErr describe the last committed tuple.
	generation       vectorCheckpointGeneration
	checkpointLoaded bool
	checkpointDigest string
	publicationErr   error
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

// stateVersion is the tuple's derivative identity: live count, committed slot
// count and the generation that changes on every committed content change, so
// a derivative (ANN overlay, graph) can tell its own source apart.
func (v *VectorFileStore) stateVersion() (count int, slots int64, generation vectorCheckpointGeneration) {
	if v == nil {
		return 0, 0, vectorCheckpointGeneration{}
	}
	v.mu.RLock()
	count = len(v.idToOrdinal)
	slots = v.nextOrdinal
	generation = v.generation
	v.mu.RUnlock()
	return count, slots, generation
}

// vectorCheckpointGeneration identifies a source lineage and its mutations.
// Only a new lineage allocates an epoch; normal Add/Remove operations increment
// a value counter rather than generating and allocating a UUID per vector.
type vectorCheckpointGeneration struct {
	Epoch    string `msgpack:"epoch"`
	Revision uint64 `msgpack:"revision"`
}

func (g *vectorCheckpointGeneration) advance() {
	g.Revision++
	if g.Revision == 0 {
		g.Epoch = uuid.NewString()
	}
}

// VectorFileStoreMeta is persisted to the .meta file (msgpack).
type VectorFileStoreMeta struct {
	Version               int                          `msgpack:"v"`
	Dimensions            int                          `msgpack:"dim"`
	IDToOrdinal           map[string]int64             `msgpack:"id2ord"`
	DataSlots             int64                        `msgpack:"slots"`
	BuildIndexedCount     int64                        `msgpack:"build_count,omitempty"` // last checkpoint count during BuildIndexes; used for resume
	NodeLabels            map[string][]string          `msgpack:"node_labels,omitempty"`
	NodeTypes             map[string]string            `msgpack:"node_types,omitempty"`
	NodeNamedVectors      map[string]map[string]string `msgpack:"node_named_vectors,omitempty"`
	NodePropertyVectors   map[string]map[string]string `msgpack:"node_property_vectors,omitempty"`
	NodeChunkVectors      map[string][]string          `msgpack:"node_chunk_vectors,omitempty"`
	NodeMetadataPersisted bool                         `msgpack:"node_metadata_persisted,omitempty"`
	NodeTypesPersisted    bool                         `msgpack:"node_types_persisted,omitempty"`
	// Integrity and derivative identity of this exact tuple. A .meta without
	// them cannot certify its payload/associations and is rebuilt.
	MetadataDigest string                     `msgpack:"metadata_digest,omitempty"`
	PayloadDigest  string                     `msgpack:"payload_digest,omitempty"`
	Generation     vectorCheckpointGeneration `msgpack:"generation,omitempty"`
}

// NewVectorFileStore creates a new file-backed store and opens the vector file for append.
// vecBasePath is the path prefix: .vec and .meta will be appended.
// If the .vec file exists it is opened for append; otherwise it is created with a header.
func NewVectorFileStore(vecBasePath string, dimensions int) (*VectorFileStore, error) {
	if dimensions <= 0 {
		return nil, localizedError(localization.SearchDimensionsMustBePositive(dimensions), nil)
	}
	if err := recoverVectorCheckpoint(vecBasePath); err != nil {
		return nil, err
	}
	vecPath := vecBasePath + ".vec"
	metaPath := vecBasePath + ".meta"

	v := &VectorFileStore{
		dimensions:          dimensions,
		generation:          vectorCheckpointGeneration{Epoch: uuid.NewString()},
		vecPath:             vecPath,
		metaPath:            metaPath,
		idToOrdinal:         make(map[string]int64),
		nodeLabels:          make(map[string][]string),
		nodeTypes:           make(map[string]string),
		nodeNamedVectors:    make(map[string]map[string]string),
		nodePropertyVectors: make(map[string]map[string]string),
		nodeChunkVectors:    make(map[string][]string),
		syncFile: func(f *os.File) error {
			return f.Sync()
		},
		writeRecord: writeVectorRecord,
		mapFile:     mapVectorFileReadOnly,
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
		return nil, rejectVectorCheckpoint(fmt.Errorf("invalid fixed-stride vector file size: %d", stat.Size()))
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
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return rejectVectorCheckpoint(err)
		}
		return err
	}
	if string(buf[:4]) != vecFileMagic {
		return rejectVectorCheckpoint(localizedError(localization.SearchVectorFileMagicInvalid(), nil))
	}
	if buf[4] != vecFileVersion {
		return rejectVectorCheckpoint(localizedError(localization.SearchVectorFileVersionUnsupported(buf[4]), nil))
	}
	dim := int(binary.LittleEndian.Uint32(buf[5:9]))
	if dim != v.dimensions {
		return rejectVectorCheckpoint(localizedError(localization.SearchVectorFileDimensionsMismatch(dim, v.dimensions), nil))
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
	if err := v.checkpointHealthLocked(); err != nil {
		v.mu.RUnlock()
		return err
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
	v.generation.advance()
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
	v.generation.advance()
	v.obsoleteCount++
	return true
}

// GetVector returns a copy of the stored (normalized) vector for id, or (nil, false) if not found.
func (v *VectorFileStore) GetVector(id string) ([]float32, bool) {
	if err := v.lockQueryRead(); err != nil {
		return nil, false
	}
	defer v.mu.RUnlock()
	stored, borrowed, ok := v.vectorLocked(id)
	if !ok {
		return nil, false
	}
	if borrowed {
		return append([]float32(nil), stored...), true
	}
	return stored, true
}

// vectorLocked reads id's row while mu is read-locked by lockQueryRead.
// A mapped row is a borrowed view valid only under that lock; a row the query
// mapping does not cover is copied with ReadAt.
func (v *VectorFileStore) vectorLocked(id string) ([]float32, bool, bool) {
	ordinal, ok := v.idToOrdinal[id]
	if !ok || ordinal < 0 || ordinal >= v.nextOrdinal || v.closed || v.file == nil {
		return nil, false, false
	}
	if v.queryReadMap != nil {
		if stored, ok := v.queryReadMap.Lookup(id); ok {
			return stored, true, true
		}
	}
	vec, err := readVectorFileRow(v.file, v.dimensions, v.vectorOffset(ordinal))
	return vec, false, err == nil
}

// scoreCandidatesDot scores candidate IDs directly from the vector file without
// allocating a []float32 per candidate. This reduces query-path allocation pressure
// for large rerank windows.
func (v *VectorFileStore) scoreCandidatesDot(ctx context.Context, normalizedQuery []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	work := newSearchBatch(ctx)
	if err := work.check(); err != nil {
		return nil, err
	}
	if err := v.lockQueryRead(); err != nil {
		return nil, err
	}
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

	batchCapacity := vectorScoreReadBatchSize
	if v.queryReadMap != nil {
		batchCapacity = 0
	}
	scratch := v.getScoreScratch(len(candidates), batchCapacity)
	defer v.putScoreScratch(scratch)
	offsets := scratch.offsets[:0]
	offsetsSorted := true
	for _, cand := range candidates {
		if work.boundary() {
			if err := work.check(); err != nil {
				return nil, err
			}
		}
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
	if v.queryReadMap != nil {
		// Rows the mapping does not cover stay in offsets for ReadAt below.
		unmapped := offsets[:0]
		for _, entry := range offsets {
			if work.boundary() {
				if err := work.check(); err != nil {
					return nil, err
				}
			}
			stored, ok := v.queryReadMap.Lookup(entry.id)
			if !ok {
				unmapped = append(unmapped, entry)
				continue
			}
			// Retain the existing scalar accumulation order and score semantics.
			var score float32
			for d := 0; d < dims; d++ {
				score += normalizedQuery[d] * stored[d]
			}
			scored = append(scored, ScoredCandidate{ID: entry.id, Score: float64(score)})
		}
		offsets = unmapped
	}
	if len(offsets) > 0 {
		vecBytes := dims * 4
		maxBatchBytes := vectorScoreReadBatchSize
		if vecBytes > maxBatchBytes {
			maxBatchBytes = vecBytes
		}
		if cap(scratch.batch) < maxBatchBytes {
			scratch.batch = make([]byte, maxBatchBytes)
		}
		batch := scratch.batch[:maxBatchBytes]
		for i := 0; i < len(offsets); {
			if err := work.check(); err != nil {
				return nil, err
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
				// A row that cannot be read is an error, not a missing candidate;
				// rows past the end of the file (io.EOF) stay absent as before.
				return nil, err
			}
			limit := n
			for k := i; k < j; k++ {
				if work.boundary() {
					if err := work.check(); err != nil {
						return nil, err
					}
				}
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
	if err := work.check(); err != nil {
		return nil, err
	}
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
	// Scoring and mapped compaction write through local slice headers.
	// The pool owns the full allocation, including their hidden tails.
	clear(s.offsets[:cap(s.offsets)])
	s.offsets = s.offsets[:0]
	v.scoreScratchPool.Put(s)
}

// Count returns the number of vectors in the store.
func (v *VectorFileStore) Count() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.idToOrdinal)
}

// IDs returns the IDs of the vectors in the store, without reading them.
func (v *VectorFileStore) IDs() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ids := make([]string, 0, len(v.idToOrdinal))
	for id := range v.idToOrdinal {
		ids = append(ids, id)
	}
	return ids
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
// fewer than chunkSize vectors on the last chunk. Callbacks run without mu:
// HNSW construction looks vectors up through Service and this store. An owned
// read handle and copied ordinals keep this snapshot valid across close or
// compaction, which replaces the store's handle and file.
func (v *VectorFileStore) IterateChunked(chunkSize int, fn func(ids []string, vecs [][]float32) error) error {
	if chunkSize <= 0 {
		chunkSize = 10000
	}
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	if v.file == nil {
		v.mu.RUnlock()
		return errVecFileClosed
	}
	if _, err := v.file.Stat(); err != nil {
		v.mu.RUnlock()
		return err
	}
	file, err := security.OpenRootedFile(v.vecPath, os.O_RDONLY, 0)
	if err != nil {
		v.mu.RUnlock()
		return err
	}
	defer file.Close()
	nextOrdinal := v.nextOrdinal

	ids := make([]string, 0, chunkSize)
	vecs := make([][]float32, 0, chunkSize)
	vectorBytes, ok := util.SafeIntProduct(v.dimensions, 4)
	if !ok {
		v.mu.RUnlock()
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
	v.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].ordinal < entries[j].ordinal })
	buf := make([]byte, vectorBytes)
	for _, entry := range entries {
		if entry.ordinal < 0 || entry.ordinal >= nextOrdinal {
			return fmt.Errorf("vector ordinal %d for %q is outside %d slots", entry.ordinal, entry.id, nextOrdinal)
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

// Save commits the ID-to-ordinal map and its associations as one recoverable
// tuple after syncing vector payloads. Appends leave the committed payload
// prefix untouched, so this publication writes metadata only; compaction and
// relocation stage a complete replacement payload and use the same decision.
func (v *VectorFileStore) Save() error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.RLock()
	if err := v.checkpointHealthLocked(); err != nil {
		v.mu.RUnlock()
		return err
	}
	meta := v.checkpointMetadataLocked()
	file := v.file
	base := v.vecPath[:len(v.vecPath)-len(".vec")]
	v.mu.RUnlock()
	if err := file.Sync(); err != nil {
		return err
	}
	var err error
	meta.PayloadDigest, err = vectorPayloadDigest(file.File, vecHeaderSize+meta.DataSlots*int64(meta.Dimensions)*4)
	if err != nil {
		return err
	}
	if err := publishVectorCheckpoint(base, &meta, false); err != nil {
		v.mu.Lock()
		v.publicationErr = err
		v.mu.Unlock()
		return err
	}
	v.mu.Lock()
	v.checkpointLoaded = true
	v.checkpointDigest = meta.MetadataDigest
	v.mu.Unlock()
	return nil
}

// checkpointMetadataLocked captures one tuple description. The caller owns mu.
// Service captures its query metadata under indexMu before calling Save, so an
// ordinal map is never paired with associations from a different index turn.
func (v *VectorFileStore) checkpointMetadataLocked() VectorFileStoreMeta {
	ordinals := make(map[string]int64, len(v.idToOrdinal))
	for id, ordinal := range v.idToOrdinal {
		ordinals[id] = ordinal
	}
	return VectorFileStoreMeta{
		Version: vecFileVersion, Dimensions: v.dimensions, DataSlots: v.nextOrdinal,
		IDToOrdinal: ordinals, BuildIndexedCount: v.buildIndexedCount,
		NodeLabels: cloneStringSlices(v.nodeLabels), NodeTypes: cloneStringMap(v.nodeTypes),
		NodeNamedVectors:      cloneNestedStringMaps(v.nodeNamedVectors),
		NodePropertyVectors:   cloneNestedStringMaps(v.nodePropertyVectors),
		NodeChunkVectors:      cloneStringSlices(v.nodeChunkVectors),
		NodeMetadataPersisted: v.nodeMetadataPersisted, NodeTypesPersisted: v.nodeTypesPersisted,
		Generation: v.generation,
	}
}

// Load is the sole vector tuple installer. It verifies metadata and the
// committed payload prefix before truncating any uncommitted append tail; an
// append tail is uncommitted because the .meta slot count is the commit point.
func (v *VectorFileStore) Load() error {
	v.appendMu.Lock()
	defer v.appendMu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.checkpointHealthLocked(); err != nil {
		return err
	}
	meta, err := decodeVectorCheckpoint(v.metaPath)
	if err != nil {
		if os.IsNotExist(err) && v.nextOrdinal == 0 {
			return nil // A new, header-only store has no committed population.
		}
		if os.IsNotExist(err) {
			return rejectVectorCheckpoint(err)
		}
		return err
	}
	if meta.Dimensions != v.dimensions {
		return rejectVectorCheckpoint(fmt.Errorf("meta dimensions %d do not match vector store dimensions %d", meta.Dimensions, v.dimensions))
	}
	if meta.DataSlots > v.nextOrdinal {
		return rejectVectorCheckpoint(fmt.Errorf("vector checkpoint slots %d exceed payload slots %d", meta.DataSlots, v.nextOrdinal))
	}
	stride, _ := v.vectorStride()
	digest, err := vectorPayloadDigest(v.file.File, vecHeaderSize+meta.DataSlots*stride)
	if err != nil {
		return err
	}
	if digest != meta.PayloadDigest {
		return rejectVectorCheckpoint(errors.New("vector payload integrity mismatch"))
	}
	if meta.DataSlots < v.nextOrdinal {
		// Uncommitted append tail: retire it through the same replacement
		// protocol instead of truncating the shared payload in place.
		base := v.vecPath[:len(v.vecPath)-len(".vec")]
		if err := v.stageCheckpointPrefixLocked(base, meta); err != nil {
			return err
		}
		return v.replaceCheckpointLocked(base, meta)
	}
	v.installCheckpointLocked(meta)
	return nil
}

// SetNodeQueryMetadata stores node-to-vector associations in the sidecar.
func (v *VectorFileStore) SetNodeQueryMetadata(labels map[string][]string, named, properties map[string]map[string]string, chunks map[string][]string) {
	v.setNodeQueryMetadata(labels, nil, named, properties, chunks, false)
}

// SetNodeQueryMetadataWithTypes also persists the legacy string type-property
// projection used by metadata-only search candidate filtering.
func (v *VectorFileStore) SetNodeQueryMetadataWithTypes(labels map[string][]string, types map[string]string, named, properties map[string]map[string]string, chunks map[string][]string) {
	v.setNodeQueryMetadata(labels, types, named, properties, chunks, true)
}

func (v *VectorFileStore) setNodeQueryMetadata(labels map[string][]string, types map[string]string, named, properties map[string]map[string]string, chunks map[string][]string, typesPersisted bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Empty and nil containers have the same query meaning, including after
	// an optional field's wire omission. They cannot create a new source
	// generation on every unchanged checkpoint. Keys and ordered label/chunk
	// values remain significant; the persisted-presence flags remain separate.
	if !maps.EqualFunc(v.nodeLabels, labels, slices.Equal[[]string]) || !maps.Equal(v.nodeTypes, types) ||
		!maps.EqualFunc(v.nodeNamedVectors, named, maps.Equal[map[string]string, map[string]string]) ||
		!maps.EqualFunc(v.nodePropertyVectors, properties, maps.Equal[map[string]string, map[string]string]) ||
		!maps.EqualFunc(v.nodeChunkVectors, chunks, slices.Equal[[]string]) || !v.nodeMetadataPersisted || v.nodeTypesPersisted != typesPersisted {
		v.generation.advance()
	}
	v.nodeLabels = cloneStringSlices(labels)
	v.nodeTypes = cloneStringMap(types)
	v.nodeNamedVectors = cloneNestedStringMaps(named)
	v.nodePropertyVectors = cloneNestedStringMaps(properties)
	v.nodeChunkVectors = cloneStringSlices(chunks)
	v.nodeMetadataPersisted = true
	v.nodeTypesPersisted = typesPersisted
}

// NodeQueryMetadata returns durable node-to-vector associations, if available.
func (v *VectorFileStore) NodeQueryMetadata() (map[string][]string, map[string]map[string]string, map[string]map[string]string, map[string][]string, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return cloneStringSlices(v.nodeLabels), cloneNestedStringMaps(v.nodeNamedVectors), cloneNestedStringMaps(v.nodePropertyVectors), cloneStringSlices(v.nodeChunkVectors), v.nodeMetadataPersisted
}

// NodeQueryMetadataWithTypes returns query metadata only when the sidecar has
// the complete type-property projection required for storage-free filtering.
func (v *VectorFileStore) NodeQueryMetadataWithTypes() (map[string][]string, map[string]string, map[string]map[string]string, map[string]map[string]string, map[string][]string, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return cloneStringSlices(v.nodeLabels), cloneStringMap(v.nodeTypes), cloneNestedStringMaps(v.nodeNamedVectors), cloneNestedStringMaps(v.nodePropertyVectors), cloneStringSlices(v.nodeChunkVectors), v.nodeMetadataPersisted && v.nodeTypesPersisted
}

func cloneStringSlices(source map[string][]string) map[string][]string {
	copy := make(map[string][]string, len(source))
	for key, values := range source {
		copy[key] = append([]string(nil), values...)
	}
	return copy
}

func cloneNestedStringMaps(source map[string]map[string]string) map[string]map[string]string {
	copy := make(map[string]map[string]string, len(source))
	for key, values := range source {
		valueCopy := make(map[string]string, len(values))
		for valueKey, value := range values {
			valueCopy[valueKey] = value
		}
		copy[key] = valueCopy
	}
	return copy
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
	if err := v.publicationErr; err != nil {
		v.mu.RUnlock()
		return err
	}
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
	if err := v.publicationErr; err != nil {
		return false, err
	}
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
	tmpPath := v.vecPath + ".next"
	tmp, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	publicationStarted := false
	defer func() {
		_ = tmp.Close()
		if !publicationStarted {
			_ = security.RemoveRootedPath(tmpPath)
		}
	}()

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
	meta := v.checkpointMetadataLocked()
	meta.IDToOrdinal, meta.DataSlots = newOrdinals, int64(len(newOrdinals))
	meta.BuildIndexedCount = int64(len(newOrdinals))
	publicationStarted = true
	if err := v.replaceCheckpointLocked(v.vecPath[:len(v.vecPath)-len(".vec")], meta); err != nil {
		return err
	}
	return nil
}

func (v *VectorFileStore) readVectorAtLocked(ordinal int64) ([]float32, error) {
	if v.file == nil {
		return nil, errVecFileClosed
	}
	if ordinal < 0 || ordinal >= v.nextOrdinal {
		return nil, fmt.Errorf("vector ordinal %d is outside %d slots", ordinal, v.nextOrdinal)
	}
	return readVectorFileRow(v.file, v.dimensions, v.vectorOffset(ordinal))
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
	mappingErr := v.releaseQueryReadMapLocked()
	if v.file != nil {
		err := v.file.Close()
		v.file = nil
		return errors.Join(mappingErr, err)
	}
	return mappingErr
}
