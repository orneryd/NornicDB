// Checkpoint admission validation: Codex NOR-283 implementation lane, 29 October 2026.
package search

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/vmihailenco/msgpack/v5"
)

type bm25V2Snapshot struct {
	Version        string                    `msgpack:"v"`
	Documents      map[string]string         `msgpack:"docs"`
	DocIDToNum     map[string]uint32         `msgpack:"doc_id_to_num"`
	DocNumToID     []string                  `msgpack:"doc_num_to_id"`
	DocLengths     []uint32                  `msgpack:"doc_lengths"`
	TermIndex      map[string]*bm25TermState `msgpack:"terms"`
	Lexicon        []string                  `msgpack:"lexicon"`
	AvgDocLength   float64                   `msgpack:"avg_doc_len"`
	DocCount       int                       `msgpack:"doc_count"`
	TotalDocLength int64                     `msgpack:"total_doc_len"`
}

// EncodeMsgpack streams the snapshot and then its integrity trailer through
// the existing atomic snapshot writer. The digest covers the exact encoded
// MessagePack object, including its version and every map entry; it excludes
// the trailer itself and unused file capacity. No second index or payload
// buffer is built. Older, digest-free formats are rebuilt once by Load.
func (s *bm25V2Snapshot) EncodeMsgpack(encoder *msgpack.Encoder) error {
	type payload bm25V2Snapshot // Do not recursively invoke this encoder.
	digest := xxhash.New()
	if err := msgpack.NewEncoder(io.MultiWriter(encoder.Writer(), digest)).Encode((*payload)(s)); err != nil {
		return err
	}
	var trailer [8]byte
	binary.LittleEndian.PutUint64(trailer[:], digest.Sum64())
	_, err := encoder.Writer().Write(trailer[:])
	return err
}

var bm25IntegrityBuffers = sync.Pool{
	New: func() any { return new([32 * 1024]byte) },
}

// validBM25Integrity hashes the same payload bytes that were decoded, without
// re-encoding maps or tokenizing documents. The decoder's buffered position
// locates the trailer, so preallocated file capacity is not snapshot content.
func validBM25Integrity(file *os.File, reader *bufio.Reader) bool {
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return false
	}
	payloadBytes := position - int64(reader.Buffered())
	var trailer [8]byte
	if _, err := io.ReadFull(reader, trailer[:]); err != nil {
		return false
	}
	buffer := bm25IntegrityBuffers.Get().(*[32 * 1024]byte)
	defer bm25IntegrityBuffers.Put(buffer)
	digest := xxhash.New()
	if _, err := io.CopyBuffer(digest, io.NewSectionReader(file, 0, payloadBytes), buffer[:]); err != nil {
		return false
	}
	return digest.Sum64() == binary.LittleEndian.Uint64(trailer[:])
}

func (f *FulltextIndexV2) Save(path string) error {
	f.lockWithCurrentLexicon()
	docs := make(map[string]string, len(f.documents))
	for k, v := range f.documents {
		docs[k] = v
	}
	docIDToNum := make(map[string]uint32, len(f.docIDToNum))
	for k, v := range f.docIDToNum {
		docIDToNum[k] = v
	}
	docNumToID := append([]string(nil), f.docNumToID...)
	docLens := append([]uint32(nil), f.docLengths...)
	termIndex := make(map[string]*bm25TermState, len(f.termIndex))
	for term, st := range f.termIndex {
		// Live postings only: a removed document's postings (docLengths 0)
		// are not persisted (#826).
		postings := make([]bm25Posting, 0, st.liveDocumentFrequency())
		for _, posting := range st.Postings {
			if f.docLengths[posting.DocNum] != 0 {
				postings = append(postings, posting)
			}
		}
		termIndex[term] = &bm25TermState{
			Postings: postings,
			IDF:      st.IDF,
		}
	}
	lexicon := append([]string(nil), f.lexicon...)
	avg := f.avgDocLength
	docCount := f.docCount
	total := f.totalDocLength
	version := f.version
	f.mu.RUnlock()

	return saveBM25V2Snapshot(path, bm25V2Snapshot{
		Version:        bm25V2FormatVersion,
		Documents:      docs,
		DocIDToNum:     docIDToNum,
		DocNumToID:     docNumToID,
		DocLengths:     docLens,
		TermIndex:      termIndex,
		Lexicon:        lexicon,
		AvgDocLength:   avg,
		DocCount:       docCount,
		TotalDocLength: total,
	}, func() {
		f.markPersisted(version)
	})
}

func (f *FulltextIndexV2) SaveNoCopy(path string) error {
	f.lockWithCurrentLexicon()
	version := f.version
	snap := bm25V2Snapshot{
		Version:        bm25V2FormatVersion,
		Documents:      f.documents,
		DocIDToNum:     f.docIDToNum,
		DocNumToID:     f.docNumToID,
		DocLengths:     f.docLengths,
		TermIndex:      f.termIndex,
		Lexicon:        f.lexicon,
		AvgDocLength:   f.avgDocLength,
		DocCount:       f.docCount,
		TotalDocLength: f.totalDocLength,
	}
	f.mu.RUnlock()
	return saveBM25V2Snapshot(path, snap, func() {
		f.markPersisted(version)
	})
}

func saveBM25V2Snapshot(path string, snap bm25V2Snapshot, onSuccess func()) error {
	if err := writeMsgpackSnapshot(path, &snap); err != nil {
		return err
	}
	onSuccess()
	return nil
}

// Load replaces the fulltext index with the one stored at path (msgpack format).
// A missing file leaves the index as it is and no error is returned; a file
// that is not a compatible BM25 snapshot empties the index so the caller
// rebuilds it from graph storage. Only
// unexpected I/O (e.g. permission denied) returns an error.
func (f *FulltextIndexV2) Load(path string) error {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	var snap bm25V2Snapshot
	reader := bufio.NewReader(file.File)
	if err := msgpack.NewDecoder(reader).Decode(&snap); err != nil {
		logSearchPrintf("⚠️ BM25 load: failed decoding %s as v2 snapshot: %v", path, err)
		f.Clear()
		return nil
	}
	if snap.Version == "" || !searchIndexVersionCompatible(snap.Version, bm25V2FormatVersion, "BM25") {
		f.Clear()
		return nil
	}
	if !validBM25Integrity(file.File, reader) || !f.validV2Snapshot(snap) {
		logSearchPrintf("⚠️ BM25 load: inconsistent checkpoint %s; rebuilding from storage", path)
		f.Clear()
		return nil
	}
	f.applyV2Snapshot(snap)
	return nil
}

// validV2Snapshot is the structural admission owner. The integrity trailer
// binds term names/frequencies to the writer's payload, so loading never
// re-analyzes stored text. One postings pass accumulates lengths and duplicate
// stamps in a preallocated document-number array. SaveNoCopy's dead postings
// and the writer's uint16 frequency cap remain valid.
func (f *FulltextIndexV2) validV2Snapshot(s bm25V2Snapshot) bool {
	if s.DocCount != len(s.Documents) || s.DocCount != len(s.DocIDToNum) ||
		len(s.DocNumToID) != len(s.DocLengths) {
		return false
	}
	live := 0
	var total int64
	for number, id := range s.DocNumToID {
		length := s.DocLengths[number]
		if id == "" {
			if length != 0 {
				return false
			}
			continue
		}
		mapped, ok := s.DocIDToNum[id]
		if !ok || uint64(mapped) != uint64(number) || length == 0 {
			return false
		}
		if _, ok := s.Documents[id]; !ok {
			return false
		}
		live++
		total += int64(length)
	}
	if live != s.DocCount || total != s.TotalDocLength {
		return false
	}
	average := float64(0)
	if live != 0 {
		average = float64(total) / float64(live)
	}
	if s.AvgDocLength != average {
		return false
	}
	for id, number := range s.DocIDToNum {
		if id == "" || uint64(number) >= uint64(len(s.DocNumToID)) || s.DocNumToID[number] != id {
			return false
		}
	}
	if len(s.Lexicon) != len(s.TermIndex) {
		return false
	}
	// Writers already sort the lexicon. Preserve equivalent reordered input
	// without copying it; only that exceptional ordering needs normalization.
	if !sort.StringsAreSorted(s.Lexicon) {
		sort.Strings(s.Lexicon)
	}
	type documentCheck struct {
		tokens uint64
		term   int
	}
	checks := make([]documentCheck, len(s.DocLengths))
	const capped = uint64(1) << 63
	for termNumber, term := range s.Lexicon {
		state := s.TermIndex[term]
		if state == nil {
			return false
		}
		if termNumber > 0 && s.Lexicon[termNumber-1] == term {
			return false
		}
		ordered := true
		for position, posting := range state.Postings {
			if position > 0 && state.Postings[position-1].DocNum > posting.DocNum {
				ordered = false
			}
			if uint64(posting.DocNum) >= uint64(len(s.DocLengths)) {
				return false
			}
			if s.DocLengths[posting.DocNum] == 0 {
				continue // Lazy deletion: applyV2Snapshot compacts these away.
			}
			check := &checks[posting.DocNum]
			if posting.TF == 0 || check.term == termNumber+1 {
				return false
			}
			check.term = termNumber + 1
			check.tokens += uint64(posting.TF)
			if posting.TF == ^uint16(0) {
				check.tokens |= capped
			}
			if check.tokens & ^capped > uint64(s.DocLengths[posting.DocNum]) {
				return false
			}
		}
		if !ordered {
			// Normal writers append document numbers in order. Equivalent
			// reordered input is normalized in place, without another index.
			sort.Slice(state.Postings, func(i, j int) bool { return state.Postings[i].DocNum < state.Postings[j].DocNum })
		}
	}
	for number, check := range checks {
		if s.DocLengths[number] != 0 {
			if check.tokens == 0 || (check.tokens&capped == 0 && check.tokens != uint64(s.DocLengths[number])) {
				return false
			}
		}
	}
	return true
}

// applyV2Snapshot installs admitted content. Load is its sole production caller.
func (f *FulltextIndexV2) applyV2Snapshot(s bm25V2Snapshot) {
	if s.Documents == nil {
		s.Documents = make(map[string]string)
	}
	if s.DocIDToNum == nil {
		s.DocIDToNum = make(map[string]uint32)
	}
	if s.TermIndex == nil {
		s.TermIndex = make(map[string]*bm25TermState)
	}
	if s.DocNumToID == nil {
		s.DocNumToID = []string{}
	}
	if s.DocLengths == nil {
		s.DocLengths = []uint32{}
	}
	if s.Lexicon == nil {
		s.Lexicon = []string{}
	}

	f.mu.Lock()
	f.documents = s.Documents
	f.docIDToNum = s.DocIDToNum
	f.docNumToID = s.DocNumToID
	f.docLengths = s.DocLengths
	f.docIDsLexicalByNum = docIDsAreLexicalByNumber(s.DocNumToID)
	f.termIndex = s.TermIndex
	// SaveNoCopy writes the term index as it is, with the postings of
	// removed documents, and dead counts are not persisted: drop those
	// postings, and terms left without any (#826).
	for term, st := range f.termIndex {
		if st != nil {
			st.compactLocked(f.docLengths)
		}
		if st == nil || len(st.Postings) == 0 {
			delete(f.termIndex, term)
		}
	}
	f.lexicon = s.Lexicon
	f.lexiconDirty = len(f.lexicon) != len(f.termIndex) || !sort.StringsAreSorted(f.lexicon)
	f.avgDocLength = s.AvgDocLength
	f.docCount = s.DocCount
	f.totalDocLength = s.TotalDocLength
	if f.totalDocLength == 0 {
		for _, l := range f.docLengths {
			f.totalDocLength += int64(l)
		}
	}
	if f.docCount == 0 {
		f.docCount = len(f.docIDToNum)
	}
	f.version = 1
	f.persistedVersion = 1
	f.queryPlanCache.Clear()
	f.mu.Unlock()
}

func docIDsAreLexicalByNumber(docIDs []string) bool {
	previous := ""
	for _, id := range docIDs {
		if id == "" {
			continue
		}
		if previous != "" && previous >= id {
			return false
		}
		previous = id
	}
	return true
}
