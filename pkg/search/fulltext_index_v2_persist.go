package search

import (
	"errors"
	"os"
	"sort"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
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

func (f *FulltextIndex) Save(path string) error {
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

func (f *FulltextIndex) SaveNoCopy(path string) error {
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
func (f *FulltextIndex) Load(path string) error {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	var snap bm25V2Snapshot
	if err := util.DecodeMsgpackFile(file.File, &snap); err != nil {
		logSearchPrintf("⚠️ BM25 load: failed decoding %s as v2 snapshot: %v", path, err)
		f.Clear()
		return nil
	}
	if snap.Version == "" || !searchIndexVersionCompatible(snap.Version, bm25V2FormatVersion, "BM25") {
		f.Clear()
		return nil
	}
	f.applyV2Snapshot(snap)
	return nil
}

func (f *FulltextIndex) applyV2Snapshot(s bm25V2Snapshot) {
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
