package search

import (
	"errors"
	"log"
	"math"
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

type bm25V1Snapshot struct {
	Version       string
	Documents     map[string]string
	InvertedIndex map[string]map[string]int
	DocLengths    map[string]int
	AvgDocLength  float64
	DocCount      int
}

func (f *FulltextIndexV2) Save(path string) error {
	f.mu.RLock()
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
		postings := append([]bm25Posting(nil), st.Postings...)
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
	f.mu.RLock()
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

func (f *FulltextIndexV2) Load(path string) error {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	var v2 bm25V2Snapshot
	if err := util.DecodeMsgpackFile(file.File, &v2); err == nil && v2.Version != "" &&
		searchIndexVersionCompatible(v2.Version, bm25V2FormatVersion, "BM25 V2") {
		f.applyV2Snapshot(v2)
		return nil
	} else if err != nil {
		log.Printf("⚠️ BM25 V2 load: failed decoding %s as v2 snapshot: %v", path, err)
	}

	// Fallback: try legacy BM25 V1 file format and migrate to V2 in-memory.
	fileLegacy, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer fileLegacy.Close()

	var v1 bm25V1Snapshot
	if err := util.DecodeMsgpackFile(fileLegacy.File, &v1); err != nil {
		log.Printf("⚠️ BM25 V2 load: failed decoding %s as legacy snapshot: %v", path, err)
		f.Clear()
		return nil
	}
	if !searchIndexVersionCompatible(v1.Version, "1.0.0", "BM25") {
		f.Clear()
		return nil
	}
	if v1.Documents == nil {
		v1.Documents = make(map[string]string)
	}
	if v1.InvertedIndex == nil {
		v1.InvertedIndex = make(map[string]map[string]int)
	}
	if v1.DocLengths == nil {
		v1.DocLengths = make(map[string]int)
	}
	f.migrateFromV1Snapshot(v1)
	return nil
}

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
	f.lexicon = s.Lexicon
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

func (f *FulltextIndexV2) migrateFromV1Snapshot(v1 bm25V1Snapshot) {
	f.Clear()
	f.mu.Lock()
	defer f.mu.Unlock()

	f.documents = make(map[string]string, len(v1.Documents))
	for id, text := range v1.Documents {
		f.documents[id] = text
	}

	// Stable doc-number assignment for compact postings.
	docIDs := make([]string, 0, len(v1.DocLengths))
	for id := range v1.DocLengths {
		docIDs = append(docIDs, id)
	}
	sort.Strings(docIDs)

	f.docIDToNum = make(map[string]uint32, len(docIDs))
	f.docNumToID = make([]string, len(docIDs))
	f.docLengths = make([]uint32, len(docIDs))
	f.docIDsLexicalByNum = true

	var total int64
	for i, id := range docIDs {
		docNum := uint32(i)
		f.docIDToNum[id] = docNum
		f.docNumToID[docNum] = id
		l := v1.DocLengths[id]
		if l < 0 {
			l = 0
		}
		f.docLengths[docNum] = uint32(l)
		total += int64(l)
	}
	f.docCount = len(docIDs)
	f.totalDocLength = total
	if v1.DocCount > 0 {
		f.docCount = v1.DocCount
	}
	f.updateAvgDocLengthLocked()

	f.termIndex = make(map[string]*bm25TermState, len(v1.InvertedIndex))
	f.lexicon = make([]string, 0, len(v1.InvertedIndex))
	for term, docs := range v1.InvertedIndex {
		if len(docs) == 0 {
			continue
		}
		postings := make([]bm25Posting, 0, len(docs))
		for id, tf := range docs {
			docNum, ok := f.docIDToNum[id]
			if !ok || tf <= 0 {
				continue
			}
			postings = append(postings, bm25Posting{
				DocNum: docNum,
				TF:     uint16(minInt(tf, math.MaxUint16)),
			})
		}
		if len(postings) == 0 {
			continue
		}
		sort.Slice(postings, func(i, j int) bool { return postings[i].DocNum < postings[j].DocNum })
		st := &bm25TermState{Postings: postings}
		st.IDF = f.calculateIDFLocked(len(postings))
		f.termIndex[term] = st
		f.lexicon = append(f.lexicon, term)
	}
	sort.Strings(f.lexicon)
	f.markDirtyLocked()
	f.persistedVersion = f.version
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
