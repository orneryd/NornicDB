package search

import (
	"container/heap"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

const (
	bm25V2FormatVersion = "2.1.0"
	bm25PrefixWeight    = 0.8
)

type bm25Posting struct {
	DocNum uint32 `msgpack:"d"`
	TF     uint16 `msgpack:"t"`
}

type bm25TermState struct {
	Postings []bm25Posting `msgpack:"p"`
	IDF      float64       `msgpack:"i"`
}

// FulltextIndexV2 provides a BM25 index optimized for large datasets.
// It stores compact postings (docNum/tf) and uses bounded prefix expansion + top-k scoring.
type FulltextIndexV2 struct {
	mu sync.RWMutex

	documents map[string]string

	docIDToNum map[string]uint32
	docNumToID []string
	docLengths []uint32

	termIndex map[string]*bm25TermState
	lexicon   []string

	avgDocLength   float64
	docCount       int
	totalDocLength int64

	version          uint64
	persistedVersion uint64
	queryPlanCache   sync.Map

	maxPrefixExpansions int
	minPrefixLength     int
}

func NewFulltextIndexV2() *FulltextIndexV2 {
	maxPrefixExpansions := envutil.GetInt("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", 0)
	if maxPrefixExpansions < 0 {
		maxPrefixExpansions = 0
	}
	minPrefixLength := envutil.GetInt("NORNICDB_BM25_PREFIX_MIN_LEN", 3)
	if minPrefixLength < 1 {
		minPrefixLength = 1
	}
	return &FulltextIndexV2{
		documents:           make(map[string]string),
		docIDToNum:          make(map[string]uint32),
		termIndex:           make(map[string]*bm25TermState),
		maxPrefixExpansions: maxPrefixExpansions,
		minPrefixLength:     minPrefixLength,
	}
}

func (f *FulltextIndexV2) IsDirty() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.version != f.persistedVersion
}

func (f *FulltextIndexV2) markDirtyLocked() {
	f.version++
	f.queryPlanCache.Clear()
}

func (f *FulltextIndexV2) markPersisted(version uint64) {
	f.mu.Lock()
	if f.version == version {
		f.persistedVersion = version
	}
	f.mu.Unlock()
}

func (f *FulltextIndexV2) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.docCount
}

func (f *FulltextIndexV2) GetDocument(id string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	text, ok := f.documents[id]
	return text, ok
}

func (f *FulltextIndexV2) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.docCount == 0 && len(f.documents) == 0 && len(f.termIndex) == 0 {
		return
	}
	f.documents = make(map[string]string)
	f.docIDToNum = make(map[string]uint32)
	f.docNumToID = nil
	f.docLengths = nil
	f.termIndex = make(map[string]*bm25TermState)
	f.lexicon = nil
	f.docCount = 0
	f.totalDocLength = 0
	f.avgDocLength = 0
	f.markDirtyLocked()
}

func (f *FulltextIndexV2) Index(id string, text string) {
	f.IndexBatch([]FulltextBatchEntry{{ID: id, Text: text}})
}

func (f *FulltextIndexV2) IndexBatch(entries []FulltextBatchEntry) {
	if len(entries) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	dirty := false
	for _, e := range entries {
		if e.ID == "" {
			continue
		}
		if f.removeInternalLocked(e.ID) {
			dirty = true
		}
		tokens := tokenize(e.Text)
		if len(tokens) == 0 {
			continue
		}

		docNum, ok := f.docIDToNum[e.ID]
		if !ok {
			docNum = uint32(len(f.docNumToID))
			f.docIDToNum[e.ID] = docNum
			f.docNumToID = append(f.docNumToID, e.ID)
			f.docLengths = append(f.docLengths, 0)
		} else {
			f.docNumToID[docNum] = e.ID
		}

		termFreq := make(map[string]int, len(tokens))
		for _, t := range tokens {
			termFreq[t]++
		}

		f.documents[e.ID] = e.Text
		f.docLengths[docNum] = uint32(len(tokens))
		f.docCount++
		f.totalDocLength += int64(len(tokens))

		for term, tf := range termFreq {
			if tf <= 0 {
				continue
			}
			st, exists := f.termIndex[term]
			if !exists {
				st = &bm25TermState{}
				f.termIndex[term] = st
				f.insertLexiconTermLocked(term)
			}
			st.Postings = append(st.Postings, bm25Posting{DocNum: docNum, TF: uint16(minInt(tf, math.MaxUint16))})
			st.IDF = f.calculateIDFLocked(len(st.Postings))
		}

		dirty = true
	}

	f.updateAvgDocLengthLocked()
	// IDF depends on total N; update all terms after any batch mutation.
	if dirty {
		for _, st := range f.termIndex {
			st.IDF = f.calculateIDFLocked(len(st.Postings))
		}
		f.markDirtyLocked()
	}
}

func (f *FulltextIndexV2) Remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeInternalLocked(id) {
		for _, st := range f.termIndex {
			st.IDF = f.calculateIDFLocked(len(st.Postings))
		}
		f.markDirtyLocked()
	}
}

func (f *FulltextIndexV2) removeInternalLocked(id string) bool {
	docNum, ok := f.docIDToNum[id]
	if !ok {
		return false
	}

	text, hasDoc := f.documents[id]
	if !hasDoc {
		delete(f.docIDToNum, id)
		return false
	}

	tokens := tokenize(text)
	seen := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		if _, exists := seen[t]; exists {
			continue
		}
		seen[t] = struct{}{}
		st := f.termIndex[t]
		if st == nil {
			continue
		}
		dst := st.Postings[:0]
		for _, p := range st.Postings {
			if p.DocNum != docNum {
				dst = append(dst, p)
			}
		}
		st.Postings = dst
		if len(st.Postings) == 0 {
			delete(f.termIndex, t)
			f.removeLexiconTermLocked(t)
		} else {
			st.IDF = f.calculateIDFLocked(len(st.Postings))
		}
	}

	delete(f.documents, id)
	delete(f.docIDToNum, id)
	f.docNumToID[docNum] = ""
	oldLen := f.docLengths[docNum]
	f.docLengths[docNum] = 0
	f.docCount--
	f.totalDocLength -= int64(oldLen)
	f.updateAvgDocLengthLocked()
	return true
}

func (f *FulltextIndexV2) Search(query string, limit int) []indexResult {
	if limit <= 0 {
		return nil
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.docCount == 0 || f.avgDocLength <= 0 {
		return nil
	}

	queryTerms := tokenize(query)
	if len(queryTerms) == 0 {
		return nil
	}

	var weightedTerms []weightedTermPostings
	if cached, ok := f.queryPlanCache.Load(query); ok && cached.(bm25QueryPlan).version == f.version {
		plan := cached.(bm25QueryPlan)
		weightedTerms = plan.terms
	} else {
		weightedTerms = f.expandAndWeightTermsLocked(queryTerms)
		if len(weightedTerms) == 0 {
			return nil
		}
		if len(query) <= 256 {
			f.queryPlanCache.Store(query, bm25QueryPlan{
				version: f.version,
				terms:   weightedTerms,
			})
		}
	}
	if len(weightedTerms) == 0 {
		return nil
	}

	scores := make(map[uint32]float64, 512)
	for _, wt := range weightedTerms {
		for _, p := range wt.postings {
			docLen := f.docLengths[p.DocNum]
			if docLen == 0 {
				continue
			}
			tf := float64(p.TF)
			numerator := tf * (bm25K1 + 1)
			denominator := tf + bm25K1*(1-bm25B+bm25B*(float64(docLen)/f.avgDocLength))
			scores[p.DocNum] += wt.weight * wt.idf * (numerator / denominator)
		}
	}

	top := topKFromScoresWithIDs(scores, limit, f.docNumToID)
	out := make([]indexResult, 0, len(top))
	for _, s := range top {
		docID := s.id
		if docID == "" {
			continue
		}
		out = append(out, indexResult{ID: docID, Score: s.score})
	}

	return out
}

func (f *FulltextIndexV2) PhraseSearch(phrase string, limit int) []indexResult {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if limit <= 0 {
		return nil
	}
	phrase = strings.ToLower(phrase)
	var results []indexResult
	for id, text := range f.documents {
		lower := strings.ToLower(text)
		if strings.Contains(lower, phrase) {
			idx := strings.Index(lower, phrase)
			score := 1.0 / (1.0 + float64(idx)/100.0)
			results = append(results, indexResult{ID: id, Score: score})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

func (f *FulltextIndexV2) LexicalSeedDocIDs(maxTerms, docsPerTerm int) []string {
	if maxTerms <= 0 || docsPerTerm <= 0 {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()

	type termEntry struct {
		term string
		idf  float64
		df   int
	}
	terms := make([]termEntry, 0, len(f.termIndex))
	for term, st := range f.termIndex {
		df := len(st.Postings)
		if df < lexicalSeedMinDocumentFrequency() {
			continue
		}
		idf := st.IDF
		if idf > 0 {
			terms = append(terms, termEntry{term: term, idf: idf, df: df})
		}
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].idf == terms[j].idf {
			if terms[i].df == terms[j].df {
				return terms[i].term < terms[j].term
			}
			return terms[i].df < terms[j].df
		}
		return terms[i].idf > terms[j].idf
	})
	if len(terms) > maxTerms {
		terms = terms[:maxTerms]
	}

	seen := make(map[string]struct{}, maxTerms*docsPerTerm)
	out := make([]string, 0, maxTerms*docsPerTerm)
	for _, t := range terms {
		st := f.termIndex[t.term]
		if st == nil {
			continue
		}
		type docTF struct {
			id string
			tf uint16
		}
		docs := make([]docTF, 0, len(st.Postings))
		for _, p := range st.Postings {
			id := f.docNumToID[p.DocNum]
			if id == "" {
				continue
			}
			docs = append(docs, docTF{id: id, tf: p.TF})
		}
		sort.Slice(docs, func(i, j int) bool {
			if docs[i].tf == docs[j].tf {
				return docs[i].id < docs[j].id
			}
			return docs[i].tf > docs[j].tf
		})
		lim := docsPerTerm
		if lim > len(docs) {
			lim = len(docs)
		}
		for i := 0; i < lim; i++ {
			id := docs[i].id
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func (f *FulltextIndexV2) updateAvgDocLengthLocked() {
	if f.docCount <= 0 {
		f.docCount = 0
		f.totalDocLength = 0
		f.avgDocLength = 0
		return
	}
	f.avgDocLength = float64(f.totalDocLength) / float64(f.docCount)
}

func (f *FulltextIndexV2) calculateIDFLocked(df int) float64 {
	if df <= 0 || f.docCount <= 0 {
		return 0
	}
	n := float64(f.docCount)
	d := float64(df)
	idf := math.Log(1 + (n-d+0.5)/(d+0.5))
	if idf < 0 {
		return 0
	}
	return idf
}

func (f *FulltextIndexV2) insertLexiconTermLocked(term string) {
	i := sort.SearchStrings(f.lexicon, term)
	if i < len(f.lexicon) && f.lexicon[i] == term {
		return
	}
	f.lexicon = append(f.lexicon, "")
	copy(f.lexicon[i+1:], f.lexicon[i:])
	f.lexicon[i] = term
}

func (f *FulltextIndexV2) removeLexiconTermLocked(term string) {
	i := sort.SearchStrings(f.lexicon, term)
	if i >= len(f.lexicon) || f.lexicon[i] != term {
		return
	}
	copy(f.lexicon[i:], f.lexicon[i+1:])
	f.lexicon = f.lexicon[:len(f.lexicon)-1]
}

type weightedTermPostings struct {
	postings []bm25Posting
	idf      float64
	weight   float64
}

type bm25QueryPlan struct {
	version uint64
	terms   []weightedTermPostings
}

func (f *FulltextIndexV2) expandAndWeightTermsLocked(queryTerms []string) []weightedTermPostings {
	termWeights := make(map[string]float64, len(queryTerms))
	for _, term := range queryTerms {
		termWeights[term] += 1.0
		if utf8.RuneCountInString(term) < f.minPrefixLength || f.maxPrefixExpansions == 0 {
			continue
		}
		start := sort.SearchStrings(f.lexicon, term)
		added := 0
		for i := start; i < len(f.lexicon); i++ {
			candidate := f.lexicon[i]
			if !strings.HasPrefix(candidate, term) {
				break
			}
			if candidate == term {
				continue
			}
			termWeights[candidate] += bm25PrefixWeight
			added++
			if added >= f.maxPrefixExpansions {
				break
			}
		}
	}

	terms := make([]weightedTermPostings, 0, len(termWeights))
	for term, weight := range termWeights {
		st := f.termIndex[term]
		if st == nil || len(st.Postings) == 0 {
			continue
		}
		upper := weight * st.IDF * (bm25K1 + 1)
		if upper <= 0 {
			continue
		}
		terms = append(terms, weightedTermPostings{
			postings: st.Postings,
			idf:      st.IDF,
			weight:   weight,
		})
	}
	return terms
}

type scoredDoc struct {
	docNum uint32
	id     string
	score  float64
}

type minScoreHeap []scoredDoc

func (h minScoreHeap) Len() int            { return len(h) }
func (h minScoreHeap) Less(i, j int) bool  { return scoredDocWorse(h[i], h[j]) }
func (h minScoreHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minScoreHeap) Push(x interface{}) { *h = append(*h, x.(scoredDoc)) }
func (h *minScoreHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func topKMinScore(scores map[uint32]float64, k int) float64 {
	if k <= 0 || len(scores) < k {
		return 0
	}
	h := make(minScoreHeap, 0, k)
	for docNum, score := range scores {
		if len(h) < k {
			heap.Push(&h, scoredDoc{docNum: docNum, score: score})
			continue
		}
		candidate := scoredDoc{docNum: docNum, score: score}
		if scoredDocBetter(candidate, h[0]) {
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}
	if len(h) < k {
		return 0
	}
	return h[0].score
}

func topKFromScores(scores map[uint32]float64, k int) []scoredDoc {
	return topKFromScoresWithIDs(scores, k, nil)
}

func topKFromScoresWithIDs(scores map[uint32]float64, k int, docNumToID []string) []scoredDoc {
	if k <= 0 || len(scores) == 0 {
		return nil
	}
	h := make(minScoreHeap, 0, k)
	for docNum, score := range scores {
		id := ""
		if int(docNum) < len(docNumToID) {
			id = docNumToID[docNum]
		}
		candidate := scoredDoc{docNum: docNum, id: id, score: score}
		if len(h) < k {
			heap.Push(&h, candidate)
			continue
		}
		if scoredDocBetter(candidate, h[0]) {
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}
	out := make([]scoredDoc, len(h))
	for i := len(h) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&h).(scoredDoc)
	}
	return out
}

func scoredDocWorse(left, right scoredDoc) bool {
	if left.score != right.score {
		return left.score < right.score
	}
	if left.id != right.id {
		return left.id > right.id
	}
	return left.docNum > right.docNum
}

func scoredDocBetter(left, right scoredDoc) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	if left.id != right.id {
		return left.id < right.id
	}
	return left.docNum < right.docNum
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
