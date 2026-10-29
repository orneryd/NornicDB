// Package search provides full-text indexing with BM25 scoring.
package search

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/envutil"
)

// BM25 parameters (standard values)
const (
	bm25K1              = 1.2  // Term frequency saturation
	bm25B               = 0.75 // Length normalization
	bm25AnalyzerVersion = bm25TokenizerVersion
)

// bm25V2FormatVersion is the semver written into saved BM25 index files.
// On load, a file whose version is not equal to this is not loaded (caller rebuilds).
const bm25V2FormatVersion = "2.1.0"

const (
	bm25PrefixWeight           = 0.8
	bm25SparseMinDocSlots      = 100_000
	bm25DenseMinPostingVisits  = 4096
	bm25DenseDensityDivisor    = 8
	bm25MaxPooledDenseDocSlots = 1 << 20
	bm25MaxPooledSparseScores  = 4096
)

type bm25Posting struct {
	DocNum uint32 `msgpack:"d"`
	TF     uint16 `msgpack:"t"`
}

type bm25TermState struct {
	Postings []bm25Posting `msgpack:"p"`
	IDF      float64       `msgpack:"i"`
	// dead counts postings of removed documents still in Postings. Removing
	// a document only counts its postings dead (its docLengths entry becomes
	// 0, which scoring skips); a term's list is compacted once half of it is
	// dead, so removing N documents that share a term costs O(N) instead of
	// copying the list per removal (O(N²), #826). Not persisted: snapshots
	// write live postings only.
	dead int
}

// liveDocumentFrequency is the number of live documents holding the term.
func (st *bm25TermState) liveDocumentFrequency() int {
	return len(st.Postings) - st.dead
}

// compactLocked drops the postings of removed documents (docLengths 0, or no
// docLengths entry).
func (st *bm25TermState) compactLocked(docLengths []uint32) {
	live := st.Postings[:0]
	for _, posting := range st.Postings {
		if int(posting.DocNum) < len(docLengths) && docLengths[posting.DocNum] != 0 {
			live = append(live, posting)
		}
	}
	clear(st.Postings[len(live):])
	st.Postings = live
	st.dead = 0
}

// FulltextIndex provides BM25-based full-text search.
// It stores compact postings (docNum/tf) and uses bounded prefix expansion + top-k scoring.
type FulltextIndex struct {
	mu sync.RWMutex

	documents map[string]string

	docIDToNum         map[string]uint32
	docNumToID         []string
	docLengths         []uint32
	docIDsLexicalByNum bool

	termIndex map[string]*bm25TermState
	lexicon   []string
	// lexicon is rebuilt lazily because maintaining a sorted slice on every new
	// term makes vocabulary growth quadratic. Exact-term search uses termIndex;
	// only prefix expansion and persistence require the sorted snapshot.
	lexiconDirty bool

	avgDocLength   float64
	docCount       int
	totalDocLength int64

	version          uint64
	persistedVersion uint64
	queryPlanCache   sync.Map
	scoreScratchPool sync.Pool

	maxPrefixExpansions int
	minPrefixLength     int
	analyzer            Analyzer
}

// NewFulltextIndex creates a full-text search index with the supplied analyzer.
// A nil analyzer selects the default analyzer.
func NewFulltextIndex(analyzer Analyzer) *FulltextIndex {
	maxPrefixExpansions := envutil.GetInt("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", 0)
	if maxPrefixExpansions < 0 {
		maxPrefixExpansions = 0
	}
	minPrefixLength := envutil.GetInt("NORNICDB_BM25_PREFIX_MIN_LEN", 3)
	if minPrefixLength < 1 {
		minPrefixLength = 1
	}
	return &FulltextIndex{
		documents:           make(map[string]string),
		docIDToNum:          make(map[string]uint32),
		termIndex:           make(map[string]*bm25TermState),
		docIDsLexicalByNum:  true,
		maxPrefixExpansions: maxPrefixExpansions,
		minPrefixLength:     minPrefixLength,
		analyzer:            normalizeAnalyzer(analyzer),
	}
}

func (f *FulltextIndex) IsDirty() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.version != f.persistedVersion
}

func (f *FulltextIndex) markDirtyLocked() {
	f.version++
	f.queryPlanCache.Clear()
}

func (f *FulltextIndex) markPersisted(version uint64) {
	f.mu.Lock()
	if f.version == version {
		f.persistedVersion = version
	}
	f.mu.Unlock()
}

func (f *FulltextIndex) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.docCount
}

func (f *FulltextIndex) GetDocument(id string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	text, ok := f.documents[id]
	return text, ok
}

func (f *FulltextIndex) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.docCount == 0 && len(f.documents) == 0 && len(f.termIndex) == 0 {
		return
	}
	f.documents = make(map[string]string)
	f.docIDToNum = make(map[string]uint32)
	f.docNumToID = nil
	f.docLengths = nil
	f.docIDsLexicalByNum = true
	f.termIndex = make(map[string]*bm25TermState)
	f.lexicon = nil
	f.lexiconDirty = false
	f.docCount = 0
	f.totalDocLength = 0
	f.avgDocLength = 0
	f.markDirtyLocked()
}

// Index adds or updates a document. When prepared is false, it analyzes text
// itself and ignores terms and frequency. When prepared is true, those inputs
// must contain the analyzer's tokens and their frequencies for the text.
func (f *FulltextIndex) Index(id, text string, terms []string, frequency map[string]int, prepared bool) {
	if id == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	dirty := f.applyDocumentLocked(id, text, terms, frequency, prepared)
	f.updateAvgDocLengthLocked()
	if dirty {
		f.markDirtyLocked()
	}
}

// FulltextBatchEntry is one (id, text) pair for IndexBatch.
type FulltextBatchEntry struct {
	ID   string
	Text string
}

// IndexBatch adds or updates many documents under one lock and updates
// avgDocLength once at the end. Use this during bulk build to reduce lock
// contention and avoid O(N) avg updates.
func (f *FulltextIndex) IndexBatch(entries []FulltextBatchEntry) {
	if len(entries) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	dirty := false
	for _, entry := range entries {
		if entry.ID != "" && f.applyDocumentLocked(entry.ID, entry.Text, nil, nil, false) {
			dirty = true
		}
	}

	f.updateAvgDocLengthLocked()
	if dirty {
		f.markDirtyLocked()
	}
}

// applyDocumentLocked is the shared single-document mutation for live and
// batched indexing. The caller updates aggregate metadata once per batch.
func (f *FulltextIndex) applyDocumentLocked(id, text string, terms []string, frequency map[string]int, prepared bool) bool {
	removed := f.removeInternalLocked(id)
	if !prepared {
		terms = f.analyze(text)
	}
	if len(terms) == 0 {
		return removed
	}
	docNum, ok := f.docIDToNum[id]
	if !ok {
		docNum = uint32(len(f.docNumToID))
		if f.docIDsLexicalByNum {
			for previous := len(f.docNumToID) - 1; previous >= 0; previous-- {
				if previousID := f.docNumToID[previous]; previousID != "" {
					f.docIDsLexicalByNum = previousID < id
					break
				}
			}
		}
		f.docIDToNum[id] = docNum
		f.docNumToID = append(f.docNumToID, id)
		f.docLengths = append(f.docLengths, 0)
	} else {
		f.docNumToID[docNum] = id
	}
	if !prepared {
		frequency = make(map[string]int, len(terms))
		for _, term := range terms {
			frequency[term]++
		}
	}
	f.documents[id] = text
	f.docLengths[docNum] = uint32(len(terms))
	f.docCount++
	f.totalDocLength += int64(len(terms))
	for term, tf := range frequency {
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
	}
	return true
}

func (f *FulltextIndex) Remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeInternalLocked(id) {
		f.markDirtyLocked()
	}
}

func (f *FulltextIndex) removeInternalLocked(id string) bool {
	docNum, ok := f.docIDToNum[id]
	if !ok {
		return false
	}

	text, hasDoc := f.documents[id]
	if !hasDoc {
		delete(f.docIDToNum, id)
		return false
	}

	// The document is marked removed first (docLengths 0, docNumToID ""):
	// its postings stay in place, counted dead, until their term is
	// compacted (bm25TermState.dead).
	oldLen := f.docLengths[docNum]
	f.docNumToID[docNum] = ""
	f.docLengths[docNum] = 0
	tokens := f.analyze(text)
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
		st.dead++
		switch {
		case st.liveDocumentFrequency() <= 0:
			delete(f.termIndex, t)
			f.removeLexiconTermLocked(t)
		case st.dead*2 > len(st.Postings):
			st.compactLocked(f.docLengths)
		}
	}

	delete(f.documents, id)
	delete(f.docIDToNum, id)
	f.docCount--
	f.totalDocLength -= int64(oldLen)
	f.updateAvgDocLengthLocked()
	return true
}

func (f *FulltextIndex) Search(query string, limit int) []indexResult {
	return f.SearchAllowed(query, limit, nil)
}

// SearchAllowed is Search over the documents allowed accepts: a document it
// rejects is never scored or returned, so the top limit are the best allowed
// documents however many better ones it rejects (#938). allowed is called at
// most once per document, while the index's read lock is held; nil allows
// every document.
func (f *FulltextIndex) SearchAllowed(query string, limit int, allowed func(docID string) bool) []indexResult {
	if limit <= 0 {
		return nil
	}

	f.lockForSearch()
	defer f.mu.RUnlock()
	if f.docCount == 0 || f.avgDocLength <= 0 {
		return nil
	}

	var weightedTerms []weightedTermPostings
	if cached, ok := f.queryPlanCache.Load(query); ok && cached.(bm25QueryPlan).version == f.version {
		plan := cached.(bm25QueryPlan)
		weightedTerms = plan.terms
	} else {
		queryTerms := f.analyze(query)
		if len(queryTerms) == 0 {
			return nil
		}
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

	postingVisits := 0
	for _, weightedTerm := range weightedTerms {
		postingVisits += len(weightedTerm.postings)
	}
	useDenseScores := shouldUseDenseBM25Scores(len(f.docLengths), postingVisits)
	scratch := f.getScoreScratch(useDenseScores, postingVisits)
	lengthNormOffset := bm25K1 * (1 - bm25B)
	lengthNormScale := bm25K1 * bm25B / f.avgDocLength
	denseTouchedAfterFirstTerm := 0
	// The unfiltered dense loop is kept free of the allowed check: with it,
	// unfiltered searches measured about 8% slower (#938).
	if useDenseScores && allowed == nil {
		for termIndex, wt := range weightedTerms {
			termScale := wt.weight * wt.idf * (bm25K1 + 1)
			for _, p := range wt.postings {
				docLen := f.docLengths[p.DocNum]
				if docLen == 0 {
					continue
				}
				if scratch.generations[p.DocNum] != scratch.generation {
					scratch.generations[p.DocNum] = scratch.generation
					scratch.scores[p.DocNum] = 0
					scratch.touched = append(scratch.touched, p.DocNum)
				}
				tf := float64(p.TF)
				denominator := tf + lengthNormOffset + lengthNormScale*float64(docLen)
				scratch.scores[p.DocNum] += termScale * tf / denominator
			}
			if termIndex == 0 {
				denseTouchedAfterFirstTerm = len(scratch.touched)
			}
		}
	} else if useDenseScores {
		for termIndex, wt := range weightedTerms {
			termScale := wt.weight * wt.idf * (bm25K1 + 1)
			for _, p := range wt.postings {
				docLen := f.docLengths[p.DocNum]
				if docLen == 0 {
					continue
				}
				if scratch.generations[p.DocNum] != scratch.generation {
					scratch.generations[p.DocNum] = scratch.generation
					scratch.scores[p.DocNum] = 0
					// A rejected document is never touched, so it is never
					// ranked; later terms may add to its score, which nothing
					// reads.
					if allowed != nil && !allowed(f.docNumToID[p.DocNum]) {
						continue
					}
					scratch.touched = append(scratch.touched, p.DocNum)
				}
				tf := float64(p.TF)
				denominator := tf + lengthNormOffset + lengthNormScale*float64(docLen)
				scratch.scores[p.DocNum] += termScale * tf / denominator
			}
			if termIndex == 0 {
				denseTouchedAfterFirstTerm = len(scratch.touched)
			}
		}
	} else {
		var admitted map[uint32]bool
		if allowed != nil {
			admitted = make(map[uint32]bool)
		}
		for _, wt := range weightedTerms {
			termScale := wt.weight * wt.idf * (bm25K1 + 1)
			for _, p := range wt.postings {
				docLen := f.docLengths[p.DocNum]
				if docLen == 0 {
					continue
				}
				if allowed != nil {
					ok, seen := admitted[p.DocNum]
					if !seen {
						ok = allowed(f.docNumToID[p.DocNum])
						admitted[p.DocNum] = ok
					}
					if !ok {
						continue
					}
				}
				tf := float64(p.TF)
				denominator := tf + lengthNormOffset + lengthNormScale*float64(docLen)
				scratch.sparseScores[p.DocNum] += termScale * tf / denominator
			}
		}
	}

	var top []scoredDoc
	var tieDocIDs []string
	if !f.docIDsLexicalByNum {
		tieDocIDs = f.docNumToID
	}
	if useDenseScores {
		ascendingLexicalCandidates := tieDocIDs == nil && denseTouchedAfterFirstTerm == len(scratch.touched)
		top = topKFromDenseScores(scratch.scores, scratch.touched, tieDocIDs, ascendingLexicalCandidates, limit, scratch.top)
	} else {
		top = topKFromSparseScores(scratch.sparseScores, tieDocIDs, limit, scratch.top)
	}
	scratch.top = top
	out := make([]indexResult, 0, len(top))
	for _, s := range top {
		docID := f.docNumToID[s.docNum]
		if docID == "" {
			continue
		}
		out = append(out, indexResult{ID: docID, Score: s.score})
	}

	f.putScoreScratch(scratch)
	return out
}

func (f *FulltextIndex) PhraseSearch(phrase string, limit int) []indexResult {
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

func (f *FulltextIndex) LexicalSeedDocIDs(maxTerms, docsPerTerm int) []string {
	hints := f.LexicalSeedHints(maxTerms, docsPerTerm)
	if len(hints) == 0 {
		return nil
	}
	out := make([]string, len(hints))
	for i := range hints {
		out[i] = hints[i].ID
	}
	return out
}

// LexicalSeedHints returns ranked compact lexical metadata directly from the
// inverted index, without executing a BM25 search for each document.
func (f *FulltextIndex) LexicalSeedHints(maxTerms, docsPerTerm int) []LexicalSeedHint {
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
		df := st.liveDocumentFrequency()
		if df < lexicalSeedMinDocumentFrequency() {
			continue
		}
		idf := f.calculateIDFLocked(df)
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

	selectedTerms := make([]lexicalSeedTerm, 0, len(terms))
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
		selected := lexicalSeedTerm{term: t.term, idf: t.idf, docs: make([]lexicalSeedDocument, lim)}
		for i := 0; i < lim; i++ {
			selected.docs[i] = lexicalSeedDocument{id: docs[i].id, tf: uint32(docs[i].tf)}
		}
		selectedTerms = append(selectedTerms, selected)
	}
	return lexicalSeedHints(selectedTerms)
}

func (f *FulltextIndex) updateAvgDocLengthLocked() {
	if f.docCount <= 0 {
		f.docCount = 0
		f.totalDocLength = 0
		f.avgDocLength = 0
		return
	}
	f.avgDocLength = float64(f.totalDocLength) / float64(f.docCount)
}

// TermIDF is the BM25 IDF of an analyzed term, the weight a match on it
// gets: higher for a rarer term, 0 for one no live document holds.
func (f *FulltextIndex) TermIDF(term string) float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	st, ok := f.termIndex[term]
	if !ok {
		return 0
	}
	df := st.liveDocumentFrequency()
	if df == 0 {
		return 0
	}
	return f.calculateIDFLocked(df)
}

func (f *FulltextIndex) calculateIDFLocked(df int) float64 {
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

func (f *FulltextIndex) insertLexiconTermLocked(term string) {
	f.lexiconDirty = true
}

func (f *FulltextIndex) removeLexiconTermLocked(term string) {
	f.lexiconDirty = true
}

func (f *FulltextIndex) rebuildLexiconLocked() {
	if !f.lexiconDirty {
		return
	}
	lexicon := make([]string, 0, len(f.termIndex))
	for term, state := range f.termIndex {
		if state != nil && state.liveDocumentFrequency() > 0 {
			lexicon = append(lexicon, term)
		}
	}
	sort.Strings(lexicon)
	f.lexicon = lexicon
	f.lexiconDirty = false
}

// lockWithCurrentLexicon returns with the read lock held and guarantees that
// no mutation can make the sorted lexicon stale between rebuilding and use.
func (f *FulltextIndex) lockWithCurrentLexicon() {
	for {
		f.mu.RLock()
		if !f.lexiconDirty {
			return
		}
		f.mu.RUnlock()

		f.mu.Lock()
		f.rebuildLexiconLocked()
		f.mu.Unlock()
	}
}

func (f *FulltextIndex) lockForSearch() {
	if f.maxPrefixExpansions == 0 {
		f.mu.RLock()
		return
	}
	f.lockWithCurrentLexicon()
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

type bm25ScoreScratch struct {
	scores       []float64
	generations  []uint32
	touched      []uint32
	sparseScores map[uint32]float64
	top          minScoreHeap
	generation   uint32
}

func shouldUseDenseBM25Scores(docSlots, postingVisits int) bool {
	return docSlots < bm25SparseMinDocSlots ||
		postingVisits >= bm25DenseMinPostingVisits &&
			postingVisits >= (docSlots+bm25DenseDensityDivisor-1)/bm25DenseDensityDivisor
}

func (f *FulltextIndex) getScoreScratch(dense bool, postingVisits int) *bm25ScoreScratch {
	var scratch *bm25ScoreScratch
	if pooled := f.scoreScratchPool.Get(); pooled != nil {
		scratch = pooled.(*bm25ScoreScratch)
	} else {
		scratch = &bm25ScoreScratch{}
	}
	if dense {
		if cap(scratch.scores) < len(f.docLengths) {
			scratch.scores = make([]float64, len(f.docLengths))
			scratch.generations = make([]uint32, len(f.docLengths))
		} else {
			scratch.scores = scratch.scores[:len(f.docLengths)]
			scratch.generations = scratch.generations[:len(f.docLengths)]
		}
		scratch.generation++
		if scratch.generation == 0 {
			clear(scratch.generations)
			scratch.generation = 1
		}
		scratch.touched = scratch.touched[:0]
		scratch.sparseScores = nil
	} else {
		scratch.scores = nil
		scratch.generations = nil
		scratch.touched = nil
		if scratch.sparseScores == nil {
			scratch.sparseScores = make(map[uint32]float64, minInt(postingVisits, bm25MaxPooledSparseScores))
		} else {
			clear(scratch.sparseScores)
		}
	}
	return scratch
}

func (f *FulltextIndex) putScoreScratch(scratch *bm25ScoreScratch) {
	if cap(scratch.scores) > bm25MaxPooledDenseDocSlots {
		scratch.scores = nil
		scratch.generations = nil
		scratch.touched = nil
	}
	if len(scratch.sparseScores) > bm25MaxPooledSparseScores {
		scratch.sparseScores = nil
	}
	f.scoreScratchPool.Put(scratch)
}

func (f *FulltextIndex) expandAndWeightTermsLocked(queryTerms []string) []weightedTermPostings {
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
		if st == nil || st.liveDocumentFrequency() <= 0 {
			continue
		}
		idf := f.calculateIDFLocked(st.liveDocumentFrequency())
		upper := weight * idf * (bm25K1 + 1)
		if upper <= 0 {
			continue
		}
		terms = append(terms, weightedTermPostings{
			postings: st.Postings,
			idf:      idf,
			weight:   weight,
		})
	}
	return terms
}

func (f *FulltextIndex) analyze(text string) []string {
	return normalizeAnalyzer(f.analyzer).Analyze(text)
}

func lexicalSeedMinDocumentFrequency() int {
	minFrequency := envutil.GetInt("NORNICDB_BM25_IDF_MIN_DOC_FREQ", 2)
	if minFrequency < 1 {
		return 1
	}
	return minFrequency
}

// tokenize splits canonically equivalent Unicode text into exact terms.
func tokenize(text string) []string {
	return DefaultTextAnalyzer().Analyze(text)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
