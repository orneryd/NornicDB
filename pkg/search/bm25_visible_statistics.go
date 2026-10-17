package search

import (
	"math"
	"sort"
)

// visibleStatisticsIndex is a BM25 index that ranks a restricted reader's
// search as an index holding only the documents the reader may see would:
// the same plan (prefix expansions included), statistics, scores and ranks.
// documentIDs snapshots the indexed documents so the caller can decide
// visibility before any index lock is taken; SearchVisible's visible then
// only looks that decision up and never reads storage.
type visibleStatisticsIndex interface {
	documentIDs() []string
	SearchVisible(query string, limit int, visible, allowed func(docID string) bool) []indexResult
}

// bm25VisibleIDF is BM25's IDF over a visible corpus of n documents, df of
// which hold the term (the +1 smoothed form both engines use).
func bm25VisibleIDF(n, df int) float64 {
	idf := math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))
	if idf < 0 {
		return 0
	}
	return idf
}

// bm25VisibleTermScore is one term's BM25 contribution to a document.
func bm25VisibleTermScore(weight, idf float64, tf, docLen int, averageLength float64) float64 {
	t := float64(tf)
	return weight * idf * t * (bm25K1 + 1) / (t + bm25K1*(1-bm25B+bm25B*float64(docLen)/averageLength))
}

// rankVisible sorts results by score, then ID, and cuts them to limit.
func rankVisible(out []indexResult, limit int) []indexResult {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (f *FulltextIndexV2) documentIDs() []string {
	f.lockForSearch()
	defer f.mu.RUnlock()
	ids := make([]string, 0, len(f.docNumToID))
	for docNum, id := range f.docNumToID {
		if id != "" && docNum < len(f.docLengths) && f.docLengths[docNum] != 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// SearchVisible is SearchAllowed with BM25's plan and statistics (the number
// of documents, their average length, each term's document frequency, and
// which prefix expansions are taken) over the documents visible accepts, not
// the whole index: a document the reader can't see changes none of its
// scores, ranks or results. allowed (nil: every visible document) further
// limits which documents are scored, as SearchAllowed's does. visible runs
// under the index's read lock, so it must not read storage: Service.Search
// passes a lookup of visibility it decided beforehand (documentIDs). A nil
// visible is SearchAllowed.
func (f *FulltextIndexV2) SearchVisible(query string, limit int, visible, allowed func(docID string) bool) []indexResult {
	if visible == nil {
		return f.SearchAllowed(query, limit, allowed)
	}
	if limit <= 0 {
		return nil
	}
	f.lockForSearch()
	defer f.mu.RUnlock()
	queryTerms := f.analyze(query)
	if f.docCount == 0 || len(queryTerms) == 0 {
		return nil
	}
	seen := make(map[uint32]bool)
	visibleDoc := func(docNum uint32) bool {
		ok, known := seen[docNum]
		if !known {
			ok = int(docNum) < len(f.docNumToID) && int(docNum) < len(f.docLengths) && f.docLengths[docNum] != 0 &&
				f.docNumToID[docNum] != "" && visible(f.docNumToID[docNum])
			seen[docNum] = ok
		}
		return ok
	}
	visibleCount, visibleLength := 0, 0
	for docNum := range f.docNumToID {
		if visibleDoc(uint32(docNum)) {
			visibleCount++
			visibleLength += int(f.docLengths[docNum])
		}
	}
	if visibleCount == 0 {
		return nil
	}
	visibleDF := func(term string) int {
		st := f.termIndex[term]
		if st == nil {
			return 0
		}
		df := 0
		for _, p := range st.Postings {
			if visibleDoc(p.DocNum) {
				df++
			}
		}
		return df
	}
	plan := f.expandTermsLocked(queryTerms, func(term string) bool { return visibleDF(term) > 0 })
	averageLength := float64(visibleLength) / float64(visibleCount)
	admitted := make(map[uint32]bool)
	scores := make(map[uint32]float64)
	for term, weight := range plan {
		idf := bm25VisibleIDF(visibleCount, visibleDF(term))
		for _, p := range f.termIndex[term].Postings {
			if !visibleDoc(p.DocNum) {
				continue
			}
			if allowed != nil {
				ok, known := admitted[p.DocNum]
				if !known {
					ok = allowed(f.docNumToID[p.DocNum])
					admitted[p.DocNum] = ok
				}
				if !ok {
					continue
				}
			}
			scores[p.DocNum] += bm25VisibleTermScore(weight, idf, int(p.TF), int(f.docLengths[p.DocNum]), averageLength)
		}
	}
	out := make([]indexResult, 0, len(scores))
	for docNum, score := range scores {
		out = append(out, indexResult{ID: f.docNumToID[docNum], Score: score})
	}
	return rankVisible(out, limit)
}

func (f *FulltextIndex) documentIDs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make([]string, 0, len(f.docLengths))
	for id := range f.docLengths {
		ids = append(ids, id)
	}
	return ids
}

// SearchVisible is FulltextIndexV2.SearchVisible for the V1 engine: its
// exact and prefix matches (every indexed term a query term prefixes, at
// 0.8 of its IDF) scored with the statistics of the documents visible
// accepts. visible must not read storage, as for V2.
func (f *FulltextIndex) SearchVisible(query string, limit int, visible, allowed func(docID string) bool) []indexResult {
	if visible == nil {
		return f.SearchAllowed(query, limit, allowed)
	}
	if limit <= 0 {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	queryTerms := f.analyze(query)
	if f.docCount == 0 || len(queryTerms) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	visibleDoc := func(docID string) bool {
		ok, known := seen[docID]
		if !known {
			_, indexed := f.docLengths[docID]
			ok = indexed && visible(docID)
			seen[docID] = ok
		}
		return ok
	}
	visibleCount, visibleLength := 0, 0
	for docID, length := range f.docLengths {
		if visibleDoc(docID) {
			visibleCount++
			visibleLength += length
		}
	}
	if visibleCount == 0 {
		return nil
	}
	visibleDF := func(term string) int {
		df := 0
		for docID := range f.invertedIndex[term] {
			if visibleDoc(docID) {
				df++
			}
		}
		return df
	}
	averageLength := float64(visibleLength) / float64(visibleCount)
	admitted := make(map[string]bool)
	scores := make(map[string]float64)
	score := func(term string, weight float64) {
		df := visibleDF(term)
		if df == 0 {
			return
		}
		idf := bm25VisibleIDF(visibleCount, df)
		for docID, tf := range f.invertedIndex[term] {
			if !visibleDoc(docID) {
				continue
			}
			if allowed != nil {
				ok, known := admitted[docID]
				if !known {
					ok = allowed(docID)
					admitted[docID] = ok
				}
				if !ok {
					continue
				}
			}
			scores[docID] += bm25VisibleTermScore(weight, idf, tf, f.docLengths[docID], averageLength)
		}
	}
	for _, term := range queryTerms {
		score(term, 1)
		for indexedTerm := range f.invertedIndex {
			if indexedTerm != term && len(indexedTerm) > len(term) && indexedTerm[:len(term)] == term {
				score(indexedTerm, 0.8)
			}
		}
	}
	out := make([]indexResult, 0, len(scores))
	for docID, s := range scores {
		out = append(out, indexResult{ID: docID, Score: s})
	}
	return rankVisible(out, limit)
}

// visibleIDFReader is a BM25 index that can weigh a query term by its IDF
// over a restricted reader's visible documents (match term weights,
// matchQueryTermsVisible). visible must not read storage.
type visibleIDFReader interface {
	visibleTermIDF(term string, visible func(docID string) bool) float64
}

func (f *FulltextIndexV2) visibleTermIDF(term string, visible func(docID string) bool) float64 {
	f.lockForSearch()
	defer f.mu.RUnlock()
	visibleDoc := func(docNum int) bool {
		return docNum < len(f.docNumToID) && docNum < len(f.docLengths) && f.docLengths[docNum] != 0 &&
			f.docNumToID[docNum] != "" && visible(f.docNumToID[docNum])
	}
	count := 0
	for docNum := range f.docNumToID {
		if visibleDoc(docNum) {
			count++
		}
	}
	df := 0
	if st := f.termIndex[term]; st != nil {
		for _, p := range st.Postings {
			if visibleDoc(int(p.DocNum)) {
				df++
			}
		}
	}
	if df == 0 {
		return 0
	}
	return bm25VisibleIDF(count, df)
}

func (f *FulltextIndex) visibleTermIDF(term string, visible func(docID string) bool) float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	count := 0
	for docID := range f.docLengths {
		if visible(docID) {
			count++
		}
	}
	df := 0
	for docID := range f.invertedIndex[term] {
		if visible(docID) {
			df++
		}
	}
	if df == 0 {
		return 0
	}
	return bm25VisibleIDF(count, df)
}
