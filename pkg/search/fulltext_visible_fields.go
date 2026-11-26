package search

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

// visibleFieldCorpus is policy-derived visibility metadata over one shared
// FulltextIndexV2 revision. It holds field masks and corpus statistics, never
// a second copy of the index's document text or postings.
type visibleFieldCorpus struct {
	index       *FulltextIndexV2
	version     uint64
	masks       []uint64
	extraMasks  map[uint32][]uint64
	docCount    int
	totalLength int64
	stale       atomic.Bool
}

type visibleBM25Index struct{ corpus *visibleFieldCorpus }

func (v visibleBM25Index) Search(query string, limit int) []indexResult {
	return v.corpus.search(query, limit, nil)
}

func (v visibleBM25Index) SearchAllowed(query string, limit int, allowed func(string) bool) []indexResult {
	return v.corpus.search(query, limit, allowed)
}

func (v visibleBM25Index) Count() int { return v.corpus.docCount }

func (v visibleBM25Index) TermIDF(term string) float64 { return v.corpus.termIDF(term) }

func (v *visibleFieldCorpus) fieldAllowed(docNum uint32, field int) bool {
	if int(docNum) >= len(v.masks) {
		return false
	}
	if field < 64 {
		return v.masks[docNum]&(uint64(1)<<uint(field)) != 0
	}
	extra := v.extraMasks[docNum]
	word := (field - 64) / 64
	return word < len(extra) && extra[word]&(uint64(1)<<uint(field%64)) != 0
}

func (v *visibleFieldCorpus) visibleLengthLocked(docNum uint32) uint32 {
	if int(docNum) >= len(v.index.docNumToID) {
		return 0
	}
	doc := v.index.fieldDocuments[v.index.docNumToID[docNum]]
	var length uint32
	for field, part := range doc.Fields {
		if v.fieldAllowed(docNum, field) {
			length += part.Length
		}
	}
	return length
}

func (v *visibleFieldCorpus) visibleTermFrequencyLocked(docNum uint32, term string) uint16 {
	if int(docNum) >= len(v.index.docNumToID) {
		return 0
	}
	doc := v.index.fieldDocuments[v.index.docNumToID[docNum]]
	var frequency uint32
	for field, part := range doc.Fields {
		if !v.fieldAllowed(docNum, field) {
			continue
		}
		position := sort.Search(len(part.Terms), func(i int) bool { return part.Terms[i].Term >= term })
		if position < len(part.Terms) && part.Terms[position].Term == term {
			frequency += uint32(part.Terms[position].Frequency)
			if frequency >= 1<<16-1 {
				return 1<<16 - 1
			}
		}
	}
	return uint16(frequency)
}

func (v *visibleFieldCorpus) visibleDocumentFrequencyLocked(term string) int {
	state := v.index.termIndex[term]
	if state == nil {
		return 0
	}
	frequency := 0
	for _, posting := range state.Postings {
		if int(posting.DocNum) < len(v.index.docLengths) && v.index.docLengths[posting.DocNum] != 0 &&
			v.visibleTermFrequencyLocked(posting.DocNum, term) != 0 {
			frequency++
		}
	}
	return frequency
}

func (v *visibleFieldCorpus) termIDF(term string) float64 {
	f := v.index
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.version != v.version {
		v.stale.Store(true)
		return 0
	}
	return v.idf(v.visibleDocumentFrequencyLocked(term))
}

func (v *visibleFieldCorpus) idf(df int) float64 {
	if df <= 0 || v.docCount <= 0 {
		return 0
	}
	n, d := float64(v.docCount), float64(df)
	return math.Log(1 + (n-d+0.5)/(d+0.5))
}

// search scores visible fields from the shared index's postings. Rejected
// documents affect neither visible corpus statistics nor the ranked page.
func (v *visibleFieldCorpus) search(query string, limit int, allowed func(string) bool) []indexResult {
	if limit <= 0 || v.docCount == 0 || v.totalLength == 0 {
		return nil
	}
	f := v.index
	f.lockForSearch()
	defer f.mu.RUnlock()
	if f.version != v.version {
		v.stale.Store(true)
		return nil
	}
	queryTerms := f.analyze(query)
	if len(queryTerms) == 0 {
		return nil
	}
	weights := make(map[string]float64, len(queryTerms))
	for _, term := range queryTerms {
		weights[term] += 1
		if f.maxPrefixExpansions == 0 || utf8.RuneCountInString(term) < f.minPrefixLength {
			continue
		}
		added := 0
		for i := sort.SearchStrings(f.lexicon, term); i < len(f.lexicon); i++ {
			candidate := f.lexicon[i]
			if !strings.HasPrefix(candidate, term) {
				break
			}
			if candidate == term || v.visibleDocumentFrequencyLocked(candidate) == 0 {
				continue
			}
			weights[candidate] += bm25PrefixWeight
			added++
			if added >= f.maxPrefixExpansions {
				break
			}
		}
	}
	lengthOffset := bm25K1 * (1 - bm25B)
	lengthScale := bm25K1 * bm25B / (float64(v.totalLength) / float64(v.docCount))
	scores := make(map[uint32]float64)
	var admissions map[uint32]bool
	if allowed != nil {
		admissions = make(map[uint32]bool)
	}
	for term, weight := range weights {
		state := f.termIndex[term]
		if state == nil {
			continue
		}
		idf := v.idf(v.visibleDocumentFrequencyLocked(term))
		if idf <= 0 {
			continue
		}
		termScale := weight * idf * (bm25K1 + 1)
		for _, posting := range state.Postings {
			docNum := posting.DocNum
			if int(docNum) >= len(f.docLengths) || f.docLengths[docNum] == 0 {
				continue
			}
			tf := v.visibleTermFrequencyLocked(docNum, term)
			if tf == 0 {
				continue
			}
			if allowed != nil {
				admitted, seen := admissions[docNum]
				if !seen {
					admitted = allowed(f.docNumToID[docNum])
					admissions[docNum] = admitted
				}
				if !admitted {
					continue
				}
			}
			docLength := v.visibleLengthLocked(docNum)
			if docLength == 0 {
				continue
			}
			t := float64(tf)
			scores[docNum] += termScale * t / (t + lengthOffset + lengthScale*float64(docLength))
		}
	}
	top := topKFromSparseScores(scores, f.docNumToID, limit, nil)
	out := make([]indexResult, 0, len(top))
	for _, hit := range top {
		out = append(out, indexResult{ID: f.docNumToID[hit.docNum], Score: hit.score})
	}
	return out
}
