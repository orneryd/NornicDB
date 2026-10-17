package search

import (
	"github.com/orneryd/nornicdb/pkg/envutil"
	"slices"
)

// LexicalSeedHint is compact build metadata derived directly from the BM25
// inverted index. Rank preserves high-IDF selection order, while Signature is
// a weighted SimHash used only to break equal vector-distance choices.
type LexicalSeedHint struct {
	ID        string
	Rank      uint32
	Signature uint64
}

type lexicalSeedTerm struct {
	term string
	idf  float64
	docs []lexicalSeedDocument
}

type lexicalSeedDocument struct {
	id string
	tf uint32
}

type lexicalSeedAccumulator struct {
	hint LexicalSeedHint
	bits [64]int64
}

func lexicalSeedHints(terms []lexicalSeedTerm) []LexicalSeedHint {
	if len(terms) == 0 {
		return nil
	}
	byID := make(map[string]*lexicalSeedAccumulator)
	ordered := make([]*lexicalSeedAccumulator, 0)
	for _, term := range terms {
		hash := stableLexicalHash(term.term)
		weight := int64(term.idf*1024) + 1
		for _, doc := range term.docs {
			acc := byID[doc.id]
			if acc == nil {
				acc = &lexicalSeedAccumulator{hint: LexicalSeedHint{ID: doc.id, Rank: uint32(len(ordered))}}
				byID[doc.id] = acc
				ordered = append(ordered, acc)
			}
			docWeight := weight * int64(max(uint32(1), doc.tf))
			for bit := uint(0); bit < 64; bit++ {
				if hash&(uint64(1)<<bit) != 0 {
					acc.bits[bit] += docWeight
				} else {
					acc.bits[bit] -= docWeight
				}
			}
		}
	}
	out := make([]LexicalSeedHint, len(ordered))
	for i, acc := range ordered {
		for bit, weight := range acc.bits {
			if weight >= 0 {
				acc.hint.Signature |= uint64(1) << uint(bit)
			}
		}
		if acc.hint.Signature == 0 {
			acc.hint.Signature = 1
		}
		out[i] = acc.hint
	}
	return out
}

func stableLexicalHash(value string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= prime
	}
	return hash
}

type lexicalSeedProvider interface {
	LexicalSeedDocIDs(maxTerms, perTerm int) []string
}

// bm25SeedDocIDs returns lexical seed doc IDs using configured limits.
// This helper centralizes BM25 seed selection so multiple ANN builders
// (k-means, IVF/PQ, HNSW insertion order) can reuse identical seed inputs.
func bm25SeedDocIDs(fulltext lexicalSeedProvider) []string {
	if fulltext == nil {
		return nil
	}
	maxTerms := envutil.GetInt("NORNICDB_KMEANS_SEED_MAX_TERMS", 256)
	if maxTerms < 16 {
		maxTerms = 16
	}
	perTerm := envutil.GetInt("NORNICDB_KMEANS_SEED_DOCS_PER_TERM", 1)
	if perTerm < 1 {
		perTerm = 1
	}
	return fulltext.LexicalSeedDocIDs(maxTerms, perTerm)
}

// bm25SeedEmbeddingIDs resolves ranked BM25 documents through the existing
// index metadata. Cluster/PQ builders own embedding IDs, including named,
// property and chunk embeddings, rather than full-text document IDs.
func (s *Service) bm25SeedEmbeddingIDs(nodeIDs []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		start := len(ids)
		ids = append(ids, s.nodeChunkVectors[nodeID]...)
		for _, id := range s.nodeNamedVector[nodeID] {
			ids = append(ids, id)
		}
		for _, id := range s.nodePropVector[nodeID] {
			ids = append(ids, id)
		}
		if len(ids) == start {
			ids = append(ids, nodeID)
		}
		// Map iteration must not change ranked construction order across backends.
		slices.Sort(ids[start:])
	}
	return ids
}
