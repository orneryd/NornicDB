package search

import (
	"cmp"
	"slices"
	"strings"
)

func hnswVectorFileStoreIterator(vfs *VectorFileStore, seedHints nodeLexicalHints) hnswBuildIterator {
	return hnswVectorChunkIterator(vfs.IterateChunked, seedHints)
}

func hnswVectorReadLeaseIterator(lease *vectorFileReadLease, seedHints nodeLexicalHints) hnswBuildIterator {
	return hnswVectorChunkIterator(lease.IterateChunked, seedHints)
}

func hnswVectorChunkIterator(iterate func(int, func([]string, [][]float32) error) error, seedHints nodeLexicalHints) hnswBuildIterator {
	return func(batchSize int, fn func([]hnswBuildPair) error) error {
		if batchSize <= 0 {
			batchSize = 10000
		}
		seededIDs := make(map[string]struct{})
		if len(seedHints) > 0 {
			seedPairs := make([]hnswBuildPair, 0, len(seedHints))
			if err := iterate(batchSize, func(ids []string, vecs [][]float32) error {
				for i := range ids {
					hint, ok := lexicalHintForVectorID(ids[i], seedHints)
					if !ok {
						continue
					}
					seedPairs = append(seedPairs, hnswBuildPair{
						id: ids[i], vec: append([]float32(nil), vecs[i]...), hint: hint, hasHint: true,
					})
				}
				return nil
			}); err != nil {
				return err
			}
			seedPairs = prioritizedLexicalBuildPairs(seedPairs)
			for _, pair := range seedPairs {
				seededIDs[pair.id] = struct{}{}
			}
			for start := 0; start < len(seedPairs); start += batchSize {
				end := min(start+batchSize, len(seedPairs))
				if err := fn(seedPairs[start:end]); err != nil {
					return err
				}
			}
		}
		return iterate(batchSize, func(ids []string, vecs [][]float32) error {
			batch := make([]hnswBuildPair, 0, len(ids))
			for i := range ids {
				if _, seeded := seededIDs[ids[i]]; seeded {
					continue
				}
				hint, hasHint := lexicalHintForVectorID(ids[i], seedHints)
				batch = append(batch, hnswBuildPair{id: ids[i], vec: vecs[i], hint: hint, hasHint: hasHint})
			}
			if len(batch) == 0 {
				return nil
			}
			return fn(batch)
		})
	}
}

func hnswOrderedVectorIndexPairs(vi *VectorIndex, seedHints nodeLexicalHints) []hnswBuildPair {
	vi.mu.RLock()
	pairs := make([]hnswBuildPair, 0, len(vi.vectors))
	for id, vec := range vi.vectors {
		pairs = append(pairs, hnswBuildPair{id: id, vec: vec})
	}
	vi.mu.RUnlock()
	if len(seedHints) == 0 || len(pairs) == 0 {
		return pairs
	}
	for i := range pairs {
		pairs[i].hint, pairs[i].hasHint = lexicalHintForVectorID(pairs[i].id, seedHints)
	}
	seedPairs := prioritizedLexicalBuildPairs(pairs)
	seededIDs := make(map[string]struct{}, len(seedPairs))
	for _, pair := range seedPairs {
		seededIDs[pair.id] = struct{}{}
	}
	otherPairs := make([]hnswBuildPair, 0, len(pairs)-len(seedPairs))
	for _, pair := range pairs {
		if _, seeded := seededIDs[pair.id]; !seeded {
			otherPairs = append(otherPairs, pair)
		}
	}

	pairs = append(seedPairs, otherPairs...)
	logSearchPrintf("[HNSW] 🧭 Lexical-seeded build order: %d seeded vectors prioritized", len(seedPairs))
	return pairs
}

// A directly indexed document remains its construction anchor. Otherwise
// named/property/chunk embeddings carry the document's ranked insertion slot.
// All other chunks retain annotations for graph linking without delaying the
// next document anchor. Memory and file-backed builders share this decision.
func prioritizedLexicalBuildPairs(pairs []hnswBuildPair) []hnswBuildPair {
	anchors := make(map[vectorNodeID]struct{})
	for _, pair := range pairs {
		if pair.hasHint && pair.id == mainKeyForLexicalHint(pair.hint) {
			anchors[pair.hint.ID] = struct{}{}
		}
	}
	out := make([]hnswBuildPair, 0)
	for _, pair := range pairs {
		if !pair.hasHint {
			continue
		}
		_, hasAnchor := anchors[pair.hint.ID]
		if !hasAnchor || pair.id == mainKeyForLexicalHint(pair.hint) {
			out = append(out, pair)
		}
	}
	slices.SortFunc(out, compareLexicalBuildPairs)
	return out
}

func compareLexicalBuildPairs(left, right hnswBuildPair) int {
	if order := cmp.Compare(left.hint.Rank, right.hint.Rank); order != 0 {
		return order
	}
	return strings.Compare(left.id, right.id)
}

func (s *Service) hnswLexicalSeedHints(ft bm25Index, graphM int) nodeLexicalHints {
	if ft == nil { return nil }
	if graphM < 2 { graphM = DefaultHNSWConfig().M }
	return nodeHintsFromValues(ft.LexicalSeedHints(graphM*graphM, max(1, graphM/2)))
}

// HNSW construction receives document-keyed hints only. Cache expansion is an
// explicit, separate entryLexicalHintsForNodes projection at the key owner.
func (s *Service) hnswBuildLexicalSeedHints(ft bm25Index, graphM int) nodeLexicalHints {
	return s.hnswLexicalSeedHints(ft, graphM)
}
