package search

import (
	"cmp"
	"slices"
	"strings"
)

func hnswVectorFileStoreIterator(vfs *VectorFileStore, seedHints map[string]LexicalSeedHint) hnswBuildIterator {
	return hnswVectorChunkIterator(vfs.IterateChunked, seedHints)
}

func hnswVectorReadLeaseIterator(lease *vectorFileReadLease, seedHints map[string]LexicalSeedHint) hnswBuildIterator {
	return hnswVectorChunkIterator(lease.IterateChunked, seedHints)
}

func hnswVectorChunkIterator(iterate func(int, func([]string, [][]float32) error) error, seedHints map[string]LexicalSeedHint) hnswBuildIterator {
	return func(batchSize int, fn func([]hnswBuildPair) error) error {
		if batchSize <= 0 {
			batchSize = 10000
		}
		if len(seedHints) > 0 {
			seedPairs := make([]hnswBuildPair, 0, len(seedHints))
			if err := iterate(batchSize, func(ids []string, vecs [][]float32) error {
				for i := range ids {
					hint, ok := seedHints[ids[i]]
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
			slices.SortFunc(seedPairs, compareLexicalBuildPairs)
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
				if _, seeded := seedHints[ids[i]]; seeded {
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

func hnswOrderedVectorIndexPairs(vi *VectorIndex, seedHints map[string]LexicalSeedHint) []hnswBuildPair {
	vi.mu.RLock()
	pairs := make([]hnswBuildPair, 0, len(vi.vectors))
	for id, vec := range vi.vectors {
		pairs = append(pairs, hnswBuildPair{id: id, vec: vec})
	}
	vi.mu.RUnlock()
	if len(seedHints) == 0 || len(pairs) == 0 {
		return pairs
	}
	seedPairs := make([]hnswBuildPair, 0, len(seedHints))
	otherPairs := make([]hnswBuildPair, 0, len(pairs))
	for _, pair := range pairs {
		if hint, ok := seedHints[pair.id]; ok {
			pair.hint, pair.hasHint = hint, true
			seedPairs = append(seedPairs, pair)
			continue
		}
		if hint, ok := lexicalHintForVectorID(pair.id, seedHints); ok {
			pair.hint, pair.hasHint = hint, true
		}
		otherPairs = append(otherPairs, pair)
	}
	slices.SortStableFunc(seedPairs, compareLexicalBuildPairs)
	pairs = append(seedPairs, otherPairs...)
	logSearchPrintf("[HNSW] 🧭 Lexical-seeded build order: %d seeded vectors prioritized", len(seedPairs))
	return pairs
}

func compareLexicalBuildPairs(left, right hnswBuildPair) int {
	if order := cmp.Compare(left.hint.Rank, right.hint.Rank); order != 0 {
		return order
	}
	return strings.Compare(left.id, right.id)
}

func (s *Service) hnswLexicalSeedHints(ft bm25Index, graphM int) map[string]LexicalSeedHint {
	if ft == nil {
		return nil
	}
	if graphM < 2 {
		graphM = DefaultHNSWConfig().M
	}
	hints := ft.LexicalSeedHints(graphM*graphM, max(1, graphM/2))
	out := make(map[string]LexicalSeedHint, len(hints))
	for _, hint := range hints {
		id := strings.TrimSpace(hint.ID)
		if id == "" {
			continue
		}
		hint.ID = id
		out[id] = hint
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *Service) hnswLexicalSeedNodeSet(ft bm25Index) map[string]struct{} {
	hints := s.hnswLexicalSeedHints(ft, DefaultHNSWConfig().M)
	if len(hints) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(hints))
	for id := range hints {
		out[id] = struct{}{}
	}
	return out
}

func lexicalHintForVectorID(vectorID string, hints map[string]LexicalSeedHint) (LexicalSeedHint, bool) {
	if len(hints) == 0 || vectorID == "" {
		return LexicalSeedHint{}, false
	}
	hint, ok := hints[normalizeVectorResultIDToNodeID(vectorID)]
	return hint, ok
}

func lexicalHintValues(hints map[string]LexicalSeedHint) []LexicalSeedHint {
	if len(hints) == 0 {
		return nil
	}
	out := make([]LexicalSeedHint, 0, len(hints))
	for _, hint := range hints {
		out = append(out, hint)
	}
	return out
}

func vectorIDInSeedNodeSet(vectorID string, seedNodeIDs map[string]struct{}) bool {
	if len(seedNodeIDs) == 0 || vectorID == "" {
		return false
	}
	_, ok := seedNodeIDs[normalizeVectorResultIDToNodeID(vectorID)]
	return ok
}
