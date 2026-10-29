package search

import "slices"

type scoredDoc struct {
	docNum uint32
	score  float64
}

type minScoreHeap []scoredDoc

func pushMinScore(h minScoreHeap, candidate scoredDoc, docIDs []string) minScoreHeap {
	h = append(h, candidate)
	child := len(h) - 1
	if len(docIDs) == 0 {
		for child > 0 {
			parent := (child - 1) / 2
			if !scoredDocNumRanksBefore(h[parent], h[child]) {
				break
			}
			h[parent], h[child] = h[child], h[parent]
			child = parent
		}
		return h
	}
	for child > 0 {
		parent := (child - 1) / 2
		if !scoredDocRanksBefore(h[parent], h[child], docIDs) {
			break
		}
		h[parent], h[child] = h[child], h[parent]
		child = parent
	}
	return h
}

func replaceMinScore(h minScoreHeap, candidate scoredDoc, docIDs []string) {
	h[0] = candidate
	parent := 0
	if len(docIDs) == 0 {
		for {
			left := parent*2 + 1
			if left >= len(h) {
				return
			}
			smallest := left
			right := left + 1
			if right < len(h) && scoredDocNumRanksBefore(h[left], h[right]) {
				smallest = right
			}
			if !scoredDocNumRanksBefore(h[parent], h[smallest]) {
				return
			}
			h[parent], h[smallest] = h[smallest], h[parent]
			parent = smallest
		}
	}
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(h) && scoredDocRanksBefore(h[left], h[right], docIDs) {
			smallest = right
		}
		if !scoredDocRanksBefore(h[parent], h[smallest], docIDs) {
			return
		}
		h[parent], h[smallest] = h[smallest], h[parent]
		parent = smallest
	}
}

func sortScoreHeapDescending(h minScoreHeap, docIDs []string) []scoredDoc {
	if len(docIDs) == 0 {
		slices.SortFunc(h, func(left, right scoredDoc) int {
			switch {
			case scoredDocNumRanksBefore(left, right):
				return -1
			case scoredDocNumRanksBefore(right, left):
				return 1
			default:
				return 0
			}
		})
		return h
	}
	slices.SortFunc(h, func(left, right scoredDoc) int {
		switch {
		case scoredDocRanksBefore(left, right, docIDs):
			return -1
		case scoredDocRanksBefore(right, left, docIDs):
			return 1
		default:
			return 0
		}
	})
	return h
}

func scoredDocNumRanksBefore(left, right scoredDoc) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	return left.docNum < right.docNum
}

func scoredDocRanksBefore(left, right scoredDoc, docIDs []string) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	if int(left.docNum) < len(docIDs) && int(right.docNum) < len(docIDs) {
		leftID := docIDs[left.docNum]
		rightID := docIDs[right.docNum]
		if leftID != rightID {
			return leftID < rightID
		}
	}
	return left.docNum < right.docNum
}

func topKMinScore(scores map[uint32]float64, k int) float64 {
	if k <= 0 || len(scores) < k {
		return 0
	}
	h := make(minScoreHeap, 0, k)
	for docNum, score := range scores {
		h = offerScoredDoc(h, scoredDoc{docNum: docNum, score: score}, nil, k)
	}
	if len(h) < k {
		return 0
	}
	return h[0].score
}

func topKFromScores(scores map[uint32]float64, k int) []scoredDoc {
	return topKFromSparseScores(scores, nil, k, nil)
}

func topKFromSparseScores(scores map[uint32]float64, docIDs []string, k int, h minScoreHeap) []scoredDoc {
	if k <= 0 || len(scores) == 0 {
		return nil
	}
	heapCapacity := minInt(k, len(scores))
	if cap(h) < heapCapacity {
		h = make(minScoreHeap, 0, heapCapacity)
	} else {
		h = h[:0]
	}
	for docNum, score := range scores {
		h = offerScoredDoc(h, scoredDoc{docNum: docNum, score: score}, docIDs, k)
	}
	return sortScoreHeapDescending(h, docIDs)
}

// offerScoredDoc offers candidate to the min-heap h of the k best documents:
// it is added while h has room, and afterwards replaces the weakest kept
// document only when it ranks before it.
func offerScoredDoc(h minScoreHeap, candidate scoredDoc, docIDs []string, k int) minScoreHeap {
	if len(h) < k {
		return pushMinScore(h, candidate, docIDs)
	}
	if scoredDocRanksBefore(candidate, h[0], docIDs) {
		replaceMinScore(h, candidate, docIDs)
	}
	return h
}

func topKFromDenseScores(scores []float64, touched []uint32, docIDs []string, ascendingLexicalCandidates bool, k int, h minScoreHeap) []scoredDoc {
	if k <= 0 || len(touched) == 0 {
		return nil
	}
	heapCapacity := minInt(k, len(touched))
	if cap(h) < heapCapacity {
		h = make(minScoreHeap, 0, heapCapacity)
	} else {
		h = h[:0]
	}
	for _, docNum := range touched {
		score := scores[docNum]
		if len(h) < k {
			h = pushMinScore(h, scoredDoc{docNum: docNum, score: score}, docIDs)
			continue
		}
		if score > h[0].score {
			replaceMinScore(h, scoredDoc{docNum: docNum, score: score}, docIDs)
		} else if !ascendingLexicalCandidates && score == h[0].score {
			if len(docIDs) == 0 && docNum < h[0].docNum {
				replaceMinScore(h, scoredDoc{docNum: docNum, score: score}, docIDs)
			} else if len(docIDs) != 0 && scoredDocRanksBefore(scoredDoc{docNum: docNum, score: score}, h[0], docIDs) {
				replaceMinScore(h, scoredDoc{docNum: docNum, score: score}, docIDs)
			}
		}
	}
	return sortScoreHeapDescending(h, docIDs)
}
