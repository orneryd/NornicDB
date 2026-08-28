package search

import (
	"context"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// SearchApprox searches compressed IVF/PQ lists and returns approximate candidates.
func (i *IVFPQIndex) SearchApprox(ctx context.Context, query []float32, k int, minSimilarity float64, nprobe int) ([]Candidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if i == nil || len(i.centroids) == 0 || len(i.codebooks) == 0 {
		return nil, nil
	}
	if nprobe <= 0 {
		nprobe = i.profile.NProbe
	}
	if nprobe <= 0 {
		nprobe = 1
	}
	centroidNorm := i.centroidNorm
	if len(centroidNorm) != len(i.centroids) {
		centroidNorm = normalizeCentroids(i.centroids)
	}
	queryNorm := vector.Normalize(query)
	centroidIDs := ivfpqTopCentroidsByQuery(queryNorm, centroidNorm, nprobe)
	if len(centroidIDs) == 0 {
		return []Candidate{}, nil
	}

	totalLimit := boundCandidateLimit(k)
	if i.profile.RerankTopK > 0 && totalLimit > i.profile.RerankTopK {
		totalLimit = i.profile.RerankTopK
	}
	if totalLimit <= 0 {
		return []Candidate{}, nil
	}
	scratch := i.getScratch(totalLimit)
	defer i.putScratch(scratch)
	ivfpqQueryLUTInto(scratch.lut, queryNorm, i.codebooks)
	h := newCandidateMinHeapWithBuffer(scratch.heapData, totalLimit)
	for _, lid := range centroidIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lid < 0 || lid >= len(i.lists) {
			continue
		}
		list := i.lists[lid]
		base := float64(vector.DotProduct(queryNorm, centroidNorm[lid]))
		codeSize := list.CodeSize
		if codeSize <= 0 {
			continue
		}
		maxCodes := len(list.Codes) / codeSize
		if len(list.IDs) < maxCodes {
			maxCodes = len(list.IDs)
		}
		for idx := 0; idx < maxCodes; idx++ {
			score := base + ivfpqResidualScoreAt(scratch.lut, list.Codes, idx*codeSize, codeSize)
			if score < minSimilarity {
				continue
			}
			h.push(Candidate{
				ID:    list.IDs[idx],
				Score: score,
			})
		}
	}
	candidates := h.toSortedDescending()
	if len(candidates) > totalLimit {
		candidates = candidates[:totalLimit]
	}
	return candidates, nil
}

func ivfpqQueryLUT(query []float32, codebooks []ivfpqCodebook) [][]float32 {
	out := make([][]float32, len(codebooks))
	offset := 0
	for seg := range codebooks {
		subDim := codebooks[seg].SubDim
		out[seg] = make([]float32, len(codebooks[seg].Codeword))
		for cIdx, cw := range codebooks[seg].Codeword {
			var score float32
			for d := 0; d < subDim; d++ {
				score += query[offset+d] * cw[d]
			}
			out[seg][cIdx] = score
		}
		offset += subDim
	}
	return out
}

func ivfpqQueryLUTInto(out [][]float32, query []float32, codebooks []ivfpqCodebook) {
	offset := 0
	for seg := range codebooks {
		subDim := codebooks[seg].SubDim
		segOut := out[seg]
		for cIdx, cw := range codebooks[seg].Codeword {
			var score float32
			for d := 0; d < subDim; d++ {
				score += query[offset+d] * cw[d]
			}
			segOut[cIdx] = score
		}
		offset += subDim
	}
}

func ivfpqResidualScore(lut [][]float32, code []byte) float64 {
	var score float32
	segCount := len(lut)
	if len(code) < segCount {
		segCount = len(code)
	}
	for seg := 0; seg < segCount; seg++ {
		score += lut[seg][int(code[seg])]
	}
	return float64(score)
}

func ivfpqResidualScoreAt(lut [][]float32, codes []byte, offset, codeSize int) float64 {
	var score float32
	segCount := len(lut)
	if codeSize < segCount {
		segCount = codeSize
	}
	for seg := 0; seg < segCount; seg++ {
		score += lut[seg][int(codes[offset+seg])]
	}
	return float64(score)
}

type candidateMinHeap struct {
	data []Candidate
	cap  int
}

func newCandidateMinHeap(capacity int) *candidateMinHeap {
	if capacity < 1 {
		capacity = 1
	}
	return &candidateMinHeap{
		data: make([]Candidate, 0, capacity),
		cap:  capacity,
	}
}

func newCandidateMinHeapWithBuffer(buf []Candidate, capacity int) *candidateMinHeap {
	if capacity < 1 {
		capacity = 1
	}
	if cap(buf) < capacity {
		buf = make([]Candidate, 0, capacity)
	} else {
		buf = buf[:0]
	}
	return &candidateMinHeap{
		data: buf,
		cap:  capacity,
	}
}

func (h *candidateMinHeap) push(c Candidate) {
	if len(h.data) < h.cap {
		h.data = append(h.data, c)
		h.up(len(h.data) - 1)
		return
	}
	// Heap root is the smallest score currently kept.
	if c.Score <= h.data[0].Score {
		return
	}
	h.data[0] = c
	h.down(0)
}

func (h *candidateMinHeap) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h.data[p].Score <= h.data[i].Score {
			break
		}
		h.data[p], h.data[i] = h.data[i], h.data[p]
		i = p
	}
}

func (h *candidateMinHeap) down(i int) {
	n := len(h.data)
	for {
		l := 2*i + 1
		r := l + 1
		s := i
		if l < n && h.data[l].Score < h.data[s].Score {
			s = l
		}
		if r < n && h.data[r].Score < h.data[s].Score {
			s = r
		}
		if s == i {
			return
		}
		h.data[i], h.data[s] = h.data[s], h.data[i]
		i = s
	}
}

func (h *candidateMinHeap) toSortedDescending() []Candidate {
	sort.Slice(h.data, func(i, j int) bool { return h.data[i].Score > h.data[j].Score })
	return h.data
}

func (i *IVFPQIndex) initScratchPool() {
	i.scratchPool = sync.Pool{
		New: func() any {
			s := &ivfpqScratch{
				lut: make([][]float32, len(i.codebooks)),
			}
			for seg := range i.codebooks {
				s.lut[seg] = make([]float32, len(i.codebooks[seg].Codeword))
			}
			return s
		},
	}
}

func (i *IVFPQIndex) getScratch(candidateCap int) *ivfpqScratch {
	if len(i.codebooks) == 0 {
		return &ivfpqScratch{}
	}
	if i.scratchPool.New == nil {
		i.initScratchPool()
	}
	s, _ := i.scratchPool.Get().(*ivfpqScratch)
	if s == nil {
		s = &ivfpqScratch{}
	}
	if len(s.lut) != len(i.codebooks) {
		s.lut = make([][]float32, len(i.codebooks))
	}
	for seg := range i.codebooks {
		want := len(i.codebooks[seg].Codeword)
		if cap(s.lut[seg]) < want {
			s.lut[seg] = make([]float32, want)
		} else {
			s.lut[seg] = s.lut[seg][:want]
		}
	}
	if cap(s.heapData) < candidateCap {
		s.heapData = make([]Candidate, 0, candidateCap)
	} else {
		s.heapData = s.heapData[:0]
	}
	return s
}

func (i *IVFPQIndex) putScratch(s *ivfpqScratch) {
	if s == nil || i.scratchPool.New == nil {
		return
	}
	i.scratchPool.Put(s)
}
