package search

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"
)

func TestSolIVFPQReaderOwnership(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	idx := &IVFPQIndex{
		profile:   IVFPQProfile{Dimensions: 1, NProbe: 1},
		centroids: [][]float32{{1}},
		codebooks: []ivfpqCodebook{{SubDim: 1, Codeword: [][]float32{{0}}}},
		lists:     []ivfpqList{{IDs: []string{"a", "b"}, CodeSize: 1, Codes: []byte{0, 0}}},
	}
	for n := 0; n < 20; n++ {
		first, err := idx.SearchApprox(context.Background(), []float32{1}, 2, -2, 1)
		if err != nil || len(first) != 2 || first[0].Score != 1 {
			t.Fatalf("first result: %v, %v", first, err)
		}
		_, err = idx.SearchApprox(context.Background(), []float32{-1}, 2, -2, 1)
		if err != nil {
			t.Fatal(err)
		}
		if first[0].Score != 1 {
			t.Fatalf("later search overwrote held result: score=%v", first[0].Score)
		}
	}
}

func TestSolIVFPQScratchRetiredIDCollectible(t *testing.T) {
	idx := &IVFPQIndex{}
	idx.initScratchPool()
	scratch := &ivfpqScratch{heapData: make([]Candidate, 0, 32)}
	ref := func() weak.Pointer[byte] {
		id := strings.Repeat("retired-id", 8192)
		h := newCandidateMinHeapWithBuffer(scratch.heapData, 32)
		h.push(Candidate{ID: id, Score: 1})
		ref := weak.Make(unsafe.StringData(id))
		idx.putScratch(scratch)
		runtime.KeepAlive(h)
		return ref
	}()
	runtime.GC()
	if ref.Value() != nil {
		t.Error("returned IVF/PQ scratch still retains retired candidate ID")
	}
	runtime.KeepAlive(scratch)
	runtime.KeepAlive(idx)
}
