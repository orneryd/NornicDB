package search

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"
)

func TestSolScoreScratchRetiredIDCollectible(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "local_header", true: "mapped_compaction"}[compact], func(t *testing.T) {
			store := &VectorFileStore{}
			scratch := store.getScoreScratch(32, 0)
			ref := func() weak.Pointer[byte] {
				id := strings.Repeat("retired-id", 8192)
				// scoreCandidatesDot writes through a local offsets header. The
				// pooled scratch header stays length zero, with this capacity.
				offsets := append(scratch.offsets[:0], vfsCandidateOffset{id: "kept"}, vfsCandidateOffset{id: id})
				if len(offsets) != 2 || len(scratch.offsets) != 0 {
					t.Fatal("unexpected scratch alias")
				}
				if compact {
					// A mapped candidate leaves only the unmapped prefix in the local
					// header. Its retired ID is now in the hidden tail.
					offsets = offsets[:1]
				}
				ref := weak.Make(unsafe.StringData(id))
				store.putScoreScratch(scratch)
				runtime.KeepAlive(offsets)
				return ref
			}()
			runtime.GC()
			if ref.Value() != nil {
				t.Error("returned scoring scratch still retains retired candidate ID")
			}
			runtime.KeepAlive(scratch)
			runtime.KeepAlive(store)
		})
	}
}
