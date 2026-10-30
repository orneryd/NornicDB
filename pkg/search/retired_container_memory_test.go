package search

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/require"
)

func TestRetiredRankingPayloadCollectible(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(map[bool]string{false: "evict", true: "reset"}[reset], func(t *testing.T) {
			owner := &KalmanSearchAdapter{config: KalmanSearchConfig{StabilityWindow: 1}}
			reference := func() weak.Pointer[string] {
				docs := make([]string, 8)
				owner.recentRankings = []queryRanking{{TopDocs: docs}}
				ref := weak.Make(&docs[0])
				if reset {
					owner.Reset()
				} else {
					owner.recordRanking("next", nil)
				}
				return ref
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}

func TestRetiredStrategyDeltasCollectible(t *testing.T) {
	for _, applied := range []uint64{0, 1} {
		owner := &Service{}
		reference := func() weak.Pointer[byte] {
			id := strings.Repeat("retired", 64)
			owner.strategyTransitionDeltas = []strategyDeltaMutation{{id: "kept", seq: 2}, {id: id, seq: 1}}
			ref := weak.Make(unsafe.StringData(id))
			owner.clearTransitionDeltaLogLocked(applied)
			if applied != 0 {
				require.Len(t, owner.strategyTransitionDeltas, 1)
				require.Equal(t, "kept", owner.strategyTransitionDeltas[0].id)
			}
			return ref
		}()
		runtime.GC()
		require.Nil(t, reference.Value())
		runtime.KeepAlive(owner)
	}
}
