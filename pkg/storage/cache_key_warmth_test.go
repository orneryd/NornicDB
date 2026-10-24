package storage

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRelationshipCachesKeepUnrelatedWritesWarm(t *testing.T) {
	b := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:warm-a", "test:warm-b", "other:warm-u"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:warm-e", StartNode: "test:warm-a", EndNode: "test:warm-b", Type: "WARM"}
	require.NoError(t, b.CreateEdge(edge))
	for i := 0; i < 2; i++ {
		_, err := b.GetEdge(edge.ID)
		require.NoError(t, err)
		_, err = b.GetOutgoingEdges(edge.StartNode)
		require.NoError(t, err)
		_, err = b.GetIncomingEdges(edge.EndNode)
		require.NoError(t, err)
		_, err = b.GetEdgesByType(edge.Type)
		require.NoError(t, err)
	}
	check := func() {
		_, ok := b.cacheLoadEdge(edge.ID)
		require.True(t, ok, "relationship body lost warmth")
		_, ok = b.adjCacheLoadOutgoing(edge.StartNode)
		require.True(t, ok, "outgoing endpoint lost warmth")
		_, ok = b.adjCacheLoadIncoming(edge.EndNode)
		require.True(t, ok, "incoming endpoint lost warmth")
		// A warm type read returns the same immutable list, without a refill.
		b.edgeTypeCacheMu.RLock()
		before := b.edgeTypeCache[edge.Type].edges
		b.edgeTypeCacheMu.RUnlock()
		got, err := b.GetEdgesByType(edge.Type)
		require.NoError(t, err)
		require.NotEmpty(t, before)
		require.Same(t, before[0], got[0], "type list was refilled")
	}
	check()
	require.NoError(t, b.UpdateNode(&Node{ID: "other:warm-u", Properties: map[string]any{"v": int64(2)}}))
	check()
}

func TestNumericListDecodeAllocationProfile(t *testing.T) {
	for _, n := range []int{16, 1024} {
		ints, floats, overflow := make([]int64, n), make([]float64, n), make([]uint64, n)
		for i := range ints {
			ints[i] = int64(128 + i)
			floats[i] = float64(i) + 0.5
			overflow[i] = math.MaxUint64 - uint64(i)
		}
		for name, input := range map[string]any{"int64": ints, "float64": floats, "uint64": overflow} {
			t.Run(fmt.Sprintf("n=%d/%s", n, name), func(t *testing.T) {
				encoded, err := msgpack.Marshal(input)
				require.NoError(t, err)
				allocs := testing.AllocsPerRun(50, func() {
					_, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(encoded)))
					if err != nil {
						panic(err)
					}
				})
				require.LessOrEqual(t, allocs, float64(5), "one backing allocation per numeric list; decoder overhead is constant")
			})
		}
	}
}
