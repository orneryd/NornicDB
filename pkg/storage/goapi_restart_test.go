package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Behaviour change (Go API): a body read right after a write is the body a
// read returns after a restart, for every value kind whose Go form the codec
// changes (narrow integer and float widths, Go int, generic and typed lists,
// zoned times, nested maps, bytes) as for the ones it keeps. Before, a read
// served from the entity cache returned the writer's own values and a read
// after eviction or restart the stored ones.
func TestGoAPIReadAfterWriteMatchesReadAfterRestart(t *testing.T) {
	zone := time.FixedZone("plus-one", 3600)
	kinds := map[string]any{
		"int": 30, "int8": int8(30), "int16": int16(30), "int32": int32(30), "int64": int64(30),
		"uint": uint(30), "uint8": uint8(30), "uint16": uint16(30), "uint32": uint32(30), "uint64": uint64(30),
		"float32": float32(0.5), "float64": 0.5, "string": "x", "bool": true, "nil": nil, "bytes": []byte("ab"),
		"generic strings": []any{"a", "b"}, "generic go ints": []any{1, 2}, "generic int64s": []any{int64(1), int64(2)},
		"generic mixed": []any{int64(1), 2.5}, "generic floats": []any{0.5, 1.5}, "generic nested": []any{[]any{"a"}},
		"strings": []string{"a", "b"}, "ints": []int{1, 2}, "int32s": []int32{1}, "int64s": []int64{1, 2},
		"float32s": []float32{0.5}, "float64s": []float64{0.5}, "bools": []bool{true},
		"time": time.Date(2024, 1, 2, 3, 4, 5, 6, zone), "map": map[string]any{"x": []any{1, "y"}, "z": int32(2)},
	}
	for _, buffered := range []bool{false, true} {
		for _, update := range []bool{false, true} {
			t.Run(fmt.Sprintf("write-behind=%v/update=%v", buffered, update), func(t *testing.T) {
				dir := t.TempDir()
				open := func() *BadgerEngine {
					engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
					require.NoError(t, err)
					return engine
				}
				engine := open()
				properties := func() map[string]any {
					out := make(map[string]any, len(kinds))
					for k, v := range kinds {
						out[k] = v
					}
					return out
				}
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"E"}})
					require.NoError(t, err)
				}
				_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"K"}, Properties: properties()})
				require.NoError(t, err)
				require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: properties()}))
				if update {
					require.NoError(t, engine.UpdateNode(&Node{ID: "test:n", Labels: []string{"K"}, Properties: properties()}))
					require.NoError(t, engine.UpdateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: properties()}))
				}
				afterWrite, err := engine.GetNode("test:n")
				require.NoError(t, err)
				afterWriteEdge, err := engine.GetEdge("test:e")
				require.NoError(t, err)
				require.NoError(t, engine.Close())

				engine = open()
				t.Cleanup(func() { _ = engine.Close() })
				afterRestart, err := engine.GetNode("test:n")
				require.NoError(t, err)
				afterRestartEdge, err := engine.GetEdge("test:e")
				require.NoError(t, err)
				for name := range kinds {
					require.Equal(t, afterRestart.Properties[name], afterWrite.Properties[name], "node %s", name)
					require.Equal(t, afterRestartEdge.Properties[name], afterWriteEdge.Properties[name], "relationship %s", name)
				}
				require.Equal(t, publicNodeImage(afterRestart), publicNodeImage(afterWrite))
				require.Equal(t, comparableStoredEdge(afterRestartEdge), comparableStoredEdge(afterWriteEdge))
			})
		}
	}
}

// Every stored integer reads back as int64, as Cypher integers are, whatever
// width it was written in and whatever code msgpack chose for it (a Go int
// takes the narrowest code that holds it, Uint64 above 2^32-1); only a value
// above MaxInt64, which no int64 holds, reads as uint64. Lists follow the same
// rule element by element. It holds right after the write and after a
// restart, for data the codec wrote in any width.
func TestStoredIntegersReadAsInt64(t *testing.T) {
	big := uint64(math.MaxInt64) + 1
	written := map[string]any{
		"small": 5, "negative": -5, "uint32 max": 4294967295, "2^32": 4294967296, "int max": int(math.MaxInt64),
		"int8": int8(-3), "uint16": uint16(300), "int32": int32(-70000), "uint32": uint32(math.MaxUint32),
		"uint64 fits": uint64(math.MaxInt64), "uint64 above": big, "uint above": uint(big),
		"go ints": []any{1, 4294967296}, "typed ints": []int{1, 2}, "int64s": []int64{-1, 2},
		"uint64s": []any{uint64(1), big}, "float32": float32(0.5), "float32s": []float32{0.5},
	}
	want := map[string]any{
		"small": int64(5), "negative": int64(-5), "uint32 max": int64(4294967295), "2^32": int64(4294967296), "int max": int64(math.MaxInt64),
		"int8": int64(-3), "uint16": int64(300), "int32": int64(-70000), "uint32": int64(math.MaxUint32),
		"uint64 fits": int64(math.MaxInt64), "uint64 above": big, "uint above": big,
		"go ints": []int64{1, 4294967296}, "typed ints": []int64{1, 2}, "int64s": []int64{-1, 2},
		"uint64s": []any{int64(1), big}, "float32": float32(0.5), "float32s": []float32{0.5},
	}
	for _, buffered := range []bool{false, true} {
		dir := t.TempDir()
		open := func() *BadgerEngine {
			engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			return engine
		}
		engine := open()
		_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"K"}, Properties: written})
		require.NoError(t, err)
		afterWrite, err := engine.GetNode("test:n")
		require.NoError(t, err)
		require.NoError(t, engine.Close())
		engine = open()
		afterRestart, err := engine.GetNode("test:n")
		require.NoError(t, err)
		require.NoError(t, engine.Close())
		for name, expected := range want {
			require.Equal(t, expected, afterWrite.Properties[name], "write-behind=%v %s after write", buffered, name)
			require.Equal(t, expected, afterRestart.Properties[name], "write-behind=%v %s after restart", buffered, name)
		}
	}
}
