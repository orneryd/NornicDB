package storage

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// Each sweep starts after an unrelated durable write. Setup/write time is
// excluded so this measures cache warmth rather than unrelated writer work.
func BenchmarkRelationshipCacheAfterUnrelatedWrite(b *testing.B) {
	for _, route := range []string{"body", "point", "outgoing", "incoming", "type"} {
		b.Run(route, func(b *testing.B) {
			engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), LowMemory: true, EdgeTypeCacheMaxTypes: 256})
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()
			const count = 64
			edges := make([]*Edge, count)
			for i := range edges {
				a, c := NodeID(fmt.Sprintf("test:cost-a-%d", i)), NodeID(fmt.Sprintf("test:cost-c-%d", i))
				if _, err := engine.CreateNode(&Node{ID: a}); err != nil {
					b.Fatal(err)
				}
				if _, err := engine.CreateNode(&Node{ID: c}); err != nil {
					b.Fatal(err)
				}
				edges[i] = &Edge{ID: EdgeID(fmt.Sprintf("test:cost-e-%d", i)), StartNode: a, EndNode: c, Type: fmt.Sprintf("COST_%d", i)}
				if err := engine.CreateEdge(edges[i]); err != nil {
					b.Fatal(err)
				}
			}
			unrelated := &Node{ID: "other:cost-writer", Properties: map[string]any{"v": int64(1)}}
			if _, err := engine.CreateNode(unrelated); err != nil {
				b.Fatal(err)
			}
			read := func(i int) error {
				edge := edges[i]
				switch route {
				case "body":
					if _, hit := engine.cacheLoadEdge(edge.ID); !hit {
						b.Fatal("relationship body cache cooled after unrelated write")
					}
				case "point":
					_, err = engine.GetEdge(edge.ID)
				case "outgoing":
					_, err = engine.GetOutgoingEdges(edge.StartNode)
				case "incoming":
					_, err = engine.GetIncomingEdges(edge.EndNode)
				case "type":
					_, err = engine.GetEdgesByType(edge.Type)
				}
				return err
			}
			for sweep := 0; sweep < 2; sweep++ {
				for i := range edges {
					if err := read(i); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%count == 0 {
					b.StopTimer()
					if err := engine.UpdateNode(unrelated); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				if err := read(i % count); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCanonicalNumericListDecode(b *testing.B) {
	ints, floats, overflow := make([]int64, 1024), make([]float64, 1024), make([]uint64, 1024)
	for i := range ints {
		ints[i] = int64(128 + i)
		floats[i] = float64(i) + 0.5
		overflow[i] = math.MaxUint64 - uint64(i)
	}
	for name, value := range map[string]any{"float64": floats, "int64": ints, "uint64": overflow} {
		b.Run(name, func(b *testing.B) {
			encoded, err := msgpack.Marshal(value)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(encoded))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
