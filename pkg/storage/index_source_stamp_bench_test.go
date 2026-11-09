package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// BenchmarkIndexSourceStamp measures validation independently from population
// hydration, including all metadata keys needed to qualify a source identity.
func BenchmarkIndexSourceStamp(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine := fixtureowner.Own(b, fixtureowner.Track(b, NewMemoryEngine(), ErrStorageClosed)).BadgerEngine
			for i := 0; i < size; i++ {
				_, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("test:stamp-%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "source certification"}})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stamp, err := engine.IndexSourceStamp(b.Context())
				if err != nil || stamp == "" {
					b.Fatalf("source stamp=%q error=%v", stamp, err)
				}
			}
		})
	}
}
