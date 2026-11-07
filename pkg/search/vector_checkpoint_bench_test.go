package search

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkVectorCheckpointMutation isolates append/update work from checkpoint
// publication, using the same bounded ID set on the release and candidate.
func BenchmarkVectorCheckpointMutation(b *testing.B) {
	store, err := NewVectorFileStore(filepath.Join(b.TempDir(), "vectors"), 128)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	vector := make([]float32, 128)
	vector[0] = 1
	ids := make([]string, 64)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Add(ids[i%len(ids)], vector); err != nil {
			b.Fatal(err)
		}
	}
}
