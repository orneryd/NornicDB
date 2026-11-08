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

// BenchmarkVectorCheckpointSave includes synchronization and integrity work,
// separately from mutation throughput. The fixture is identical on release.
func BenchmarkVectorCheckpointSave(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
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
			for i := 0; i < count; i++ {
				if err := store.Add(fmt.Sprintf("node-%d", i), vector); err != nil {
					b.Fatal(err)
				}
			}
			if err := store.Save(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.Save(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
