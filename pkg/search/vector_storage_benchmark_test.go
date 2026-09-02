package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	mathvector "github.com/orneryd/nornicdb/pkg/math/vector"
)

const (
	vectorStorageBenchmarkDimensions = 384
	vectorStorageBenchmarkCount      = 10_000
)

func BenchmarkVectorStorageAdd(b *testing.B) {
	vector := benchmarkStorageVector(vectorStorageBenchmarkDimensions, 1)
	b.Run("memory", func(b *testing.B) {
		index := NewVectorIndex(vectorStorageBenchmarkDimensions)
		b.ReportAllocs()
		b.ResetTimer()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if err := index.Add(fmt.Sprintf("vector-%08d", indexValue), vector); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("file", func(b *testing.B) {
		store, err := NewVectorFileStore(filepath.Join(b.TempDir(), "vectors"), vectorStorageBenchmarkDimensions)
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if err := store.Add(fmt.Sprintf("vector-%08d", indexValue), vector); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkVectorStorageGet(b *testing.B) {
	memory, file, ids, _ := benchmarkVectorStores(b, vectorStorageBenchmarkCount, vectorStorageBenchmarkDimensions)
	b.Run("memory", func(b *testing.B) {
		b.ReportAllocs()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if _, ok := memory.GetVector(ids[indexValue%len(ids)]); !ok {
				b.Fatal("missing vector")
			}
		}
	})
	b.Run("file", func(b *testing.B) {
		b.ReportAllocs()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if _, ok := file.GetVector(ids[indexValue%len(ids)]); !ok {
				b.Fatal("missing vector")
			}
		}
	})
}

func BenchmarkVectorStorageScore10K(b *testing.B) {
	memory, file, ids, candidates := benchmarkVectorStores(b, vectorStorageBenchmarkCount, vectorStorageBenchmarkDimensions)
	query := benchmarkStorageVector(vectorStorageBenchmarkDimensions, 2)
	b.Run("memory", func(b *testing.B) {
		b.ReportAllocs()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if _, err := memory.Search(context.Background(), query, 10, -1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("file", func(b *testing.B) {
		normalized := mathvector.Normalize(query)
		b.ReportAllocs()
		for indexValue := 0; indexValue < b.N; indexValue++ {
			if _, err := file.scoreCandidatesDot(context.Background(), normalized, candidates); err != nil {
				b.Fatal(err)
			}
		}
	})
	_ = ids
}

func BenchmarkVectorStorageLoad10K(b *testing.B) {
	base := filepath.Join(b.TempDir(), "vectors")
	store, err := NewVectorFileStore(base, vectorStorageBenchmarkDimensions)
	if err != nil {
		b.Fatal(err)
	}
	for indexValue := 0; indexValue < vectorStorageBenchmarkCount; indexValue++ {
		id := fmt.Sprintf("vector-%08d", indexValue)
		if err := store.Add(id, benchmarkStorageVector(vectorStorageBenchmarkDimensions, int64(indexValue+1))); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.Save(); err != nil {
		b.Fatal(err)
	}
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for indexValue := 0; indexValue < b.N; indexValue++ {
		loaded, err := NewVectorFileStore(base, vectorStorageBenchmarkDimensions)
		if err != nil {
			b.Fatal(err)
		}
		if err := loaded.Load(); err != nil {
			_ = loaded.Close()
			b.Fatal(err)
		}
		if err := loaded.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkVectorStores(tb testing.TB, count, dimensions int) (*VectorIndex, *VectorFileStore, []string, []Candidate) {
	tb.Helper()
	memory := NewVectorIndex(dimensions)
	file, err := NewVectorFileStore(filepath.Join(tb.TempDir(), "vectors"), dimensions)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	ids := make([]string, count)
	candidates := make([]Candidate, count)
	for indexValue := 0; indexValue < count; indexValue++ {
		id := fmt.Sprintf("vector-%08d", indexValue)
		vector := benchmarkStorageVector(dimensions, int64(indexValue+1))
		if err := memory.Add(id, vector); err != nil {
			tb.Fatal(err)
		}
		if err := file.Add(id, vector); err != nil {
			tb.Fatal(err)
		}
		ids[indexValue] = id
		candidates[indexValue] = Candidate{ID: id}
	}
	return memory, file, ids, candidates
}

func benchmarkStorageVector(dimensions int, seed int64) []float32 {
	random := rand.New(rand.NewSource(seed))
	vector := make([]float32, dimensions)
	for index := range vector {
		vector[index] = random.Float32()*2 - 1
	}
	return vector
}
