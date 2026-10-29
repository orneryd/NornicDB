package search

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Measure the shared file/chunk iterator's hint binding and ranked construction
// input preparation. This does not measure total HNSW build or query latency.
func BenchmarkVectorEntryLexicalConstructionInputs(b *testing.B) {
	const documents, chunks = 256, 4
	ft := NewFulltextIndex(nil)
	ids := make([]string, 0, documents*chunks)
	vectors := make([][]float32, 0, documents*chunks)
	for i := 0; i < documents; i++ {
		id := fmt.Sprintf("document-%04d", i)
		ft.Index(id, fmt.Sprintf("topic%d topic%d", i%8, i%8), nil, nil, false)
		for chunk := 0; chunk < chunks; chunk++ {
			ids = append(ids, nodeChunkVectorKey(id, chunk))
			vectors = append(vectors, []float32{1, float32(i + 1)})
		}
	}
	service := NewService(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	defer service.Close()
	hints := service.hnswLexicalSeedHints(ft, 16)
	if len(hints) == 0 {
		b.Fatal("benchmark must exercise lexical construction")
	}
	iterate := func(size int, visit func([]string, [][]float32) error) error {
		for start := 0; start < len(ids); start += size {
			end := min(start+size, len(ids))
			if err := visit(ids[start:end], vectors[start:end]); err != nil {
				return err
			}
		}
		return nil
	}
	buildInputs := hnswVectorChunkIterator(iterate, hints)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := buildInputs(128, func(pairs []hnswBuildPair) error {
			count += len(pairs)
			return nil
		})
		if err != nil || count != len(ids) {
			b.Fatalf("construction population: count=%d err=%v", count, err)
		}
	}
}
