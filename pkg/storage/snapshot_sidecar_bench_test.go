package storage

import (
	"bytes"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

type snapshotByteCounter int64

func (w *snapshotByteCounter) Write(data []byte) (int, error) {
	*w += snapshotByteCounter(len(data))
	return len(data), nil
}

func snapshotSidecarFixtureVectors(chunks, dimensions int) [][]float32 {
	vectors := make([][]float32, chunks)
	for i := range vectors {
		vectors[i] = make([]float32, dimensions)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*dimensions+j) / 7
		}
	}
	return vectors
}

// BenchmarkSnapshotSidecarVectors measures the public writer on real stored
// sidecars. Setup and a complete read-back comparison are outside the timer.
func BenchmarkSnapshotSidecarVectors(b *testing.B) {
	for _, count := range []int{128, 1024} {
		b.Run(fmt.Sprintf("nodes_%d", count), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
			require.NoError(b, err)
			defer engine.Close()
			vectors := snapshotSidecarFixtureVectors(4, 256)
			for i := 0; i < count; i++ {
				id := NodeID(fmt.Sprintf("snapshot:n%d", i))
				_, err := engine.CreateNode(b.Context(), &Node{ID: id, Labels: []string{"Document"}, Properties: map[string]any{"text": "source text"}})
				require.NoError(b, err)
				node, err := engine.GetNode(b.Context(), id)
				require.NoError(b, err)
				node.ChunkEmbeddings = vectors
				node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 4, "embedding_dimensions": 256}
				require.NoError(b, engine.UpdateNodeEmbeddingSidecar(b.Context(), node))
			}
			var first bytes.Buffer
			require.NoError(b, WriteSnapshot(b.Context(), engine, &first, SnapshotOptions{}))
			visitor := &collectingSnapshotVisitor{}
			metadata, err := ReadSnapshot(b.Context(), bytes.NewReader(first.Bytes()), visitor)
			require.NoError(b, err)
			require.EqualValues(b, count, metadata.NodeCount)
			require.Len(b, visitor.nodes, count)
			for _, node := range visitor.nodes {
				require.Equal(b, vectors, node.ChunkEmbeddings)
			}
			outputBytes := first.Len()
			b.SetBytes(int64(count * 4 * 256 * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var counter snapshotByteCounter
				require.NoError(b, WriteSnapshot(b.Context(), engine, &counter, SnapshotOptions{}))
				require.EqualValues(b, outputBytes, counter)
			}
			b.StopTimer()
			b.ReportMetric(float64(outputBytes), "snapshot-B")
		})
	}
}
