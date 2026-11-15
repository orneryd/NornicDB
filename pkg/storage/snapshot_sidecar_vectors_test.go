package storage

import (
	"bytes"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestSnapshotLargeSidecarVectorsRespectRecordLimit(t *testing.T) {
	engine := newSnapshotBadger(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "snapshot:large", Properties: map[string]any{"text": "source"}})
	require.NoError(t, err)
	node, err := engine.GetNode(t.Context(), "snapshot:large")
	require.NoError(t, err)
	vectors := snapshotSidecarFixtureVectors(17000, 256)
	node.ChunkEmbeddings = vectors
	node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": len(vectors), "embedding_dimensions": 256}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(t.Context(), node))

	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(t.Context(), engine, &output, SnapshotOptions{}))
	require.Greater(t, output.Len(), defaultSnapshotRecordSize)
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshot(t.Context(), bytes.NewReader(output.Bytes()), visitor)
	require.NoError(t, err)
	require.EqualValues(t, 1, metadata.NodeCount)
	require.Len(t, visitor.nodes, 1)
	require.Equal(t, vectors, visitor.nodes[0].ChunkEmbeddings)
	requireSameStoredMap(t, node.EmbedMeta, visitor.nodes[0].EmbedMeta, "embedding metadata")

	var limited bytes.Buffer
	err = WriteSnapshot(t.Context(), engine, &limited, SnapshotOptions{MaxRecordBytes: defaultSnapshotRecordSize})
	require.ErrorContains(t, err, "exceeds limit 16777216")
}

// Reports 45 and 53.5: a snapshot taken while the store holds the vectors its
// reads return must carry them. Automatic compaction writes that snapshot
// (createSnapshotAndCompact) and then truncates the journal, so the snapshot
// is the last copy of a worker's vectors kept beside the node's body. A
// rebuild from it must hold the live store's vectors: an older inline image
// (report 45) or no vectors at all (report 53.5) leaves the node looking
// embedded with different vectors, or looking like it needs embedding when
// its vectors were published.
func TestPeriodicSnapshotPreservesTheVectorsReadsReturn(t *testing.T) {
	cases := []struct {
		name string
		seed func(l *rrvLab)
	}{
		{
			name: "the worker's vectors replaced the client's inline vectors",
			seed: func(l *rrvLab) {
				l.create("alpha", rrvVectors(2, 1), map[string]any{
					"has_embedding": true, "chunk_count": 2, "embedding_dimensions": rrvDims, "embedding_model": "client",
				}, nil)
				l.sidecar(func(n *Node) {
					n.ChunkEmbeddings, n.EmbedMeta = rrvVectors(3, 100), rrvWorkerMeta(3)
				})
			},
		},
		{
			name: "the node carries only the worker's vectors",
			seed: func(l *rrvLab) {
				l.create("alpha", nil, nil, nil)
				l.sidecar(func(n *Node) {
					n.ChunkEmbeddings, n.EmbedMeta = rrvVectors(3, 100), rrvWorkerMeta(3)
				})
			},
		},
	}
	for _, seed := range cases {
		t.Run(seed.name, func(t *testing.T) {
			lab := rrvOpen(t, t.TempDir())
			seed.seed(lab)
			live := rrvRead(t, lab.engine)
			require.Equal(t, rrvBits(rrvVectors(3, 100)), rrvBits(live.ChunkEmbeddings), "the live node holds the published vectors")
			require.False(t, NodeNeedsEmbedding(live))

			snapshot := takePeriodicSnapshot(t, lab.journal, lab.dir)
			crashClose(t, lab.engine, lab.wal)

			destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(lab.dir, "wal"), snapshot)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%v", result.Errors)

			rebuilt := rrvRead(t, destination)
			requireSameNode(t, live, rebuilt)
			require.False(t, NodeNeedsEmbedding(rebuilt))
		})
	}
}
