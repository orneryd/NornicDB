package storage

import (
	"bytes"
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The interface wrapper intentionally exposes only the general stream. It
// preserves stored visibility while denying access to physical Badger bodies.
type snapshotGeneralEdgeStream struct {
	StreamingEngine
}

func (s snapshotGeneralEdgeStream) StreamEdges(ctx context.Context, visit func(*Edge) error) error {
	if stored, ok := s.StreamingEngine.(interface {
		StreamStoredEdgesByType(context.Context, string, func(*Edge) error) error
	}); ok {
		return stored.StreamStoredEdgesByType(ctx, "", visit)
	}
	return s.StreamingEngine.StreamEdges(ctx, visit)
}

type snapshotTransparentEngine struct{ StreamingEngine }

func (s snapshotTransparentEngine) GetInnerEngine() Engine { return s.StreamingEngine }

func snapshotStreamDifferential(t *testing.T, engine Engine, expectedEdges int) []byte {
	t.Helper()
	var fast, general bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), engine, &fast, SnapshotOptions{Sequence: 7}))
	wrapper := snapshotGeneralEdgeStream{StreamingEngine: engine.(StreamingEngine)}
	require.NoError(t, WriteSnapshot(context.Background(), wrapper, &general, SnapshotOptions{Sequence: 7}))
	// Overlay maps do not promise enumeration order. Compare every complete
	// frame by entity ID, including its byte length and checksum.
	require.Equal(t, snapshotFramesByID(t, general.Bytes()), snapshotFramesByID(t, fast.Bytes()))
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshot(bytes.NewReader(fast.Bytes()), visitor)
	require.NoError(t, err)
	require.Equal(t, uint64(expectedEdges), metadata.EdgeCount)
	return bytes.Clone(fast.Bytes())
}

func snapshotFramesByID(t *testing.T, snapshot []byte) map[string][]byte {
	t.Helper()
	const headerBytes = len(streamSnapshotMagic) + 1 + 8 + 8
	data := snapshot[headerBytes:]
	reader := bytes.NewReader(data)
	frames := map[string][]byte{}
	for reader.Len() > 0 {
		start := len(data) - reader.Len()
		kind, payload, err := readSnapshotFrame(reader)
		require.NoError(t, err)
		key := "footer"
		switch kind {
		case streamSnapshotNode:
			var node Node
			require.NoError(t, decodeRecoveryPayload(payload, &node))
			key = "node:" + string(node.ID)
		case streamSnapshotEdge:
			var edge Edge
			require.NoError(t, decodeRecoveryPayload(payload, &edge))
			key = "edge:" + string(edge.ID)
		}
		require.NotContains(t, frames, key)
		frames[key] = bytes.Clone(data[start : len(data)-reader.Len()])
	}
	return frames
}

func TestSnapshotTranscoderStoredStreamDifferential(t *testing.T) {
	// Persistent Badger supports value-log records above the in-memory
	// engine's 64 KiB ceiling, including the large-value corpus member.
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for _, id := range []NodeID{"test:a", "test:b", "other:a", "other:b"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	corpus := snapshotTranscoderCorpus()
	for i, value := range corpus {
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:e-%03d", i)), StartNode: "test:a", EndNode: "test:b", Type: "CALLS",
			Properties: map[string]any{"value": value}, VisibilitySuppressed: i%3 == 0}))
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "other:e", StartNode: "other:a", EndNode: "other:b", Type: "CALLS", VisibilitySuppressed: true}))
	for name, source := range map[string]Engine{
		"badger": engine, "wal": NewWALEngine(engine, nil),
		"transparent": snapshotTransparentEngine{StreamingEngine: engine},
	} {
		t.Run(name, func(t *testing.T) { snapshotStreamDifferential(t, source, len(corpus)+1) })
	}
	t.Run("namespace", func(t *testing.T) {
		snapshotStreamDifferential(t, NewNamespacedEngine(engine, "test"), len(corpus))
	})
	path := filepath.Join(t.TempDir(), "snapshot.nds")
	require.NoError(t, SaveStreamingSnapshot(context.Background(), engine, path, SnapshotOptions{}))
	generalPath := filepath.Join(t.TempDir(), "general.nds")
	require.NoError(t, SaveStreamingSnapshot(context.Background(), snapshotGeneralEdgeStream{StreamingEngine: engine}, generalPath, SnapshotOptions{}))
	recovered, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	_, status, err := RecoverIntoEngine(recovered, t.TempDir(), path)
	require.NoError(t, err)
	require.Equal(t, uint64(len(corpus)+1), status.SnapshotEdges)
	reference, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reference.Close()) })
	_, referenceStatus, err := RecoverIntoEngine(reference, t.TempDir(), generalPath)
	require.NoError(t, err)
	require.Equal(t, status.SnapshotEdges, referenceStatus.SnapshotEdges)
	var before, after bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), reference, &before, SnapshotOptions{}))
	require.NoError(t, WriteSnapshot(context.Background(), recovered, &after, SnapshotOptions{}))
	left, right := &collectingSnapshotVisitor{}, &collectingSnapshotVisitor{}
	_, err = ReadSnapshot(&before, left)
	require.NoError(t, err)
	_, err = ReadSnapshot(&after, right)
	require.NoError(t, err)
	// General recovery already normalizes some list widths on re-import.
	// B must produce precisely the same recovered state, including that policy.
	require.Equal(t, left.edges, right.edges, "fast and general snapshots recover equivalent hidden flags, metadata and strict property types")
}

func TestSnapshotTranscoderOverlayDifferential(t *testing.T) {
	engine := snapshotOverlayFixture(t)
	require.NoError(t, engine.FlushWriteBehind())
	for _, id := range []EdgeID{"test:stable-r", "test:gone-r"} {
		require.NoError(t, engine.CreateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "REL", VisibilitySuppressed: true, Properties: map[string]any{"v": int64(2)}}))
	}
	require.NoError(t, engine.FlushWriteBehind())
	// The stored-read owner must keep an untouched committed body, shadow an
	// updated body, omit an acknowledged deletion, and include a pending edge.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", VisibilitySuppressed: true, Properties: map[string]any{"v": int64(3)}}))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:new-r", StartNode: "test:a", EndNode: "test:b", Type: "REL", VisibilitySuppressed: true}))
	require.NoError(t, tx.DeleteEdge("test:gone-r"))
	require.NoError(t, tx.Commit())
	snapshotStreamDifferential(t, engine, 3)
}
