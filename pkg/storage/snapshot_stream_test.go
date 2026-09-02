package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type collectingSnapshotVisitor struct {
	nodes []*Node
	edges []*Edge
}

func (v *collectingSnapshotVisitor) VisitNode(node *Node) error {
	v.nodes = append(v.nodes, node)
	return nil
}

func (v *collectingSnapshotVisitor) VisitEdge(edge *Edge) error {
	v.edges = append(v.edges, edge)
	return nil
}

func TestStreamingSnapshotRoundTripAndFooter(t *testing.T) {
	engine := NewMemoryEngine()
	_, err := engine.CreateNode(&Node{ID: "nornic:n1", Labels: []string{"Doc"}, Properties: map[string]any{"name": "alpha"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:n2"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "nornic:e1", StartNode: "nornic:n1", EndNode: "nornic:n2", Type: "LINKS"}))

	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{Sequence: 42}))

	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshot(bytes.NewReader(output.Bytes()), visitor)
	require.NoError(t, err)
	require.Equal(t, uint64(42), metadata.Sequence)
	require.Len(t, visitor.nodes, 2)
	require.Len(t, visitor.edges, 1)

	_, err = ReadSnapshot(bytes.NewReader(output.Bytes()[:output.Len()-1]), &collectingSnapshotVisitor{})
	require.Error(t, err)
}

type materializingOnlyEngine struct{ Engine }

type failingSnapshotVisitor struct {
	nodeErr error
	edgeErr error
}

func (v *failingSnapshotVisitor) VisitNode(*Node) error { return v.nodeErr }
func (v *failingSnapshotVisitor) VisitEdge(*Edge) error { return v.edgeErr }

type failingStreamingSnapshotEngine struct {
	*MemoryEngine
	nodeErr error
	edgeErr error
}

func (e *failingStreamingSnapshotEngine) StreamNodes(context.Context, func(*Node) error) error {
	if e.nodeErr != nil {
		return e.nodeErr
	}
	return nil
}

func (e *failingStreamingSnapshotEngine) StreamEdges(context.Context, func(*Edge) error) error {
	return e.edgeErr
}

func streamingSnapshotHeader(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, binary.Write(&output, binary.LittleEndian, []byte(streamSnapshotMagic)))
	require.NoError(t, output.WriteByte(streamSnapshotVersion))
	require.NoError(t, binary.Write(&output, binary.LittleEndian, uint64(7)))
	require.NoError(t, binary.Write(&output, binary.LittleEndian, int64(0)))
	return &output
}

func TestWriteSnapshotRequiresStreamingEngine(t *testing.T) {
	var output bytes.Buffer
	err := WriteSnapshot(context.Background(), &materializingOnlyEngine{Engine: NewMemoryEngine()}, &output, SnapshotOptions{})
	require.ErrorIs(t, err, ErrStreamingSnapshotUnsupported)
}

func TestWriteSnapshotRejectsUnreadableRecordLimit(t *testing.T) {
	var output bytes.Buffer
	err := WriteSnapshot(context.Background(), NewMemoryEngine(), &output, SnapshotOptions{
		MaxRecordBytes: defaultSnapshotRecordSize + 1,
	})
	require.ErrorContains(t, err, "snapshot max record bytes")
	require.Empty(t, output.Bytes())
}

func TestWriteSnapshotPropagatesStreamingFailures(t *testing.T) {
	nodeErr := errors.New("forced node stream failure")
	edgeErr := errors.New("forced edge stream failure")

	for _, test := range []struct {
		name   string
		engine *failingStreamingSnapshotEngine
		want   error
		prefix string
	}{
		{name: "nodes", engine: &failingStreamingSnapshotEngine{MemoryEngine: NewMemoryEngine(), nodeErr: nodeErr}, want: nodeErr, prefix: "stream snapshot nodes"},
		{name: "edges", engine: &failingStreamingSnapshotEngine{MemoryEngine: NewMemoryEngine(), edgeErr: edgeErr}, want: edgeErr, prefix: "stream snapshot edges"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := WriteSnapshot(context.Background(), test.engine, &output, SnapshotOptions{})
			require.ErrorIs(t, err, test.want)
			require.ErrorContains(t, err, test.prefix)
		})
	}
}

func TestReadSnapshotRejectsInvalidStructure(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		_, err := ReadSnapshot(bytes.NewReader([]byte("bad")), &collectingSnapshotVisitor{})
		require.ErrorContains(t, err, "invalid streaming snapshot header")
	})

	t.Run("version", func(t *testing.T) {
		input := append([]byte(streamSnapshotMagic), streamSnapshotVersion+1)
		_, err := ReadSnapshot(bytes.NewReader(input), &collectingSnapshotVisitor{})
		require.ErrorContains(t, err, "unsupported streaming snapshot version")
	})

	t.Run("unknown frame", func(t *testing.T) {
		input := streamingSnapshotHeader(t)
		require.NoError(t, writeSnapshotFrame(input, 77, map[string]string{"value": "unknown"}, defaultSnapshotRecordSize))
		_, err := ReadSnapshot(bytes.NewReader(input.Bytes()), &collectingSnapshotVisitor{})
		require.ErrorContains(t, err, "unknown streaming snapshot frame type 77")
	})

	t.Run("footer count mismatch", func(t *testing.T) {
		input := streamingSnapshotHeader(t)
		require.NoError(t, writeSnapshotFrame(input, streamSnapshotFooter, struct {
			Nodes uint64 `json:"nodes"`
			Edges uint64 `json:"edges"`
		}{Nodes: 1}, defaultSnapshotRecordSize))
		_, err := ReadSnapshot(bytes.NewReader(input.Bytes()), &collectingSnapshotVisitor{})
		require.ErrorContains(t, err, "footer count mismatch")
	})

	t.Run("trailing data", func(t *testing.T) {
		input := streamingSnapshotHeader(t)
		require.NoError(t, writeSnapshotFrame(input, streamSnapshotFooter, struct {
			Nodes uint64 `json:"nodes"`
			Edges uint64 `json:"edges"`
		}{}, defaultSnapshotRecordSize))
		input.WriteByte(0)
		_, err := ReadSnapshot(bytes.NewReader(input.Bytes()), &collectingSnapshotVisitor{})
		require.ErrorContains(t, err, "trailing data")
	})
}

func TestReadSnapshotPropagatesVisitorErrors(t *testing.T) {
	engine := NewMemoryEngine()
	_, err := engine.CreateNode(&Node{ID: "nornic:n1"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:n2"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "nornic:e1", StartNode: "nornic:n1", EndNode: "nornic:n2", Type: "LINKS"}))
	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{}))

	nodeErr := errors.New("forced node visitor failure")
	_, err = ReadSnapshot(bytes.NewReader(output.Bytes()), &failingSnapshotVisitor{nodeErr: nodeErr})
	require.ErrorIs(t, err, nodeErr)

	edgeErr := errors.New("forced edge visitor failure")
	_, err = ReadSnapshot(bytes.NewReader(output.Bytes()), &failingSnapshotVisitor{edgeErr: edgeErr})
	require.ErrorIs(t, err, edgeErr)
}

func TestReadSnapshotFrameRejectsCorruption(t *testing.T) {
	t.Run("oversized payload", func(t *testing.T) {
		var input bytes.Buffer
		input.WriteByte(streamSnapshotNode)
		require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(defaultSnapshotRecordSize+1)))
		_, _, err := readSnapshotFrame(&input)
		require.ErrorContains(t, err, "exceeds reader limit")
	})

	t.Run("truncated payload", func(t *testing.T) {
		var input bytes.Buffer
		input.WriteByte(streamSnapshotNode)
		require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(3)))
		input.WriteString("x")
		_, _, err := readSnapshotFrame(&input)
		require.Error(t, err)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		var input bytes.Buffer
		input.WriteByte(streamSnapshotNode)
		payload := []byte(`{"id":"n1"}`)
		require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(len(payload))))
		_, err := input.Write(payload)
		require.NoError(t, err)
		require.NoError(t, binary.Write(&input, binary.LittleEndian, uint32(0)))
		_, _, err = readSnapshotFrame(&input)
		require.ErrorContains(t, err, "checksum mismatch")
	})
}
