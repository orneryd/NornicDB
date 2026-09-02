package storage

import (
	"bytes"
	"context"
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

func TestWriteSnapshotRequiresStreamingEngine(t *testing.T) {
	var output bytes.Buffer
	err := WriteSnapshot(context.Background(), &materializingOnlyEngine{Engine: NewMemoryEngine()}, &output, SnapshotOptions{})
	require.ErrorIs(t, err, ErrStreamingSnapshotUnsupported)
}
