package linkpredict

import (
	"bytes"
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T) storage.Engine {
	t.Helper()
	return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
}

func TestGraphStreamer_StreamEdges(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()

	n1 := &storage.Node{ID: "nornic:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &storage.Node{ID: "nornic:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := eng.CreateNode(n1)
	require.NoError(t, err)
	_, err = eng.CreateNode(n2)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "nornic:e1", StartNode: "nornic:n1", EndNode: "nornic:n2", Type: "KNOWS", Properties: map[string]interface{}{}}
	err = eng.CreateEdge(edge)
	require.NoError(t, err)

	streamer := NewGraphStreamer(eng, nil)

	var collected []*storage.Edge
	err = streamer.StreamEdges(ctx, "nornic:n1", func(e *storage.Edge) error {
		collected = append(collected, e)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "nornic:e1", string(collected[0].ID))
}

func TestGraphStreamer_StreamEdges_ContextCancelled(t *testing.T) {
	eng := newTestEngine(t)

	n1 := &storage.Node{ID: "nornic:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &storage.Node{ID: "nornic:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := eng.CreateNode(n1)
	require.NoError(t, err)
	_, err = eng.CreateNode(n2)
	require.NoError(t, err)
	edge := &storage.Edge{ID: "nornic:e1", StartNode: "nornic:n1", EndNode: "nornic:n2", Type: "KNOWS", Properties: map[string]interface{}{}}
	err = eng.CreateEdge(edge)
	require.NoError(t, err)

	streamer := NewGraphStreamer(eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err = streamer.StreamEdges(ctx, "nornic:n1", func(e *storage.Edge) error {
		return nil
	})
	assert.Error(t, err)
}

func TestGraphStreamer_StreamEdges_Empty(t *testing.T) {
	eng := newTestEngine(t)
	n1 := &storage.Node{ID: "nornic:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	_, err := eng.CreateNode(n1)
	require.NoError(t, err)

	streamer := NewGraphStreamer(eng, nil)
	ctx := context.Background()

	var count int
	err = streamer.StreamEdges(ctx, "nornic:n1", func(e *storage.Edge) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestNewGraphStreamer_NilConfig(t *testing.T) {
	eng := newTestEngine(t)
	streamer := NewGraphStreamer(eng, nil)
	assert.NotNil(t, streamer)
	assert.NotNil(t, streamer.config)
}

func TestExportToWriter(t *testing.T) {
	g := Graph{
		"n1": NodeSet{"n2": struct{}{}, "n3": struct{}{}},
	}

	var buf bytes.Buffer
	err := ExportToWriter(g, &buf)
	require.NoError(t, err)
	out := buf.String()
	assert.Contains(t, out, "n1")
	assert.Contains(t, out, "n2")
	assert.Contains(t, out, "n3")
}

func TestExportToWriter_Empty(t *testing.T) {
	g := Graph{}
	var buf bytes.Buffer
	err := ExportToWriter(g, &buf)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}
