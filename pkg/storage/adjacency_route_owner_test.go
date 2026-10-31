package storage

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type adjacencyOverrideDecorator struct {
	*BadgerEngine
	calls int
	rows  []*Edge
}

func (d *adjacencyOverrideDecorator) GetAdjacentEdges(NodeID) ([]*Edge, []*Edge, error) {
	d.calls++
	return d.rows, d.rows, nil
}

type adjacencyDirectionalFailure struct {
	Engine
	calls   []string
	rows    []*Edge
	failure error
}

func (d *adjacencyDirectionalFailure) GetOutgoingEdges(NodeID) ([]*Edge, error) {
	d.calls = append(d.calls, "out")
	return d.rows, nil
}
func (d *adjacencyDirectionalFailure) GetIncomingEdges(NodeID) ([]*Edge, error) {
	d.calls = append(d.calls, "in")
	return d.rows, d.failure
}

func TestAdjacencyDispatcherPreservesActualDecoratorAndErrorPolicy(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	rows := []*Edge{{ID: "test:r", StartNode: "test:hub", EndNode: "test:a"}}
	decorated := &adjacencyOverrideDecorator{BadgerEngine: b, rows: rows}
	out, in, err := NewNamespacedEngine(decorated, "test").GetAdjacentEdges("hub")
	require.NoError(t, err)
	require.Equal(t, 1, decorated.calls)
	require.Equal(t, EdgeID("r"), out[0].ID)
	require.Equal(t, EdgeID("r"), in[0].ID)
	require.NotSame(t, rows[0], out[0])
	require.Equal(t, EdgeID("test:r"), rows[0].ID)
	failure := errors.New("incoming read failed")
	for _, namespaced := range []bool{false, true} {
		d := &adjacencyDirectionalFailure{Engine: b, rows: rows, failure: failure}
		var engine Engine = d
		if namespaced {
			engine = NewNamespacedEngine(d, "test")
		}
		out, in, err = readAdjacency(engine, adjacencyBoth, "hub", "", nil, visibleRead)
		require.ErrorIs(t, err, failure)
		require.Equal(t, []string{"out", "in"}, d.calls)
		if namespaced {
			require.Nil(t, out)
			require.Nil(t, in)
		} else {
			require.Equal(t, rows, out)
			require.Equal(t, rows, in)
		}
	}
}

func TestAdjacencyNativeReaderOwnsOneSnapshotAndReleasesCacheLease(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	for _, id := range []NodeID{"test:hub", "test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:out", StartNode: "test:hub", EndNode: "test:a", Type: "R"}))
	incoming := &Edge{ID: "test:in", StartNode: "test:b", EndNode: "test:hub", Type: "R", Properties: map[string]any{"v": int64(1)}}
	require.NoError(t, b.CreateEdge(incoming))
	b.invalidateCachesAfterRestore()
	activeReads := func() int {
		b.db.oracle.mu.Lock()
		defer b.db.oracle.mu.Unlock()
		n := 0
		for _, count := range b.db.oracle.readers {
			n += count
		}
		return n
	}

	reader, err := b.captureAdjacencyReader("test:hub", true, true, visibleRead)
	require.NoError(t, err)
	require.NotNil(t, reader.view)
	defer reader.view.Close()
	require.Equal(t, 1, activeReads())
	out, err := reader.deliver(true)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, EdgeID("test:out"), out[0].ID)
	updated := copyEdge(incoming)
	updated.Properties["v"] = int64(2)
	require.NoError(t, b.UpdateEdge(EdgeUpdateRequest{After: updated}))
	in, err := reader.deliver(false)
	require.NoError(t, err)
	require.Len(t, in, 1)
	require.Equal(t, int64(1), in[0].Properties["v"])
	require.Equal(t, 1, activeReads())
	reader.view.Close()
	require.Zero(t, activeReads())
	current, err := b.GetIncomingEdges("test:hub")
	require.NoError(t, err)
	require.Equal(t, int64(2), current[0].Properties["v"])
	_, _, err = b.GetAdjacentEdges("test:hub")
	require.NoError(t, err)
	cached, err := b.captureAdjacencyReader("test:hub", true, true, visibleRead)
	require.NoError(t, err)
	require.Nil(t, cached.view)
	require.Zero(t, activeReads())
	locked := b.writeBehind.mu.TryLock()
	require.True(t, locked, "cached request consumption must follow retirement unlock")
	if locked {
		b.writeBehind.mu.Unlock()
	}
	out, err = cached.deliver(true)
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:out"), out[0].ID)
	in, err = cached.deliver(false)
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:in"), in[0].ID)

	out, in, err = readAdjacency(b, adjacencyIncoming, "test:hub", "", nil, visibleRead)
	require.NoError(t, err)
	require.Nil(t, out)
	require.Equal(t, EdgeID("test:in"), in[0].ID)
}
