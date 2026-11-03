package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// walPartialStreamEngine streams the records it was given and then fails, which
// is how a streaming enumeration reports the count it reached together with its
// own error.
type walPartialStreamEngine struct {
	Engine
	nodes []*Node
	edges []*Edge
	err   error
}

func (e *walPartialStreamEngine) StreamNodes(_ context.Context, fn func(*Node) error) error {
	for _, node := range e.nodes {
		if err := fn(node); err != nil {
			return err
		}
	}
	return e.err
}

func (e *walPartialStreamEngine) StreamEdges(_ context.Context, fn func(*Edge) error) error {
	for _, edge := range e.edges {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return e.err
}

func (e *walPartialStreamEngine) StreamNodeChunks(_ context.Context, _ int, fn func([]*Node) error) error {
	if len(e.nodes) > 0 {
		if err := fn(e.nodes); err != nil {
			return err
		}
	}
	return e.err
}

func newPrefixCountWALEngine(t *testing.T, engine Engine) *WALEngine {
	t.Helper()
	wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	return NewWALEngine(engine, wal)
}

// The WAL engine's prefix count keeps the error/result convention of the
// capability that answered it: a streaming enumeration that fails returns the
// count it reached, and a prefix no record matches counts nothing. Both the
// node and the edge count take that same decision.
func TestWALEnginePrefixCountPartialResults(t *testing.T) {
	streamErr := errors.New("stream failed")
	engine := &walPartialStreamEngine{
		Engine: newTestMemoryEngine(t),
		nodes:  []*Node{{ID: "test:n1"}, {ID: "test:n2"}, {ID: "other:n3"}},
		edges:  []*Edge{{ID: "test:e1", Type: "REL"}, {ID: "other:e2", Type: "REL"}},
		err:    streamErr,
	}
	walEngine := newPrefixCountWALEngine(t, engine)

	outside := newPrefixCountWALEngine(t, &walPartialStreamEngine{
		Engine: newTestMemoryEngine(t),
		nodes:  []*Node{{ID: "test:n1"}},
		edges:  []*Edge{{ID: "test:e1", Type: "REL"}},
	})
	unmatched, err := outside.NodeCountByPrefix(context.Background(), "absent:")
	require.NoError(t, err)
	require.Zero(t, unmatched, "a prefix no record matches counts nothing")

	nodes, err := walEngine.NodeCountByPrefix(context.Background(), "test:")
	require.ErrorIs(t, err, streamErr, "the streaming read's own error reaches the caller")
	require.Equal(t, int64(2), nodes, "a streaming read keeps the count it reached")

	edges, err := walEngine.EdgeCountByPrefix(context.Background(), "test:")
	require.ErrorIs(t, err, streamErr)
	require.Equal(t, int64(1), edges)
}

// A whole-collection read that fails returns no count for either kind.
func TestWALEnginePrefixCountFailsWithoutACount(t *testing.T) {
	readErr := errors.New("all nodes failed")
	walEngine := newPrefixCountWALEngine(t, &exportableOnlyEngine{
		Engine:  newTestMemoryEngine(t),
		nodeErr: readErr,
		edgeErr: readErr,
	})

	nodes, err := walEngine.NodeCountByPrefix(context.Background(), "test:")
	require.ErrorIs(t, err, readErr)
	require.Zero(t, nodes)

	edges, err := walEngine.EdgeCountByPrefix(context.Background(), "test:")
	require.ErrorIs(t, err, readErr)
	require.Zero(t, edges)
}
