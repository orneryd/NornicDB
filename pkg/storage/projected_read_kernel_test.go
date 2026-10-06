package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// projectionReaderOnlyEngine serves projected reads directly and fails loudly
// if the kernel falls back to a full GetNode read.
type projectionReaderOnlyEngine struct {
	Engine
	node     *Node
	err      error
	getCalls int
}

func (f *projectionReaderOnlyEngine) GetNodeProjected(NodeID, []string) (*Node, error) {
	return f.node, f.err
}

func (f *projectionReaderOnlyEngine) GetNode(NodeID) (*Node, error) {
	f.getCalls++
	return nil, errors.New("GetNode must not be called when the reader serves the read")
}

// projectionFallbackOnlyEngine has no projection capability, so the kernel
// must read the full node and project it on the way out.
type projectionFallbackOnlyEngine struct {
	Engine
	node     *Node
	err      error
	getCalls int
}

func (f *projectionFallbackOnlyEngine) GetNode(NodeID) (*Node, error) {
	f.getCalls++
	return f.node, f.err
}

// TestGetNodeProjectedThrough_KernelContract pins the shared projection tail
// used by AsyncEngine, WALEngine and CompositeEngine: a projection-capable
// reader serves the read without a fallback, and every other engine gets a
// projected copy of its full read with errors propagated verbatim.
func TestGetNodeProjectedThrough_KernelContract(t *testing.T) {
	node := &Node{ID: "test:node", Properties: map[string]any{"name": "n", "age": int64(1)}}

	t.Run("projection_reader_serves_the_read", func(t *testing.T) {
		fake := &projectionReaderOnlyEngine{node: node}
		got, err := getNodeProjectedThrough(fake, node.ID, []string{"name"})
		require.NoError(t, err)
		require.Same(t, node, got)
		require.Zero(t, fake.getCalls)
	})

	t.Run("reader_errors_propagate_without_fallback", func(t *testing.T) {
		sentinel := errors.New("reader failure")
		fake := &projectionReaderOnlyEngine{err: sentinel}
		_, err := getNodeProjectedThrough(fake, node.ID, nil)
		require.ErrorIs(t, err, sentinel)
		require.Zero(t, fake.getCalls)
	})

	t.Run("fallback_projects_the_full_read", func(t *testing.T) {
		fake := &projectionFallbackOnlyEngine{node: node}
		got, err := getNodeProjectedThrough(fake, node.ID, []string{"name"})
		require.NoError(t, err)
		require.Equal(t, 1, fake.getCalls)
		require.NotSame(t, node, got)
		require.Equal(t, node.ID, got.ID)
		require.Equal(t, map[string]any{"name": "n"}, got.Properties)
	})

	t.Run("fallback_nil_properties_copy_the_node", func(t *testing.T) {
		fake := &projectionFallbackOnlyEngine{node: node}
		got, err := getNodeProjectedThrough(fake, node.ID, nil)
		require.NoError(t, err)
		require.NotSame(t, node, got)
		require.Equal(t, node.Properties, got.Properties)
	})

	t.Run("fallback_not_found_propagates", func(t *testing.T) {
		fake := &projectionFallbackOnlyEngine{err: ErrNotFound}
		_, err := getNodeProjectedThrough(fake, node.ID, nil)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, 1, fake.getCalls)
	})

	t.Run("fallback_error_propagates", func(t *testing.T) {
		sentinel := errors.New("read failure")
		fake := &projectionFallbackOnlyEngine{err: sentinel}
		_, err := getNodeProjectedThrough(fake, node.ID, []string{"name"})
		require.ErrorIs(t, err, sentinel)
	})
}
