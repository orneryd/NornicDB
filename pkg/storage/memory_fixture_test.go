package storage

import "github.com/orneryd/nornicdb/internal/testfixture"

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test fixtures own their Badger workers even when a wrapper's Close is a
// no-op or an injected error. Register cleanup at construction so a failed
// assertion cannot leave those workers and their arenas in later tests.
func newTestMemoryEngine(t testing.TB) *MemoryEngine {
	t.Helper()
	return testfixture.Own(t, testfixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
}

// mustReadNode asserts the read contract every selected node read shares: the
// read succeeds and returns a node. The assertion body lives here once; each
// caller passes its own read so GetNode, GetQueryNode and
// GetNodeLatestVisible keep their distinct semantics.
func mustReadNode(t *testing.T, read func(NodeID) (*Node, error), id NodeID) *Node {
	t.Helper()
	node, err := read(id)
	require.NoError(t, err)
	require.NotNil(t, node)
	return node
}
