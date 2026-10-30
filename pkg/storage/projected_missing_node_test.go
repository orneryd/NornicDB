package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A projected read of a node that doesn't exist reports ErrNotFound through
// the Badger engine, a namespaced view of it, and a composite over that view.
func TestGetNodeProjectedMissingNode(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.GetNodeProjected("test:missing", []string{"name"})
	require.ErrorIs(t, err, ErrNotFound)

	namespaced := NewNamespacedEngine(engine, "test")
	_, err = namespaced.GetNodeProjected("missing", []string{"name"})
	require.ErrorIs(t, err, ErrNotFound)

	composite := NewCompositeEngine(map[string]Engine{"test": namespaced}, nil, map[string]string{"test": "read"})
	_, err = composite.GetNodeProjected("missing", []string{"name"})
	require.ErrorIs(t, err, ErrNotFound)
}
