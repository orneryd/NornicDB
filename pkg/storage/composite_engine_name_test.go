package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompositeEngine_CompositeName: a composite engine has no name until
// the database manager records one, then reports the recorded name (the
// name its queries run under); a later record replaces it.
func TestCompositeEngine_CompositeName(t *testing.T) {
	composite := NewCompositeEngine(
		map[string]Engine{"a": NewMemoryEngine()},
		map[string]string{"a": "da"},
		map[string]string{"a": "read_write"},
	)
	require.Equal(t, "", composite.CompositeName())

	composite.SetCompositeName("cmp")
	require.Equal(t, "cmp", composite.CompositeName())

	composite.SetCompositeName("renamed")
	require.Equal(t, "renamed", composite.CompositeName())

	var named interface{ CompositeName() string } = composite
	require.Equal(t, "renamed", named.CompositeName())
}
