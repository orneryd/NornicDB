package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompositeEngine_WithConstituents: a view over other storage for the
// constituents routes as the composite does, to the storage resolve gives
// each constituent; the composite itself, its name and its constituents
// are unchanged, and an absent constituent stays absent.
func TestCompositeEngine_WithConstituents(t *testing.T) {
	one, two := NewNamespacedEngine(NewMemoryEngine(), "one"), NewNamespacedEngine(NewMemoryEngine(), "two")
	composite := NewCompositeEngine(
		map[string]Engine{"one": one, "two": nil},
		map[string]string{"one": "one", "two": "two"},
		map[string]string{"one": "read_write", "two": "read"},
	)
	composite.SetCompositeName("cmp")

	resolved := map[string]Engine{}
	view := composite.WithConstituents(func(alias string, engine Engine) Engine {
		require.Same(t, one, engine)
		resolved[alias] = two
		return two
	})
	require.Equal(t, map[string]Engine{"one": two}, resolved, "an absent constituent is not resolved")
	require.Equal(t, "cmp", view.CompositeName())

	id, err := view.CreateNode(&Node{ID: "routed", Labels: []string{"Routed"}})
	require.NoError(t, err)
	stored, err := two.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, []string{"Routed"}, stored.Labels)
	_, err = one.GetNode(id)
	require.ErrorIs(t, err, ErrNotFound, "the composite's own constituent is untouched")
	routed, err := composite.GetConstituentByAlias("one")
	require.NoError(t, err)
	require.Same(t, one, routed, "the composite keeps its constituents")
	_, err = view.GetConstituentByAlias("two")
	require.NoError(t, err)
}
