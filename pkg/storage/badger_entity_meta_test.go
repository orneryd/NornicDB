package storage

import (
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_GetEntityMeta_NodeRoundTrip(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{
		ID:     "test:n",
		Labels: []string{"L1", "L2"},
		Properties: map[string]any{
			"a": int64(1),
			"b": "two",
			"c": true,
		},
	})
	require.NoError(t, err)

	meta, err := e.GetEntityMeta("test:n")
	require.NoError(t, err)
	require.Equal(t, knowledgepolicy.ScopeNode, meta.Scope)
	gotLabels := append([]string(nil), meta.Labels...)
	sort.Strings(gotLabels)
	require.Equal(t, []string{"L1", "L2"}, gotLabels)
	require.Empty(t, meta.EdgeType, "node meta must not carry an edge type")

	gotKeys := append([]string(nil), meta.PropertyKeys...)
	sort.Strings(gotKeys)
	require.Equal(t, []string{"a", "b", "c"}, gotKeys)

	// CreatedAtNanos and VersionAtNanos are populated even when
	// UpdatedAt was never set (engine writes a CreatedAt at insert).
	require.NotZero(t, meta.CreatedAtNanos)
	require.NotZero(t, meta.VersionAtNanos)
}

func TestBadgerEngine_GetEntityMeta_EdgeRoundTrip(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{ID: "test:a", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = e.CreateNode(&Node{ID: "test:b", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"k1": "v1", "k2": int64(7)},
	}))

	meta, err := e.GetEntityMeta("test:e1")
	require.NoError(t, err)
	require.Equal(t, knowledgepolicy.ScopeEdge, meta.Scope)
	require.Equal(t, "REL", meta.EdgeType)
	require.Empty(t, meta.Labels, "edge meta must not carry node labels")

	gotKeys := append([]string(nil), meta.PropertyKeys...)
	sort.Strings(gotKeys)
	require.Equal(t, []string{"k1", "k2"}, gotKeys)
}

func TestBadgerEngine_GetEntityMeta_NotFoundReturnsError(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.GetEntityMeta("test:nonexistent")
	require.Error(t, err, "missing entity must error (looked up as edge after node miss)")
}
