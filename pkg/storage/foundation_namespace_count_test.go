package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationNoPrefixStatistics struct{ Engine }

func TestFoundationNamespaceCountScope(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, id := range []NodeID{"tenant:n", "tenant:other:shadow", "other:sentinel"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "tenant:e", StartNode: "tenant:n", EndNode: "tenant:other:shadow", Type: "R"}))
	for _, statistics := range []bool{false, true} {
		var native Engine = b
		if !statistics {
			native = foundationNoPrefixStatistics{b}
		}
		for _, word := range []string{"", "T", "W", "TW", "WT"} {
			t.Run(word+map[bool]string{true: "/indexed", false: "/ordinary"}[statistics], func(t *testing.T) {
				namespace := NewNamespacedEngine(native, "tenant")
				var view Engine = namespace
				for _, token := range word {
					if token == 'T' {
						view = NewTracedEngine(view)
					} else {
						view = NewWALEngine(view, nil)
					}
				}
				composite := NewCompositeEngine(map[string]Engine{"a": view}, map[string]string{"a": "tenant"}, map[string]string{"a": "read"})
				count, err := composite.NodeCountByLabelInNamespace("tenant", "L")
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
				count, err = composite.NodeCountByLabelInNamespace("other", "L")
				require.NoError(t, err)
				require.Zero(t, count, "logical ID other:shadow is still in tenant")
				for _, prefix := range []string{"n", "tenant:n"} {
					count, err = namespace.NodeCountByPrefix(prefix)
					require.NoError(t, err)
					require.Equal(t, int64(1), count)
				}
				for _, prefix := range []string{"e", "tenant:e"} {
					count, err = namespace.EdgeCountByPrefix(prefix)
					require.NoError(t, err)
					require.Equal(t, int64(1), count)
				}
			})
		}
	}
}
