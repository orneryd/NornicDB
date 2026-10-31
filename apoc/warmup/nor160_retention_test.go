// NOR-160 lane (Claude), 30 October 2026: the warmup cache hands out caller copies.
package warmup

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionWarmupCache(t *testing.T) {
	previous := Storage
	t.Cleanup(func() { Storage = previous; Clear() })
	s := storage.NewInMemoryStorage()
	props := func() map[string]interface{} {
		return map[string]interface{}{"payload": map[string]interface{}{"value": "original"}}
	}
	a, err := s.CreateNode([]string{"Held"}, props())
	require.NoError(t, err)
	b, err := s.CreateNode([]string{"Held"}, props())
	require.NoError(t, err)
	r, err := s.CreateRelationship(a.ID, b.ID, "T", props())
	require.NoError(t, err)
	Storage = s
	Clear()
	Run()

	// Keys are fixed before any read: a shared image could change a.ID itself.
	nodeID, relID := a.ID, r.ID
	reads := map[string]func(*testing.T) any{
		"GetCachedNode": func(t *testing.T) any {
			n, ok := GetCachedNode(nodeID)
			require.True(t, ok)
			return n
		},
		"GetCachedRelationship": func(t *testing.T) any {
			rel, ok := GetCachedRelationship(relID)
			require.True(t, ok)
			return rel
		},
		"GetNodesByLabel": func(t *testing.T) any {
			n, ok := GetNodesByLabel("Held")
			require.True(t, ok)
			return n
		},
		"GetRelationshipsByType": func(t *testing.T) any {
			rels, ok := GetRelationshipsByType("T")
			require.True(t, ok)
			return rels
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			want := readownership.Capture(read(t))
			for pass := 0; pass < 3; pass++ {
				got := read(t)
				require.Equal(t, want, readownership.Capture(got), "pass %d", pass)
				readownership.Mutate(got)
			}
		})
	}
}
