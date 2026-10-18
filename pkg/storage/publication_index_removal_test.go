package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicationIndexRemovalPreservesLookups(t *testing.T) {
	for _, properties := range [][]string{{"p"}, {"p", "q"}} {
		t.Run(fmt.Sprint(properties), func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddPropertyIndex("posting", "L", properties))
			idx, ok := schema.GetCompositeIndex("posting")
			require.True(t, ok)
			props := map[string]interface{}{"p": "same", "q": "same"}
			values := make([]interface{}, len(properties))
			for i := range values {
				values[i] = "same"
			}
			for _, id := range []NodeID{"a", "b", "c", "d"} {
				require.NoError(t, idx.IndexNode(id, props))
			}
			snapshot := idx.LookupFull(values...)
			expected := []NodeID{"b", "c", "d"}
			for _, id := range []NodeID{"a", "c", "d", "missing", "b"} {
				idx.RemoveNode(id, props)
				if len(expected) == 0 {
					require.Empty(t, idx.LookupFull(values...))
					require.Empty(t, idx.LookupPrefix("same"))
				} else {
					require.Equal(t, expected, idx.LookupFull(values...))
					require.Equal(t, expected, idx.LookupPrefix("same"))
				}
				switch id {
				case "a":
					expected = []NodeID{"b", "d"}
				case "c":
					expected = []NodeID{"b"}
				case "missing":
					expected = []NodeID{}
				}
			}
			require.Equal(t, []NodeID{"a", "b", "c", "d"}, snapshot)
			require.NoError(t, idx.IndexNode("a", props))
			require.Equal(t, []NodeID{"a"}, idx.LookupFull(values...))
		})
	}
}

// Keep the original removal algorithm only as a benchmark control.
func referencePublicationRemoveID(ids []NodeID, id NodeID) []NodeID {
	for i, existing := range ids {
		if existing == id {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

func BenchmarkPublicationPostingRemoval(b *testing.B) {
	for _, size := range []int{1, 1000, 50000} {
		for _, position := range []string{"first", "middle", "missing"} {
			for _, tc := range []struct {
				name   string
				remove func([]NodeID, NodeID) []NodeID
			}{
				{"original", referencePublicationRemoveID}, {"current", removeNodeID},
			} {
				b.Run(fmt.Sprintf("%d/%s/%s", size, position, tc.name), func(b *testing.B) {
					ids := make([]NodeID, size)
					for i := range ids {
						ids[i] = NodeID(fmt.Sprint(i))
					}
					target := ids[0]
					if position == "middle" {
						target = ids[size/2]
					}
					if position == "missing" {
						target = "absent"
					}
					b.ReportAllocs()
					for b.Loop() {
						_ = tc.remove(ids, target)
						// Keep the target at the same position in both variants.
						// Changes to the suffix do not affect locating that position.
						if position == "first" {
							ids[0] = target
						}
						if position == "middle" {
							ids[size/2] = target
						}
					}
				})
			}
		}
	}
}
