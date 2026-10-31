// NOR-160 lane (Claude), 30 October 2026: the APOC graph hands out caller copies.
package storage

import (
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func nor160Props() map[string]interface{} {
	return map[string]interface{}{"payload": map[string]interface{}{"value": "original"}, "list": []interface{}{"a", []interface{}{"b"}}}
}

func nor160Graph(t *testing.T) (*InMemoryStorage, int64, int64, int64) {
	t.Helper()
	s := NewInMemoryStorage()
	a, err := s.CreateNode([]string{"Held"}, nor160Props())
	require.NoError(t, err)
	b, err := s.CreateNode([]string{"Held"}, nor160Props())
	require.NoError(t, err)
	r, err := s.CreateRelationship(a.ID, b.ID, "T", nor160Props())
	require.NoError(t, err)
	return s, a.ID, b.ID, r.ID
}

// Every public read, collection, traversal and visitor delivery is a caller
// copy; a write is visible only through the update operations.
func TestNOR160RetentionAPOCGraph(t *testing.T) {
	reads := map[string]func(*InMemoryStorage, int64, int64, int64) any{
		"GetNode":         func(s *InMemoryStorage, a, _, _ int64) any { n, _ := s.GetNode(a); return n },
		"GetRelationship": func(s *InMemoryStorage, _, _, r int64) any { rel, _ := s.GetRelationship(r); return rel },
		"AllNodes": func(s *InMemoryStorage, _, _, _ int64) any {
			n, _ := s.AllNodes()
			sort.Slice(n, func(i, j int) bool { return n[i].ID < n[j].ID })
			return n
		},
		"AllRelationships": func(s *InMemoryStorage, _, _, _ int64) any {
			r, _ := s.AllRelationships()
			sort.Slice(r, func(i, j int) bool { return r[i].ID < r[j].ID })
			return r
		},
		"GetNodeRelationships": func(s *InMemoryStorage, a, _, _ int64) any {
			r, _ := s.GetNodeRelationships(a, "", DirectionBoth)
			return r
		},
		"GetNodeNeighbors": func(s *InMemoryStorage, a, _, _ int64) any {
			n, _ := s.GetNodeNeighbors(a, "", DirectionBoth)
			return n
		},
		"FindShortestPath": func(s *InMemoryStorage, a, b, _ int64) any { p, _ := s.FindShortestPath(a, b, "", 3); return p },
		"FindAllPaths":     func(s *InMemoryStorage, a, b, _ int64) any { p, _ := s.FindAllPaths(a, b, "", 3); return p },
		"BFS": func(s *InMemoryStorage, a, _, _ int64) any {
			var seen []*Node
			_ = s.BFS(a, "", 2, func(n *Node) bool { seen = append(seen, n); return true })
			return seen
		},
		"DFS": func(s *InMemoryStorage, a, _, _ int64) any {
			var seen []*Node
			_ = s.DFS(a, "", 2, func(n *Node) bool { seen = append(seen, n); return true })
			return seen
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			s, a, b, r := nor160Graph(t)
			want := readownership.Capture(read(s, a, b, r))
			for pass := 0; pass < 3; pass++ {
				got := read(s, a, b, r)
				require.Equal(t, want, readownership.Capture(got), "pass %d", pass)
				readownership.Mutate(got)
			}
		})
	}
}

// The store keeps its own image of created and updated values.
func TestNOR160RetentionAPOCAdmission(t *testing.T) {
	s := NewInMemoryStorage()
	labels, props := []string{"Held"}, nor160Props()
	created, err := s.CreateNode(labels, props)
	require.NoError(t, err)
	id := created.ID
	update := map[string]interface{}{"added": []interface{}{"x"}}
	require.NoError(t, s.UpdateNode(id, update))
	want := readownership.Capture(&Node{ID: id, Labels: []string{"Held"}, Properties: map[string]interface{}{
		"payload": map[string]interface{}{"value": "original"}, "list": []interface{}{"a", []interface{}{"b"}}, "added": []interface{}{"x"},
	}})
	readownership.Mutate(labels)
	readownership.Mutate(props)
	readownership.Mutate(update)
	readownership.Mutate(created)
	got, err := s.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, want, readownership.Capture(got))
}
