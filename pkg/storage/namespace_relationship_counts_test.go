package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type namespaceCountReadProbe struct {
	*BadgerEngine
	scopes []string
}

func (p *namespaceCountReadProbe) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	p.scopes = append(p.scopes, namespace)
	return p.BadgerEngine.EdgeCountByTypeInNamespace(namespace, edgeType)
}

func (p *namespaceCountReadProbe) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	p.scopes = append(p.scopes, namespace)
	return p.BadgerEngine.EdgeCountByStartLabelInNamespace(namespace, label, edgeType)
}

func (p *namespaceCountReadProbe) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	p.scopes = append(p.scopes, namespace)
	return p.BadgerEngine.EdgeCountByEndLabelInNamespace(namespace, label, edgeType)
}

func (*namespaceCountReadProbe) AllEdges() ([]*Edge, error) { panic("count materialized edges") }
func (*namespaceCountReadProbe) GetEdgesByType(string) ([]*Edge, error) {
	panic("count materialized a relationship type")
}

func TestNamespaceRelationshipCounterPointReads(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for namespace, count := range map[string]int{"a": 1, "b": 2} {
		view := NewNamespacedEngine(base, namespace)
		_, err := view.CreateNode(&Node{ID: "start", Labels: []string{"Start"}})
		require.NoError(t, err)
		_, err = view.CreateNode(&Node{ID: "end", Labels: []string{"End"}})
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			require.NoError(t, view.CreateEdge(&Edge{ID: EdgeID(string(rune('x' + i))), StartNode: "start", EndNode: "end", Type: "T"}))
		}
	}
	probe := &namespaceCountReadProbe{BadgerEngine: base.BadgerEngine}
	for name, inner := range map[string]Engine{
		"wal":                 NewWALEngine(probe, nil),
		"trace-wal":           NewTracedEngine(NewWALEngine(probe, nil)),
		"namespace-trace-wal": NewNamespacedEngine(NewTracedEngine(NewWALEngine(probe, nil)), "a"),
	} {
		t.Run(name, func(t *testing.T) {
			counter, ok := inner.(NamespaceEdgeTypeStatsProvider)
			require.True(t, ok)
			for namespace, want := range map[string]int64{"a": 1, "b": 2} {
				probe.scopes = nil
				count, err := counter.EdgeCountByTypeInNamespace(namespace, "T")
				require.NoError(t, err)
				require.Equal(t, want, count)
				count, err = counter.EdgeCountByStartLabelInNamespace(namespace, "Start", "T")
				require.NoError(t, err)
				require.Equal(t, want, count)
				count, err = counter.EdgeCountByEndLabelInNamespace(namespace, "End", "T")
				require.NoError(t, err)
				require.Equal(t, want, count)
				require.Equal(t, []string{namespace, namespace, namespace}, probe.scopes)
			}
		})
	}
	// A narrowed Engine deliberately hides the capability. It must neither
	// return its global total nor silently switch to edge materialization.
	missing := struct{ Engine }{Engine: probe}
	for _, tier := range []RelationshipCountTier{RelationshipCountByType, RelationshipCountByStartLabel, RelationshipCountByEndLabel} {
		_, err := NamespaceRelationshipCount(missing, "a", "T", "Start", tier)
		require.ErrorIs(t, err, ErrNotImplemented)
	}
}
