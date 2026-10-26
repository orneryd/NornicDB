package search

import (
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type r5UnavailablePredicate struct{ storage.Engine }

func (r5UnavailablePredicate) ReadCapabilityAvailable(reflect.Type) bool { return false }
func (r5UnavailablePredicate) IsCurrentTemporalNode(*storage.Node, time.Time) (bool, error) {
	return false, storage.ErrNotImplemented
}

func TestReadCapabilityR5SearchPredicateRefusal(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := storage.NewTracedEngine(storage.NewWALEngine(r5UnavailablePredicate{base}, nil))
	service := NewService(view)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	err := service.IndexNode(&storage.Node{ID: "tenant:n", Labels: []string{"Fact"}, Properties: map[string]any{"text": "fact"}})
	require.ErrorIs(t, err, storage.ErrNotImplemented, "unavailable visibility must not become successful indexing")
}

type r5MissingPredicate struct{ storage.Engine }

func TestReadCapabilityR5SearchMissingPredicate(t *testing.T) {
	for _, constrained := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary_unconstrained", true: "refuse_constrained"}[constrained], func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			if constrained {
				require.NoError(t, base.GetSchema().AddConstraint(storage.Constraint{Name: "window", Label: "Fact", Type: storage.ConstraintTemporal, Properties: []string{"key", "from", "to"}}))
			}
			service := NewService(r5MissingPredicate{base})
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			err := service.IndexNode(&storage.Node{ID: "tenant:n", Labels: []string{"Fact"}, Properties: map[string]any{"key": "k", "from": time.Now().Add(-2 * time.Hour), "to": time.Now().Add(-time.Hour)}})
			if constrained {
				require.ErrorIs(t, err, storage.ErrNotImplemented)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
