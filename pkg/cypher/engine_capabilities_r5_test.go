package cypher_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type r5TemporalRefusal struct {
	storage.Engine
	calls       int
	unavailable bool
}

func (r *r5TemporalRefusal) ReadCapabilityAvailable(reflect.Type) bool { return !r.unavailable }
func (r *r5TemporalRefusal) IsCurrentTemporalNodeInNamespace(string, *storage.Node, time.Time) (bool, error) {
	r.calls++
	return false, storage.ErrNotImplemented
}

func TestEngineDecoratorsR5TemporalRefusal(t *testing.T) {
	testR5TemporalRefusal(t, false)
}

func TestEngineDecoratorsR5TemporalUnavailable(t *testing.T) {
	testR5TemporalRefusal(t, true)
}

func testR5TemporalRefusal(t *testing.T, unavailable bool) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	r := &r5TemporalRefusal{Engine: base, unavailable: unavailable}
	tenant := storage.NewNamespacedEngine(r, "tenant")
	at := func(year int) time.Time { return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC) }
	for _, v := range []struct {
		id   string
		from time.Time
		to   any
	}{{"old", at(2022), at(2023)}, {"new", at(2023), nil}} {
		_, err := tenant.CreateNode(&storage.Node{ID: storage.NodeID(v.id), Labels: []string{"Fact"}, Properties: map[string]any{"key": "k", "valid_from": v.from, "valid_to": v.to, "v": v.id}})
		require.NoError(t, err)
	}
	require.NoError(t, tenant.GetSchema().AddConstraint(storage.Constraint{Name: "fact_window", Type: storage.ConstraintTemporal, Label: "Fact", Properties: []string{"key", "valid_from", "valid_to"}}))
	exec := cypher.NewStorageExecutor(storage.NewTracedEngine(storage.NewWALEngine(tenant, nil)))
	ctx := cypher.WithTemporalViewport(context.Background(), cypher.AsOfTemporalViewport(time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)))
	result, err := exec.Execute(ctx, "MATCH (n:Fact) RETURN n.v", nil)
	if unavailable {
		require.Zero(t, r.calls, "unavailable predicates must not be invoked")
	} else {
		require.Positive(t, r.calls, "refusal must occur inside predicate execution")
	}
	if err != nil {
		require.ErrorIs(t, err, storage.ErrNotImplemented)
		return
	}
	require.Equal(t, [][]any{{"old"}}, result.Rows, "refusal cannot admit the successor")
}
