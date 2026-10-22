package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A valid Engine with no optional methods. It is an unknown read-view
// boundary, so the declared discovery contract must not bypass it.
type engineCapabilityCoreOnly struct{ storage.Engine }

func TestEngineDecoratorsAbsentProjectedRead(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	_, err := tenant.CreateNode(&storage.Node{ID: "n", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	other := storage.NewNamespacedEngine(base, "other")
	_, err = other.CreateNode(&storage.Node{ID: "n", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(99)}})
	require.NoError(t, err)
	core := &engineCapabilityCoreOnly{Engine: base}
	wt := func(s storage.Engine) storage.Engine { return storage.NewWALEngine(storage.NewTracedEngine(s), nil) }
	tw := func(s storage.Engine) storage.Engine { return storage.NewTracedEngine(storage.NewWALEngine(s, nil)) }
	for name, engine := range map[string]storage.Engine{
		"direct_namespace_control":       tenant,
		"core_outside_namespace_control": &engineCapabilityCoreOnly{Engine: tenant},
		"core_inside_namespace":          storage.NewNamespacedEngine(core, "tenant"),
		"WT_namespace_core":              wt(storage.NewNamespacedEngine(core, "tenant")),
		"TW_namespace_core":              tw(storage.NewNamespacedEngine(core, "tenant")),
		"namespace_WT_core":              storage.NewNamespacedEngine(wt(core), "tenant"),
		"namespace_TW_core":              storage.NewNamespacedEngine(tw(core), "tenant"),
	} {
		t.Run(name, func(t *testing.T) {
			// The scoped core label read already produces the correct answer.
			nodes, readErr := engine.GetNodesByLabel("R2Absent")
			require.NoError(t, readErr)
			require.Len(t, nodes, 1)
			require.Equal(t, int64(1), nodes[0].Properties["v"])
			result, queryErr := NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n:R2Absent) RETURN n.v", nil)
			if result != nil {
				t.Logf("rows=%v", result.Rows)
			}
			require.NoError(t, queryErr, "a missing optional projection must retain the core read fallback")
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}
