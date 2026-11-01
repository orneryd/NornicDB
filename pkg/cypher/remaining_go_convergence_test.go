package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStretchMapKeysUseCanonicalReader(t *testing.T) {
	var keys stretchTokens
	addStretchMapKeys("{`odd``key`: source.read, outer: {nested: 1}, plain: 'x.y'}", &keys)
	require.Equal(t, map[string]struct{}{"odd`key": {}, "outer": {}, "plain": {}}, keys.keys)
	require.False(t, keys.anyKey)
	effects, err := clauseEffectsForTest(t.Context(), "CREATE (n:CanonicalMapKeys {`odd``key`: source.read, outer: {nested: 1}})")
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"odd`key": {}, "outer": {}}, effects.writes.keys)
	require.Contains(t, effects.reads.keys, "read")
	require.NotContains(t, effects.writes.keys, "read")
	require.NotContains(t, effects.writes.keys, "nested")
}

func TestPatternBindingPlanRemainsSyntaxOnly(t *testing.T) {
	var calls int64
	withPluginFunctions(t, map[string]interface{}{"inspection.planvalue": func() int64 { calls++; return calls }})
	e := NewStorageExecutor(newTestMemoryEngine(t))
	pattern := "(n:CanonicalPlanOwnership {id:inspection.planvalue()})"
	plan := e.compilePatternRow(pattern, nil)
	require.NoError(t, plan.err)
	require.Zero(t, calls)
	ctx := withParams(context.Background(), map[string]interface{}{"__nornic_pattern_property_0": int64(99)})
	for i := int64(1); i <= 2; i++ {
		bound, text, err := e.bindPatternProperties(ctx, pattern, nil)
		require.NoError(t, err)
		require.Equal(t, i, calls)
		require.Equal(t, int64(99), getParamsFromContext(bound)["__nornic_pattern_property_0"])
		require.NotEqual(t, pattern, text)
		_, same, err := e.bindPatternProperties(bound, text, nil)
		require.NoError(t, err)
		require.Equal(t, text, same)
		require.Equal(t, i, calls)
	}
	cached, ok := patternRowPlans.get(pattern)
	require.True(t, ok)
	require.Equal(t, plan.pattern, cached.pattern)
	require.Equal(t, "__nornic_pattern_property_0", cached.maps[0].parameters[0])
}

// CST Code removes comment tokens while preserving comment-looking string values.
func TestStretchMapCodeCommentsPreserveQuotedValues(t *testing.T) {
	clause := "CREATE (n:MapCodeComments { /* key */ written: 'source.read /* quoted */', copied: source.read /* tail */})"
	effects, err := clauseEffectsForTest(t.Context(), clause)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"written": {}, "copied": {}}, effects.writes.keys)
	require.Contains(t, effects.reads.keys, "read")
	require.NotContains(t, effects.writes.keys, "read")
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_code_comments"))
	result, err := e.Execute(t.Context(), "WITH {read:7} AS source "+clause+" RETURN n.written,n.copied", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"source.read /* quoted */", int64(7)}}, result.Rows)
}
