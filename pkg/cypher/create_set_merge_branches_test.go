package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSetMerge_Branches covers CREATE ... SET x += ..., which applies
// through the shared SET applicator (applySetToNodeWithContext /
// applySetToRelationshipWithContext) like every other SET route.
func TestCreateSetMerge_Branches(t *testing.T) {
	ctx := context.Background()
	run := func(t *testing.T, query string, params map[string]interface{}) (*ExecuteResult, error) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		return exec.Execute(ctx, query, params)
	}

	for name, tc := range map[string]struct {
		query  string
		params map[string]interface{}
	}{
		"parameter name missing after $": {query: "CREATE (n:N) SET n += $ RETURN n"},
		"parameter not supplied":         {query: "CREATE (n:N) SET n += $props RETURN n"},
		"parameter not found":            {query: "CREATE (n:N) SET n += $props RETURN n", params: map[string]interface{}{"other": map[string]interface{}{"x": 1}}},
		"empty right-hand side":          {query: "CREATE (n:N) SET n += RETURN n"},
		"inline map trailing comma":      {query: "CREATE (n:N) SET n += {a: 1,} RETURN n"},
		"unknown variable":               {query: "CREATE (n:N)-[r:REL]->(m:N) SET x += {a: 1} RETURN n"},
		"map variable missing in scope":  {query: "CREATE (n:N) SET n += row RETURN n"},
		"scalar parameter is not a map":  {query: "CREATE (n:N) SET n += $props RETURN n", params: map[string]interface{}{"props": int64(1)}},
		"map value that is itself a map": {query: "CREATE (n:N) SET n += {a: {b: 1}} RETURN n"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := run(t, tc.query, tc.params)
			require.Error(t, err, tc.query)
		})
	}

	t.Run("node update from inline map", func(t *testing.T) {
		res, err := run(t, "CREATE (n:N) SET n += {age: 30, city: 'NY'} RETURN n.age AS age, n.city AS city", nil)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int64(30), "NY"}, res.Rows[0])
		assert.Equal(t, 2, res.Stats.PropertiesSet)
	})

	t.Run("edge update from params map", func(t *testing.T) {
		res, err := run(t, "CREATE (:N)-[r:REL]->(:N) SET r += $props RETURN r.weight AS w", map[string]interface{}{"props": map[string]interface{}{"weight": int64(7)}})
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int64(7)}, res.Rows[0])
		assert.Equal(t, 1, res.Stats.PropertiesSet)
	})

	t.Run("node update from dotted parameter map", func(t *testing.T) {
		res, err := run(t, "CREATE (n:N) SET n += $row.properties RETURN n.country AS c", map[string]interface{}{
			"row": map[string]interface{}{"properties": map[string]interface{}{"country": "US"}},
		})
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"US"}, res.Rows[0])
		assert.Equal(t, 1, res.Stats.PropertiesSet)
	})
}

func TestExecuteCreateSetMergeFromParamVariable(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:ParamSetVar) SET n += $row RETURN n", map[string]interface{}{
		"row": map[string]interface{}{
			"id":   "p-1",
			"name": "param-var",
		},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:ParamSetVar {id:'p-1'}) RETURN n.name", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "param-var", res.Rows[0][0])
}

func TestParseSetMergeMapLiteralStrict_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	t.Run("missing braces", func(t *testing.T) {
		ctx := context.Background()
		_, err := exec.parseSetMergeMapLiteralStrict(ctx, "a:1")
		require.Error(t, err)
	})

	t.Run("empty map", func(t *testing.T) {
		ctx := context.Background()
		props, err := exec.parseSetMergeMapLiteralStrict(ctx, "{}")
		require.NoError(t, err)
		require.Empty(t, props)
	})

	t.Run("trailing comma entry", func(t *testing.T) {
		ctx := context.Background()
		_, err := exec.parseSetMergeMapLiteralStrict(ctx, "{a: 1,}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty map entry")
	})

	t.Run("missing colon", func(t *testing.T) {
		ctx := context.Background()
		_, err := exec.parseSetMergeMapLiteralStrict(ctx, "{a}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("empty key", func(t *testing.T) {
		ctx := context.Background()
		_, err := exec.parseSetMergeMapLiteralStrict(ctx, "{: 1}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("empty value", func(t *testing.T) {
		ctx := context.Background()
		_, err := exec.parseSetMergeMapLiteralStrict(ctx, "{a: }")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("quoted key and nested value", func(t *testing.T) {
		ctx := context.Background()
		props, err := exec.parseSetMergeMapLiteralStrict(ctx, "{'a': 1, 'key:key': 'value', b: {x: 2}, c: [1,2]}")
		require.NoError(t, err)
		assert.EqualValues(t, int64(1), props["a"])
		assert.Equal(t, "value", props["key:key"])
		assert.NotNil(t, props["b"])
		assert.NotNil(t, props["c"])
	})
}
