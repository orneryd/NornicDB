package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiledBindingWhere_SupportedAndFallback(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"status": "active", "name": "alice", "age": int64(30)}},
		"b": &storage.Node{ID: "n2", Properties: map[string]interface{}{"status": "pending", "name": "bob", "age": int64(40)}},
	}
	params := map[string]interface{}{"statuses": []interface{}{"active", "pending"}, "prefix": "al"}

	ctx := context.Background()
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.status IN $statuses", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "b.age > a.age", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name STARTS WITH $prefix", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name IS NOT NULL", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.missing IS NULL", nil))

	compiled := exec.getCompiledBindingWhere(ctx, "a.status IN $statuses AND b.age > a.age AND a.name STARTS WITH $prefix")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, params))

	compiled = exec.getCompiledBindingWhere(ctx, "a.name IS NOT NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))

	compiled = exec.getCompiledBindingWhere(ctx, "a.missing IS NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))

	compiled = exec.getCompiledBindingWhere(ctx, "a = b")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	assert.True(t, compiled(binding{"a": bindingRow["a"], "b": bindingRow["a"]}, nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "size(a.name) > 0", nil))
}

func TestCompiledBindingWhere_UnsupportedFallsBackCompliantly(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}},
	}
	ctx := context.Background()
	compiled := exec.getCompiledBindingWhere(ctx, "a.name")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.missing", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name", nil))
}

func TestCompiledBindingWhere_NodeOrderingUsesNumericComparisonForNumericIDs(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	assert.True(t, exec.compareNodeIDs("2", "10", "<"))
	assert.False(t, exec.compareNodeIDs("2", "10", ">"))
	assert.True(t, exec.compareNodeIDs("2", "10", "<="))
	assert.False(t, exec.compareNodeIDs("2", "10", ">="))
	assert.True(t, exec.compareNodeIDs("alpha", "beta", "<"))
	assert.False(t, exec.compareNodeIDs("alpha", "beta", ">"))
}
