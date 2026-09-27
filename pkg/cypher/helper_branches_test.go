package cypher

import (
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallSharedUtils_Branches(t *testing.T) {
	t.Run("splitTopLevelComma handles nested and escaped quotes", func(t *testing.T) {
		parts := splitTopLevelComma(`a, 'b\,c', "d,e", {x: [1,2,3]}, (x, y), z`)
		require.Len(t, parts, 6)
		assert.Equal(t, "a", parts[0])
		assert.Equal(t, "'b\\,c'", parts[1])
		assert.Equal(t, "\"d,e\"", parts[2])
		assert.Equal(t, "{x: [1,2,3]}", parts[3])
		assert.Equal(t, "(x, y)", parts[4])
		assert.Equal(t, "z", parts[5])
	})

	t.Run("splitTopLevelComma keeps multi-byte text and leaves empty middle parts", func(t *testing.T) {
		assert.Equal(t, []string{"'é,ü'", "ß", "", "[日,本]"}, splitTopLevelComma(`'é,ü', ß, , [日,本], `))
	})

	t.Run("toBool covers bool, good string, bad string and default", func(t *testing.T) {
		v, ok := toBool(true)
		assert.True(t, ok)
		assert.True(t, v)

		v, ok = toBool("  FALSE  ")
		assert.True(t, ok)
		assert.False(t, v)

		_, ok = toBool("definitely-not-bool")
		assert.False(t, ok)

		_, ok = toBool(123)
		assert.False(t, ok)
	})

	t.Run("toInt and ragToFloat64 cover parse failures", func(t *testing.T) {
		i, ok := toInt(int64(8))
		assert.True(t, ok)
		assert.Equal(t, 8, i)

		_, ok = toInt("not-int")
		assert.False(t, ok)

		f, ok := ragToFloat64(float32(2.25))
		assert.True(t, ok)
		assert.InDelta(t, 2.25, f, 1e-6)

		_, ok = ragToFloat64(struct{}{})
		assert.False(t, ok)
	})

	t.Run("toStringSlice covers []string and []interface{}", func(t *testing.T) {
		assert.Equal(t, []string{"x", "y"}, toStringSlice([]string{"x", "y"}))
		assert.Equal(t, []string{"x", "y"}, toStringSlice([]interface{}{"x", " ", 1, "y"}))
		assert.Nil(t, toStringSlice(map[string]any{"x": 1}))
	})
}

func TestFunctionMatch_HelperBranches(t *testing.T) {
	re1 := getFuncMatcher("count")
	re2 := getFuncMatcher("count")
	assert.Equal(t, re1.String(), re2.String())

	assert.True(t, isFunctionCallWS(`count('x(y)')`, "count"))
	assert.False(t, isFunctionCallWS(`count('x(y)'`, "count"))
	assert.False(t, isFunctionCallWS(`prefix_count(n)`, "count"))

	args, idx := extractFuncArgsLen("sum (n.value)", "sum")
	assert.Equal(t, "n.value", args)
	assert.Greater(t, idx, 0)

	args, idx = extractFuncArgsLen("sum (n.value", "sum")
	assert.Equal(t, "", args)
	assert.Equal(t, -1, idx)

	a, suffix, ok := extractFuncArgsWithSuffix(`collect({a: "(x)"})[..10]`, "collect")
	assert.True(t, ok)
	assert.Equal(t, `{a: "(x)"}`, a)
	assert.Equal(t, "[..10]", suffix)

	_, _, ok = extractFuncArgsWithSuffix(`collect({a: "(x)"}`, "collect")
	assert.False(t, ok)
}

func TestVectorRegistry_HelperBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "tenant_helper_cov")
	exec := NewStorageExecutor(store)

	// No registry: no-op
	exec.registerVectorSpace("idx_no_registry", "Doc", "embedding", 4, "cosine")

	reg := vectorspace.NewIndexRegistry()
	exec.SetVectorRegistry(reg)

	// Invalid similarity: ignored
	exec.registerVectorSpace("idx_bad_similarity", "Doc", "embedding", 4, "minkowski")
	_, ok := exec.vectorIndexSpaces["idx_bad_similarity"]
	assert.False(t, ok)

	// Dims <= 0: ignored
	exec.registerVectorSpace("idx_zero_dims", "Doc", "embedding", 0, "cosine")
	_, ok = exec.vectorIndexSpaces["idx_zero_dims"]
	assert.False(t, ok)

	// Empty property uses default vector name
	exec.registerVectorSpace("idx_default_name", "Doc", "   ", 4, "dot")
	key, ok := exec.vectorIndexSpaces["idx_default_name"]
	require.True(t, ok)
	assert.Equal(t, vectorspace.DefaultVectorName, key.VectorName)

	// Unregister known + unknown index branches
	exec.unregisterVectorSpace("idx_default_name")
	_, ok = exec.vectorIndexSpaces["idx_default_name"]
	assert.False(t, ok)
	exec.unregisterVectorSpace("idx_missing")

	// databaseName() fallback branch via env when not namespaced.
	prev := os.Getenv("NORNICDB_DEFAULT_DATABASE")
	t.Cleanup(func() { _ = os.Setenv("NORNICDB_DEFAULT_DATABASE", prev) })
	require.NoError(t, os.Setenv("NORNICDB_DEFAULT_DATABASE", "env_db_cov"))
	execNoNS := NewStorageExecutor(base)
	assert.Equal(t, "env_db_cov", execNoNS.databaseName())
}
