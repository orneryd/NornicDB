package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueToLiteral_AllTypeBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// Nil
	assert.Equal(t, "null", exec.valueToLiteral(nil))

	// String escaping
	assert.Equal(t, `'a''b\\c'`, exec.valueToLiteral("a'b\\c"))

	// Signed ints
	assert.Equal(t, "1", exec.valueToLiteral(int(1)))
	assert.Equal(t, "2", exec.valueToLiteral(int8(2)))
	assert.Equal(t, "3", exec.valueToLiteral(int16(3)))
	assert.Equal(t, "4", exec.valueToLiteral(int32(4)))
	assert.Equal(t, "5", exec.valueToLiteral(int64(5)))

	// Unsigned ints
	assert.Equal(t, "6", exec.valueToLiteral(uint(6)))
	assert.Equal(t, "7", exec.valueToLiteral(uint8(7)))
	assert.Equal(t, "8", exec.valueToLiteral(uint16(8)))
	assert.Equal(t, "9", exec.valueToLiteral(uint32(9)))
	assert.Equal(t, "10", exec.valueToLiteral(uint64(10)))

	// Floats
	assert.Equal(t, "1.25", exec.valueToLiteral(float32(1.25)))
	assert.Equal(t, "2.5", exec.valueToLiteral(float64(2.5)))

	// Bool
	assert.Equal(t, "true", exec.valueToLiteral(true))
	assert.Equal(t, "false", exec.valueToLiteral(false))

	// Slices
	assert.Equal(t, "['x', 'y']", exec.valueToLiteral([]string{"x", "y"}))
	assert.Equal(t, "[1, 2]", exec.valueToLiteral([]int{1, 2}))
	assert.Equal(t, "[3, 4]", exec.valueToLiteral([]int64{3, 4}))
	assert.Equal(t, "[1.5, 2.5]", exec.valueToLiteral([]float64{1.5, 2.5}))
	assert.Equal(t, "[1.5, 2.5]", exec.valueToLiteral([]float32{1.5, 2.5}))
	assert.Equal(t, "[1, 'x', true]", exec.valueToLiteral([]interface{}{1, "x", true}))

	// Map[string]interface{} order is undefined; assert content.
	mapLit := exec.valueToLiteral(map[string]interface{}{"a": 1, "b": "x"})
	assert.True(t, strings.HasPrefix(mapLit, "{"))
	assert.True(t, strings.HasSuffix(mapLit, "}"))
	assert.Contains(t, mapLit, "a: 1")
	assert.Contains(t, mapLit, "b: 'x'")

	// Map[interface{}]interface{} includes string keys only.
	mixedMap := exec.valueToLiteral(map[interface{}]interface{}{"a": 1, 2: "ignored"})
	assert.Contains(t, mixedMap, "a: 1")
	assert.NotContains(t, mixedMap, "ignored")

	// Slices of maps
	listOfStringMaps := exec.valueToLiteral([]map[string]interface{}{{"a": 1}, {"b": "x"}})
	assert.True(t, strings.HasPrefix(listOfStringMaps, "["))
	assert.Contains(t, listOfStringMaps, "{a: 1}")
	assert.Contains(t, listOfStringMaps, "{b: 'x'}")

	listOfIfaceMaps := exec.valueToLiteral([]map[interface{}]interface{}{{"a": 1}, {"b": "x"}})
	assert.True(t, strings.HasPrefix(listOfIfaceMaps, "["))
	assert.Contains(t, listOfIfaceMaps, "{a: 1}")
	assert.Contains(t, listOfIfaceMaps, "{b: 'x'}")
}

type customLiteral struct{ s string }

func (c customLiteral) String() string { return c.s }

func TestValueToLiteral_DefaultFallbackBranch(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	got := exec.valueToLiteral(customLiteral{s: "fallback-value"})
	require.Equal(t, "'fallback-value'", got)
	got = exec.valueToLiteral(customLiteral{s: "O'Reilly"})
	require.Equal(t, "'O''Reilly'", got)
}
