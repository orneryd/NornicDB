package cypher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApocHelpers_FindMatchingParen(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, -1, e.findMatchingParen("abc", 0))
	assert.Equal(t, -1, e.findMatchingParen("(abc", 1))
	assert.Equal(t, 5, e.findMatchingParen("(a(b))", 0))
	assert.Equal(t, 8, e.findMatchingParen("('x)';(a))", 6))
}

func TestMatchingDelimiterSkipsCommentsAndBackticks(t *testing.T) {
	require.Equal(t, len("(a /* ) */ + b)")-1, findMatchingDelimiter("(a /* ) */ + b)", 0, '(', ')'))
	require.Equal(t, len("(a // )\n+ b)")-1, findMatchingDelimiter("(a // )\n+ b)", 0, '(', ')'))
	require.Equal(t, len("(`a)b`)")-1, findMatchingDelimiter("(`a)b`)", 0, '(', ')'))
	require.Equal(t, len("(a /* ) */ + b)")-1, findMatchingParen("(a /* ) */ + b)", 0))
	require.Equal(t, len("(a /* ) */ + b)")-1, findMatchingParenAt("(a /* ) */ + b)", 0))
	require.Equal(t, -1, findMatchingParen("(a /* ) */ + b", 0))
}

func TestApocHelpers_FindMatchingBrace(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, -1, e.findMatchingBrace("abc", 0))
	assert.Equal(t, 6, e.findMatchingBrace("{a:{b}}", 0))
	assert.Equal(t, 12, e.findMatchingBrace("{a:'x}y',b:1}", 0))
}

func TestApocHelpers_SplitBySemicolon(t *testing.T) {
	e := &StorageExecutor{}
	parts := e.splitBySemicolon("RETURN 1; RETURN ';'; RETURN 3")
	assert.Equal(t, []string{"RETURN 1", " RETURN ';'", " RETURN 3"}, parts)
	assert.Equal(t, []string{"RETURN 1"}, e.splitBySemicolon("RETURN 1"))
}

func TestApocHelpers_ExtractProcedureName(t *testing.T) {
	assert.Equal(t, "db.labels", extractProcedureName("CALL db.labels()"))
	assert.Equal(t, "apoc.cypher.run", extractProcedureName("call apoc.cypher.run('RETURN 1',{})"))
	long := "THIS TEXT HAS NO PROC KEYWORD AND SHOULD FALL BACK TO TRUNCATED QUERY STRING BEHAVIOR"
	got := extractProcedureName(long)
	assert.Len(t, got, 63)
	assert.Equal(t, long[:60]+"...", got)
}

func TestApocHelpers_SharedCallUtilities(t *testing.T) {
	parts := splitTopLevelComma(`a, [1,2], {x: 'a,b'}, "c,d"`)
	assert.Equal(t, []string{"a", "[1,2]", "{x: 'a,b'}", `"c,d"`}, parts)
	assert.Nil(t, splitTopLevelComma("   "))

	m := map[string]interface{}{"a": 1, "b": "x"}
	assert.Equal(t, "x", firstPresent(m, "missing", "b"))
	assert.Nil(t, firstPresent(m, "missing"))
	assert.Equal(t, "fallback", stringOr(1, "fallback"))
	assert.Equal(t, "ok", stringOr("ok", "fallback"))

	b, ok := toBool("true")
	assert.True(t, ok)
	assert.True(t, b)
	_, ok = toBool(123)
	assert.False(t, ok)

	i, ok := toInt("42")
	assert.True(t, ok)
	assert.Equal(t, 42, i)
	i, ok = toInt(float64(7.9))
	assert.True(t, ok)
	assert.Equal(t, 7, i)

	f, ok := ragToFloat64("1.25")
	assert.True(t, ok)
	assert.Equal(t, 1.25, f)
	f32, ok := toFloat32(int64(5))
	assert.True(t, ok)
	assert.Equal(t, float32(5), f32)

	assert.Equal(t, []string{"a", "b"}, toStringSlice([]interface{}{"a", " ", "b", 9}))
	assert.Nil(t, toStringSlice(123))
}
