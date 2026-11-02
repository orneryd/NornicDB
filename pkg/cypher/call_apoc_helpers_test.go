package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	require.Equal(t, len("(a /* ) */ + b)")-1, cyphertext.FindMatchingDelimiter("(a /* ) */ + b)", 0, '(', ')', cyphertext.OpeningDelimiter))
	require.Equal(t, len("(a // )\n+ b)")-1, cyphertext.FindMatchingDelimiter("(a // )\n+ b)", 0, '(', ')', cyphertext.OpeningDelimiter))
	require.Equal(t, len("(`a)b`)")-1, cyphertext.FindMatchingDelimiter("(`a)b`)", 0, '(', ')', cyphertext.OpeningDelimiter))
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
	name, err := extractProcedureName(t.Context(), "CALL db.labels()")
	require.NoError(t, err)
	assert.Equal(t, "db.labels", name)
	name, err = extractProcedureName(t.Context(), "call apoc.cypher.run('RETURN 1',{})")
	require.NoError(t, err)
	assert.Equal(t, "apoc.cypher.run", name)
	long := "THIS TEXT HAS NO PROC KEYWORD AND SHOULD FALL BACK TO TRUNCATED QUERY STRING BEHAVIOR"
	name, err = extractProcedureName(t.Context(), long)
	require.Error(t, err, "an unparseable fragment is the reader's error, not a name")
	assert.Empty(t, name, "a declined syntax read must not invent a procedure name")

	// A context that ended before the read is the read's error, not an empty
	// name: callers may not look up a procedure the fragment never named.
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	name, err = extractProcedureName(ended, "CALL db.labels()")
	assert.Empty(t, name)
	require.ErrorIs(t, err, context.Canceled)
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
