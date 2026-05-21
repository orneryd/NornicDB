package cypher

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParserParseAdditionalBranches(t *testing.T) {
	parser := NewParser()

	t.Run("skips unknown tokens before parsing recognized clauses", func(t *testing.T) {
		query, err := parser.Parse("USE foo MATCH (n) RETURN n")
		require.NoError(t, err)
		require.Len(t, query.Clauses, 2)
		_, ok := query.Clauses[0].(*MatchClause)
		require.True(t, ok)
		_, ok = query.Clauses[1].(*ReturnClause)
		require.True(t, ok)
		assert.Equal(t, QueryMatch, query.Type)
	})

	t.Run("marks optional match clauses", func(t *testing.T) {
		query, err := parser.Parse("OPTIONAL MATCH (n) RETURN n")
		require.NoError(t, err)
		require.Len(t, query.Clauses, 2)
		matchClause, ok := query.Clauses[0].(*MatchClause)
		require.True(t, ok)
		assert.True(t, matchClause.Optional)
	})

	t.Run("walks all supported clause types in sequence", func(t *testing.T) {
		query, err := parser.Parse("MATCH (n) WHERE n.name = 'a' CREATE (m) SET m.name = 'b' DELETE m RETURN m")
		require.NoError(t, err)
		require.Len(t, query.Clauses, 6)
		assert.IsType(t, &MatchClause{}, query.Clauses[0])
		assert.IsType(t, &WhereClause{}, query.Clauses[1])
		assert.IsType(t, &CreateClause{}, query.Clauses[2])
		assert.IsType(t, &SetClause{}, query.Clauses[3])
		assert.IsType(t, &DeleteClause{}, query.Clauses[4])
		assert.IsType(t, &ReturnClause{}, query.Clauses[5])
		assert.Equal(t, QueryDelete, query.Type)
	})
}

func TestParserDirectClauseHelpers(t *testing.T) {
	parser := NewParser()

	matchClause, pos := parser.parseMatch([]string{"MATCH", "(", "n", ")"}, 0)
	require.NotNil(t, matchClause)
	assert.Equal(t, 1, pos)

	createClause, pos := parser.parseCreate([]string{"CREATE", "(", "n", ")"}, 0)
	require.NotNil(t, createClause)
	assert.Equal(t, 1, pos)

	returnClause, pos := parser.parseReturn([]string{"RETURN", "n"}, 0)
	require.NotNil(t, returnClause)
	assert.NotNil(t, returnClause.Items)
	assert.Equal(t, 1, pos)

	whereClause, pos := parser.parseWhere([]string{"WHERE", "n", ".", "name"}, 0)
	require.NotNil(t, whereClause)
	assert.Equal(t, 1, pos)

	setClause, pos := parser.parseSet([]string{"SET", "n", ".", "name", "=", "'a'"}, 0)
	require.NotNil(t, setClause)
	assert.NotNil(t, setClause.Items)
	assert.Equal(t, 1, pos)

	deleteClause, pos := parser.parseDelete([]string{"DELETE", "n"}, 0)
	require.NotNil(t, deleteClause)
	assert.False(t, deleteClause.Detach)
	assert.Equal(t, 1, pos)

	deleteClause, pos = parser.parseDelete([]string{"DETACH", "DELETE", "n"}, 0)
	require.NotNil(t, deleteClause)
	assert.True(t, deleteClause.Detach)
	assert.Equal(t, 2, pos)
}

func TestParserMarkerMethodsDirect(t *testing.T) {
	(&MatchClause{}).clauseMarker()
	(&CreateClause{}).clauseMarker()
	(&ReturnClause{}).clauseMarker()
	(&WhereClause{}).clauseMarker()
	(&SetClause{}).clauseMarker()
	(&DeleteClause{}).clauseMarker()

	(&PropertyAccess{}).exprMarker()
	(&Comparison{}).exprMarker()
	(&Literal{}).exprMarker()
	(&Parameter{}).exprMarker()
	(&FunctionCall{}).exprMarker()
}

func TestTokenizeAdditionalBranches(t *testing.T) {
	t.Run("handles double quoted strings and operators", func(t *testing.T) {
		tokens, err := tokenize(`RETURN "Alice Smith" + n.score / 2`)
		require.NoError(t, err)
		assert.Equal(t,
			[]string{"RETURN", "\"Alice Smith\"", "+", "n", ".", "score", "/", "2"},
			tokens,
		)
	})
}

func TestParserRejectsUnterminatedStringLiteral(t *testing.T) {
	parser := NewParser()

	_, err := parser.Parse("CREATE (n {name: 'Alice})")
	require.Error(t, err)
	require.ErrorContains(t, err, "unterminated string")

	executor := NewExecutor()
	_, err = executor.Execute(context.Background(), `RETURN "Alice`, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "unterminated string")
}

func TestParserRejectsBareOptionalClause(t *testing.T) {
	parser := NewParser()

	done := make(chan error, 1)
	go func() {
		_, err := parser.Parse("OPTIONAL CREATE (n)")
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorContains(t, err, "OPTIONAL must be followed by MATCH")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Parse should not hang on invalid OPTIONAL clause")
	}
}

func TestExecutorExecuteAdditionalBranches(t *testing.T) {
	executor := NewExecutor()

	result, err := executor.Execute(context.Background(), "RETURN 1", map[string]any{"x": 1})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)
	assert.Equal(t, 0, result.RowCount())
}

func TestPatternParserHelpers(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	t.Run("containsReservedKeyword only flags dangerous forms", func(t *testing.T) {
		assert.False(t, containsReservedKeyword("Order"))
		assert.True(t, containsReservedKeyword("bad name"))
		assert.True(t, containsReservedKeyword("semi;colon"))
		assert.True(t, containsReservedKeyword("quoted'value"))
		assert.True(t, containsReservedKeyword("(grouped)"))
	})

	t.Run("normalizePropertyKey handles plain quoted and escaped keys", func(t *testing.T) {
		assert.Equal(t, "name", normalizePropertyKey(" name "))
		assert.Equal(t, "odd key", normalizePropertyKey("'odd key'"))
		assert.Equal(t, "double key", normalizePropertyKey(`"double key"`))
		assert.Equal(t, "back`tick", normalizePropertyKey("`back``tick`"))
	})

	t.Run("looksLikeFunctionCall distinguishes valid identifiers", func(t *testing.T) {
		assert.False(t, looksLikeFunctionCall(""))
		assert.False(t, looksLikeFunctionCall("count"))
		assert.False(t, looksLikeFunctionCall("1abc()"))
		assert.False(t, looksLikeFunctionCall("(x)"))
		assert.False(t, looksLikeFunctionCall("x + y"))
		assert.False(t, looksLikeFunctionCall("bad-name()"))
		assert.True(t, looksLikeFunctionCall("toUpper('x')"))
		assert.True(t, looksLikeFunctionCall("apoc.coll.sum([1,2])"))
		assert.True(t, looksLikeFunctionCall("foo1.bar2()"))
		assert.True(t, looksLikeFunctionCall("_hidden()"))
	})

	t.Run("parseProperties handles nested values and malformed pairs", func(t *testing.T) {
		ctx := context.Background()

		props := exec.parseProperties(ctx, `{name: 'Alice', age: 30, active: true, tags: ['a', 'b'], meta: {score: 1}, "quoted key": "ok", invalid, title: toUpper('friend')}`)
		assert.Equal(t, "Alice", props["name"])
		assert.Equal(t, int64(30), props["age"])
		assert.Equal(t, true, props["active"])
		assert.Equal(t, []interface{}{"a", "b"}, props["tags"])
		assert.Equal(t, map[string]interface{}{"score": int64(1)}, props["meta"])
		assert.Equal(t, "ok", props["quoted key"])
		assert.Equal(t, "FRIEND", props["title"])
		_, exists := props["invalid"]
		assert.False(t, exists)
		assert.Empty(t, exec.parseProperties(ctx, " { } "))
	})

	t.Run("parsePropertyValue covers literals functions and invalid values", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, exec.parsePropertyValue(ctx, ""))
		assert.Nil(t, exec.parsePropertyValue(ctx, "null"))
		assert.Equal(t, "it's", exec.parsePropertyValue(ctx, "'it''s'"))
		assert.Equal(t, `a "quote"`, exec.parsePropertyValue(ctx, `"a \"quote\""`))
		assert.Equal(t, true, exec.parsePropertyValue(ctx, "true"))
		assert.Equal(t, int64(42), exec.parsePropertyValue(ctx, "42"))
		assert.Equal(t, 3.5, exec.parsePropertyValue(ctx, "3.5"))
		assert.Equal(t, []interface{}{int64(1), map[string]interface{}{"x": int64(2)}, []interface{}{"a", "b"}}, exec.parsePropertyValue(ctx, `[1, {x: 2}, ['a', 'b']]`))
		assert.Equal(t, map[string]interface{}{"nested": "ok"}, exec.parsePropertyValue(ctx, `{nested: 'ok'}`))
		assert.Equal(t, "HELLO", exec.parsePropertyValue(ctx, `toUpper('hello')`))

		invalid := exec.parsePropertyValue(ctx, "foo:bar")
		typed, ok := invalid.(invalidPropertyValue)
		require.True(t, ok)
		assert.Equal(t, "foo:bar", typed.raw)
	})

	t.Run("parseNodePattern extracts variable labels and properties", func(t *testing.T) {
		ctx := context.Background()
		info := exec.parseNodePattern(ctx, `(n:Person:Employee {name: 'Alice', score: 3})`)
		assert.Equal(t, "n", info.variable)
		assert.Equal(t, []string{"Person", "Employee"}, info.labels)
		assert.Equal(t, map[string]interface{}{"name": "Alice", "score": int64(3)}, info.properties)

		info = exec.parseNodePattern(ctx, `(:OnlyLabel)`)
		assert.Equal(t, "", info.variable)
		assert.Equal(t, []string{"OnlyLabel"}, info.labels)
		assert.Empty(t, info.properties)
	})

	t.Run("split helpers respect quotes and nesting", func(t *testing.T) {
		pairs := exec.splitPropertyPairs(`name: 'a,b', data: {nested: true}, arr: [1, 2], expr: toUpper('x')`)
		assert.Equal(t, []string{
			"name: 'a,b'",
			"data: {nested: true}",
			"arr: [1, 2]",
			"expr: toUpper('x')",
		}, pairs)

		elements := exec.splitArrayElements(`'a,b', {x: [1,2]}, ["y,z"], 9`)
		assert.True(t, reflect.DeepEqual([]string{"'a,b'", "{x: [1,2]}", `["y,z"]`, "9"}, elements), "unexpected elements: %#v", elements)

		elements = exec.splitArrayElements(`"a\\\"b", 'c'`)
		assert.True(t, reflect.DeepEqual([]string{`"a\\\"b"`, "'c'"}, elements), "unexpected escaped elements: %#v", elements)
	})
}
