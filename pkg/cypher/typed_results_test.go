package cypher

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedExecute_MemoryNode(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a memory node
	_, err := executor.Execute(ctx, `
		CREATE (n:Memory {
			id: 'mem-1',
			title: 'Test Memory',
			content: 'This is test content',
			type: 'memory',
			weight: 1.0,
			decay: 0.95
		}) RETURN n
	`, nil)
	require.NoError(t, err)

	// Query with typed result
	result, err := TypedExecute[MemoryNode](ctx, executor, `
		MATCH (n:Memory {id: 'mem-1'})
		RETURN n.id, n.title, n.content, n.type, n.weight, n.decay
	`, nil)
	require.NoError(t, err)

	node, ok := result.First()
	require.True(t, ok, "should have one result")

	assert.Equal(t, "mem-1", node.ID)
	assert.Equal(t, "Test Memory", node.Title)
	assert.Equal(t, "This is test content", node.Content)
	assert.Equal(t, "memory", node.Type)
	assert.Equal(t, 1.0, node.Weight)
	assert.Equal(t, 0.95, node.Decay)
}

func TestTypedExecute_NodeCount(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create some nodes
	_, err := executor.Execute(ctx, `CREATE (n:TestLabel {name: 'a'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (n:TestLabel {name: 'b'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (n:OtherLabel {name: 'c'})`, nil)
	require.NoError(t, err)

	// Count by label
	result, err := TypedExecute[NodeCount](ctx, executor, `
		MATCH (n:TestLabel) RETURN 'TestLabel' as label, count(n) as count
	`, nil)
	require.NoError(t, err)

	count, ok := result.First()
	require.True(t, ok)
	assert.Equal(t, "TestLabel", count.Label)
	assert.Equal(t, int64(2), count.Count)
}

func TestTypedExecute_WithParameters(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create node
	_, err := executor.Execute(ctx, `
		CREATE (n:Memory {id: $id, title: $title, content: $content})
	`, map[string]interface{}{
		"id":      "param-test",
		"title":   "Param Title",
		"content": "Content with -> arrows and MATCH keywords",
	})
	require.NoError(t, err)

	// Query back
	result, err := TypedExecute[MemoryNode](ctx, executor, `
		MATCH (n:Memory {id: $id}) RETURN n.id, n.title, n.content
	`, map[string]interface{}{"id": "param-test"})
	require.NoError(t, err)

	node, ok := result.First()
	require.True(t, ok)
	assert.Equal(t, "param-test", node.ID)
	assert.Equal(t, "Param Title", node.Title)
	assert.Equal(t, "Content with -> arrows and MATCH keywords", node.Content)
}

func TestTypedExecuteResult_Helpers(t *testing.T) {
	result := &TypedExecuteResult[MemoryNode]{
		Columns: []string{"id", "title"},
		Rows: []MemoryNode{
			{ID: "1", Title: "First"},
			{ID: "2", Title: "Second"},
		},
	}

	assert.False(t, result.IsEmpty())
	assert.Equal(t, 2, result.Count())

	first, ok := result.First()
	assert.True(t, ok)
	assert.Equal(t, "1", first.ID)

	// Empty result
	emptyResult := &TypedExecuteResult[MemoryNode]{Rows: []MemoryNode{}}
	assert.True(t, emptyResult.IsEmpty())
	_, ok = emptyResult.First()
	assert.False(t, ok)
}

func TestTypedDecodeRow_ValidationAndScalarPaths(t *testing.T) {
	var scalar int
	require.NoError(t, decodeRow([]string{"count"}, []interface{}{float64(7)}, &scalar))
	assert.Equal(t, 7, scalar)

	var badDest int
	assert.Error(t, decodeRow([]string{"count"}, []interface{}{1}, badDest))

	var unsupported map[string]interface{}
	assert.Error(t, decodeRow([]string{"x", "y"}, []interface{}{1, 2}, &unsupported))
}

func TestTypedDecodeRow_MapAndConversions(t *testing.T) {
	type row struct {
		Name    string    `json:"name"`
		Active  bool      `json:"active"`
		Count   int64     `json:"count"`
		Tags    []string  `json:"tags"`
		Created time.Time `json:"created"`
	}

	var out row
	err := decodeRow(
		[]string{"payload"},
		[]interface{}{map[string]interface{}{
			"name":    "alice",
			"active":  int64(1),
			"count":   float64(3),
			"tags":    []interface{}{"x", "y"},
			"created": "2024-01-02 03:04:05",
		}},
		&out,
	)
	require.NoError(t, err)
	assert.Equal(t, "alice", out.Name)
	assert.True(t, out.Active)
	assert.Equal(t, int64(3), out.Count)
	assert.Equal(t, []string{"x", "y"}, out.Tags)
	assert.Equal(t, 2024, out.Created.Year())
}

func TestAssignValue_TimeAndErrorBranches(t *testing.T) {
	var ts time.Time
	v := reflect.ValueOf(&ts).Elem()
	require.NoError(t, assignValue(v, int64(1700000000)))
	assert.False(t, ts.IsZero())

	require.NoError(t, assignValue(v, "2024-01-02T03:04:05Z"))
	assert.Equal(t, 2024, ts.Year())

	assert.Error(t, assignValue(v, "not-a-time"))

	var b bool
	bv := reflect.ValueOf(&b).Elem()
	require.NoError(t, assignValue(bv, int(0)))
	assert.False(t, b)

	var f float64
	fv := reflect.ValueOf(&f).Elem()
	require.NoError(t, assignValue(fv, int64(9)))
	assert.Equal(t, 9.0, f)
}

func TestTypedExecute_ErrorPaths(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Raw execute error path.
	_, err := TypedExecute[MemoryNode](ctx, executor, "THIS IS NOT CYPHER", nil)
	require.Error(t, err)

	// Decode error path from incompatible destination type.
	type badRow struct {
		Age int `cypher:"age"`
	}
	_, err = executor.Execute(ctx, `CREATE (:Person {age: 'not-int'})`, nil)
	require.NoError(t, err)
	_, err = TypedExecute[badRow](ctx, executor, `MATCH (n:Person) RETURN n.age AS age`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode row")
}

func TestTypedDecodeAndAssign_AdditionalBranches(t *testing.T) {
	type tagged struct {
		Skip  string `json:"-"`
		Name  string `json:"name,omitempty"`
		Count int32  `cypher:"count"`
		Raw   string
	}

	var out tagged
	err := decodeStruct(
		[]string{"name", "count", "raw", "missing"},
		[]interface{}{"alice", int64(3), "x", "y"},
		reflect.ValueOf(&out).Elem(),
	)
	require.NoError(t, err)
	assert.Equal(t, "alice", out.Name)
	assert.Equal(t, int32(3), out.Count)
	assert.Equal(t, "x", out.Raw)

	// decodeMap fallback to field-name lookup branch.
	type fieldNameStruct struct {
		FieldOnly string
	}
	var byField fieldNameStruct
	err = decodeMap(map[string]interface{}{"FieldOnly": "v"}, reflect.ValueOf(&byField).Elem())
	require.NoError(t, err)
	assert.Equal(t, "v", byField.FieldOnly)

	// assignValue direct assignable path.
	var i64 int64
	err = assignValue(reflect.ValueOf(&i64).Elem(), int64(11))
	require.NoError(t, err)
	assert.Equal(t, int64(11), i64)

	// assignValue convertible path.
	type myInt int64
	var mi myInt
	err = assignValue(reflect.ValueOf(&mi).Elem(), int64(9))
	require.NoError(t, err)
	assert.Equal(t, myInt(9), mi)

	// assignValue unsupported bool conversion path.
	var b bool
	err = assignValue(reflect.ValueOf(&b).Elem(), "true")
	require.Error(t, err)

	// assignValue slice recursion error path.
	var ints []int
	err = assignValue(reflect.ValueOf(&ints).Elem(), []interface{}{"bad"})
	require.Error(t, err)
}
