package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedExecute_MemoryNode(t *testing.T) {
	engine := storage.NewMemoryEngine()
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
	engine := storage.NewMemoryEngine()
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
	engine := storage.NewMemoryEngine()
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
