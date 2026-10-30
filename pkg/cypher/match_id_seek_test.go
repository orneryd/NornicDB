package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchWhereIDEquality_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for id seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "target-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE id(n) = "target-id"
		RETURN n.text AS text
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "prompt text", res.Rows[0][0])
}

func TestMatchWhereIDEquality_AcceptsCanonicalElementIDString(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "raw-id-1",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE id(n) = "4:nornicdb:raw-id-1"
		RETURN n.text AS text
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "prompt text", res.Rows[0][0])
}

func TestMatchWhereElementIDEquality_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for elementId seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "target-elem-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE elementId(n) = "4:test:target-elem-id"
		RETURN n.text AS text
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "prompt text", res.Rows[0][0])
}

func TestMatchWhereIDInParam_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for id IN seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "id-in-1",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text 1"},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{
		ID:         "id-in-2",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text 2"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n:SystemPrompt)
		WHERE id(n) IN $ids
		RETURN n.text AS text
		ORDER BY text
	`, map[string]interface{}{
		"ids": []interface{}{"id-in-2", "id-in-1"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "prompt text 1", res.Rows[0][0])
	require.Equal(t, "prompt text 2", res.Rows[1][0])
}

func TestMatchWhereElementIDInParam_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for elementId IN seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "el-in-1",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text 1"},
	})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{
		ID:         "el-in-2",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text 2"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n:SystemPrompt)
		WHERE elementId(n) IN $ids
		RETURN n.text AS text
		ORDER BY text
	`, map[string]interface{}{
		"ids": []interface{}{"4:test:el-in-2", "4:test:el-in-1"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "prompt text 1", res.Rows[0][0])
	require.Equal(t, "prompt text 2", res.Rows[1][0])
}

func TestMatchWhereElementIDEqualityParam_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for elementId = $param seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "param-elem-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "param prompt text"},
	})
	require.NoError(t, err)

	// elementId(n) = $id with canonical element ID parameter
	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE elementId(n) = $id
		RETURN n.text AS text
		LIMIT 1
	`, map[string]interface{}{
		"id": "4:test:param-elem-id",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "param prompt text", res.Rows[0][0])
}

func TestMatchWhereIDEqualityParam_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for id = $param seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "param-raw-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "raw param text"},
	})
	require.NoError(t, err)

	// id(n) = $id with raw ID parameter
	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE id(n) = $id
		RETURN n.text AS text
		LIMIT 1
	`, map[string]interface{}{
		"id": "param-raw-id",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "raw param text", res.Rows[0][0])
}

func TestMatchWhereElementIDEqualityParam_WithLabel_UsesDirectLookup(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for labeled elementId = $param seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "labeled-param-id",
		Labels:     []string{"Task"},
		Properties: map[string]interface{}{"title": "my task"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (t:Task)
		WHERE elementId(t) = $id
		RETURN t.title AS title
	`, map[string]interface{}{
		"id": "4:test:labeled-param-id",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"title"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "my task", res.Rows[0][0])
}
