package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type allNodesForbiddenEngine struct {
	*storage.MemoryEngine
	forbidScan bool
}

func (e *allNodesForbiddenEngine) AllNodes() ([]*storage.Node, error) {
	if !e.forbidScan {
		return e.MemoryEngine.AllNodes()
	}
	return nil, fmt.Errorf("AllNodes should not be called for indexed equality lookup")
}

func (e *allNodesForbiddenEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if !e.forbidScan {
		return e.MemoryEngine.GetNodesByLabel(label)
	}
	return nil, fmt.Errorf("GetNodesByLabel should not be called for indexed fast path")
}

func TestMatchUsesPropertyIndexForUnlabeledEquality(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "k-1",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-2",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "k-2",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_text_key_128 FOR (n:MongoDocument) ON (n.textKey128)", nil)
	require.NoError(t, err)
	eng.forbidScan = true
	res, err := exec.Execute(context.Background(), "MATCH (n) WHERE n.textKey128 = 'k-2' RETURN n.textKey128 AS key", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"key"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "k-2", res.Rows[0][0])
}

func TestMatchUsesPropertyIndexForFabricRecordBindingEquality(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-a",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "h-a",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-b",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "h-b",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_text_key_128 FOR (n:MongoDocument) ON (n.textKey128)", nil)
	require.NoError(t, err)
	eng.forbidScan = true
	exec.fabricRecordBindings = map[string]interface{}{
		"textKey128": "h-a",
	}

	res, err := exec.Execute(context.Background(), "MATCH (n) WHERE n.textKey128 = textKey128 RETURN n.textKey128 AS key", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"key"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "h-a", res.Rows[0][0])
}

func TestMatchUsesPropertyIndexForInParamList(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-in-a",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-a",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-in-b",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-b",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-in-c",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-c",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_translation_id FOR (n:MongoDocument) ON (n.translationId)", nil)
	require.NoError(t, err)
	lookup := eng.GetSchema().PropertyIndexLookup("MongoDocument", "translationId", "src-a")
	require.NotEmpty(t, lookup)
	nodePattern := nodePatternInfo{variable: "n", labels: []string{"MongoDocument"}}
	nodes, used, idxErr := exec.tryCollectNodesFromPropertyIndexIn(nodePattern, "n.translationId IN $keys", map[string]interface{}{"keys": []interface{}{"src-a", "src-c"}})
	require.NoError(t, idxErr)
	require.True(t, used)
	require.Len(t, nodes, 2)
	eng.forbidScan = true

	res, err := exec.Execute(context.Background(),
		"MATCH (n:MongoDocument) WHERE n.translationId IN $keys RETURN n.translationId AS id ORDER BY n.translationId",
		map[string]interface{}{"keys": []interface{}{"src-a", "src-c"}},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "src-a", res.Rows[0][0])
	require.Equal(t, "src-c", res.Rows[1][0])
}

func TestMatchUsesPropertyIndexForOrInParamList_NoScan(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-or-a",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "k-a",
			"textKey128": "h-a",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-or-b",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "k-b",
			"textKey128": "h-b",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-or-c",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey":    "k-c",
			"textKey128": "h-c",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_or_textkey FOR (n:OriginalText) ON (n.textKey)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_or_textkey128 FOR (n:OriginalText) ON (n.textKey128)", nil)
	require.NoError(t, err)

	eng.forbidScan = true
	res, err := exec.Execute(
		context.Background(),
		"MATCH (o:OriginalText) WHERE o.textKey IN $keys OR o.textKey128 IN $keys RETURN elementId(o) AS id ORDER BY id",
		map[string]interface{}{"keys": []interface{}{"k-a", "h-c"}},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, res.Columns)
	require.Len(t, res.Rows, 2)
}

func TestParseSimpleIndexedInParam(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	prop, vals, ok := exec.parseSimpleIndexedInParam("n", "n.translationId IN $keys", map[string]interface{}{
		"keys": []interface{}{"src-a", "src-c"},
	})
	require.True(t, ok)
	require.Equal(t, "translationId", prop)
	require.Len(t, vals, 2)
}

func TestParseSimpleIndexedInLiteral(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	ctx := context.Background()

	prop, vals, ok := exec.parseSimpleIndexedInLiteral(ctx, "n", "n.translationId IN ['src-a','src-c']")
	require.True(t, ok)
	require.Equal(t, "translationId", prop)
	require.Len(t, vals, 2)
}

func TestMatchUsesPropertyIndexForInLiteralList(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-lit-a",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-a",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-lit-b",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-b",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-lit-c",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"translationId": "src-c",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_translation_id_lit FOR (n:MongoDocument) ON (n.translationId)", nil)
	require.NoError(t, err)
	eng.forbidScan = true

	res, err := exec.Execute(
		context.Background(),
		"MATCH (n:MongoDocument) WHERE n.translationId IN ['src-a','src-c'] RETURN n.translationId AS id ORDER BY n.translationId",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "src-a", res.Rows[0][0])
	require.Equal(t, "src-c", res.Rows[1][0])
}

func TestMatchUsesPropertyIndexForIsNotNullOrderByLimit(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	for i := 0; i < 10; i++ {
		_, err := eng.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nornic:doc-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId": fmt.Sprintf("src-%03d", i),
			},
		})
		require.NoError(t, err)
	}

	exec := NewStorageExecutor(eng)
	_, err := exec.Execute(context.Background(), "CREATE INDEX idx_source_id FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)
	eng.forbidScan = true

	res, err := exec.Execute(context.Background(), "MATCH (n:MongoDocument) WHERE n.sourceId IS NOT NULL RETURN n.sourceId AS sourceId ORDER BY n.sourceId LIMIT 3", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId"}, res.Columns)
	require.Len(t, res.Rows, 3)
	require.Equal(t, "src-000", res.Rows[0][0])
	require.Equal(t, "src-001", res.Rows[1][0])
	require.Equal(t, "src-002", res.Rows[2][0])
}

func TestMatchUsesPropertyIndexForIsNotNullOrderByLimit_WithConstantAndConjuncts(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	for i := 0; i < 10; i++ {
		_, err := eng.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nornic:doc-and-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId": fmt.Sprintf("src-%03d", i),
			},
		})
		require.NoError(t, err)
	}

	exec := NewStorageExecutor(eng)
	_, err := exec.Execute(context.Background(), "CREATE INDEX idx_source_id_and FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)
	eng.forbidScan = true

	res, err := exec.Execute(
		context.Background(),
		"MATCH (n:MongoDocument) WHERE n.sourceId IS NOT NULL AND 'cache-bust' <> '' AND 2 = 2 RETURN n.sourceId AS sourceId ORDER BY n.sourceId LIMIT 3",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId"}, res.Columns)
	require.Len(t, res.Rows, 3)
	require.Equal(t, "src-000", res.Rows[0][0])
	require.Equal(t, "src-001", res.Rows[1][0])
	require.Equal(t, "src-002", res.Rows[2][0])
}

func TestMatchFallsBackWhenPropertyIndexMetadataIsStale(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	_, err := base.CreateNode(&storage.Node{
		ID:     "nornic:doc-stale-1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId": "src-stale-1",
			"textKey":  "k-stale-1",
		},
	})
	require.NoError(t, err)

	_, err = base.CreateNode(&storage.Node{
		ID:     "nornic:doc-stale-2",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"sourceId": "src-stale-2",
			"textKey":  "k-stale-2",
		},
	})
	require.NoError(t, err)

	// Inject index metadata directly without backfilling values to simulate stale/empty index entries.
	err = base.GetSchema().AddPropertyIndex("idx_source_id_stale", "MongoDocument", []string{"sourceId"})
	require.NoError(t, err)

	exec := NewStorageExecutor(base)
	res, err := exec.Execute(context.Background(), "MATCH (n:MongoDocument) WHERE n.sourceId IS NOT NULL RETURN n.sourceId AS sourceId ORDER BY n.sourceId LIMIT 2", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "src-stale-1", res.Rows[0][0])
	require.Equal(t, "src-stale-2", res.Rows[1][0])

	res, err = exec.Execute(context.Background(), "MATCH (n:MongoDocument) WHERE n.sourceId = 'src-stale-2' RETURN n.textKey AS textKey", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "k-stale-2", res.Rows[0][0])
}
