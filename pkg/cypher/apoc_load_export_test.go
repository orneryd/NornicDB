package cypher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApocLoadJson(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("load_json_from_file", func(t *testing.T) {
		// Create a temp JSON file
		tmpDir := t.TempDir()
		jsonFile := filepath.Join(tmpDir, "test.json")
		data := map[string]interface{}{
			"name": "Test",
			"value": 42,
		}
		jsonBytes, _ := json.Marshal(data)
		require.NoError(t, os.WriteFile(jsonFile, jsonBytes, 0644))

		result, err := exec.Execute(ctx, "CALL apoc.load.json('"+jsonFile+"') YIELD value", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, []string{"value"}, result.Columns)

		// Verify the loaded data
		valueMap := result.Rows[0][0].(map[string]interface{})
		assert.Equal(t, "Test", valueMap["name"])
		assert.Equal(t, float64(42), valueMap["value"]) // JSON numbers are float64
	})

	t.Run("load_json_array", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonFile := filepath.Join(tmpDir, "array.json")
		data := []interface{}{
			map[string]interface{}{"id": 1, "name": "Alice"},
			map[string]interface{}{"id": 2, "name": "Bob"},
		}
		jsonBytes, _ := json.Marshal(data)
		require.NoError(t, os.WriteFile(jsonFile, jsonBytes, 0644))

		result, err := exec.Execute(ctx, "CALL apoc.load.json('"+jsonFile+"') YIELD value", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Array items should be returned as separate rows
		assert.Len(t, result.Rows, 2)
	})
}

func TestApocLoadCsv(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("load_csv_with_header", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "test.csv")
		csvContent := `name,age,city
Alice,30,NYC
Bob,25,LA`
		require.NoError(t, os.WriteFile(csvFile, []byte(csvContent), 0644))

		result, err := exec.Execute(ctx, "CALL apoc.load.csv('"+csvFile+"') YIELD lineNo, list, map", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 2) // 2 data rows (excluding header)
		assert.Equal(t, []string{"lineNo", "list", "map"}, result.Columns)

		// First data row
		row1 := result.Rows[0]
		assert.Equal(t, 2, row1[0]) // lineNo (1-indexed, after header)
		list1 := row1[1].([]interface{})
		assert.Equal(t, "Alice", list1[0])
		map1 := row1[2].(map[string]interface{})
		assert.Equal(t, "Alice", map1["name"])
		assert.Equal(t, "30", map1["age"])
	})
}

func TestApocExportJsonAll(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create test data
	require.NoError(t, engine.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}))
	require.NoError(t, engine.CreateNode(&storage.Node{
		ID:         "n2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{
		ID:        "e1",
		StartNode: "n1",
		EndNode:   "n2",
		Type:      "KNOWS",
	}))

	t.Run("export_all_to_json", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonFile := filepath.Join(tmpDir, "export.json")

		result, err := exec.Execute(ctx, "CALL apoc.export.json.all('"+jsonFile+"') YIELD file, nodes, relationships", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, jsonFile, result.Rows[0][0])
		assert.Equal(t, 2, result.Rows[0][1]) // 2 nodes
		assert.Equal(t, 1, result.Rows[0][2]) // 1 relationship

		// Verify file was created
		_, err = os.Stat(jsonFile)
		assert.NoError(t, err)

		// Verify file content
		content, err := os.ReadFile(jsonFile)
		require.NoError(t, err)
		var exported map[string]interface{}
		require.NoError(t, json.Unmarshal(content, &exported))

		nodes := exported["nodes"].([]interface{})
		assert.Len(t, nodes, 2)

		rels := exported["relationships"].([]interface{})
		assert.Len(t, rels, 1)
	})
}

func TestApocExportCsvAll(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create test data
	require.NoError(t, engine.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}))

	t.Run("export_all_to_csv", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "export.csv")

		result, err := exec.Execute(ctx, "CALL apoc.export.csv.all('"+csvFile+"') YIELD file, nodes, relationships", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, csvFile, result.Rows[0][0])

		// Verify file was created
		_, err = os.Stat(csvFile)
		assert.NoError(t, err)
	})
}

func TestApocImportJson(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("import_json_graph", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonFile := filepath.Join(tmpDir, "graph.json")

		// Create graph data to import
		graphData := map[string]interface{}{
			"nodes": []interface{}{
				map[string]interface{}{
					"id":         "imported1",
					"labels":     []interface{}{"Person"},
					"properties": map[string]interface{}{"name": "Charlie"},
				},
				map[string]interface{}{
					"id":         "imported2",
					"labels":     []interface{}{"Person"},
					"properties": map[string]interface{}{"name": "Diana"},
				},
			},
			"relationships": []interface{}{
				map[string]interface{}{
					"id":         "rel1",
					"type":       "KNOWS",
					"startNode":  "imported1",
					"endNode":    "imported2",
					"properties": map[string]interface{}{},
				},
			},
		}
		jsonBytes, _ := json.MarshalIndent(graphData, "", "  ")
		require.NoError(t, os.WriteFile(jsonFile, jsonBytes, 0644))

		result, err := exec.Execute(ctx, "CALL apoc.import.json('"+jsonFile+"') YIELD source, nodes, relationships", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, jsonFile, result.Rows[0][0])
		assert.Equal(t, 2, result.Rows[0][1]) // 2 nodes imported
		assert.Equal(t, 1, result.Rows[0][2]) // 1 relationship imported

		// Verify nodes were imported
		node1, err := engine.GetNode("imported1")
		require.NoError(t, err)
		assert.Equal(t, "Charlie", node1.Properties["name"])

		node2, err := engine.GetNode("imported2")
		require.NoError(t, err)
		assert.Equal(t, "Diana", node2.Properties["name"])

		// Verify relationship was imported
		edge, err := engine.GetEdge("rel1")
		require.NoError(t, err)
		assert.Equal(t, "KNOWS", edge.Type)
	})
}

func TestApocLoadJsonArray(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("load_json_array_file", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonFile := filepath.Join(tmpDir, "array.json")
		data := []interface{}{1, 2, 3, 4, 5}
		jsonBytes, _ := json.Marshal(data)
		require.NoError(t, os.WriteFile(jsonFile, jsonBytes, 0644))

		result, err := exec.Execute(ctx, "CALL apoc.load.jsonArray('"+jsonFile+"') YIELD value", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 5)
	})
}
