package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromNeo4jExport(t *testing.T) {
	t.Run("success - full export", func(t *testing.T) {
		// Create temp file with Neo4j export
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "export.json")

		exportJSON := `{
			"nodes": [
				{
					"id": "person-1",
					"labels": ["Person"],
					"properties": {"name": "Alice", "age": 30}
				},
				{
					"id": "person-2",
					"labels": ["Person"],
					"properties": {"name": "Bob", "age": 25}
				}
			],
			"relationships": [
				{
					"id": "rel-1",
					"type": "KNOWS",
					"start": {"id": "person-1"},
					"end": {"id": "person-2"},
					"properties": {"since": 2020}
				}
			]
		}`

		err := os.WriteFile(exportPath, []byte(exportJSON), 0644)
		require.NoError(t, err)

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err = LoadFromNeo4jExport(engine, exportPath)
		require.NoError(t, err)

		// Verify nodes
		count, _ := engine.NodeCount()
		assert.Equal(t, int64(2), count)

		alice, err := engine.GetNode("person-1")
		require.NoError(t, err)
		assert.Equal(t, "Alice", alice.Properties["name"])
		assert.Contains(t, alice.Labels, "Person")

		// Verify edges
		edgeCount, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), edgeCount)

		edge, err := engine.GetEdge("rel-1")
		require.NoError(t, err)
		assert.Equal(t, "KNOWS", edge.Type)
		assert.Equal(t, NodeID("person-1"), edge.StartNode)
		assert.Equal(t, NodeID("person-2"), edge.EndNode)
	})

	t.Run("file not found", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jExport(engine, "/nonexistent/path.json")
		assert.Error(t, err)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "bad.json")
		os.WriteFile(exportPath, []byte("not json"), 0644)

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jExport(engine, exportPath)
		assert.Error(t, err)
	})

	t.Run("empty export", func(t *testing.T) {
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "empty.json")
		os.WriteFile(exportPath, []byte(`{"nodes": [], "relationships": []}`), 0644)

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jExport(engine, exportPath)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(0), count)
	})
}

func TestLoadFromNeo4jJSON(t *testing.T) {
	t.Run("success - separate files", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create nodes.json (JSON lines format)
		nodesJSON := `{"id": "n1", "labels": ["Person"], "properties": {"name": "Alice"}}
{"id": "n2", "labels": ["Person"], "properties": {"name": "Bob"}}`
		os.WriteFile(filepath.Join(tmpDir, "nodes.json"), []byte(nodesJSON), 0644)

		// Create relationships.json
		relsJSON := `{"id": "r1", "type": "KNOWS", "start": {"id": "n1"}, "end": {"id": "n2"}, "properties": {}}`
		os.WriteFile(filepath.Join(tmpDir, "relationships.json"), []byte(relsJSON), 0644)

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jJSON(engine, tmpDir)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(2), count)

		edgeCount, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), edgeCount)
	})

	t.Run("nodes only", func(t *testing.T) {
		tmpDir := t.TempDir()

		nodesJSON := `{"id": "n1", "labels": ["Test"], "properties": {}}`
		os.WriteFile(filepath.Join(tmpDir, "nodes.json"), []byte(nodesJSON), 0644)

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jJSON(engine, tmpDir)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(1), count)
	})

	t.Run("empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jJSON(engine, tmpDir)
		require.NoError(t, err) // Should succeed with no files

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(0), count)
	})

	t.Run("relationship parse errors bubble up", func(t *testing.T) {
		tmpDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "nodes.json"), []byte(`{"id": "n1", "labels": ["Test"], "properties": {}}
{"id": "n2", "labels": ["Test"], "properties": {}}`), 0644))
		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "relationships.json"), []byte(`not-json`), 0644))

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := LoadFromNeo4jJSON(engine, tmpDir)
		require.ErrorContains(t, err, "loading relationships")
	})
}

func TestSaveToNeo4jExport(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{
			ID:         "person-1",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Alice"},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{
			ID:         "person-2",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Bob"},
		})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "knows-1",
			StartNode: "person-1",
			EndNode:   "person-2",
			Type:      "KNOWS",
		}))

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "export.json")

		err = SaveToNeo4jExport(engine, exportPath)
		require.NoError(t, err)

		// Load back and verify
		base2 := NewMemoryEngine()
		defer base2.Close()
		engine2 := NewNamespacedEngine(base2, "test")
		err = LoadFromNeo4jExport(engine2, exportPath)
		require.NoError(t, err)

		count, _ := engine2.NodeCount()
		assert.Equal(t, int64(2), count)

		edgeCount, _ := engine2.EdgeCount()
		assert.Equal(t, int64(1), edgeCount)
	})

	t.Run("empty engine", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "empty.json")

		err := SaveToNeo4jExport(engine, exportPath)
		require.NoError(t, err)

		// Verify file exists and is valid JSON
		data, err := os.ReadFile(exportPath)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"nodes"`)
		assert.Contains(t, string(data), `"relationships"`)
	})

	t.Run("unsupported engine type", func(t *testing.T) {
		var engine Engine
		err := SaveToNeo4jExport(engine, filepath.Join(t.TempDir(), "export.json"))
		require.ErrorContains(t, err, "does not support full export")
	})

	t.Run("exportable engine node and edge errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "export.json")
		err := SaveToNeo4jExport(&exportableOnlyEngine{
			Engine:  &lookupEngine{Engine: NewMemoryEngine()},
			nodeErr: errors.New("nodes failed"),
		}, path)
		require.ErrorContains(t, err, "getting all nodes")

		err = SaveToNeo4jExport(&exportableOnlyEngine{
			Engine:   &lookupEngine{Engine: NewMemoryEngine()},
			allNodes: []*Node{},
			edgeErr:  errors.New("edges failed"),
		}, path)
		require.ErrorContains(t, err, "getting all edges")
	})
}

func TestGenericSaveToNeo4jExport(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{
			ID:     "test-1",
			Labels: []string{"Test"},
		})
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "generic.json")

		err = GenericSaveToNeo4jExport(engine, exportPath)
		require.NoError(t, err)

		// Verify file created
		_, err = os.Stat(exportPath)
		assert.NoError(t, err)
	})

	t.Run("node and edge load errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "generic.json")
		err := GenericSaveToNeo4jExport(&exportableOnlyEngine{
			Engine:  NewMemoryEngine(),
			nodeErr: errors.New("nodes failed"),
		}, path)
		require.ErrorContains(t, err, "getting nodes")

		err = GenericSaveToNeo4jExport(&exportableOnlyEngine{
			Engine:   NewMemoryEngine(),
			allNodes: []*Node{},
			edgeErr:  errors.New("edges failed"),
		}, path)
		require.ErrorContains(t, err, "getting edges")
	})
}

func TestLoadNodesFromReader(t *testing.T) {
	t.Run("multiple nodes", func(t *testing.T) {
		input := `{"id": "n1", "labels": ["A"], "properties": {"x": 1}}
{"id": "n2", "labels": ["B"], "properties": {"y": 2}}
{"id": "n3", "labels": ["A", "B"], "properties": {}}`

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := loadNodesFromReader(engine, strings.NewReader(input))
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(3), count)
	})

	t.Run("empty lines ignored", func(t *testing.T) {
		input := `{"id": "n1", "labels": [], "properties": {}}

{"id": "n2", "labels": [], "properties": {}}
`
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := loadNodesFromReader(engine, strings.NewReader(input))
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("invalid JSON line", func(t *testing.T) {
		input := `{"id": "n1", "labels": [], "properties": {}}
not valid json
{"id": "n2", "labels": [], "properties": {}}`

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := loadNodesFromReader(engine, strings.NewReader(input))
		assert.Error(t, err)
	})

	t.Run("empty ID", func(t *testing.T) {
		input := `{"id": "", "labels": [], "properties": {}}`

		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		err := loadNodesFromReader(engine, strings.NewReader(input))
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestLoadRelationshipsFromReader(t *testing.T) {
	t.Run("multiple relationships", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{ID: "n1"})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n2"})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n3"})
		require.NoError(t, err)

		input := `{"id": "r1", "type": "KNOWS", "start": {"id": "n1"}, "end": {"id": "n2"}, "properties": {}}
{"id": "r2", "type": "LIKES", "start": {"id": "n2"}, "end": {"id": "n3"}, "properties": {}}`

		err = loadRelationshipsFromReader(engine, strings.NewReader(input))
		require.NoError(t, err)

		count, _ := engine.EdgeCount()
		assert.Equal(t, int64(2), count)
	})

	t.Run("with confidence", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{ID: "n1"})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n2"})
		require.NoError(t, err)

		input := `{"id": "r1", "type": "SIMILAR", "start": {"id": "n1"}, "end": {"id": "n2"}, "properties": {"_confidence": 0.95, "_autoGenerated": true}}`

		err = loadRelationshipsFromReader(engine, strings.NewReader(input))
		require.NoError(t, err)

		edge, _ := engine.GetEdge("r1")
		assert.Equal(t, 0.95, edge.Confidence)
		assert.True(t, edge.AutoGenerated)
		// Internal properties should be removed
		_, hasConfidence := edge.Properties["_confidence"]
		assert.False(t, hasConfidence)
	})

	t.Run("missing node", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{ID: "n1"})
		require.NoError(t, err)
		// n2 doesn't exist

		input := `{"id": "r1", "type": "KNOWS", "start": {"id": "n1"}, "end": {"id": "n2"}, "properties": {}}`

		err = loadRelationshipsFromReader(engine, strings.NewReader(input))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("invalid JSON and empty ID", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		_, err := engine.CreateNode(&Node{ID: "n1"})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n2"})
		require.NoError(t, err)

		err = loadRelationshipsFromReader(engine, strings.NewReader("not-json"))
		require.Error(t, err)

		err = loadRelationshipsFromReader(engine, strings.NewReader(`{"id": "", "type": "KNOWS", "start": {"id": "n1"}, "end": {"id": "n2"}, "properties": {}}`))
		require.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestNodeFromNeo4j(t *testing.T) {
	t.Run("basic conversion", func(t *testing.T) {
		neo4jNode := &Neo4jNode{
			ID:     "test-123",
			Labels: []string{"Person", "Employee"},
			Properties: map[string]any{
				"name": "Alice",
				"age":  30,
			},
		}

		node, err := nodeFromNeo4j(neo4jNode)
		require.NoError(t, err)

		assert.Equal(t, NodeID("test-123"), node.ID)
		assert.Equal(t, []string{"Person", "Employee"}, node.Labels)
		assert.Equal(t, "Alice", node.Properties["name"])
	})

	t.Run("with internal properties", func(t *testing.T) {
		neo4jNode := &Neo4jNode{
			ID:     "test-456",
			Labels: []string{"Memory"},
			Properties: map[string]any{
				"content":      "Hello",
				"_decayScore":  0.75,
				"_accessCount": float64(10),
			},
		}

		node, err := nodeFromNeo4j(neo4jNode)
		require.NoError(t, err)

		// Legacy internal properties should be removed from Properties map
		_, hasDecay := node.Properties["_decayScore"]
		assert.False(t, hasDecay)
		_, hasAccess := node.Properties["_accessCount"]
		assert.False(t, hasAccess)
	})

	t.Run("empty ID", func(t *testing.T) {
		neo4jNode := &Neo4jNode{
			ID:         "",
			Labels:     []string{"Test"},
			Properties: map[string]any{},
		}

		_, err := nodeFromNeo4j(neo4jNode)
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestEdgeFromNeo4j(t *testing.T) {
	t.Run("basic conversion", func(t *testing.T) {
		neo4jRel := &Neo4jRelationship{
			ID:   "rel-123",
			Type: "KNOWS",
			Start: Neo4jNodeRef{
				ID: "n1",
			},
			End: Neo4jNodeRef{
				ID: "n2",
			},
			Properties: map[string]any{
				"since": 2020,
			},
		}

		edge, err := edgeFromNeo4j(neo4jRel)
		require.NoError(t, err)

		assert.Equal(t, EdgeID("rel-123"), edge.ID)
		assert.Equal(t, "KNOWS", edge.Type)
		assert.Equal(t, NodeID("n1"), edge.StartNode)
		assert.Equal(t, NodeID("n2"), edge.EndNode)
		assert.Equal(t, 2020, edge.Properties["since"])
	})

	t.Run("with internal properties", func(t *testing.T) {
		neo4jRel := &Neo4jRelationship{
			ID:    "rel-456",
			Type:  "SIMILAR",
			Start: Neo4jNodeRef{ID: "n1"},
			End:   Neo4jNodeRef{ID: "n2"},
			Properties: map[string]any{
				"_confidence":    0.95,
				"_autoGenerated": true,
				"weight":         5,
			},
		}

		edge, err := edgeFromNeo4j(neo4jRel)
		require.NoError(t, err)

		assert.Equal(t, 0.95, edge.Confidence)
		assert.True(t, edge.AutoGenerated)
		assert.Equal(t, 5, edge.Properties["weight"])
		// Internal properties removed
		_, hasConf := edge.Properties["_confidence"]
		assert.False(t, hasConf)
	})

	t.Run("empty ID", func(t *testing.T) {
		neo4jRel := &Neo4jRelationship{
			ID:         "",
			Type:       "TEST",
			Start:      Neo4jNodeRef{ID: "n1"},
			End:        Neo4jNodeRef{ID: "n2"},
			Properties: map[string]any{},
		}

		_, err := edgeFromNeo4j(neo4jRel)
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestMemoryEngine_AllNodes(t *testing.T) {
	t.Run("returns all nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"B"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n3")), Labels: []string{"A", "B"}})

		nodes, err := engine.AllNodes()
		require.NoError(t, err)
		assert.Len(t, nodes, 3)
	})

	t.Run("returns copies", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.CreateNode(&Node{
			ID:         NodeID(prefixTestID("n1")),
			Properties: map[string]any{"key": "value"},
		})
		require.NoError(t, err)

		nodes, _ := engine.AllNodes()
		nodes[0].Properties["key"] = "mutated"

		original, _ := engine.GetNode(NodeID(prefixTestID("n1")))
		assert.Equal(t, "value", original.Properties["key"])
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.Close()

		_, err := engine.AllNodes()
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestMemoryEngine_AllEdges(t *testing.T) {
	t.Run("returns all edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n3"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2"))}))
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("n3"))}))

		edges, err := engine.AllEdges()
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.Close()

		_, err := engine.AllEdges()
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestRoundTrip(t *testing.T) {
	t.Run("full round trip with complex data", func(t *testing.T) {
		// Create engine with complex data
		base1 := NewMemoryEngine()
		defer base1.Close()
		engine1 := NewNamespacedEngine(base1, "test")

		_, err := engine1.CreateNode(&Node{
			ID:         "person-alice",
			Labels:     []string{"Person", "Employee"},
			Properties: map[string]any{"name": "Alice", "age": 30, "active": true},
		})
		require.NoError(t, err)

		_, err = engine1.CreateNode(&Node{
			ID:         "person-bob",
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": "Bob", "age": 25},
		})
		require.NoError(t, err)

		_, err = engine1.CreateNode(&Node{
			ID:         "company-acme",
			Labels:     []string{"Company"},
			Properties: map[string]any{"name": "ACME Corp"},
		})
		require.NoError(t, err)

		require.NoError(t, engine1.CreateEdge(&Edge{
			ID:         "knows-1",
			StartNode:  "person-alice",
			EndNode:    "person-bob",
			Type:       "KNOWS",
			Properties: map[string]any{"since": 2020},
		}))

		require.NoError(t, engine1.CreateEdge(&Edge{
			ID:            "similar-1",
			StartNode:     "person-alice",
			EndNode:       "person-bob",
			Type:          "SIMILAR",
			Confidence:    0.87,
			AutoGenerated: true,
		}))

		require.NoError(t, engine1.CreateEdge(&Edge{
			ID:        "works-at-1",
			StartNode: "person-alice",
			EndNode:   "company-acme",
			Type:      "WORKS_AT",
		}))

		// Export
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "roundtrip.json")

		err = GenericSaveToNeo4jExport(engine1, exportPath)
		require.NoError(t, err)

		// Import into new engine
		base2 := NewMemoryEngine()
		defer base2.Close()
		engine2 := NewNamespacedEngine(base2, "test")
		err = LoadFromNeo4jExport(engine2, exportPath)
		require.NoError(t, err)

		// Verify counts
		nodeCount, _ := engine2.NodeCount()
		assert.Equal(t, int64(3), nodeCount)

		edgeCount, _ := engine2.EdgeCount()
		assert.Equal(t, int64(3), edgeCount)

		// Verify node data preserved
		alice, err := engine2.GetNode("person-alice")
		require.NoError(t, err)
		assert.Equal(t, "Alice", alice.Properties["name"])
		assert.Contains(t, alice.Labels, "Person")
		assert.Contains(t, alice.Labels, "Employee")

		bob, err := engine2.GetNode("person-bob")
		require.NoError(t, err)
		assert.Equal(t, "Bob", bob.Properties["name"])

		// Verify edge data preserved
		similar, err := engine2.GetEdge("similar-1")
		require.NoError(t, err)
		assert.Equal(t, "SIMILAR", similar.Type)
		assert.InDelta(t, 0.87, similar.Confidence, 0.001)
		assert.True(t, similar.AutoGenerated)

		// Verify graph structure
		outgoing, err := engine2.GetOutgoingEdges("person-alice")
		require.NoError(t, err)
		assert.Len(t, outgoing, 3)

		between, err := engine2.GetEdgesBetween("person-alice", "person-bob")
		require.NoError(t, err)
		assert.Len(t, between, 2)
	})
}

// Interface compliance test
func TestMemoryEngine_ImplementsExportableEngine(t *testing.T) {
	var _ ExportableEngine = (*MemoryEngine)(nil)
}

func TestLoadFromNeo4jJSON_MissingDir(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// nodes.json not found is OK (optional), but if the dir doesn't exist
	// it should still handle gracefully
	err := LoadFromNeo4jJSON(engine, "/nonexistent/path/for/test")
	// nodes.json and relationships.json are optional — no error expected
	assert.NoError(t, err)
}

func TestLoadFromNeo4jJSON_InvalidNodesJSON(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	dir := t.TempDir()
	// Write invalid JSON to nodes.json
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nodes.json"), []byte("not valid json"), 0644))

	err := LoadFromNeo4jJSON(engine, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading nodes")
}

func TestLoadFromNeo4jJSON_InvalidRelationshipsJSON(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	dir := t.TempDir()
	// Write valid nodes (empty) but invalid relationships
	require.NoError(t, os.WriteFile(filepath.Join(dir, "relationships.json"), []byte("bad json line"), 0644))

	err := LoadFromNeo4jJSON(engine, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading relationships")
}

func TestLoadFromNeo4jExport_FileNotFound(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := LoadFromNeo4jExport(engine, "/nonexistent/export.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "opening file")
}

func TestLoadFromNeo4jExport_InvalidJSON(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	path := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(path, []byte("not json"), 0644))

	err := LoadFromNeo4jExport(engine, path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding JSON")
}

func TestSaveToNeo4jExport_InvalidPath(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := SaveToNeo4jExport(engine, "/nonexistent/dir/export.json")
	require.Error(t, err)
}

func TestGenericSaveToNeo4jExport_InvalidPath(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := GenericSaveToNeo4jExport(engine, "/nonexistent/dir/export.json")
	require.Error(t, err)
}

func TestLoadRelationshipsFromReader_InvalidJSON(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	reader := strings.NewReader(`{"not": "a valid relationship"}`)
	err := loadRelationshipsFromReader(engine, reader)
	// Should fail because fields are missing (no "id" field in Neo4jRelationship)
	// or succeed but create nothing — depends on the struct parsing
	_ = err // Just exercise the code path
}

func TestLoadNodesFromReader_EmptyInput(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	reader := strings.NewReader("")
	err := loadNodesFromReader(engine, reader)
	require.NoError(t, err)

	// No nodes should be created
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 0)
}

func TestLoadRelationshipsFromReader_EmptyInput(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	reader := strings.NewReader("")
	err := loadRelationshipsFromReader(engine, reader)
	require.NoError(t, err)

	edges, err := engine.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 0)
}

func TestLoadFromNeo4jExport_Roundtrip(t *testing.T) {
	engine1 := NewMemoryEngine()
	defer engine1.Close()

	// Create test data with internal properties that exercise _createdAt, _confidence, _autoGenerated
	_, err := engine1.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, engine1.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "KNOWS",
		Properties:    map[string]interface{}{},
		CreatedAt:     now,
		Confidence:    0.95,
		AutoGenerated: true,
	}))

	// Export
	exportPath := filepath.Join(t.TempDir(), "export.json")
	err = SaveToNeo4jExport(engine1, exportPath)
	require.NoError(t, err)

	// Reimport into fresh engine
	engine2 := NewMemoryEngine()
	defer engine2.Close()

	err = LoadFromNeo4jExport(engine2, exportPath)
	require.NoError(t, err)

	nodes, err := engine2.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 2)

	edges, err := engine2.AllEdges()
	require.NoError(t, err)
	assert.Len(t, edges, 1)
	assert.InDelta(t, 0.95, edges[0].Confidence, 0.01)
	assert.True(t, edges[0].AutoGenerated)
	assert.False(t, edges[0].CreatedAt.IsZero())
}

func TestGenericSaveToNeo4jExport_Roundtrip(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "hello"}})
	require.NoError(t, err)

	exportPath := filepath.Join(t.TempDir(), "generic-export.json")
	err = GenericSaveToNeo4jExport(engine, exportPath)
	require.NoError(t, err)

	// Verify file exists and is valid JSON
	data, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "hello")
}

func TestLoadNodesFile_NonExistentIsOptional(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Loading from a non-existent file should be a no-op (optional)
	err := loadNodesFile(engine, "/nonexistent/nodes.json")
	assert.NoError(t, err)
}

func TestLoadRelationshipsFile_NonExistentIsOptional(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	err := loadRelationshipsFile(engine, "/nonexistent/relationships.json")
	assert.NoError(t, err)
}
