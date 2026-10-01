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

func TestGh810_NullPropertyMapCannotDeleteUnrelatedNodes(t *testing.T) {
	for _, schema := range []string{"", "CREATE INDEX ix FOR (n:PDRecord) ON (n.id)", "CREATE CONSTRAINT uq FOR (n:PDRecord) REQUIRE n.id IS UNIQUE"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("schema=%s/explicit=%v", schema, explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh810"))
				ctx := context.Background()
				if schema != "" {
					_, err := exec.Execute(ctx, schema, nil)
					require.NoError(t, err)
				}
				_, err := exec.Execute(ctx, "CREATE (a:PDRecord {id:'r1',kind:'a'})-[:R]->(b:PDRecord {id:'r2',kind:'b'}), (:PDRecord {id:'r3',kind:'a'}), (:PDRecord {kind:'noid'})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				_, err = exec.Execute(ctx, "MATCH (n:PDRecord {id: $id}) DETACH DELETE n", map[string]interface{}{"id": nil})
				require.NoError(t, err)
				remaining, err := exec.Execute(ctx, "MATCH (n:PDRecord) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(4)}}, remaining.Rows)
				edges, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, edges.Rows)
			})
		}
	}
}

func TestGh810_NullPropertyMapPredicates(t *testing.T) {
	nullID := map[string]interface{}{"id": nil}
	for _, testCase := range []struct {
		name, query string
		parameters  map[string]interface{}
		rows        [][]interface{}
		wantError   string
	}{
		{"parameter null", "MATCH (n:PDRecord {id:$id}) RETURN n.id", nullID, nil, ""},
		{"literal null", "MATCH (n:PDRecord {id:null}) RETURN n.id", nil, nil, ""},
		{"null count", "MATCH (n:PDRecord {id:$id}) RETURN count(n)", nullID, [][]interface{}{{int64(0)}}, ""},
		{"unlabelled null", "MATCH (n {id:$id}) RETURN n.id", nullID, nil, ""},
		{"compound null id", "MATCH (n:PDRecord {id:$id,kind:$kind}) RETURN n.id", map[string]interface{}{"id": nil, "kind": "a"}, nil, ""},
		{"compound null kind", "MATCH (n:PDRecord {id:$id,kind:$kind}) RETURN n.id", map[string]interface{}{"id": "r1", "kind": nil}, nil, ""},
		{"relationship source null", "MATCH (a:PDRecord {id:$id})-[:R]->(b) RETURN b.id", nullID, nil, ""},
		{"relationship target null", "MATCH (a)-[:R]->(b:PDRecord {id:$id}) RETURN a.id", nullID, nil, ""},
		{"where null control", "MATCH (n:PDRecord) WHERE n.id=$id RETURN n.id", nullID, nil, ""},
		{"optional null", "OPTIONAL MATCH (n:PDRecord {id:$id}) RETURN n.id", nullID, [][]interface{}{{nil}}, ""},
		{"absent id", "MATCH (n:PDRecord {id:$id}) RETURN n.id", map[string]interface{}{"id": "absent"}, nil, ""},
		{"present id", "MATCH (n:PDRecord {id:$id}) RETURN n.id", map[string]interface{}{"id": "r1"}, [][]interface{}{{"r1"}}, ""},
		{"missing parameter", "MATCH (n:PDRecord {id:$id}) DETACH DELETE n", nil, nil, "parameter"},
		{"null set", "MATCH (n:PDRecord {id:$id}) SET n.touched=true RETURN count(n)", nullID, [][]interface{}{{int64(0)}}, ""},
		{"null delete count", "MATCH (n:PDRecord {id:$id}) DETACH DELETE n RETURN count(*)", nullID, [][]interface{}{{int64(0)}}, ""},
		{"unlabelled null delete", "MATCH (n {id:$id}) DETACH DELETE n RETURN count(*)", nullID, [][]interface{}{{int64(0)}}, ""},
		{"null merge", "MERGE (n:PDRecord {id:$id}) RETURN n.id", nullID, nil, "null property"},
	} {
		for _, schema := range []string{"", "CREATE INDEX ix FOR (n:PDRecord) ON (n.id)", "CREATE CONSTRAINT uq FOR (n:PDRecord) REQUIRE n.id IS UNIQUE"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/schema=%s/explicit=%v", testCase.name, schema, explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh810"))
					ctx := context.Background()
					if schema != "" {
						_, err := exec.Execute(ctx, schema, nil)
						require.NoError(t, err)
					}
					_, err := exec.Execute(ctx, "CREATE (a:PDRecord {id:'r1',kind:'a'})-[:R]->(b:PDRecord {id:'r2',kind:'b'}), (:PDRecord {id:'r3',kind:'a'}), (:PDRecord {id:null,kind:'noid'})", nil)
					require.NoError(t, err)
					before, err := exec.Execute(ctx, "MATCH (n:PDRecord) RETURN properties(n) AS props ORDER BY n.kind,n.id", nil)
					require.NoError(t, err)
					require.Len(t, before.Rows, 4)
					require.Equal(t, map[string]interface{}{"kind": "noid"}, before.Rows[3][0])
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, testCase.query, testCase.parameters)
					if testCase.wantError != "" {
						require.Error(t, err)
						require.Contains(t, err.Error(), testCase.wantError)
						if explicit {
							_, err = exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}
					} else {
						require.NoError(t, err)
						if testCase.rows == nil {
							require.Empty(t, result.Rows)
						} else {
							require.Equal(t, testCase.rows, result.Rows)
						}
					}
					after, err := exec.Execute(ctx, "MATCH (n:PDRecord) RETURN properties(n) AS props ORDER BY n.kind,n.id", nil)
					require.NoError(t, err)
					require.Equal(t, before.Rows, after.Rows)
					edges, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, edges.Rows)
				})
			}
		}
	}
}

func TestGh809_IndexedTransactionReadYourWrites(t *testing.T) {
	for _, indexedProperty := range []string{"", "document_id", "kind"} {
		t.Run("index="+indexedProperty, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh809"))
			ctx := context.Background()
			if indexedProperty != "" {
				_, err := exec.Execute(ctx, fmt.Sprintf("CREATE INDEX ix FOR (n:PDRecord) ON (n.%s)", indexedProperty), nil)
				require.NoError(t, err)
			}
			rows := make([]interface{}, 0, 48)
			ids := make([]interface{}, 0, 16)
			for document := 0; document < 16; document++ {
				documentID := fmt.Sprintf("doc-%d", document)
				ids = append(ids, documentID)
				for _, kind := range []string{"document", "version", "origin"} {
					rows = append(rows, map[string]interface{}{"properties": map[string]interface{}{
						"id": documentID + "-" + kind, "document_id": documentID, "kind": kind,
						"revision": int64(1), "body": "body", "created_at": "created", "updated_at": "updated",
					}})
				}
			}
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			written, err := exec.Execute(ctx, "UNWIND $rows AS row CREATE (n:PDRecord) SET n = row.properties RETURN n.id AS id", map[string]interface{}{"rows": rows})
			require.NoError(t, err)
			require.Len(t, written.Rows, 48)
			params := map[string]interface{}{"ids": ids, "kinds": []interface{}{"version", "origin", "unused"}}
			for _, selector := range []struct {
				name  string
				query string
				count int
			}{
				{"projected conjunction", "MATCH (n:PDRecord) WHERE n.document_id IN $ids AND n.kind IN $kinds RETURN n.id AS id,n.kind AS kind,n.revision AS revision,n.body AS body,n.created_at AS created_at,n.updated_at AS updated_at", 32},
				{"document parameter list", "MATCH (n:PDRecord) WHERE n.document_id IN $ids RETURN n.id AS id", 48},
				{"document equality", "MATCH (n:PDRecord) WHERE n.document_id = 'doc-3' RETURN n.id AS id", 3},
				{"document literal list", "MATCH (n:PDRecord) WHERE n.document_id IN ['doc-1','doc-2'] RETURN n.id AS id", 6},
				{"kind literal list", "MATCH (n:PDRecord) WHERE n.kind IN ['version','origin'] RETURN n.id AS id", 32},
				{"pattern property", "MATCH (n:PDRecord {document_id: 'doc-3'}) RETURN n.id AS id", 3},
				{"ordered not null", "MATCH (n:PDRecord) WHERE n.document_id IS NOT NULL RETURN n.id AS id ORDER BY n.document_id,n.id LIMIT 5", 5},
				{"unfiltered scan", "MATCH (n:PDRecord) RETURN n.id AS id", 48},
			} {
				t.Run(selector.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, selector.query, params)
					require.NoError(t, err)
					require.Len(t, result.Rows, selector.count)
				})
			}
			count, err := exec.Execute(ctx, "MATCH (n:PDRecord) WHERE n.document_id IN $ids AND n.kind IN $kinds RETURN count(n)", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(32)}}, count.Rows)
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			committed, err := exec.Execute(ctx, "MATCH (n:PDRecord) WHERE n.document_id IN $ids AND n.kind IN $kinds RETURN n.id", params)
			require.NoError(t, err)
			require.Len(t, committed.Rows, 32)
		})
	}
}

func TestGh809_IndexedTransactionMixedRowsAndWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh809"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX ix FOR (n:P) ON (n.k)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:P {k: 'a', src: 'committed'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	_, err = exec.Execute(ctx, "CREATE (:P {k: 'a', src: 'tx'}), (:P {k: 'b', src: 'tx'})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:P) WHERE n.k = 'a' RETURN n.src AS src ORDER BY src",
		"MATCH (n:P {k: 'a'}) RETURN n.src AS src ORDER BY src",
		"MATCH (n:P) WHERE n.k = 'a' OR n.k = 'missing' RETURN n.src AS src ORDER BY src",
		"MATCH (n:P) WHERE n.k IN ['a'] OR n.k IN ['missing'] RETURN n.src AS src ORDER BY src",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"committed"}, {"tx"}}, result.Rows)
		})
	}
	t.Run("indexed match set", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'b' SET n.seen = true RETURN count(*)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		stored, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'b' RETURN n.seen", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, stored.Rows)
	})
	t.Run("indexed property update", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:P {src: 'committed'}) SET n.k = 'b'", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'b' RETURN n.src AS src ORDER BY src", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"committed"}, {"tx"}}, result.Rows)
	})
	t.Run("indexed deletion", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'a' DETACH DELETE n", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k IN ['a','b'] RETURN n.src AS src ORDER BY src", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"committed"}, {"tx"}}, result.Rows)
	})
	t.Run("rollback preserves committed index", func(t *testing.T) {
		_, err := exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		for _, query := range []string{
			"MATCH (n:P) WHERE n.k = 'a' RETURN n.src",
			"MATCH (n:P) RETURN n.src",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"committed"}}, result.Rows)
		}
		result, err := exec.Execute(ctx, "MATCH (n:P) WHERE n.k = 'b' RETURN n.src", nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
	})
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

// TestMatchUsesIndexFilledFromExistingNodes: CREATE INDEX over existing nodes
// fills the index, so an index-backed ordered scan returns them (the index
// is trusted; there is no scan fallback for an empty index result, #719).
func TestMatchUsesIndexFilledFromExistingNodes(t *testing.T) {
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

	exec := NewStorageExecutor(base)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_source_id_stale FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)
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
