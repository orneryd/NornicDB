package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBug1_SetMapReplacementDropsProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"d": map[string]interface{}{
			"uuid":     "n1",
			"name":     "Ada",
			"group_id": "g",
		},
	}

	query := `
		MERGE (n:Bug {uuid: $d.uuid})
		SET n = $d
		RETURN properties(n) AS p
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	expectedProps := map[string]interface{}{
		"uuid":     "n1",
		"name":     "Ada",
		"group_id": "g",
	}

	p := result.Rows[0][0].(map[string]interface{})

	assert.Equal(t, expectedProps, p, "Properties should not be dropped during map replacement")
}

func TestBug1_ChainedSetLabelAndMapPreservesPropertiesAndLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "label_then_map",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n:Extra
				SET n = $d
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
		{
			name: "map_then_label",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n = $d
				SET n:Extra
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
		{
			name: "dynamic_label_then_map",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n:$(d.labels)
				SET n = $d
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]interface{}{
				"d": map[string]interface{}{
					"uuid":     "k-" + tc.name,
					"name":     "Ada",
					"group_id": "g",
					"labels":   []interface{}{"Extra"},
				},
			}

			res, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)

			props, ok := res.Rows[0][0].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, map[string]interface{}{
				"uuid":     "k-" + tc.name,
				"name":     "Ada",
				"group_id": "g",
				"labels":   []interface{}{"Extra"},
			}, props)

			labels, ok := res.Rows[0][1].([]interface{})
			require.True(t, ok)
			assert.Contains(t, labels, "M")
			assert.Contains(t, labels, "Extra")
		})
	}
}

func TestBug1_UnwindDynamicLabelAndMapPreservesPropertiesAndLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `
		UNWIND $nodes AS node
		MERGE (n:M {uuid: node.uuid})
		SET n:$(node.labels)
		SET n = node
		RETURN properties(n) AS p, labels(n) AS labels
	`, map[string]interface{}{
		"nodes": []map[string]interface{}{
			{
				"uuid":     "bulk-k",
				"name":     "Ada",
				"group_id": "g",
				"labels":   []interface{}{"Extra"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	props, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, map[string]interface{}{
		"uuid":     "bulk-k",
		"name":     "Ada",
		"group_id": "g",
		"labels":   []interface{}{"Extra"},
	}, props)

	labels, ok := res.Rows[0][1].([]interface{})
	require.True(t, ok)
	assert.Contains(t, labels, "M")
	assert.Contains(t, labels, "Extra")
}

func TestBug2_UnwindSetAliasCorruptsMergeKey(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "good-uuid", "name": "x"},
		},
	}

	query := `
		UNWIND $rows AS row
		MERGE (n:Bug {uuid: row.uuid})
		SET n += row
		RETURN n.uuid AS uuid
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	uuid := result.Rows[0][0].(string)

	assert.Equal(t, "good-uuid", uuid, "Merge key should be the value from the row, not the literal string 'row.uuid'")
}

func TestBug3_DynamicLabelsAreApplied(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"uuid":   "d1",
				"labels": []interface{}{"Person", "Author"},
			},
		},
	}

	query := `
		UNWIND $rows AS row
		MERGE (n:Bug {uuid: row.uuid})
		SET n:$(row.labels)
		RETURN labels(n) AS labels
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	labels, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok)
	assert.Contains(t, labels, "Bug")
	assert.Contains(t, labels, "Person")
	assert.Contains(t, labels, "Author")
}

func TestBug4_SetNodeVectorPropertySetsPropertyAndPreservesReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"Bug"},
		Properties: map[string]interface{}{
			"uuid": "v1",
		},
	})
	require.NoError(t, err)

	query := `
		MATCH (n:Bug {uuid:'v1'})
		WITH n
		CALL db.create.setNodeVectorProperty('v1', 'emb', $v)
		RETURN n.uuid AS uuid
	`

	result, err := exec.Execute(ctx, query, map[string]interface{}{
		"v": []interface{}{0.1, 0.2, 0.3},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "v1", result.Rows[0][0])

	updated, err := exec.Execute(ctx, `MATCH (n:Bug {uuid:'v1'}) RETURN n.emb AS emb`, nil)
	require.NoError(t, err)
	require.Len(t, updated.Rows, 1)

	emb, ok := updated.Rows[0][0].([]float64)
	require.True(t, ok)
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, emb)
}

func TestBug5_SetNodeVectorPropertyAcceptsNodeVariableArgument(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"Bug"},
		Properties: map[string]interface{}{
			"uuid": "v2",
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (n:Bug {uuid:'v2'})
		CALL db.create.setNodeVectorProperty(n, 'emb', $v)
		RETURN n.uuid AS uuid
	`, map[string]interface{}{
		"v": []interface{}{0.1, 0.2, 0.3},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx_bug_emb', 'Bug', 'emb', 3, 'cosine')", nil)
	require.NoError(t, err)

	knnRes, err := exec.Execute(ctx, `
		CALL db.index.vector.queryNodes('idx_bug_emb', 1, [0.1, 0.2, 0.3])
		YIELD node, score
		RETURN node.uuid AS uuid
	`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, knnRes.Rows)
	assert.Equal(t, "v2", knnRes.Rows[0][0])
}

func TestBug6_UnwindMergeSetMapWithIndexedMapPropertyDoesNotPanic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	err := store.GetSchema().AddPropertyIndex("idx_bulk_payload", "BulkNode", []string{"payload"})
	require.NoError(t, err)

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"uuid": "b1",
				"name": "a",
				"payload": map[string]interface{}{
					"k": "v",
				},
			},
			{
				"uuid": "b2",
				"name": "b",
			},
		},
	}

	// A map is not a property value on any SET route (Neo4j TypeError), so the
	// row carrying a map payload fails the statement loudly - without a panic
	// and without writing a partial batch.
	require.NotPanics(t, func() {
		_, qErr := exec.Execute(ctx, `
			UNWIND $rows AS row
			MERGE (n:BulkNode {uuid: row.uuid})
			SET n = row
			RETURN n.uuid AS uuid
		`, params)
		require.Error(t, qErr)
		assert.Contains(t, qErr.Error(), "TypeError")
	})
	count, err := exec.Execute(ctx, "MATCH (n:BulkNode) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count.Rows[0][0])
}

func TestBug7_UnwindMergeWithInlineSetNodeVectorPropertyPersistsRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	query := `
		UNWIND $nodes AS node
		MERGE (n:Entity {uuid: node.uuid})
		SET n:$(node.labels)
		SET n = node
		WITH n, node
		CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)
		RETURN n.uuid AS uuid
	`

	params := map[string]interface{}{
		"nodes": []map[string]interface{}{
			{
				"uuid":           "g1",
				"name":           "Ada",
				"labels":         []interface{}{"Extra"},
				"name_embedding": []interface{}{0.1, 0.2, 0.3},
			},
			{
				"uuid":           "g2",
				"name":           "Grace",
				"labels":         []interface{}{"Extra"},
				"name_embedding": []interface{}{0.3, 0.2, 0.1},
			},
		},
	}

	res, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	uuids := make(map[string]bool, len(res.Rows))
	for _, row := range res.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		uuids[id] = true
	}
	assert.True(t, uuids["g1"])
	assert.True(t, uuids["g2"])

	check, err := exec.Execute(ctx, `
		MATCH (n:Entity)
		WHERE n.uuid IN ['g1', 'g2']
		RETURN n.uuid AS uuid, labels(n) AS labels, n.name_embedding AS emb
	`, nil)
	require.NoError(t, err)
	require.Len(t, check.Rows, 2)
	for _, row := range check.Rows {
		assert.NotNil(t, row[2], "name_embedding should persist on entity")
		labels, ok := row[1].([]interface{})
		require.True(t, ok)
		assert.Contains(t, labels, "Entity")
		assert.Contains(t, labels, "Extra")
	}
}

func TestBug8_MatchUnwindCreateWithListPropertyPersistsAllRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Anchor {uuid:'a'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (x:Anchor {uuid:'a'})
		UNWIND [{id:'r0', vec:[0.1,0.2]}, {id:'r1', vec:[0.3,0.4]}, {id:'r2', vec:[0.5,0.6]}] AS r
		CREATE (c:Item {id:r.id, vec:r.vec})
	`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (c:Item) RETURN count(c) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	assert.Equal(t, int64(3), res.Rows[0][0])
}

func TestBug8_MatchUnwindCreateWithListProperty_NArityMatrix(t *testing.T) {
	testCases := []struct {
		name          string
		leadingMatchN int
		rowCount      int
		nestedVec     bool
		extraListProp bool
	}{
		{name: "1match_1row_vec", leadingMatchN: 1, rowCount: 1},
		{name: "1match_5rows_vec", leadingMatchN: 1, rowCount: 5},
		{name: "2match_3rows_vec", leadingMatchN: 2, rowCount: 3},
		{name: "2match_7rows_vec_and_tags", leadingMatchN: 2, rowCount: 7, extraListProp: true},
		{name: "3match_4rows_nested_vec_and_tags", leadingMatchN: 3, rowCount: 4, nestedVec: true, extraListProp: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			_, err := exec.Execute(ctx, `CREATE (:Anchor {uuid:'a1'})`, nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, `CREATE (:Anchor2 {uuid:'a2'})`, nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, `CREATE (:Anchor3 {uuid:'a3'})`, nil)
			require.NoError(t, err)

			rows := make([]map[string]interface{}, 0, tc.rowCount)
			for i := 0; i < tc.rowCount; i++ {
				row := map[string]interface{}{
					"id": fmt.Sprintf("r%d", i),
					"vec": []interface{}{
						float64(i) + 0.1,
						float64(i) + 0.2,
					},
				}
				if tc.nestedVec {
					row["vec"] = []interface{}{
						[]interface{}{float64(i) + 0.1, float64(i) + 0.2},
						[]interface{}{float64(i) + 0.3, float64(i) + 0.4},
					}
				}
				if tc.extraListProp {
					row["tags"] = []interface{}{fmt.Sprintf("t%d", i), "shared"}
				}
				rows = append(rows, row)
			}

			leadingMatches := "MATCH (x:Anchor {uuid:'a1'})"
			if tc.leadingMatchN >= 2 {
				leadingMatches += "\nMATCH (y:Anchor2 {uuid:'a2'})"
			}
			if tc.leadingMatchN >= 3 {
				leadingMatches += "\nMATCH (z:Anchor3 {uuid:'a3'})"
			}

			createMap := "id:r.id, vec:r.vec"
			if tc.extraListProp {
				createMap += ", tags:r.tags"
			}

			query := fmt.Sprintf(`
				%s
				UNWIND $rows AS r
				CREATE (c:Item { %s })
			`, leadingMatches, createMap)

			_, err = exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
			require.NoError(t, err)

			countRes, err := exec.Execute(ctx, `MATCH (c:Item) RETURN count(c) AS n`, nil)
			require.NoError(t, err)
			require.Len(t, countRes.Rows, 1)
			require.Len(t, countRes.Rows[0], 1)
			assert.Equal(t, int64(tc.rowCount), countRes.Rows[0][0], "row cardinality must be preserved across MATCH+UNWIND+CREATE with list-valued properties")

			idRes, err := exec.Execute(ctx, `MATCH (c:Item) RETURN count(DISTINCT c.id) AS n`, nil)
			require.NoError(t, err)
			require.Len(t, idRes.Rows, 1)
			require.Len(t, idRes.Rows[0], 1)
			assert.Equal(t, int64(tc.rowCount), idRes.Rows[0][0], "created rows should remain one-per-unwind-item")
		})
	}
}

func TestBug9_UnwindRelationshipMergeSetWithInlineVectorProcedurePreservesProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Entity {uuid:'ia'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Entity {uuid:'ib'})`, nil)
	require.NoError(t, err)

	params := map[string]interface{}{
		"entity_edges": []map[string]interface{}{
			{
				"source_node_uuid": "ia",
				"target_node_uuid": "ib",
				"uuid":             "e1",
				"name":             "DIRECTED",
				"fact":             "A directed B",
				"group_id":         "g",
				"fact_embedding":   []interface{}{0.1, 0.2, 0.3},
			},
		},
	}
	writeRes, err := exec.Execute(ctx, `
		UNWIND $entity_edges AS edge
		MATCH (source:Entity {uuid: edge.source_node_uuid})
		MATCH (target:Entity {uuid: edge.target_node_uuid})
		MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
		SET e = edge
		WITH e, edge
		CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", edge.fact_embedding)
		RETURN edge.uuid AS uuid
	`, params)
	require.NoError(t, err)
	_ = writeRes

	check, err := exec.Execute(ctx, `
		MATCH (:Entity {uuid:'ia'})-[e:RELATES_TO {uuid:'e1'}]->(:Entity {uuid:'ib'})
		RETURN e.name AS name, e.fact AS fact, e.group_id AS group_id, e.source_node_uuid AS src, e.target_node_uuid AS dst, e.fact_embedding AS emb
	`, nil)
	require.NoError(t, err)
	require.Len(t, check.Rows, 1)
	require.Len(t, check.Rows[0], 6)
	assert.Equal(t, "DIRECTED", check.Rows[0][0])
	assert.Equal(t, "A directed B", check.Rows[0][1])
	assert.Equal(t, "g", check.Rows[0][2])
	assert.Equal(t, "ia", check.Rows[0][3])
	assert.Equal(t, "ib", check.Rows[0][4])
	assert.NotNil(t, check.Rows[0][5], "fact_embedding should remain present after inline vector procedure")
}

func TestBug9_UnwindRelationshipVectorProcedure_NArityMatrix_NoSilentDropsOrErrors(t *testing.T) {
	testCases := []struct {
		name         string
		rowCount     int
		includeTags  bool
		includeMeta  bool
		nestedVector bool
		// wantTypeError: a map (meta) or nested list (nested vector) row value is
		// not a property value on any SET route, so SET e = edge fails loudly
		// and nothing is written (no silent drop).
		wantTypeError bool
	}{
		{name: "1row_base", rowCount: 1},
		{name: "3rows_with_tags", rowCount: 3, includeTags: true},
		{name: "7rows_with_tags_and_meta", rowCount: 7, includeTags: true, includeMeta: true, wantTypeError: true},
		{name: "9rows_nested_vector_and_meta", rowCount: 9, includeMeta: true, nestedVector: true, wantTypeError: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			// Build per-row source/target endpoints so each row has a unique
			// relationship identity in this engine's MERGE path.
			for i := 0; i < tc.rowCount; i++ {
				_, err := exec.Execute(ctx,
					fmt.Sprintf(`CREATE (:Entity {uuid:'s%d'})`, i),
					nil,
				)
				require.NoError(t, err)
				_, err = exec.Execute(ctx,
					fmt.Sprintf(`CREATE (:Entity {uuid:'t%d'})`, i),
					nil,
				)
				require.NoError(t, err)
			}

			edges := make([]map[string]interface{}, 0, tc.rowCount)
			for i := 0; i < tc.rowCount; i++ {
				edge := map[string]interface{}{
					"source_node_uuid": fmt.Sprintf("s%d", i),
					"target_node_uuid": fmt.Sprintf("t%d", i),
					"uuid":             fmt.Sprintf("e%d", i),
					"name":             "DIRECTED",
					"fact":             fmt.Sprintf("fact-%d", i),
					"group_id":         fmt.Sprintf("g%d", i%2),
					"fact_embedding":   []interface{}{float64(i) + 0.1, float64(i) + 0.2, float64(i) + 0.3},
				}
				if tc.nestedVector {
					edge["fact_embedding"] = []interface{}{
						[]interface{}{float64(i) + 0.1, float64(i) + 0.2},
						[]interface{}{float64(i) + 0.3, float64(i) + 0.4},
					}
				}
				if tc.includeTags {
					edge["tags"] = []interface{}{fmt.Sprintf("tag-%d", i), "shared"}
				}
				if tc.includeMeta {
					edge["meta"] = map[string]interface{}{
						"ordinal": int64(i),
						"kind":    "edge",
					}
				}
				edges = append(edges, edge)
			}

			writeRes, err := exec.Execute(ctx, `
				UNWIND $entity_edges AS edge
				MATCH (source:Entity {uuid: edge.source_node_uuid})
				MATCH (target:Entity {uuid: edge.target_node_uuid})
				MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
				SET e = edge
				WITH e, edge
				CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", edge.fact_embedding)
				RETURN edge.uuid AS uuid
			`, map[string]interface{}{"entity_edges": edges})
			if tc.wantTypeError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "TypeError")
				countRes, err := exec.Execute(ctx, `MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS n`, nil)
				require.NoError(t, err)
				assert.Equal(t, int64(0), countRes.Rows[0][0], "a rejected statement writes no relationships")
				return
			}
			require.NoError(t, err, "matrix case should not fail at execution time")
			require.Len(t, writeRes.Rows, tc.rowCount, "each input row should return one uuid")

			countRes, err := exec.Execute(ctx, `MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS n`, nil)
			require.NoError(t, err)
			require.Len(t, countRes.Rows, 1)
			require.Len(t, countRes.Rows[0], 1)
			assert.Equal(t, int64(tc.rowCount), countRes.Rows[0][0], "relationship row cardinality must be preserved")

			for i := 0; i < tc.rowCount; i++ {
				inspect, err := exec.Execute(ctx, fmt.Sprintf(`
						MATCH ()-[e:RELATES_TO]->()
						WHERE e.uuid = 'e%d'
						RETURN e.name AS name, e.fact AS fact, e.group_id AS gid, e.source_node_uuid AS src, e.target_node_uuid AS dst, e.fact_embedding AS emb, e.tags AS tags, e.meta AS meta
					`, i), nil)
				require.NoError(t, err)
				require.Len(t, inspect.Rows, 1, "edge e%d should exist", i)
				row := inspect.Rows[0]
				require.Len(t, row, 8)

				assert.Equal(t, "DIRECTED", row[0], "edge e%d name should persist", i)
				assert.Equal(t, fmt.Sprintf("fact-%d", i), row[1], "edge e%d fact should persist", i)
				assert.Equal(t, fmt.Sprintf("g%d", i%2), row[2], "edge e%d group_id should persist", i)
				assert.Equal(t, fmt.Sprintf("s%d", i), row[3], "edge e%d source_node_uuid should persist", i)
				assert.Equal(t, fmt.Sprintf("t%d", i), row[4], "edge e%d target_node_uuid should persist", i)
				assert.NotNil(t, row[5], "edge e%d embedding should remain present", i)
				if tc.includeTags {
					assert.NotNil(t, row[6], "edge e%d tags should remain present", i)
				}
				if tc.includeMeta {
					assert.NotNil(t, row[7], "edge e%d meta should remain present", i)
				}
			}
		})
	}
}

func TestBug10_RelationshipVariableSurvivesWithProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:T {uuid:'a'})-[e:REL {uuid:'rel-1', name:'X', fact:'f'}]->(b:T {uuid:'b'})
	`, nil)
	require.NoError(t, err)

	propAfterWith, err := exec.Execute(ctx, `
		MATCH (n:T)-[e:REL]->(m:T)
		WITH e
		RETURN e.name AS a
	`, nil)
	require.NoError(t, err)
	require.Len(t, propAfterWith.Rows, 1)
	require.Len(t, propAfterWith.Rows[0], 1)
	require.Equal(t, "X", propAfterWith.Rows[0][0], "relationship property access after WITH should evaluate against bound relationship var")

	propAfterWithDistinct, err := exec.Execute(ctx, `
		MATCH (n:T)-[e:REL]->(m:T)
		WITH DISTINCT e
		RETURN e.name AS a
	`, nil)
	require.NoError(t, err)
	require.Len(t, propAfterWithDistinct.Rows, 1)
	require.Len(t, propAfterWithDistinct.Rows[0], 1)
	require.Equal(t, "X", propAfterWithDistinct.Rows[0][0], "DISTINCT should preserve relationship var binding")

	propertiesAfterWith, err := exec.Execute(ctx, `
		MATCH (n:T)-[e:REL]->(m:T)
		WITH e
		RETURN properties(e) AS a
	`, nil)
	require.NoError(t, err)
	require.Len(t, propertiesAfterWith.Rows, 1)
	require.Len(t, propertiesAfterWith.Rows[0], 1)
	props, ok := propertiesAfterWith.Rows[0][0].(map[string]interface{})
	require.True(t, ok, "properties(e) after WITH should return map, got %T", propertiesAfterWith.Rows[0][0])
	require.Equal(t, "X", props["name"])
	require.Equal(t, "f", props["fact"])
	require.Equal(t, "rel-1", props["uuid"])

	keysAfterWith, err := exec.Execute(ctx, `
		MATCH (n:T)-[e:REL]->(m:T)
		WITH e
		RETURN keys(e) AS a
	`, nil)
	require.NoError(t, err)
	require.Len(t, keysAfterWith.Rows, 1)
	require.Len(t, keysAfterWith.Rows[0], 1)
	keys, ok := keysAfterWith.Rows[0][0].([]interface{})
	require.True(t, ok, "keys(e) after WITH should return key list, got %T", keysAfterWith.Rows[0][0])
	assert.Contains(t, keys, "name")
	assert.Contains(t, keys, "fact")
	assert.Contains(t, keys, "uuid")
}
