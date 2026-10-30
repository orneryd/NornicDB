package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindCreatePureCreateDoesNotLabelScanExistingPopulation(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 64; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("existing-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id":   fmt.Sprintf("existing-%03d", i),
				"text": "old",
				"k":    int64(i),
			},
		})
		require.NoError(t, err)
	}

	rows := make([]interface{}, 16)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"id":   fmt.Sprintf("new-%03d", i),
			"text": "bulk text",
			"k":    int64(i),
		}
	}

	wrapped.reset()
	result, err := exec.Execute(ctx,
		"UNWIND $rows AS r CREATE (n:Doc {id: r.id, text: r.text, k: r.k})",
		map[string]interface{}{"rows": rows},
	)
	require.NoError(t, err)
	require.Equal(t, 16, result.Stats.NodesCreated)
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"pure UNWIND CREATE must not scan the existing :Doc population")
	require.Zero(t, wrapped.AllNodesCalls(),
		"pure UNWIND CREATE must not fall back to an all-node scan")
}

func TestUnwindMatchCreateSetFallbackUsesPropertyIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	for _, ddl := range []string{
		"CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)",
		"CREATE INDEX idx_bucket_id FOR (n:Bucket) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 96; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id":   fmt.Sprintf("doc-%03d", i),
				"text": "source",
			},
		})
		require.NoError(t, err)
		_, err = exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("bucket-%03d", i)),
			Labels: []string{"Bucket"},
			Properties: map[string]any{
				"id": fmt.Sprintf("bucket-%03d", i),
			},
		})
		require.NoError(t, err)
	}

	rows := make([]interface{}, 12)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"doc_id":    fmt.Sprintf("doc-%03d", i),
			"bucket_id": fmt.Sprintf("bucket-%03d", i),
			"event_id":  fmt.Sprintf("event-%03d", i),
			"k":         int64(i),
		}
	}

	wrapped.reset()
	result, err := exec.Execute(ctx, `
UNWIND $rows AS r
MATCH (d:Doc {id: r.doc_id})
MATCH (b:Bucket {id: r.bucket_id})
CREATE (e:Event {id: r.event_id})
CREATE (d)-[:HAS_EVENT]->(e)
CREATE (e)-[:IN_BUCKET]->(b)
SET e.k = r.k
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, 12, result.Stats.NodesCreated)
	require.Equal(t, 24, result.Stats.RelationshipsCreated)
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"UNWIND MATCH...CREATE/SET fallback must use indexed property lookups instead of scanning labels")
	require.Zero(t, wrapped.AllNodesCalls(),
		"UNWIND MATCH...CREATE/SET fallback must not fall back to all-node scans")
}

func TestIndexedMatchPropertyMapExpressionValueUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 128; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
				"k":  int64(i * 10),
			},
		})
		require.NoError(t, err)
	}

	wrapped.reset()
	result, err := exec.Execute(ctx,
		"MATCH (n:Doc {id: $i + 1}) RETURN n.k",
		map[string]interface{}{"i": int64(40)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(410)}}, result.Rows)
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"expression-valued pattern property equality must use the property index")
	require.Zero(t, wrapped.AllNodesCalls(),
		"expression-valued pattern property equality must not scan all nodes")
}

func TestIndexedMatchWhereExpressionValueUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 128; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
				"k":  int64(i * 10),
			},
		})
		require.NoError(t, err)
	}

	wrapped.reset()
	result, err := exec.Execute(ctx,
		"MATCH (n:Doc) WHERE n.id = $i + 1 RETURN n.k",
		map[string]interface{}{"i": int64(40)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(410)}}, result.Rows)
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"expression-valued WHERE equality must use the property index")
	require.Zero(t, wrapped.AllNodesCalls(),
		"expression-valued WHERE equality must not scan all nodes")
}

func TestUnwindIndexedMatchExpressionValueUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 128; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
				"k":  int64(i * 10),
			},
		})
		require.NoError(t, err)
	}

	ids := make([]interface{}, 8)
	for i := range ids {
		ids[i] = int64(i)
	}

	wrapped.reset()
	result, err := exec.Execute(ctx,
		"UNWIND $ids AS i MATCH (n:Doc {id: i + 0}) RETURN count(n)",
		map[string]interface{}{"ids": ids})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(len(ids))}}, result.Rows)
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"UNWIND expression-valued property equality must use the property index per row")
	require.Zero(t, wrapped.AllNodesCalls(),
		"UNWIND expression-valued property equality must not scan all nodes per row")
}

func TestUnwindMultiMatchCreateBatchMapExpressionLookupUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
			},
		})
		require.NoError(t, err)
	}

	rows := make([]interface{}, 8)
	for i := range rows {
		rows[i] = map[string]interface{}{"offset": int64(i)}
	}

	wrapped.reset()
	result, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (a:Doc {id: row.offset})
MATCH (b:Doc {id: row.offset + 1})
CREATE (a)-[:NEXT]->(b)
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, 8, result.Stats.RelationshipsCreated)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch,
		"expression-valued map-row lookup must stay on UnwindMultiMatchCreateBatch")
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"expression-valued map-row batch lookup must use the property index")
	require.Zero(t, wrapped.AllNodesCalls(),
		"expression-valued map-row batch lookup must not scan all nodes")
}

func TestUnwindMultiMatchCreateBatchScalarExpressionLookupUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%03d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
			},
		})
		require.NoError(t, err)
	}

	ids := make([]interface{}, 8)
	for i := range ids {
		ids[i] = int64(i)
	}

	wrapped.reset()
	result, err := exec.Execute(ctx, `
UNWIND $ids AS i
MATCH (a:Doc {id: i})
MATCH (b:Doc {id: i + 1})
CREATE (a)-[:NEXT]->(b)
`, map[string]interface{}{"ids": ids})
	require.NoError(t, err)
	require.Equal(t, 8, result.Stats.RelationshipsCreated)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch,
		"expression-valued scalar UNWIND lookup must stay on UnwindMultiMatchCreateBatch")
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"expression-valued scalar batch lookup must use the property index")
	require.Zero(t, wrapped.AllNodesCalls(),
		"expression-valued scalar batch lookup must not scan all nodes")
}

func TestUnwindRelationshipMergeBatchExpressionLookupUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_component_key FOR (n:Component) ON (n.key)", nil)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("component-%03d", i)),
			Labels: []string{"Component"},
			Properties: map[string]any{
				"key": int64(i),
			},
		})
		require.NoError(t, err)
	}

	rows := make([]interface{}, 8)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"offset":    int64(i),
			"uuid":      fmt.Sprintf("edge-%03d", i),
			"embedding": []float64{float64(i), 1, 0},
		}
	}

	wrapped.reset()
	result, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (left:Component {key: row.offset})
MATCH (right:Component {key: row.offset + 1})
MERGE (left)-[rel:DEPENDS_ON {uuid: row.uuid}]->(right)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.uuid AS uuid
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, len(rows), result.Stats.RelationshipsCreated)
	require.Len(t, result.Rows, len(rows))
	require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch,
		"expression-valued relationship batch lookup must stay on UnwindRelationshipMergeBatch")
	require.Zero(t, wrapped.GetNodesByLabelCalls(),
		"expression-valued relationship batch lookup must use the property index")
	require.Zero(t, wrapped.AllNodesCalls(),
		"expression-valued relationship batch lookup must not scan all nodes")
}

func BenchmarkIndexedMatchPropertyMapExpressionValue(b *testing.B) {
	engine := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(engine, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(b, err)
	for i := 0; i < 4096; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%05d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
				"k":  int64(i * 10),
			},
		})
		require.NoError(b, err)
	}

	query := "MATCH (n:Doc {id: $i + 1}) RETURN n.k"
	params := map[string]interface{}{"i": int64(0)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params["i"] = int64(i % 4095)
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("expected one indexed row, got %d", len(result.Rows))
		}
	}
}

func BenchmarkUnwindMultiMatchCreateBatchExpressionLookup(b *testing.B) {
	engine := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(engine, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)", nil)
	require.NoError(b, err)
	for i := 0; i < 1024; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%05d", i)),
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id": int64(i),
			},
		})
		require.NoError(b, err)
	}

	const batchSize = 256
	rows := make([]interface{}, batchSize)
	for i := range rows {
		rows[i] = map[string]interface{}{"offset": int64(i)}
	}
	query := `
UNWIND $rows AS row
MATCH (a:Doc {id: row.offset})
MATCH (b:Doc {id: row.offset + 1})
CREATE (a)-[:NEXT]->(b)
`
	params := map[string]interface{}{"rows": rows}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if result.Stats.RelationshipsCreated != batchSize {
			b.Fatalf("expected %d relationships, got %d", batchSize, result.Stats.RelationshipsCreated)
		}
		if !exec.LastHotPathTrace().UnwindMultiMatchCreateBatch {
			b.Fatal("expected UnwindMultiMatchCreateBatch")
		}
	}
}

func BenchmarkUnwindRelationshipMergeBatchExpressionLookup(b *testing.B) {
	engine := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(engine, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_component_key FOR (n:Component) ON (n.key)", nil)
	require.NoError(b, err)
	for i := 0; i < 1024; i++ {
		_, err := exec.storage.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("component-%05d", i)),
			Labels: []string{"Component"},
			Properties: map[string]any{
				"key": int64(i),
			},
		})
		require.NoError(b, err)
	}

	const batchSize = 256
	rows := make([]interface{}, batchSize)
	for i := range rows {
		rows[i] = map[string]interface{}{
			"offset":    int64(i),
			"uuid":      fmt.Sprintf("edge-%03d", i),
			"embedding": []float64{float64(i), 1, 0},
		}
	}
	query := `
UNWIND $rows AS row
MATCH (left:Component {key: row.offset})
MATCH (right:Component {key: row.offset + 1})
MERGE (left)-[rel:DEPENDS_ON {uuid: row.uuid}]->(right)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.uuid AS uuid
`
	params := map[string]interface{}{"rows": rows}
	result, err := exec.Execute(ctx, query, params)
	require.NoError(b, err)
	require.Equal(b, batchSize, result.Stats.RelationshipsCreated)
	require.True(b, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err = exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != batchSize {
			b.Fatalf("expected %d rows, got %d", batchSize, len(result.Rows))
		}
		if !exec.LastHotPathTrace().UnwindRelationshipMergeBatch {
			b.Fatal("expected UnwindRelationshipMergeBatch")
		}
	}
}
