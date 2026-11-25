package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPDW161Item9TwoLabelOrderedWindow(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err := exec.Execute(ctx, "CREATE INDEX pd_w161_node_id IF NOT EXISTS FOR (n:PDW161GraphNode) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX pd_w161_doc_id IF NOT EXISTS FOR (n:PDW161Document) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX pd_w161_entity_doc IF NOT EXISTS FOR (n:PDW161Entity) ON (n.document_id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:PDW161Collection {id:'c1'}), (:PDW161Collection {id:'c2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (c1:PDW161Collection {id:'c1'}), (c2:PDW161Collection {id:'c2'})
		UNWIND range(0,3999) AS i
		WITH c1,c2,i,'d'+toString(100000+i) AS id
		CREATE (d:PDW161GraphNode:PDW161Document {id:id})-[:PDW161_MEMBER]->(c1)
		FOREACH (_ IN CASE WHEN i % 3 = 0 THEN [1] ELSE [] END | CREATE (d)-[:PDW161_MEMBER]->(c2))`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `UNWIND range(0,47999) AS i
		WITH i,'d'+toString(100000+i/12) AS d
		CREATE (:PDW161GraphNode:PDW161Entity {id:'m'+toString(1000000+i),document_id:d})
		CREATE (:PDW161GraphNode:PDW161Point {id:'p'+toString(1000000+i),document_id:d})
		FOREACH (_ IN CASE WHEN i % 6 = 0 THEN [1] ELSE [] END |
			CREATE (:PDW161GraphNode:PDW161Entity:PDW161Identity {id:'q'+toString(1000000+i),document_id:d}))`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL db.awaitIndexes()", nil)
	require.NoError(t, err)
	const page = 144
	window := `MATCH (m:PDW161GraphNode:PDW161Entity)
		WHERE m.id > $after AND NOT m:PDW161Identity
		WITH m ORDER BY m.id LIMIT $limit
		MATCH (d:PDW161Document {id:m.document_id}) RETURN m.id AS id`
	directWindow := `MATCH (m:PDW161GraphNode:PDW161Entity)
		WHERE m.id > $after AND NOT m:PDW161Identity
		RETURN m.id AS id ORDER BY m.id LIMIT $limit`
	control := `MATCH (d:PDW161Document)
		WHERE d.id > $after WITH d ORDER BY d.id LIMIT 12
		MATCH (m:PDW161Entity {document_id:d.id})
		WHERE NOT m:PDW161Identity RETURN m.id AS id`
	var windowRows, directRows [][]interface{}
	var windowHits int64
	for _, tc := range []struct{ name, query string }{{"two_label_mention_window", window}, {"direct_ordered_index_window_control", directWindow}, {"document_page_then_mentions_control", control}} {
		var elapsed time.Duration
		var got int
		params := map[string]interface{}{"after": "", "limit": page}
		for rep := 0; rep < 3; rep++ {
			start := time.Now()
			res, err := exec.Execute(ctx, "PROFILE "+tc.query, params)
			elapsed += time.Since(start)
			require.NoError(t, err, tc.name)
			got = len(res.Rows)
			if rep == 0 {
				plan, ok := res.Metadata["plan"].(*ExecutionPlan)
				require.True(t, ok, "profile plan: %T", res.Metadata["plan"])
				switch tc.name {
				case "two_label_mention_window":
					windowRows, windowHits = res.Rows, plan.TotalDBHits
				case "direct_ordered_index_window_control":
					directRows = res.Rows
				}
				t.Logf("PD_W161_CAUSE item=9 shape=%s page=%d rows=%d plan=%+v", tc.name, page, got, Neo4jPlanMap(plan, true))
			}
		}
		t.Logf("PD_W161_RESULT item=9 shape=%s nodes=48000 repeats=3 rows=%d elapsed_ms=%.3f", tc.name, got, float64(elapsed.Microseconds())/3000)
	}
	require.Equal(t, directRows, windowRows, "WITH window must retain the terminal control's ordered rows")
	require.Less(t, windowHits, int64(5_000), "WITH window should read about one ordered page, not the full label")
}
