package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestC71OrderedWithWindowCause reuses the closeout item 9 fixture and query.
func TestC71OrderedWithWindowCause(t *testing.T) {
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
	cases := []struct {
		name, query  string
		bypass       bool
		reads        int64
		orderedReads int
	}{
		{name: "with_as_is", query: window, reads: 108_000},
		{name: "with_bypass", query: window, bypass: true, reads: 4_144, orderedReads: 4_144},
		{name: "return_control", query: directWindow, reads: 4_144, orderedReads: 4_144},
	}
	var baseline *ExecuteResult
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diagnostic := &c71Diagnostic{bypass: tc.bypass, log: t.Logf}
			queryCtx := context.WithValue(ctx, c71DiagnosticKey{}, diagnostic)
			params := map[string]interface{}{"after": "", "limit": page}
			start := time.Now()
			result, err := exec.Execute(queryCtx, "PROFILE "+tc.query, params)
			elapsed := time.Since(start)
			require.NoError(t, err)
			plan, ok := result.Metadata["plan"].(*ExecutionPlan)
			require.True(t, ok, "profile plan: %T", result.Metadata["plan"])
			var candidateReads int64
			var walk func(*PlanOperator)
			walk = func(operator *PlanOperator) {
				if operator.OperatorType == "NodeByIdSeek" && operator.DBHitsObserved {
					candidateReads = max(candidateReads, operator.DBHits)
				}
				for _, child := range operator.Children {
					walk(child)
				}
			}
			walk(plan.Root)
			t.Logf("C71 profile rows=%d candidate_reads=%d total_db_hits=%d ordered_reads=%d elapsed_ms=%.3f plan=%+v", len(result.Rows), candidateReads, plan.TotalDBHits, diagnostic.reads, float64(elapsed.Microseconds())/1000, Neo4jPlanMap(plan, true))
			require.Len(t, result.Rows, page)
			require.Equal(t, tc.reads, candidateReads)
			require.Equal(t, tc.orderedReads, diagnostic.reads)
			if baseline == nil {
				baseline = result
			} else {
				require.Equal(t, baseline.Columns, result.Columns)
				require.Equal(t, baseline.Rows, result.Rows, "same projected values in the same sequence")
				t.Logf("C71 exact_rows_equal=true rows=%d first=%v last=%v", page, result.Rows[0], result.Rows[page-1])
			}
		})
	}
}
