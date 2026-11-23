package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const pdW161StoredWhere = "NOT coalesce(node.search_hidden, false)"
const pdW161DirectWhere = "NOT (EXISTS { MATCH (:PDW161Document {id: node.document_id})-[:PDW161_MEMBER]->(:PDW161Collection) } AND NOT EXISTS { MATCH (:PDW161Document {id: node.document_id})-[:PDW161_MEMBER]->(k:PDW161Collection) WHERE NOT coalesce(k.archived, false) })"

func TestPDW161Item8TypedTraversalInRetainedWhere(t *testing.T) {
	runPDW161Item8(t, "none", nil)
}

// Same fixture with the document key indexed, then with a unique constraint
// on it (declared before the data, as an application schema would be).
func TestPDW161Item8IndexedTypedTraversalInRetainedWhere(t *testing.T) {
	runPDW161Item8(t, "range_index", []string{"CREATE INDEX pd_w161_item8_doc_id IF NOT EXISTS FOR (n:PDW161Document) ON (n.id)"})
}

func TestPDW161Item8UniqueTypedTraversalInRetainedWhere(t *testing.T) {
	runPDW161Item8(t, "unique_constraint", []string{"CREATE CONSTRAINT pd_w161_item8_doc_id_unique IF NOT EXISTS FOR (n:PDW161Document) REQUIRE n.id IS UNIQUE"})
}

// pdW161PlanTotals sums DB hits over a PROFILE tree and lists its operator types.
func pdW161PlanTotals(op *PlanOperator, hits *int64, ops map[string]int64) {
	if op == nil {
		return
	}
	*hits += op.DBHits
	ops[op.OperatorType] += op.DBHits
	for _, child := range op.Children {
		pdW161PlanTotals(child, hits, ops)
	}
}

func pdW161OpsString(ops map[string]int64) string {
	names := make([]string, 0, len(ops))
	for name := range ops {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s:%d", name, ops[name]))
	}
	return strings.Join(parts, ",")
}

func runPDW161Item8(t *testing.T, schemaLabel string, schema []string) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	service := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = service.Close() })
	exec.SetSearchService(service)
	for _, statement := range schema {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	_, err := exec.Execute(ctx, "CREATE (:PDW161Collection {id:'open',archived:false}), (:PDW161Collection {id:'closed',archived:true})", nil)
	require.NoError(t, err)
	const documents, degree, batch = 2_000, 200, 100
	for from := 1; from <= documents; from += batch {
		to := from + batch - 1
		if to > documents {
			to = documents
		}
		_, err = exec.Execute(ctx, `UNWIND range($from,$to) AS i
			CREATE (d:PDW161GraphNode:PDW161Document {id:'d'+toString(i)})
			CREATE (:PDW161Version {id:'v'+toString(i),document_id:'d'+toString(i)})
			CREATE (:PDW161SearchSource {id:'s'+toString(i),document_id:'d'+toString(i),
				search_hidden:i % 3 = 1 AND i % 9 <> 4,content:'quokka report '+toString(i),view_text:'quokka report '+toString(i)})
			WITH d,i UNWIND range(1,$degree) AS j
			CREATE (d)-[:PDW161_RELATION {kind:'mentions'}]->(:PDW161GraphNode {id:'x'+toString(i)+'-'+toString(j)})`,
			map[string]interface{}{"from": from, "to": to, "degree": degree})
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `MATCH (open:PDW161Collection {id:'open'}),(closed:PDW161Collection {id:'closed'})
			UNWIND range($from,$to) AS i MATCH (d:PDW161Document {id:'d'+toString(i)})
			FOREACH (_ IN CASE WHEN i % 3 = 0 OR i % 9 = 4 THEN [1] ELSE [] END | CREATE (d)-[:PDW161_MEMBER]->(open))
			FOREACH (_ IN CASE WHEN i % 3 = 1 THEN [1] ELSE [] END | CREATE (d)-[:PDW161_MEMBER]->(closed))`,
			map[string]interface{}{"from": from, "to": to})
		require.NoError(t, err)
	}
	require.NoError(t, service.BuildIndexes(ctx))
	if len(schema) > 0 {
		shown, err := exec.Execute(ctx, "SHOW INDEXES", nil)
		require.NoError(t, err)
		t.Logf("PD_W161_SCHEMA item=8 schema=%s show_indexes_columns=%v rows=%v", schemaLabel, shown.Columns, shown.Rows)
	}
	// Controls outside the retained where: a plain keyed lookup and an
	// ordinary correlated EXISTS with the same key, on the same data.
	for _, control := range []struct{ label, query string }{
		{"plain_keyed_lookup", "PROFILE MATCH (d:PDW161Document {id:'d1000'}) RETURN d.id"},
		{"correlated_exists_match", "PROFILE MATCH (s:PDW161SearchSource) WHERE s.id IN ['s3','s4','s5','s6','s7'] AND EXISTS { MATCH (:PDW161Document {id: s.document_id})-[:PDW161_MEMBER]->(:PDW161Collection) } RETURN s.id"},
	} {
		start := time.Now()
		result, err := exec.Execute(ctx, control.query, nil)
		elapsed := time.Since(start)
		require.NoError(t, err, control.label)
		plan, ok := result.Metadata["plan"].(*ExecutionPlan)
		require.True(t, ok, "profile plan: %T", result.Metadata["plan"])
		var hits int64
		ops := map[string]int64{}
		pdW161PlanTotals(plan.Root, &hits, ops)
		t.Logf("PD_W161_CONTROL item=8 schema=%s form=%s rows=%d ms=%.3f db_hits=%d operators=%s", schemaLabel, control.label, len(result.Rows), float64(elapsed.Microseconds())/1000, hits, pdW161OpsString(ops))
	}
	type outcome struct {
		pages, rows int
		total       time.Duration
		first       time.Duration
		pageTimes   []time.Duration
		ids         []string
		firstHits   int64
		totalHits   int64
		ops         map[string]int64
	}
	measure := func(label, where string) outcome {
		out := outcome{ops: map[string]int64{}}
		request := "CALL db.rretrieve({query:'quokka',types:['PDW161SearchSource'],n:20,limit:100,where:$where,minRRFScore:0}) YIELD page RETURN page"
		params := map[string]interface{}{"where": where}
		for {
			start := time.Now()
			result, err := exec.Execute(ctx, "PROFILE "+request, params)
			elapsed := time.Since(start)
			require.NoError(t, err, label)
			if out.pages == 0 {
				out.first = elapsed
			}
			out.total += elapsed
			out.pageTimes = append(out.pageTimes, elapsed)
			if plan, ok := result.Metadata["plan"].(*ExecutionPlan); ok {
				var hits int64
				pdW161PlanTotals(plan.Root, &hits, out.ops)
				if out.pages == 0 {
					out.firstHits = hits
				}
				out.totalHits += hits
			}
			page := retainedWherePageFromMap(t, result.Rows[0][0])
			out.rows += len(page.ids)
			out.ids = append(out.ids, page.ids...)
			out.pages++
			if out.pages == 1 {
				plan, ok := result.Metadata["plan"].(*ExecutionPlan)
				require.True(t, ok, "profile plan: %T", result.Metadata["plan"])
				t.Logf("PD_W161_CAUSE item=8 schema=%s form=%s rows=%d profile=%+v", schemaLabel, label, len(page.ids), Neo4jPlanMap(plan, true))
			}
			if !page.more {
				break
			}
			require.Less(t, out.pages, 5000, "retained where paging did not terminate")
			request = "CALL db.rretrieve({qid:$qid,n:20}) YIELD page RETURN page"
			params = map[string]interface{}{"qid": page.qid}
		}
		sort.Slice(out.pageTimes, func(i, j int) bool { return out.pageTimes[i] < out.pageTimes[j] })
		t.Logf("PD_W161_RESULT item=8 schema=%s form=%s docs=%d links_per_doc=%d rows=%d pages=%d first_page_ms=%.3f median_page_ms=%.3f max_page_ms=%.3f total_ms=%.3f first_page_db_hits=%d all_pages_db_hits=%d operators_db_hits=%s",
			schemaLabel, label, documents, degree, out.rows, out.pages,
			float64(out.first.Microseconds())/1000,
			float64(out.pageTimes[len(out.pageTimes)/2].Microseconds())/1000,
			float64(out.pageTimes[len(out.pageTimes)-1].Microseconds())/1000,
			float64(out.total.Microseconds())/1000, out.firstHits, out.totalHits, pdW161OpsString(out.ops))
		return out
	}
	stored := measure("stored_verdict", pdW161StoredWhere)
	direct := measure("typed_one_hop_retained_where", pdW161DirectWhere)
	require.Equal(t, stored.rows, direct.rows, "stored and direct rules must return the same candidate set")
	sort.Strings(stored.ids)
	sort.Strings(direct.ids)
	require.Equal(t, stored.ids, direct.ids, "stored and direct rules must return the same source IDs")
	t.Logf("PD_W161_RATIO item=8 schema=%s total_direct_over_stored=%.1f first_page_direct_over_stored=%.1f",
		schemaLabel, float64(direct.total)/float64(stored.total), float64(direct.first)/float64(stored.first))
}
