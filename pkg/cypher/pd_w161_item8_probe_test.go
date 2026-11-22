package cypher

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const pdW161StoredWhere = "NOT coalesce(node.search_hidden, false)"
const pdW161DirectWhere = "NOT (EXISTS { MATCH (:PDW161Document {id: node.document_id})-[:PDW161_MEMBER]->(:PDW161Collection) } AND NOT EXISTS { MATCH (:PDW161Document {id: node.document_id})-[:PDW161_MEMBER]->(k:PDW161Collection) WHERE NOT coalesce(k.archived, false) })"

func TestPDW161Item8TypedTraversalInRetainedWhere(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	service := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = service.Close() })
	exec.SetSearchService(service)
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
	type outcome struct {
		pages, rows int
		total       time.Duration
		pageTimes   []time.Duration
	}
	measure := func(label, where string) outcome {
		out := outcome{}
		request := "CALL db.rretrieve({query:'quokka',types:['PDW161SearchSource'],n:20,limit:100,where:$where,minRRFScore:0}) YIELD page RETURN page"
		params := map[string]interface{}{"where": where}
		for {
			start := time.Now()
			result, err := exec.Execute(ctx, "PROFILE "+request, params)
			elapsed := time.Since(start)
			require.NoError(t, err, label)
			out.total += elapsed
			out.pageTimes = append(out.pageTimes, elapsed)
			page := retainedWherePageFromMap(t, result.Rows[0][0])
			out.rows += len(page.ids)
			out.pages++
			if out.pages == 1 {
				t.Logf("PD_W161_CAUSE item=8 form=%s rows=%d profile=%+v", label, len(page.ids), result.Metadata["plan"])
			}
			if !page.more {
				break
			}
			require.Less(t, out.pages, 5000, "retained where paging did not terminate")
			request = "CALL db.rretrieve({qid:$qid,n:20}) YIELD page RETURN page"
			params = map[string]interface{}{"qid": page.qid}
		}
		sort.Slice(out.pageTimes, func(i, j int) bool { return out.pageTimes[i] < out.pageTimes[j] })
		t.Logf("PD_W161_RESULT item=8 form=%s docs=%d links_per_doc=%d rows=%d pages=%d first_ms=%.3f median_ms=%.3f max_ms=%.3f total_ms=%.3f",
			label, documents, degree, out.rows, out.pages,
			float64(out.pageTimes[0].Microseconds())/1000,
			float64(out.pageTimes[len(out.pageTimes)/2].Microseconds())/1000,
			float64(out.pageTimes[len(out.pageTimes)-1].Microseconds())/1000,
			float64(out.total.Microseconds())/1000)
		return out
	}
	stored := measure("stored_verdict", pdW161StoredWhere)
	direct := measure("typed_one_hop_retained_where", pdW161DirectWhere)
	require.Equal(t, stored.rows, direct.rows, "stored and direct rules must return the same candidate set")
}
