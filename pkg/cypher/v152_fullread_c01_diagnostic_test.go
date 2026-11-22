package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestC01SingleLabelBatchCandidateOwner logs the property posting, label
// population, and selected batch candidate set at the consumer used by the
// single-label UNWIND relationship batch route.
func TestC01SingleLabelBatchCandidateOwner(t *testing.T) {
	for _, write := range []string{"CREATE", "MERGE"} {
		t.Run(write, func(t *testing.T) {
			ctx := context.Background()
			exec := ei4R5Exec(t,
				"CREATE INDEX c01_batch_k FOR (n:C01Batch) ON (n.k)",
				"CREATE (:C01Batch {k:'dup',id:'a'}),(:C01Batch {k:'dup',id:'b'}),(:C01Batch {k:'dup',id:'c'}),(:C01Batch {k:'target',id:'t'})")
			schema := exec.storage.GetSchema()
			ids, err := schema.PropertyIndexLookup(ctx, "C01Batch", "k", "dup")
			require.NoError(t, err)
			require.Len(t, ids, 3)
			for _, id := range ids[1:] {
				require.NoError(t, schema.PropertyIndexDelete("C01Batch", "k", id, "dup"))
			}
			posting, err := schema.PropertyIndexLookup(ctx, "C01Batch", "k", "dup")
			require.NoError(t, err)
			labelNodes, err := exec.storage.GetNodesByLabel(ctx, "C01Batch")
			require.NoError(t, err)

			rows := []map[string]interface{}{{"a": "dup", "b": "target"}}
			matches := []matchClauseSpec{
				{variable: "a", label: "C01Batch", propName: "k", rowField: "a"},
				{variable: "b", label: "C01Batch", propName: "k", rowField: "b"},
			}
			candidates, unique, err := exec.buildRelationshipBatchNodeMatchIndex(ctx, exec.storage, rows, matches)
			require.NoError(t, err)
			chosen := candidates[nodeBatchMatchKey{label: "C01Batch", prop: "k"}][propEqKeyBatch("dup")]
			chosenIDs := make([]string, 0, len(chosen))
			for _, node := range chosen {
				chosenIDs = append(chosenIDs, node.ID)
			}
			t.Logf("batch_consumer=buildRelationshipBatchNodeMatchIndex write=%s single_label=true index_posting_ids=%v label_scan_nodes=%d chosen_candidate_ids=%v unique=%v", write, posting, len(labelNodes), chosenIDs, unique)

			query := fmt.Sprintf("UNWIND $rows AS row MATCH (a:C01Batch {k:row.a}) MATCH (b:C01Batch {k:row.b}) %s (a)-[:C01BatchR {uuid:row.uuid}]->(b) RETURN count(*)", write)
			params := map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "dup", "b": "target", "uuid": write}}}
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err, query)
			t.Logf("batch_execute write=%s rows=%v trace=%+v", write, result.Rows, exec.LastHotPathTrace())
			require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows, "all three nodes carrying the single-label pattern must be candidates")
		})
	}
}
