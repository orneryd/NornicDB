package cypher

import (
	"context"
	"fmt"
	"runtime/debug"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const rowCancellationMatches = `MATCH (u:UploadOccurrence {batch_id: $batch_id})
MATCH (v:Version {id: u.version_id})
MATCH (f:UploadOccurrence)-[:OF_VERSION]->(v)`

const rowCancellationQuery = rowCancellationMatches + `
WITH u, v, f ORDER BY f.uploaded_at, f.id
WITH u, v, collect(f.batch_id)[0] AS first_batch_id
RETURN u.id AS upload_id, u.member AS member, u.filename AS filename,
u.parent_occurrence_id AS parent_upload_id, u.document_id AS document_id,
u.version_id AS version_id, v.original_id AS original_id,
v.processing_status AS processing_status, v.processing_error_code AS code,
v.processing_error_message AS message, v.processing_next_action AS next_action,
v.processing_workflow_id AS processing_workflow_id, v.current_source_id AS source_id,
first_batch_id ORDER BY u.member, u.id`

func rowCancellationFixture(t *testing.T, perVersion int) *StorageExecutor {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for version := 0; version < 5; version++ {
		id := fmt.Sprintf("version-%02d", version)
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Version"}, Properties: map[string]interface{}{
			"id": id, "original_id": "original", "processing_status": "complete", "current_source_id": "source",
		}})
		require.NoError(t, err)
		for occurrence := 0; occurrence < perVersion; occurrence++ {
			uploadID := fmt.Sprintf("upload-%02d-%04d", version, occurrence)
			_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(uploadID), Labels: []string{"UploadOccurrence"}, Properties: map[string]interface{}{
				"id": uploadID, "batch_id": "batch", "version_id": id, "uploaded_at": int64(occurrence), "member": int64(version*perVersion + occurrence), "filename": "archive/member.txt", "document_id": "document",
			}})
			require.NoError(t, err)
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(uploadID), StartNode: storage.NodeID(uploadID), EndNode: storage.NodeID(id), Type: "OF_VERSION"}))
		}
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}

func TestMaterializedRowCancellationCanceledContext(t *testing.T) {
	executor := rowCancellationFixture(t, 1)
	for _, operator := range []string{"sort", "collect"} {
		t.Run(operator, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			ctx := withExpressionFailureSlot(parent)
			cancel()
			rows := []pipelineRow{{"x": int64(2)}, {"x": int64(1)}}
			if operator == "sort" {
				require.False(t, executor.orderPipelineRowsWithScopes(ctx, rows, rows, []orderByTerm{{column: "x"}}))
			} else {
				_, accepted := executor.evaluatePipelineAggregateWithContext(ctx, rows, "collect", "x", false)
				require.False(t, accepted)
			}
			require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
		})
	}
}

// This is the original 1.8M-intermediate-row ORDER BY + collect reproduction.
// A soft runtime target keeps the same GC pressure; it is not a query limit.
func TestMaterializedRowCancellationDeadline(t *testing.T) {
	executor := rowCancellationFixture(t, 600)
	previous := debug.SetMemoryLimit(256 << 20)
	defer debug.SetMemoryLimit(previous)
	const timeout = 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	started := time.Now()
	result, err := executor.Execute(ctx, rowCancellationQuery, map[string]interface{}{"batch_id": "batch"})
	elapsed := time.Since(started)
	t.Logf("1.8M intermediate rows: elapsed=%s deadline=%s error=%v", elapsed, timeout, err)
	require.Less(t, elapsed, timeout+2*time.Second, "materialized work continued after its deadline")
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	} else {
		// A faster machine may finish before the deadline; that is also valid.
		require.Len(t, result.Rows, 3000)
		for _, row := range result.Rows {
			require.Equal(t, "batch", row[len(row)-1])
		}
	}
}

// A growing captured population was encoded inside every scoped CALL DISTINCT
// key. Row checkpoints alone allowed a 1s deadline to take over 8s to return.
func TestScopedCallGrowingSurvivorsDeadline(t *testing.T) {
	executor := rowCancellationFixture(t, 1)
	const query = `UNWIND range(1,$n) AS reached
 WITH collect({target_id:toString(reached),survivor:{id:toString(reached)}}) AS survivors, {} AS holdings_of, {view:'current'} AS eff_view
 CALL (survivors,holdings_of,eff_view) {
  UNWIND [each IN survivors WHERE holdings_of[each.survivor.id] IS NULL | each.survivor] AS seed
  WITH DISTINCT seed,eff_view
  CALL (seed,eff_view) { RETURN [seed] AS seed_documents }
  RETURN collect([seed.id,[document IN seed_documents | document.id]]) AS documents_of
 }
 RETURN size(documents_of)`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	result, err := executor.Execute(ctx, query, map[string]interface{}{"n": int64(10000)})
	elapsed := time.Since(started)
	t.Logf("10,000 captured survivors: elapsed=%s error=%v", elapsed, err)
	require.Less(t, elapsed, 2*time.Second, "structural key traversal exceeded its deadline")
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	} else {
		require.Less(t, elapsed, time.Second, "an expired deadline must never report success")
		require.Equal(t, [][]interface{}{{int64(10000)}}, result.Rows)
	}
	// Also expire inside the actual key owner when a branch completes the
	// whole natural workload before one second. This still enters Execute.
	checked := &rowConsumerContext{Context: context.Background(), consumer: "cypherEquivalenceHashWithCheckpoint", deadline: true, after: 2, done: make(chan struct{})}
	_, err = executor.Execute(checked, query, map[string]interface{}{"n": int64(10000)})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, checked.checks)
	require.Less(t, time.Since(checked.triggered), 2*time.Second)
	result, err = executor.Execute(context.Background(), query, map[string]interface{}{"n": int64(8)})
	require.NoError(t, err, "server remains usable after cancellation")
	require.Equal(t, [][]interface{}{{int64(8)}}, result.Rows)
}
