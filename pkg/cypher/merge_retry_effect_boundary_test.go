// Author: Claude, NOR-180 no commit work goes unjudged, 2026-10-31.
package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A lost MERGE race on COMMIT, through the executor's one decision: commit work that no
// running MERGE statement added makes the failure hard, whenever it was added.
func TestMergeRetryJudgesAllCommitWork(t *testing.T) {
	const merge = "MERGE (n {id: 'claimed'}) SET n:RaceDoc, n.by = 'x'"
	metadata := func(tx *storage.BadgerTransaction) error {
		return tx.SetMetadata(map[string]interface{}{"later": true})
	}
	image := func(tx *storage.BadgerTransaction) error {
		_, err := tx.StoredNodeImage(&storage.Node{ID: "image", Labels: []string{"A"}, Properties: map[string]interface{}{"mergeRetryNeverSeenKey": 1}, NamedEmbeddings: map[string][]float32{}})
		return err
	}
	const encoder = "mergeretry.encodeimage"
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{Name: encoder, Mode: ProcedureModeRead},
		func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
			wrapper, ok := exec.getStorage(ctx).(*transactionStorageWrapper)
			if !ok {
				t.Error("the procedure ran outside the transaction")
				return &ExecuteResult{}, nil
			}
			return &ExecuteResult{}, image(wrapper.tx)
		}))
	for _, tc := range []struct {
		name      string
		beginMeta map[string]any
		between   func(*storage.BadgerTransaction) error
		read      string
		last      func(*storage.BadgerTransaction) error
		want      string
	}{
		{name: "control", read: "RETURN 1", want: nornicerrors.TransientOutdated},
		{name: "begin_metadata_is_no_statements", beginMeta: map[string]any{"client": "begin"}, read: "RETURN 1", want: nornicerrors.TransientOutdated},
		{name: "metadata_between_statements", between: metadata, read: "RETURN 1", want: nornicerrors.ConstraintValidationFailed},
		{name: "metadata_after_the_last_statement", last: metadata, want: nornicerrors.ConstraintValidationFailed},
		{name: "image_dictionary_work_between_statements", between: image, read: "RETURN 1", want: nornicerrors.ConstraintValidationFailed},
		{name: "image_dictionary_work_after_the_last_statement", last: image, want: nornicerrors.ConstraintValidationFailed},
		{name: "image_dictionary_work_inside_a_read_procedure", read: "CALL " + encoder + "()", want: nornicerrors.ConstraintValidationFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
			loser, peer := NewStorageExecutor(engine), NewStorageExecutor(engine)
			ctx := context.Background()
			_, err := peer.Execute(ctx, "CREATE CONSTRAINT race_id FOR (n:RaceDoc) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)
			beginCtx := ctx
			if tc.beginMeta != nil {
				beginCtx = WithTransactionMetadata(ctx, tc.beginMeta)
			}
			_, err = loser.Execute(beginCtx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = loser.Execute(ctx, "ROLLBACK", nil) })
			_, err = loser.Execute(ctx, merge, nil)
			require.NoError(t, err)
			tx := loser.txContext.tx.(*storage.BadgerTransaction)
			if tc.between != nil {
				require.NoError(t, tc.between(tx))
			}
			if tc.read != "" {
				_, err = loser.Execute(ctx, tc.read, nil)
				require.NoError(t, err)
			}
			if tc.last != nil {
				require.NoError(t, tc.last(tx))
			}
			_, err = peer.Execute(ctx, merge, nil)
			require.NoError(t, err)
			_, err = loser.Execute(ctx, "COMMIT", nil)
			require.Error(t, err, "the later commit loses the race")
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, tc.want, code, "commit error: %v", err)
		})
	}
}
