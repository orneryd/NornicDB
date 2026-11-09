package bolt

import (
	"bytes"
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This drives the real RUN handler over its existing executor/connection
// boundary. The executor's accepted result/error is fixed before cancellation;
// retry-metadata preparation must not replace it or claim a retry-safe write.
// It is a protocol-handler component test, not a native commit/wire race.
func TestExplicitRunRetryMetadataKeepsAcceptedOutcome(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		for _, rejected := range []bool{false, true} {
			name := "healthy/accepted"
			if canceled {
				name = "canceled/accepted"
			}
			if rejected {
				name += "-failure"
			}
			t.Run(name, func(t *testing.T) {
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				executor := &mockTransactionalExecutor{}
				conn := &mockConn{}
				session := newTestSession(conn, executor)
				session.baseExec = executor
				session.inTransaction = true
				require.NoError(t, session.txLifecycle.begin(parent, testTxOwner(), 0, "", executor, nil, nil))
				t.Cleanup(func() { require.NoError(t, session.handleReset(nil)) })
				result := &QueryResult{Columns: []string{"n"}, Rows: [][]any{{int64(1)}}}
				failure := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "RetryMetadata", Properties: []string{"id"}}
				calls := 0
				executor.executeFunc = func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
					calls++
					require.NoError(t, ctx.Err(), "execution is admitted before real RUN cancellation")
					if canceled {
						session.cancelActiveRun(0)
						require.ErrorIs(t, ctx.Err(), context.Canceled)
						require.NoError(t, parent.Err(), "RUN cancellation must not end the explicit transaction parent")
					}
					if rejected {
						return nil, failure
					}
					return result, nil
				}
				require.NoError(t, session.handleRun(buildRunMessageData("MERGE (n:RetryMetadata {id:1}) RETURN n", nil, nil)))
				require.Equal(t, 1, calls)
				require.False(t, executor.rollbackCalled, "metadata cannot roll back accepted execution")
				require.False(t, executor.commitCalled, "metadata does not commit")
				require.Equal(t, canceled, session.txHasNonMergeWrite, "canceled classification must fail closed for later retry eligibility")
				if canceled {
					require.False(t, session.txHasMerge)
					require.Empty(t, session.txWriteInfos)
					require.Empty(t, session.txMergeStatements)
					require.False(t, session.canRetryMergeCommitConflict(t.Context(), failure), "a new healthy COMMIT context cannot upgrade unproved prior metadata")
				} else {
					require.True(t, session.txHasMerge)
					require.Len(t, session.txWriteInfos, 1)
					require.Len(t, session.txMergeStatements, 1)
					require.True(t, session.canRetryMergeCommitConflict(t.Context(), failure))
				}
				if rejected {
					require.Nil(t, session.lastResult)
					require.Contains(t, string(conn.writeData), failure.Error(), "RUN retains its original execution cause")
					require.False(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}))
				} else {
					require.Same(t, result, session.lastResult)
					require.True(t, session.pendingFlush)
					require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}), "accepted RUN result stays SUCCESS after metadata cancellation")
				}
			})
		}
	}
}
