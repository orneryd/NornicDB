package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type rejectingSuppressionEngine struct {
	storage.Engine
	cause error
}

func (e *rejectingSuppressionEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *rejectingSuppressionEngine) SetManualSuppression(storage.StoredEntityKind, string, bool, ...uint64) error {
	return e.cause
}

// The fixture must implement the capability as the executor looks it up;
// otherwise the lookup passes it by and the failure is never injected.
var _ storage.ManualSuppressionEngine = (*rejectingSuppressionEngine)(nil)

func TestCommittedActionsPreserveEveryFailureAndRunOnce(t *testing.T) {
	first, last := errors.New("first completion failure"), errors.New("last completion failure")
	calls := []int{}
	wrapper := &transactionStorageWrapper{commitActions: []func() error{
		func() error { calls = append(calls, 1); return first },
		func() error { calls = append(calls, 2); return nil },
		func() error { calls = append(calls, 3); return last },
	}}
	err := wrapper.completeCommitActions()
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, last)
	require.Equal(t, []int{1, 2, 3}, calls)
	require.NoError(t, wrapper.completeCommitActions())
	require.Equal(t, []int{1, 2, 3}, calls)
	require.Empty(t, wrapper.commitActions)
}

func TestSuppressionCompletionFailureReachesPublicCaller(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit", true: "explicit"}[explicit], func(t *testing.T) {
			inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "completion")
			_, err := inner.CreateNode(&storage.Node{ID: "node", Labels: []string{"Completion"}})
			require.NoError(t, err)
			cause := errors.New("suppression persistence rejected")
			executor := NewStorageExecutor(&rejectingSuppressionEngine{Engine: inner, cause: cause})
			ctx := context.Background()
			if explicit {
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err = executor.Execute(ctx, "MATCH (n:Completion) CALL kp.suppress(n)", nil)
			if explicit {
				require.NoError(t, err)
				_, err = executor.Execute(ctx, "COMMIT", nil)
			}
			require.ErrorIs(t, err, cause)
			require.False(t, executor.HasActiveTransaction())
			observer := NewStorageExecutor(inner)
			rows, err := observer.Execute(ctx, "MATCH (n:Completion) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, rows.Rows)
		})
	}
}
