// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package bolt

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAcceptedOutcomeCannotBecomeBoltMergeRetry(t *testing.T) {
	cause := nornicerrors.MarkMergeCommitTimeUniqueConflict(&storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "Doc", Properties: []string{"id"}})
	preacceptCode, _ := mapBoltQueryErrorForQuery(cause, "MERGE (:Doc {id: 1})")
	require.Equal(t, nornicerrors.TransientOutdated, preacceptCode)
	receipt, owner := storage.NewCommitReceipt(nil)
	require.NoError(t, owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: cause}))
	err := receipt.WaitOutcome(t.Context()).Err
	code, _ := mapBoltQueryErrorForQuery(err, "MERGE (:Doc {id: 1})")
	require.Equal(t, nornicerrors.TransactionOutcomeUnknown, code)
	require.False(t, isRetryableBoltStatus(code))
	var violation *storage.ConstraintViolationError
	require.NotErrorAs(t, err, &violation)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.True(t, nornicerrors.IsMergeCommitTimeUniqueConflict(accepted.Cause()))
	require.ErrorAs(t, accepted.Cause(), &violation)
}
