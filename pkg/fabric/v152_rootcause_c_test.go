package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCRemoteAlreadyEndedRollbackResurfaces: a remote participant whose
// server already ended its transaction answers the eager rollback
// (RollbackParticipants, called from TransactionContext.fail) with
// TransactionNotFound. end() records that as the participant's terminal
// result (remote_executor.go:105-147) and Close wraps the recorded result
// again at :290 without sending a second rollback. The healthy participant
// control closes without error.
func TestV152RCCRemoteAlreadyEndedRollbackResurfaces(t *testing.T) {
	ctx := context.Background()
	ended := &countingRemoteTx{rollbackErr: errors.New("Neo.ClientError.Transaction.TransactionNotFound: transaction not found")}
	healthy := &countingRemoteTx{}

	control := NewRemoteFragmentExecutor()
	control.txHandles["remote:comp.one"] = &remoteParticipant{tx: healthy}
	require.NoError(t, control.RollbackParticipants(ctx))
	controlClose := control.Close()
	t.Logf("RCC_REMOTE_CONTROL owner=pkg/fabric/remote_executor.go:286-292 rollbacks=%d close_error=%v", healthy.rollbacks, controlClose)
	require.NoError(t, controlClose)

	r := NewRemoteFragmentExecutor()
	r.txHandles["remote:comp.two"] = &remoteParticipant{tx: ended}
	eager := r.RollbackParticipants(ctx)
	closeErr := r.Close()
	t.Logf("RCC_REMOTE_TRIGGER owner=pkg/fabric/remote_executor.go:105-147,286-292 eager_rollback_error=%v rollbacks_sent=%d close_error=%v", eager, ended.rollbacks, closeErr)
	require.Equal(t, 1, ended.rollbacks, "Close sends no second rollback; it replays the recorded result")
	assert.NoError(t, closeErr, "a participant the remote already ended must not add a cleanup failure at Close")
}
