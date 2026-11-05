package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// cleanupFailRemoteEngine uses the existing engine contract; this fixture
// only closes it and verifies that every cleanup failure survives.
type cleanupFailRemoteEngine struct {
	remoteCypherEngine
	count    *int
	closeErr error
}

func (e *cleanupFailRemoteEngine) Close() error {
	if e.count != nil {
		(*e.count)++
	}
	return e.closeErr
}

// RollbackParticipants reports EVERY participant rollback failure, not only
// the first one: both original errors stay reachable (errors.Is) from the
// returned error. Each participant is still rolled back exactly once, and the
// recorded per-participant outcome is unchanged.
func TestRollbackParticipantsRetainsEveryFailure(t *testing.T) {
	firstErr := errors.New("remote participant 'a' rollback failed")
	secondErr := errors.New("remote participant 'b' rollback failed")
	first := &countingRemoteTx{rollbackErr: firstErr}
	second := &countingRemoteTx{rollbackErr: secondErr}
	clean := &countingRemoteTx{}
	r := NewRemoteFragmentExecutor()
	r.txHandles["a"] = &remoteParticipant{tx: first}
	r.txHandles["b"] = &remoteParticipant{tx: second}
	r.txHandles["ok"] = &remoteParticipant{tx: clean}

	err := r.RollbackParticipants(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, firstErr, "the returned error must retain the first participant's failure")
	require.ErrorIs(t, err, secondErr, "the returned error must retain the second participant's failure")
	require.Equal(t, 1, first.rollbacks)
	require.Equal(t, 1, second.rollbacks)
	require.Equal(t, 1, clean.rollbacks)

	// The participants still report their own recorded outcome, once each.
	require.ErrorIs(t, r.txHandles["a"].end(context.Background(), false), firstErr)
	require.ErrorIs(t, r.txHandles["b"].end(context.Background(), false), secondErr)
	require.NoError(t, r.txHandles["ok"].end(context.Background(), false))
	require.Equal(t, 1, first.rollbacks)
	require.Equal(t, 1, second.rollbacks)
	require.Equal(t, 1, clean.rollbacks)
}
