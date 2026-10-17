package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type countingRemoteTx struct {
	commits, rollbacks int
	rollbackErr        error
}

func (c *countingRemoteTx) QueryCypher(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}
func (c *countingRemoteTx) Commit(context.Context) error { c.commits++; return nil }
func (c *countingRemoteTx) Rollback(context.Context) error {
	c.rollbacks++
	return c.rollbackErr
}

// A remote participant rolled back early (RollbackParticipants) ends once:
// its rollback error is recorded and reported by the transaction's later
// ROLLBACK and Close without a second rollback, and a COMMIT can't commit it.
func TestRemoteParticipantEndsOnce(t *testing.T) {
	ctx := context.Background()
	failing := &countingRemoteTx{rollbackErr: errors.New("connection reset")}
	ok := &countingRemoteTx{}
	r := NewRemoteFragmentExecutor()
	r.txHandles["a"] = &remoteParticipant{tx: failing}
	r.txHandles["b"] = &remoteParticipant{tx: ok}

	require.ErrorContains(t, r.RollbackParticipants(ctx), "connection reset")
	require.Equal(t, 1, failing.rollbacks)
	require.Equal(t, 1, ok.rollbacks)

	require.ErrorContains(t, r.txHandles["a"].end(ctx, false), "connection reset")
	require.NoError(t, r.txHandles["b"].end(ctx, false))
	require.ErrorContains(t, r.txHandles["b"].end(ctx, true), "rolled back")
	require.ErrorContains(t, r.Close(), "connection reset")
	require.Equal(t, 1, failing.rollbacks)
	require.Equal(t, 1, ok.rollbacks)
	require.Zero(t, ok.commits)
}
