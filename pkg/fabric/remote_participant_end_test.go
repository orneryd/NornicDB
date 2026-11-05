package fabric

import (
	"context"
	"errors"
	"testing"
	"time"

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

type stalledRemoteTx struct{ countingRemoteTx }

func (s *stalledRemoteTx) Rollback(ctx context.Context) error {
	s.rollbacks++
	<-ctx.Done()
	return ctx.Err()
}

// Remote participants that never answer their rollback are bounded by one
// deadline, not one each: RollbackParticipants returns by then, the other
// participants are still rolled back, and a stalled one records the deadline
// and ends once.
func TestRollbackParticipantsBoundedByDeadline(t *testing.T) {
	first, second, ok := &stalledRemoteTx{}, &stalledRemoteTx{}, &countingRemoteTx{}
	r := NewRemoteFragmentExecutor()
	r.txHandles["first"] = &remoteParticipant{tx: first}
	r.txHandles["second"] = &remoteParticipant{tx: second}
	r.txHandles["ok"] = &remoteParticipant{tx: ok}
	const deadline = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	require.ErrorContains(t, r.RollbackParticipants(ctx), "deadline")
	require.Less(t, time.Since(start), deadline*18/10, "the stalled participants wait out one deadline together")
	require.Equal(t, 1, ok.rollbacks)
	require.ErrorContains(t, r.txHandles["first"].end(context.Background(), false), "deadline")
	require.Equal(t, 1, first.rollbacks)
	require.Equal(t, 1, second.rollbacks)
}
