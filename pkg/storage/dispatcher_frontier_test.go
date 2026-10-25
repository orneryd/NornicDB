package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDispatcherSnapshotClockRollbackCannotLoseWrite(t *testing.T) {
	b := writeBehindOrderEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:clock", Properties: map[string]any{"cnt": int64(0)}})
	require.NoError(t, err)
	state, err := b.namespaceMVCC("test")
	require.NoError(t, err)
	// A snapshot must still reject a later writer when wall time goes back.
	state.highWaterNanos.Store(time.Now().Add(time.Hour).UnixNano())
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	n, err := tx.GetNode("test:clock")
	require.NoError(t, err)
	require.NoError(t, b.UpdateNode(&Node{ID: n.ID, Properties: map[string]any{"cnt": int64(1)}}))
	n.Properties["cnt"] = int64(1)
	require.NoError(t, tx.UpdateNode(n))
	require.ErrorIs(t, tx.Commit(), ErrConflict, "a newer sequence must reject the stale increment")
}

func TestDispatcherFlushCapturesEntryFrontier(t *testing.T) {
	dispatcherProbe(t, func() {
		b := writeBehindOrderEngine(t)
		b.writeBehind.Close()
		entered, release := make(chan struct{}), make(chan struct{})
		b.OnNodeCreated(func(n *Node) {
			if n.ID == "test:first" {
				close(entered)
				<-release
				require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error {
					_, err := tx.CreateNode(&Node{ID: "test:successor"})
					return err
				}))
			}
		})
		created := make(chan error, 1)
		go func() { _, err := b.CreateNode(&Node{ID: "test:first"}); created <- err }()
		<-entered
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error {
			_, err := tx.CreateNode(&Node{ID: "test:pending"})
			return err
		}))
		applying := make(chan struct{})
		apply := b.writeBehind.apply
		b.writeBehind.apply = func(g *CommitBuffer) error {
			close(applying)
			return apply(g)
		}
		done := make(chan error, 1)
		go func() { done <- b.FlushWriteBehind() }()
		// The callback creates an unlanded successor after Flush has entered.
		// A dispatcher-owned barrier never extends through that successor.
		<-applying
		close(release)
		require.NoError(t, <-done)
		require.NoError(t, <-created)
		b.writeBehind.apply = apply
	})
}
