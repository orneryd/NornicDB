package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

// Buffered replay must retain the same accounting-before-listeners boundary
// as an immediate commit, even when the publication callback drives the flush.
func TestBufferedCommitPublicationBeforeReplayListener(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	// Control exactly where replay runs without changing the public commit
	// or flush paths; this is the listener-order fixture's stopped-worker mode.
	engine.writeBehind.Close()
	published := false
	var listenerPublication []bool
	engine.OnNodeCreated(func(node *Node) {
		if node.ID == "test:publication" {
			listenerPublication = append(listenerPublication, published)
		}
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "test:publication", Properties: map[string]any{"v": int64(7)}})
	require.NoError(t, err)
	var flushErr error
	require.NoError(t, tx.PublishCommit(context.Background(), CommitPublication{AfterPublication: func() {
		flushErr = engine.FlushWriteBehind()
		published = true
	}}))
	require.NoError(t, flushErr)
	require.True(t, tx.AcknowledgedBuffered(), "the reproduction must use buffered admission")
	require.Equal(t, []bool{true}, listenerPublication, "accounting/admission publication completes before replay listeners")
	stored, err := engine.GetStoredNode("test:publication")
	require.NoError(t, err)
	require.Equal(t, int64(7), stored.Properties["v"])
}

// Admission failure publishes neither data nor accounting, and must not leave
// a reservation that prevents the next accepted write's listeners from running.
func TestRejectedBufferedCommitAllowsLaterPublication(t *testing.T) {
	policy := memory.NewPolicy(256<<10, 0, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	engine := newWriteBehindTestEngine(t, time.Hour)
	engine.writeBehind.Close()
	engine.memoryPolicy = policy
	var observed []NodeID
	published := false
	engine.OnNodeCreated(func(node *Node) {
		require.True(t, published, "only the accepted write may reach listeners, after accounting")
		observed = append(observed, node.ID)
	})

	rejected, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer rejected.Rollback()
	require.NoError(t, rejected.SetImplicit(true))
	_, err = rejected.CreateNode(&Node{ID: "test:rejected-publication", Properties: map[string]any{"payload": strings.Repeat("x", 1<<20)}})
	require.NoError(t, err)
	callbackInvoked := false
	require.ErrorIs(t, rejected.PublishCommit(context.Background(), CommitPublication{AfterPublication: func() { callbackInvoked = true }}), memory.ErrLimitExceeded)
	require.False(t, callbackInvoked)
	require.Empty(t, observed)
	require.Zero(t, policy.Reserved())
	_, err = engine.GetStoredNode("test:rejected-publication")
	require.ErrorIs(t, err, ErrNotFound)

	accepted, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer accepted.Rollback()
	require.NoError(t, accepted.SetImplicit(true))
	_, err = accepted.CreateNode(&Node{ID: "test:accepted-publication"})
	require.NoError(t, err)
	require.NoError(t, accepted.PublishCommit(context.Background(), CommitPublication{AfterPublication: func() { published = true }}))
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, []NodeID{"test:accepted-publication"}, observed)
	require.Zero(t, policy.Reserved())
}
