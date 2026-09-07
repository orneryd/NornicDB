// SPDX-License-Identifier: MIT
package storage

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionNodeUpdatePreservesPostValidationPeerWrite(t *testing.T) {
	barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:         t.TempDir(),
		HighPerformance: true,
		Logger:          slog.New(barrier),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID("test:node-write-conflict")
	_, err = engine.CreateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"writer": "seed"}})
	require.NoError(t, err)

	stale, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stale.Rollback() })
	node, err := stale.GetNode(nodeID)
	require.NoError(t, err)
	node.Properties["writer"] = "stale"
	require.NoError(t, stale.UpdateNode(node))
	require.NoError(t, stale.SetMetadata(map[string]interface{}{"validation_publication_control": true}))
	barrier.target = stale.ID
	var release sync.Once
	unblock := func() { release.Do(func() { close(barrier.resume) }) }
	t.Cleanup(unblock)

	committed := make(chan error, 1)
	go func() { committed <- stale.Commit() }()
	select {
	case <-barrier.reached:
	case err := <-committed:
		t.Fatalf("stale writer finished before validation barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("stale writer did not reach post-validation barrier")
	}

	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	peerNode, err := peer.GetNode(nodeID)
	require.NoError(t, err)
	peerNode.Properties["writer"] = "peer"
	require.NoError(t, peer.UpdateNode(peerNode))
	require.NoError(t, peer.Commit())
	unblock()

	select {
	case err := <-committed:
		require.ErrorIs(t, err, ErrConflict)
	case <-time.After(5 * time.Second):
		t.Fatal("stale writer did not finish")
	}

	latest, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, "peer", latest.Properties["writer"])
}
