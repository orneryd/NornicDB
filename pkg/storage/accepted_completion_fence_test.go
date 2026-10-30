// Author: NOR-288 implementation lane (Codex), 2026-10-29.
package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntityWriteNotificationCanReenterCompletion(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	var release func()
	var delivered atomic.Int32
	reentered := make(chan struct{})
	b.OnNodeCreated(func(node *Node) {
		if node.ID != "test:created" {
			return
		}
		delivered.Add(1)
		// A real public listener reenters the actual outer completion owner.
		// The single release below is the only initial caller; no TryLock or
		// inference from a competing releaser is involved. On the old owner,
		// this bounded callback returns so cleanup can finish after the red.
		go func() { release(); close(reentered) }()
		select {
		case <-reentered:
		case <-time.After(time.Second):
			t.Error("public notification runs while its entity completion arbitration is held")
		}
	})
	// Hold the existing publication phase deterministically. This owns a
	// different entity from the public write, so it cannot block its admission.
	_, release, err = b.beginEntityWrite(context.Background(), "test:anchor", false, nil)
	require.NoError(t, err)
	t.Cleanup(release)
	require.NotNil(t, b.writeBehind, "the publication hold is part of write-behind ordering")
	_, err = b.CreateNode(&Node{ID: "test:created", Properties: map[string]any{"v": "stored"}})
	require.NoError(t, err)
	require.Zero(t, delivered.Load(), "the real notification is queued behind the outer publication")
	release()
	select {
	case <-reentered:
	case <-time.After(time.Second):
		t.Fatal("reentrant completion did not retire after outer release")
	}
	require.EqualValues(t, 1, delivered.Load())
	node, err := b.GetStoredNode("test:created")
	require.NoError(t, err)
	require.Equal(t, "stored", node.Properties["v"])
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: node.ID, Properties: map[string]any{"v": "following"}}}))
	release()
	require.EqualValues(t, 1, delivered.Load(), "reentry and repeated cleanup cannot deliver twice")
}
