package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUniqueIndexPreCanceledLookupAndRetry(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("cancel_unique", "Item", "uid"))
	schema.ApplyUniqueValue(context.Background(), "Item", "uid", "kept", "target:kept", ValidatedUniqueClaim)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, value := range []string{"kept", "missing"} {
		id, found, _, _, err := schema.LookupUniqueConstraintValue(ctx, "Item", "uid", value)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, id)
		require.False(t, found)
	}
	id, found, exists, _, err := schema.LookupUniqueConstraintValue(t.Context(), "Item", "uid", "kept")
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, found)
	require.Equal(t, NodeID("target:kept"), id)
	id, found, exists, _, err = schema.LookupUniqueConstraintValue(t.Context(), "Item", "uid", "missing")
	require.NoError(t, err)
	require.True(t, exists)
	require.False(t, found)
	require.Empty(t, id)
}

func TestUniqueIndexCanceledConstraintLockWaiter(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("wait_unique", "Item", "uid"))
	schema.ApplyUniqueValue(context.Background(), "Item", "uid", "kept", "target:kept", ValidatedUniqueClaim)
	constraint := schema.uniqueConstraints[nodePropertyKey{label: "Item", property: "uid"}]
	constraint.mu.Lock()
	var release sync.Once
	defer release.Do(constraint.mu.Unlock)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &indexWaitContext{Context: base, entered: make(chan struct{})}
	type result struct {
		id    NodeID
		found bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		id, found, _, _, err := schema.LookupUniqueConstraintValue(ctx, "Item", "uid", "kept")
		done <- result{id, found, err}
	}()
	joinAfterRelease := func() {
		release.Do(constraint.mu.Unlock)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("unique lookup did not join after releasing constraint")
		}
	}
	select {
	case <-ctx.entered:
	case <-time.After(5 * time.Second):
		cancel()
		joinAfterRelease()
		t.Fatal("unique lookup did not reach constraint lock wait")
	}
	cancel()
	select {
	case got := <-done:
		require.ErrorIs(t, got.err, context.Canceled)
		require.Empty(t, got.id)
		require.False(t, got.found)
	case <-time.After(5 * time.Second):
		joinAfterRelease()
		t.Fatal("canceled unique lookup waited for writer release")
	}
	release.Do(constraint.mu.Unlock)
	id, found, exists, _, err := schema.LookupUniqueConstraintValue(t.Context(), "Item", "uid", "kept")
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, found)
	require.Equal(t, NodeID("target:kept"), id)
}

func TestUniqueIndexPendingOverlayCancellationAndRetry(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("pending_unique", "Item", "uid"))
	schema.ApplyUniqueValue(context.Background(), "Item", "uid", "kept", "target:old", ValidatedUniqueClaim)
	buffer := newCommitBuffer()
	index := newPendingNodeIndex()
	index.tracked["Item"] = map[string]struct{}{"uid": {}}
	add := func(id NodeID, value string) {
		node := &Node{ID: id, Labels: []string{"Item"}, Properties: map[string]interface{}{"uid": value}}
		// Use the canonical generation owner so its replay order marks
		// this as a real pending mutation, not an empty read lease.
		buffer.addNode(node)
		index.add(node)
	}
	// Each namespace has its own unique holder. The target's committed holder
	// has a pending new value, so only its replacement should be returned.
	add("target:old", "moved")
	add("target:new", "kept")
	for i := 0; i < 1024; i++ {
		add(NodeID(fmt.Sprintf("other%d:holder", i)), "kept")
	}
	pending := &WriteBehindBuffer{active: buffer, pendingIndex: index}
	schema.attachPendingWrites(pending, "target")
	ctx := newIndexPollCancelContext(t, 16)
	id, found, _, _, err := schema.LookupUniqueConstraintValue(ctx, "Item", "uid", "kept")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, ctx.Context.Err(), context.Canceled)
	require.Empty(t, id)
	require.False(t, found)
	for i := 0; i < 2; i++ {
		id, found, exists, _, err := schema.LookupUniqueConstraintValue(t.Context(), "Item", "uid", "kept")
		require.NoError(t, err)
		require.True(t, exists)
		require.True(t, found)
		require.Equal(t, NodeID("target:new"), id)
	}
	id, found, exists, _, err := schema.LookupUniqueConstraintValue(t.Context(), "Item", "uid", "moved")
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, found)
	require.Equal(t, NodeID("target:old"), id)
}
