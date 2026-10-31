package storage

// Author: storage-format-owners lane (Claude), #1693, 31 October 2026.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mutationRequestKey struct{}

// The mutation owner begins its private transaction for the caller's request:
// preparation (write) reads that request, not a fresh context.
func TestMutationOwnerPreparesUnderRequest(t *testing.T) {
	engine := newTestEngine(t)
	ctx := context.WithValue(context.Background(), mutationRequestKey{}, "request")
	var seen context.Context
	handled, err := engine.inMutationTransaction(ctx, "test:n1", true, func(tx *BadgerTransaction) error {
		tx.mu.Lock()
		seen = tx.requestContextLocked()
		tx.mu.Unlock()
		return nil
	})
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, seen)
	require.Equal(t, "request", seen.Value(mutationRequestKey{}), "preparation runs under the caller's request")
}

// A request cancelled while the owner prepares fails the commit at its entry,
// before the commit's own cleanup; the owner still ends its transaction.
func TestMutationOwnerEndsTransactionWhenCommitIsCancelled(t *testing.T) {
	engine := newTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var opened *BadgerTransaction
	handled, err := engine.inMutationTransaction(ctx, "test:n1", true, func(tx *BadgerTransaction) error {
		opened = tx
		cancel()
		return nil
	})
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, opened)
	opened.mu.Lock()
	status := opened.Status
	opened.mu.Unlock()
	require.Equal(t, TxStatusRolledBack, status, "the owner's private transaction is not left open")
}
