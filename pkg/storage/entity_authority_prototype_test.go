package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEntityAuthorityUsesCanonicalReentrantOwner(t *testing.T) {
	sm := NewSchemaManager()
	ctx := context.Background()
	first, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx1", entityCommitKeys([]NodeID{"db:a"}, []EdgeID{"db:e"}))
	require.NoError(t, err)
	defer first()
	again, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx1", entityCommitKeys([]NodeID{"db:a"}, nil))
	require.NoError(t, err)
	again()
	disjoint, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", entityCommitKeys([]NodeID{"db:b"}, nil))
	require.NoError(t, err)
	disjoint()
	// Node and edge identifiers occupy distinct typed key domains.
	edge, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", entityCommitKeys(nil, []EdgeID{"db:a"}))
	require.NoError(t, err)
	edge()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = sm.acquireUniqueConstraintCommitLocks(canceled, "tx2", entityCommitKeys([]NodeID{"db:a"}, nil))
	require.ErrorIs(t, err, context.Canceled)
	first()
	next, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", entityCommitKeys([]NodeID{"db:a"}, nil))
	require.NoError(t, err)
	next()
}
