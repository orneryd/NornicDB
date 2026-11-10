package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEntityAuthorityUsesCanonicalReentrantOwner(t *testing.T) {
	sm := NewSchemaManager()
	ctx := context.Background()
	authorityKeys1, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: []NodeID{"db:a"}, edges: []EdgeID{"db:e"}, endpoints: nil, nodeBodies: []NodeID{"db:a"}, edgeBodies: []EdgeID{"db:e"}})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	first, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx1", authorityKeys1)
	require.NoError(t, err)
	defer first()
	authorityKeys2, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: []NodeID{"db:a"}, edges: nil, endpoints: nil, nodeBodies: []NodeID{"db:a"}, edgeBodies: nil})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	again, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx1", authorityKeys2)
	require.NoError(t, err)
	again()
	authorityKeys3, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: []NodeID{"db:b"}, edges: nil, endpoints: nil, nodeBodies: []NodeID{"db:b"}, edgeBodies: nil})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	disjoint, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", authorityKeys3)
	require.NoError(t, err)
	disjoint()
	// Node and edge identifiers occupy distinct typed key domains.
	authorityKeys4, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: nil, edges: []EdgeID{"db:a"}, endpoints: nil, nodeBodies: nil, edgeBodies: []EdgeID{"db:a"}})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	edge, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", authorityKeys4)
	require.NoError(t, err)
	edge()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	authorityKeys5, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: []NodeID{"db:a"}, edges: nil, endpoints: nil, nodeBodies: []NodeID{"db:a"}, edgeBodies: nil})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	_, err = sm.acquireUniqueConstraintCommitLocks(canceled, "tx2", authorityKeys5)
	require.ErrorIs(t, err, context.Canceled)
	first()
	authorityKeys6, keyErr := entityCommitKeys(t.Context(), entityAuthorityFootprint{nodes: []NodeID{"db:a"}, edges: nil, endpoints: nil, nodeBodies: []NodeID{"db:a"}, edgeBodies: nil})
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	next, err := sm.acquireUniqueConstraintCommitLocks(ctx, "tx2", authorityKeys6)
	require.NoError(t, err)
	next()
}
