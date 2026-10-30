package storage

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNamespaceForEdgeIDs_Table(t *testing.T) {
	tests := []struct {
		name    string
		ids     []EdgeID
		wantNS  string
		wantErr string
	}{
		{name: "single namespace", ids: []EdgeID{"acme:e1", "acme:e2"}, wantNS: "acme"},
		{name: "ignores empty IDs", ids: []EdgeID{"", "acme:e1", ""}, wantNS: "acme"},
		{name: "mixed namespaces", ids: []EdgeID{"acme:e1", "globex:e2"}, wantErr: "multiple namespaces"},
		{name: "unprefixed ID", ids: []EdgeID{"e1"}, wantErr: "must be prefixed with namespace"},
		{name: "no usable IDs", ids: []EdgeID{"", ""}, wantErr: "no usable IDs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns, err := namespaceForEdgeIDs(tc.ids)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantNS, ns)
		})
	}
}

func TestEnsureNamespaceMVCC_EmptyNamespace_NoError(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.EnsureNamespaceMVCC(""))
}

func TestEnsureNamespaceMVCC_WarmsColdStateWithPersistedAndRecoveredFloors(t *testing.T) {
	engine := newTestEngine(t)
	ns := "tenant"

	state, err := engine.namespaceMVCC(ns)
	require.NoError(t, err)
	state.seq.Store(0)
	state.highWaterNanos.Store(0)

	// Persisted namespace sequence floor.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 7)
		return txn.Set(mvccNamespaceSequenceKey(ns), buf)
	}))

	// Recovered floor from existing node MVCC head.
	n := &Node{ID: NodeID("tenant:n1"), Labels: []string{"L"}, Properties: map[string]any{"k": "v"}}
	_, err = engine.CreateNode(n)
	require.NoError(t, err)
	n.Properties["k"] = "v2"
	err = engine.UpdateNode(n)
	require.NoError(t, err)

	// Force cold state so EnsureNamespaceMVCC takes the recovery path.
	state.seq.Store(0)
	state.highWaterNanos.Store(0)

	require.NoError(t, engine.EnsureNamespaceMVCC(ns))

	recoveredState, err := engine.namespaceMVCC(ns)
	require.NoError(t, err)
	require.GreaterOrEqual(t, recoveredState.seq.Load(), uint64(7))
	require.Greater(t, recoveredState.highWaterNanos.Load(), int64(0))
}

func TestSnapshotNamespaceVersions_UsesHighWaterClamp(t *testing.T) {
	engine := newTestEngine(t)
	state := &namespaceMVCCState{}
	state.seq.Store(11)
	future := time.Now().Add(2 * time.Second).UnixNano()
	state.highWaterNanos.Store(future)

	engine.mvccByNamespaceMu.Lock()
	if engine.mvccByNamespace == nil {
		engine.mvccByNamespace = make(map[string]*namespaceMVCCState)
	}
	engine.mvccByNamespace["tenant"] = state
	engine.mvccByNamespaceMu.Unlock()

	snap := engine.snapshotNamespaceVersions()
	require.Contains(t, snap, "tenant")
	require.Equal(t, uint64(11), snap["tenant"].CommitSequence)
	require.GreaterOrEqual(t, snap["tenant"].CommitTimestamp.UnixNano(), future)
}
