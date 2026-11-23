package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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

	// Seed a legacy sequence before first opening the namespace.
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 7)
		return txn.Set(mvccNamespaceSequenceKey(ns), buf)
	}))

	state, err := engine.namespaceMVCC(ns)
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.seq.Load())

	// Recovered floor from existing node MVCC head.
	n := &Node{ID: NodeID("tenant:n1"), Labels: []string{"L"}, Properties: map[string]any{"k": "v"}}
	_, err = engine.CreateNode(t.Context(), n)
	require.NoError(t, err)
	n.Properties["k"] = "v2"
	err = engine.UpdateNode(t.Context(), NodeUpdateRequest{After: n})
	require.NoError(t, err)

	// Force cold state so EnsureNamespaceMVCC takes the recovery path.
	state.seq.Store(0)
	state.highWaterNanos.Store(0)

	require.NoError(t, engine.EnsureNamespaceMVCC(ns))

	recoveredState, err := engine.namespaceMVCC(ns)
	require.NoError(t, err)
	require.Equal(t, uint64(9), recoveredState.seq.Load())
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

	snap, err := engine.snapshotNamespaceVersions(t.Context())
	require.NoError(t, err)
	require.Contains(t, snap, "tenant")
	require.Equal(t, uint64(11), snap["tenant"].CommitSequence)
	require.GreaterOrEqual(t, snap["tenant"].CommitTimestamp.UnixNano(), future)
}

func TestRecoverNamespaceMVCCFloorScopesNodeAndEdgeHeads(t *testing.T) {
	engine := newTestEngine(t)
	ctx := t.Context()
	for _, id := range []NodeID{"tenant:from", "tenant:to"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "tenant:edge", StartNode: "tenant:from", EndNode: "tenant:to", Type: "NEXT"}))
	state, err := engine.namespaceMVCC("tenant")
	require.NoError(t, err)
	timestamp := state.highWaterNanos.Load()
	require.Equal(t, uint64(3), state.seq.Load())

	for _, namespace := range []string{"tenant-archive", "tenant2"} {
		for i := range 4 {
			_, err := engine.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("%s:%d", namespace, i))})
			require.NoError(t, err)
		}
	}

	seq, stamp, err := engine.recoverNamespaceMVCCFloor("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(3), seq, "the edge supplies the latest version in the target namespace")
	require.Equal(t, timestamp, stamp)

	seq, stamp, err = engine.recoverNamespaceMVCCFloor("tenant-missing")
	require.NoError(t, err)
	require.Zero(t, seq)
	require.Zero(t, stamp)
}

func BenchmarkRecoverNamespaceMVCCFloorWithUnrelatedKeys(b *testing.B) {
	for _, count := range []int{0, 30000} {
		b.Run(fmt.Sprintf("%d_nodes_and_edges", count), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, engine.Close()) })
			for start := 0; start < count; start += 1000 {
				require.NoError(b, engine.db.Update(func(txn *badger.Txn) error {
					for i := start; i < min(start+1000, count); i++ {
						id := fmt.Sprintf("code:%06d", i)
						if err := txn.Set(nodeKey(NodeID(id)), []byte{1}); err != nil {
							return err
						}
						if err := txn.Set(edgeKey(EdgeID(id)), []byte{1}); err != nil {
							return err
						}
					}
					return nil
				}))
			}
			b.ReportAllocs()
			for b.Loop() {
				seq, stamp, err := engine.recoverNamespaceMVCCFloor("app-storage")
				if err != nil || seq != 0 || stamp != 0 {
					b.Fatalf("unexpected empty namespace floor: (%d, %d, %v)", seq, stamp, err)
				}
			}
		})
	}
}
