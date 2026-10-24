package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newOwnerTestEngine opens an engine whose write-behind buffer, when enabled,
// is applied only where a step drains it.
func newOwnerTestEngine(t *testing.T, writeBehind bool) *BadgerEngine {
	t.Helper()
	e, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	if e.writeBehind != nil {
		e.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		e.writeBehind.Close()
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func mvccLogicalKey(prefix byte, num uint64) []byte {
	logical := make([]byte, 1+8)
	logical[0] = prefix
	binary.BigEndian.PutUint64(logical[1:], num)
	return logical
}

// The lifecycle pruner writes a head it read earlier: a commit that landed
// in between keeps its head, and the floor only rises.
func TestWriteMVCCHeadNeverMovesBackwards(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			e := newOwnerTestEngine(t, writeBehind)
			ctx := context.Background()
			_, err := e.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			_, err = e.CreateNode(&Node{ID: "test:b", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			require.NoError(t, e.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
			require.NoError(t, e.FlushWriteBehind())

			nodeNum, ok := e.idDict.nodeForwardLookup("test:a")
			require.True(t, ok)
			edgeNum, ok := e.idDict.edgeForwardLookup("test:r")
			require.True(t, ok)
			update := map[string]func() error{
				"node": func() error {
					return e.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(2)}})
				},
				"relationship": func() error {
					return e.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": int64(2)}})
				},
			}
			for kind, logical := range map[string][]byte{
				"node":         mvccLogicalKey(prefixMVCCNode, nodeNum),
				"relationship": mvccLogicalKey(prefixMVCCEdge, edgeNum),
			} {
				read, err := e.ReadMVCCHead(ctx, logical)
				require.NoError(t, err, kind)
				require.NoError(t, update[kind](), kind)
				require.NoError(t, e.FlushWriteBehind())
				newer, err := e.ReadMVCCHead(ctx, logical)
				require.NoError(t, err, kind)
				require.Positive(t, newer.Version.Compare(read.Version), kind)

				// The pruner writes the head it read, with a new floor.
				require.NoError(t, e.WriteMVCCHead(ctx, logical, MVCCHead{Version: read.Version, Tombstoned: read.Tombstoned, FloorVersion: read.Version}), kind)
				got, err := e.ReadMVCCHead(ctx, logical)
				require.NoError(t, err, kind)
				require.Zero(t, got.Version.Compare(newer.Version), "%s: the newer head stays", kind)
				require.GreaterOrEqual(t, got.FloorVersion.Compare(read.Version), 0, "%s: the floor rises", kind)

				// A later stale write with a lower floor does not lower it.
				require.NoError(t, e.WriteMVCCHead(ctx, logical, MVCCHead{Version: read.Version}), kind)
				got, err = e.ReadMVCCHead(ctx, logical)
				require.NoError(t, err, kind)
				require.Zero(t, got.Version.Compare(newer.Version), kind)
				require.GreaterOrEqual(t, got.FloorVersion.Compare(read.Version), 0, "%s: the floor never falls", kind)
			}

			// A number no entity holds names none to order behind.
			require.Empty(t, e.mvccLogicalEntities(mvccLogicalKey(prefixMVCCNode, 1<<60)).nodes)
			require.Empty(t, e.mvccLogicalEntities(mvccLogicalKey(prefixMVCCEdge, 1<<60)).edges)
			// A malformed key is rejected.
			require.ErrorIs(t, e.WriteMVCCHead(ctx, []byte{prefixMVCCNode}, MVCCHead{}), ErrInvalidData)
		})
	}
}

// The low-level MVCC writers are ordered after an acknowledged commit of
// their entity: it is applied before they write, so its replay cannot land
// over them.
func TestMVCCWritersApplyAcknowledgedCommitsFirst(t *testing.T) {
	e := newOwnerTestEngine(t, true)
	_, err := e.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, e.FlushWriteBehind())
	head, err := e.GetNodeCurrentHead("test:a")
	require.NoError(t, err)

	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(2)}}))
	require.NoError(t, tx.Commit())
	require.NotZero(t, e.writeBehind.PendingOps(), "the update is acknowledged, not applied")

	require.NoError(t, e.UpdateNodeCurrentHead("test:a", head.Version, false))
	require.Zero(t, e.writeBehind.PendingOps(), "the head write applied the acknowledged update first")
	stored, err := e.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["v"])
}

// A relationship delete a commit acknowledged after a transaction began, and
// the buffer still holds, conflicts with the transaction's write of it.
func TestBufferedRelationshipDeleteConflictsWithLaterCommit(t *testing.T) {
	e := newOwnerTestEngine(t, true)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	require.NoError(t, e.FlushWriteBehind())

	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.DeleteEdge("test:r"))

	peer, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, peer.SetImplicit(true))
	require.NoError(t, peer.DeleteEdge("test:r"))
	require.NoError(t, peer.Commit())
	require.NotZero(t, e.writeBehind.PendingOps())

	require.ErrorIs(t, tx.Commit(), ErrConflict)
	require.NoError(t, e.FlushWriteBehind())
	count, err := e.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, count, "the relationship is deleted and counted once")
}

// A closed engine has nothing left to flush.
func TestFlushWriteBehindAfterClose(t *testing.T) {
	e := newOwnerTestEngine(t, true)
	_, err := e.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	require.NoError(t, e.Close())
	require.NoError(t, e.FlushWriteBehind())
}

// A prefix delete of another namespace does not fail a transaction open
// across it.
func TestPrefixDeleteOfAnotherNamespaceKeepsOpenTransaction(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			e := newOwnerTestEngine(t, writeBehind)
			_, err := e.CreateNode(&Node{ID: "other:x"})
			require.NoError(t, err)
			tx, err := e.BeginTransaction()
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			_, _, err = e.DeleteByPrefix("other:")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.NoError(t, e.FlushWriteBehind())
			_, err = e.GetStoredNode("test:a")
			require.NoError(t, err)
		})
	}
}
