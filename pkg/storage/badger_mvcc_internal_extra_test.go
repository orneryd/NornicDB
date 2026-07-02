package storage

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type testSnapshotRegistry struct {
	active int64
	oldest time.Duration
}

func (r *testSnapshotRegistry) Register(info SnapshotReaderInfo) (string, func()) {
	_ = info
	return "r", func() {}
}
func (r *testSnapshotRegistry) ActiveCount() int64             { return r.active }
func (r *testSnapshotRegistry) Snapshot() []SnapshotReaderInfo { return nil }
func (r *testSnapshotRegistry) OldestReaderAge() time.Duration { return r.oldest }

type testLifecycleController struct {
	registry   *testSnapshotRegistry
	pinned     int64
	acquireErr error
}

func (c *testLifecycleController) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	_, done := c.registry.Register(info)
	return done
}
func (c *testLifecycleController) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"ok": true}
}
func (c *testLifecycleController) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	return nil
}
func (c *testLifecycleController) PauseLifecycle()  {}
func (c *testLifecycleController) ResumeLifecycle() {}
func (c *testLifecycleController) SetLifecycleSchedule(interval time.Duration) error {
	_ = interval
	return nil
}
func (c *testLifecycleController) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	if c.acquireErr != nil {
		return nil, c.acquireErr
	}
	return c.RegisterSnapshotReader(info), nil
}
func (c *testLifecycleController) EvaluateSnapshotReader(info SnapshotReaderInfo) (bool, bool) {
	_ = info
	return false, false
}
func (c *testLifecycleController) RunPruneNow(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}
func (c *testLifecycleController) StartLifecycle(ctx context.Context) { _ = ctx }
func (c *testLifecycleController) StopLifecycle()                     {}
func (c *testLifecycleController) IsLifecycleEnabled() bool           { return true }
func (c *testLifecycleController) IsLifecycleRunning() bool           { return true }
func (c *testLifecycleController) ReaderRegistry() SnapshotReaderRegistry {
	return c.registry
}
func (c *testLifecycleController) PinnedBytes() int64 { return c.pinned }

func TestMVCCAccessors_WithLifecycleController(t *testing.T) {
	engine := newTestEngine(t)
	ctrl := &testLifecycleController{
		registry: &testSnapshotRegistry{active: 3, oldest: 5 * time.Second},
		pinned:   4096,
	}

	engine.mu.Lock()
	engine.lifecycleController = ctrl
	engine.mu.Unlock()

	require.Equal(t, int64(3), engine.ActiveReaders())
	require.Equal(t, int64(4096), engine.PinnedBytes())
	require.Equal(t, 5.0, engine.OldestReaderAgeSeconds())
}

func TestBadgerEngine_AdjacencySnapshotReaderAcquisitionErrors(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	ctrl := &testLifecycleController{
		registry:   &testSnapshotRegistry{},
		acquireErr: context.Canceled,
	}
	engine.mu.Lock()
	engine.lifecycleController = ctrl
	engine.mu.Unlock()

	_, err := engine.GetOutgoingEdgesVisibleAt("test:n1", MVCCVersion{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = engine.GetIncomingEdgesVisibleAt("test:n1", MVCCVersion{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBadgerEngine_CollectVisibleAdjacencyEdgeIDsInTxn_EmptyPrefix(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		edgeIDs, err := engine.collectVisibleAdjacencyEdgeIDsInTxn(txn, nil, MVCCVersion{})
		require.NoError(t, err)
		require.Nil(t, edgeIDs)
		return nil
	}))
}

func TestBadgerEngine_CollectVisibleAdjacencyEdgeIDsInTxn_MalformedRecord(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:adj-a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:adj-b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:adj-edge", StartNode: "test:adj-a", EndNode: "test:adj-b", Type: "R"}))
	head, err := engine.GetEdgeCurrentHead("test:adj-edge")
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		key, keyErr := engine.mvccOutgoingAdjacencyKeyString(txn, "test:adj-a", "test:adj-edge", head.Version)
		if keyErr != nil {
			return keyErr
		}
		return txn.Set(key, []byte("bad-adj"))
	}))

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.collectVisibleAdjacencyEdgeIDsInTxn(txn, engine.mvccOutgoingAdjacencyPrefixString("test:adj-a"), head.Version)
		require.Error(t, err)
		return nil
	}))
}

func TestBadgerEngine_VisibleAdjacencyReaders_PropagateEdgeLookupErrors(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:ga", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:gb", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ge", StartNode: "test:ga", EndNode: "test:gb", Type: "R"}))
	head, err := engine.GetEdgeCurrentHead("test:ge")
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		edgeNum, ok := engine.idDict.lookupEdgeNumID("test:ge")
		require.True(t, ok)
		return txn.Set(mvccEdgeHeadKey(edgeNum), []byte("bad-head"))
	}))

	_, err = engine.GetOutgoingEdgesVisibleAt("test:ga", head.Version)
	require.Error(t, err)
	_, err = engine.GetIncomingEdgesVisibleAt("test:gb", head.Version)
	require.Error(t, err)
}

func TestWriteAndLoadMVCCHeadForLogicalKeyInTxn_NodeAndEdge(t *testing.T) {
	engine := newTestEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Unix(100, 0).UTC(), CommitSequence: 7}
	floor := MVCCVersion{CommitTimestamp: time.Unix(10, 0).UTC(), CommitSequence: 2}

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		nodeLogical := make([]byte, 9)
		nodeLogical[0] = prefixMVCCNode
		binary.BigEndian.PutUint64(nodeLogical[1:], 1)
		require.NoError(t, engine.writeMVCCHeadForLogicalKeyInTxn(txn, nodeLogical, version, false, floor))

		edgeLogical := make([]byte, 9)
		edgeLogical[0] = prefixMVCCEdge
		binary.BigEndian.PutUint64(edgeLogical[1:], 2)
		require.NoError(t, engine.writeMVCCHeadForLogicalKeyInTxn(txn, edgeLogical, version, true, floor))
		return nil
	}))

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		nodeLogical := make([]byte, 9)
		nodeLogical[0] = prefixMVCCNode
		binary.BigEndian.PutUint64(nodeLogical[1:], 1)
		nh, err := engine.loadMVCCHeadForLogicalKeyInTxn(txn, nodeLogical)
		require.NoError(t, err)
		require.False(t, nh.Tombstoned)
		require.Equal(t, version.CommitSequence, nh.Version.CommitSequence)
		require.Equal(t, floor.CommitSequence, nh.FloorVersion.CommitSequence)

		edgeLogical := make([]byte, 9)
		edgeLogical[0] = prefixMVCCEdge
		binary.BigEndian.PutUint64(edgeLogical[1:], 2)
		eh, err := engine.loadMVCCHeadForLogicalKeyInTxn(txn, edgeLogical)
		require.NoError(t, err)
		require.True(t, eh.Tombstoned)
		require.Equal(t, version.CommitSequence, eh.Version.CommitSequence)
		return nil
	}))
}

func TestLoadAndWriteMVCCHeadForLogicalKeyInTxn_Errors(t *testing.T) {
	engine := newTestEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		badLen := []byte{prefixMVCCNode}
		err := engine.writeMVCCHeadForLogicalKeyInTxn(txn, badLen, version, false, version)
		require.ErrorIs(t, err, ErrInvalidData)

		unknownPrefix := make([]byte, 9)
		unknownPrefix[0] = 0xAA
		binary.BigEndian.PutUint64(unknownPrefix[1:], 1)
		err = engine.writeMVCCHeadForLogicalKeyInTxn(txn, unknownPrefix, version, false, version)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown mvcc logical key prefix")
		return nil
	}))

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		badLen := []byte{prefixMVCCNode}
		_, err := engine.loadMVCCHeadForLogicalKeyInTxn(txn, badLen)
		require.ErrorIs(t, err, ErrInvalidData)

		unknownPrefix := make([]byte, 9)
		unknownPrefix[0] = 0xAA
		binary.BigEndian.PutUint64(unknownPrefix[1:], 1)
		_, err = engine.loadMVCCHeadForLogicalKeyInTxn(txn, unknownPrefix)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown mvcc logical key prefix")
		return nil
	}))
}

func TestNextScanStartAndMVCCBootstrapTime(t *testing.T) {
	k := []byte{0x01, 0x02}
	next := nextScanStart(k)
	require.Equal(t, []byte{0x01, 0x02, 0x00}, next)
	require.Equal(t, []byte{0x01, 0x02}, k)

	created := time.Date(2022, 3, 4, 5, 6, 7, 0, time.FixedZone("X", 3600))
	updated := time.Date(2023, 4, 5, 6, 7, 8, 0, time.FixedZone("Y", -3600))
	require.Equal(t, updated.UTC(), mvccBootstrapTime(created, updated))
	require.Equal(t, created.UTC(), mvccBootstrapTime(created, time.Time{}))

	now := mvccBootstrapTime(time.Time{}, time.Time{})
	require.WithinDuration(t, time.Now().UTC(), now, 2*time.Second)
}
