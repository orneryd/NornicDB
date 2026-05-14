package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubLifecycleController records calls to the various lifecycle
// methods so the wrappers on BadgerEngine can be asserted against
// without bringing up the real lifecycle/MVCCLifecycleManager. Each
// call records a flag (via atomic.Bool / counter) the test can check.
type stubLifecycleController struct {
	startCalled    atomic.Bool
	pauseCalled    atomic.Bool
	resumeCalled   atomic.Bool
	registerCalls  atomic.Int32
	statusCalls    atomic.Int32
	pruneCalls     atomic.Int32
	scheduleCalls  atomic.Int32
	scheduleErr    error
	debtKeysCalled atomic.Int32

	enabled atomic.Bool
	running atomic.Bool

	prunedRowCount int64
	pruneErr       error

	// Returned by RegisterSnapshotReader / AcquireSnapshotReader.
	deregister func()
}

func newStubLifecycleController() *stubLifecycleController {
	c := &stubLifecycleController{deregister: func() {}}
	c.enabled.Store(true)
	return c
}

// MVCCLifecycleEngine surface.

func (c *stubLifecycleController) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	c.registerCalls.Add(1)
	return c.deregister
}

func (c *stubLifecycleController) LifecycleStatus() map[string]interface{} {
	c.statusCalls.Add(1)
	return map[string]interface{}{"enabled": true, "stub": true}
}

func (c *stubLifecycleController) TriggerPruneNow(ctx context.Context) error {
	c.pruneCalls.Add(1)
	return c.pruneErr
}

func (c *stubLifecycleController) PauseLifecycle()  { c.pauseCalled.Store(true) }
func (c *stubLifecycleController) ResumeLifecycle() { c.resumeCalled.Store(true) }

// MVCCLifecycleController extras.

func (c *stubLifecycleController) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	c.registerCalls.Add(1)
	return c.deregister, nil
}

func (c *stubLifecycleController) EvaluateSnapshotReader(info SnapshotReaderInfo) (bool, bool) {
	return false, false
}

func (c *stubLifecycleController) RunPruneNow(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	return c.prunedRowCount, c.pruneErr
}

func (c *stubLifecycleController) StartLifecycle(ctx context.Context) {
	c.startCalled.Store(true)
	c.running.Store(true)
}

func (c *stubLifecycleController) StopLifecycle() {
	c.running.Store(false)
}

func (c *stubLifecycleController) IsLifecycleEnabled() bool { return c.enabled.Load() }
func (c *stubLifecycleController) IsLifecycleRunning() bool { return c.running.Load() }

func (c *stubLifecycleController) ReaderRegistry() SnapshotReaderRegistry { return nil }

// Optional schedule + debt extensions.

func (c *stubLifecycleController) SetLifecycleSchedule(interval time.Duration) error {
	c.scheduleCalls.Add(1)
	return c.scheduleErr
}

func (c *stubLifecycleController) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	c.debtKeysCalled.Add(1)
	return []MVCCLifecycleDebtKey{
		{LogicalKey: "x", DebtBytes: 1024, TombstoneDepth: 1, FloorLagVersions: 0, VersionsToDelete: 1},
	}
}

func TestBadgerEngine_Lifecycle_NilControllerBehavior(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	// No controller installed.

	// RegisterSnapshotReader returns a no-op deregister function — must
	// be safe to call.
	dereg := e.RegisterSnapshotReader(SnapshotReaderInfo{})
	require.NotNil(t, dereg)
	dereg()

	// LifecycleStatus reports enabled=false when no controller.
	status := e.LifecycleStatus()
	require.Equal(t, false, status["enabled"])

	// TriggerPruneNow is a no-op (returns nil).
	require.NoError(t, e.TriggerPruneNow(context.Background()))

	// Pause/Resume are no-ops without panicking.
	e.PauseLifecycle()
	e.ResumeLifecycle()

	// SetLifecycleSchedule returns nil when controller doesn't
	// implement the optional schedule interface.
	require.NoError(t, e.SetLifecycleSchedule(time.Minute))

	// TopLifecycleDebtKeys returns nil when controller doesn't
	// implement the optional debt interface.
	require.Nil(t, e.TopLifecycleDebtKeys(10))

	// StartLifecycleManager with a nil controller is a no-op.
	e.StartLifecycleManager(context.Background())
}

func TestBadgerEngine_Lifecycle_ControllerDelegation(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	stub := newStubLifecycleController()
	e.SetLifecycleController(stub)

	// StartLifecycleManager → controller.StartLifecycle.
	e.StartLifecycleManager(context.Background())
	require.True(t, stub.startCalled.Load())
	require.True(t, stub.IsLifecycleRunning())

	// RegisterSnapshotReader delegates and returns the stub's
	// deregister.
	var deregistered atomic.Int32
	stub.deregister = func() { deregistered.Add(1) }
	dereg := e.RegisterSnapshotReader(SnapshotReaderInfo{})
	require.NotNil(t, dereg)
	dereg()
	require.Equal(t, int32(1), deregistered.Load())
	require.Equal(t, int32(1), stub.registerCalls.Load())

	// LifecycleStatus delegates, returns stub map.
	status := e.LifecycleStatus()
	require.Equal(t, true, status["enabled"])
	require.Equal(t, true, status["stub"])
	require.Equal(t, int32(1), stub.statusCalls.Load())

	// TriggerPruneNow delegates.
	require.NoError(t, e.TriggerPruneNow(context.Background()))
	require.Equal(t, int32(1), stub.pruneCalls.Load())

	// Pause/Resume delegate.
	e.PauseLifecycle()
	require.True(t, stub.pauseCalled.Load())
	e.ResumeLifecycle()
	require.True(t, stub.resumeCalled.Load())

	// SetLifecycleSchedule delegates when implemented.
	require.NoError(t, e.SetLifecycleSchedule(5*time.Minute))
	require.Equal(t, int32(1), stub.scheduleCalls.Load())

	// TopLifecycleDebtKeys delegates when implemented.
	debt := e.TopLifecycleDebtKeys(10)
	require.Len(t, debt, 1)
	require.Equal(t, "x", debt[0].LogicalKey)
	require.Equal(t, int32(1), stub.debtKeysCalled.Load())
}

func TestBadgerEngine_AcquireAndEvaluateSnapshotReader_NoController(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// With no controller, acquireSnapshotReader bumps the simple
	// activeMVCCSnapshotReaders counter and returns a deregister fn
	// that decrements it.
	startCount := e.activeMVCCSnapshotReaders.Load()
	dereg, err := e.acquireSnapshotReader(SnapshotReaderInfo{})
	require.NoError(t, err)
	require.NotNil(t, dereg)
	require.Equal(t, startCount+1, e.activeMVCCSnapshotReaders.Load())
	dereg()
	require.Equal(t, startCount, e.activeMVCCSnapshotReaders.Load())

	// evaluateSnapshotReader returns (false, false) without controller.
	graceful, hard := e.evaluateSnapshotReader(SnapshotReaderInfo{})
	require.False(t, graceful)
	require.False(t, hard)
}

func TestBadgerEngine_LifecycleHelperFunctions(t *testing.T) {
	require.Equal(t, byte(prefixMVCCNode), headPrefixToLogicalPrefix(prefixMVCCNodeHead))
	require.Equal(t, byte(prefixMVCCEdge), headPrefixToLogicalPrefix(prefixMVCCEdgeHead))
	// Unrecognized prefix falls back to edge logical prefix per the
	// helper's else-branch contract.
	require.Equal(t, byte(prefixMVCCEdge), headPrefixToLogicalPrefix(0xFF))

	// mvccVersionPrefixForLogicalKey rejects malformed inputs.
	require.Nil(t, mvccVersionPrefixForLogicalKey([]byte{0x01}))
	logical := []byte{prefixMVCCNode, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}
	prefix := mvccVersionPrefixForLogicalKey(logical)
	require.NotNil(t, prefix)
	// The post-refactor wire layout is [prefix][numID][sortVersion]
	// with no separator — the prefix is exactly the logical key.
	require.Equal(t, logical, prefix)
}

func TestBadgerEngine_MVCCVersionKeyForLogicalKey_NodeAndEdge(t *testing.T) {
	v := MVCCVersion{
		CommitTimestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		CommitSequence:  7,
	}
	nodeLogical := []byte{prefixMVCCNode, 0, 0, 0, 0, 0, 0, 0, 5}
	edgeLogical := []byte{prefixMVCCEdge, 0, 0, 0, 0, 0, 0, 0, 9}

	nk, err := mvccVersionKeyForLogicalKey(nodeLogical, v)
	require.NoError(t, err)
	require.NotEmpty(t, nk)

	ek, err := mvccVersionKeyForLogicalKey(edgeLogical, v)
	require.NoError(t, err)
	require.NotEmpty(t, ek)

	// Bad-shape inputs reject before allocation.
	_, err = mvccVersionKeyForLogicalKey([]byte{0x99}, v)
	require.ErrorIs(t, err, ErrInvalidData)

	// Unknown logical prefix.
	bogus := []byte{0xFE, 0, 0, 0, 0, 0, 0, 0, 1}
	_, err = mvccVersionKeyForLogicalKey(bogus, v)
	require.Error(t, err)
}

func TestBadgerEngine_IterateMVCCHeads_AndVersions_ReadWrite(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// Bootstrap a node and an edge so the head index has rows.
	_, err := e.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = e.CreateNode(&Node{ID: "test:n2", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2",
		Type: "REL", Properties: map[string]any{},
	}))

	// IterateMVCCHeads must yield at least one node and one edge head.
	var sawNode, sawEdge bool
	require.NoError(t, e.IterateMVCCHeads(context.Background(), func(logicalKey []byte, head MVCCHead) error {
		require.GreaterOrEqual(t, len(logicalKey), 2)
		switch logicalKey[0] {
		case prefixMVCCNode:
			sawNode = true
		case prefixMVCCEdge:
			sawEdge = true
		}
		return nil
	}))
	require.True(t, sawNode, "expected to see a node head")
	require.True(t, sawEdge, "expected to see an edge head")

	// Append a manual MVCC version for n1 so IterateMVCCVersions has
	// material to walk. The append goes through the public API.
	v := MVCCVersion{
		CommitTimestamp: time.Now().Add(time.Hour).UTC(),
		CommitSequence:  9_999,
	}
	require.NoError(t, e.AppendNodeVersion(&Node{
		ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"phase": "future"},
	}, v))

	// Build the logical key for n1 to read its version stream.
	nodeNum, ok := e.idDict.lookupNodeNumID("test:n1")
	require.True(t, ok)
	logical := make([]byte, 9)
	logical[0] = prefixMVCCNode
	for i := 0; i < 8; i++ {
		logical[1+i] = byte(nodeNum >> (8 * (7 - i)))
	}

	// IterateMVCCVersions yields the appended version.
	versionsSeen := 0
	require.NoError(t, e.IterateMVCCVersions(context.Background(), logical, func(version MVCCVersion, tombstoned bool, sizeBytes int64) error {
		versionsSeen++
		require.GreaterOrEqual(t, sizeBytes, int64(0))
		return nil
	}))
	require.GreaterOrEqual(t, versionsSeen, 1)

	// Bad-shape logicalKey rejected.
	require.ErrorIs(t,
		e.IterateMVCCVersions(context.Background(), []byte{0x01}, func(version MVCCVersion, tombstoned bool, sizeBytes int64) error { return nil }),
		ErrInvalidData,
	)

	// ReadMVCCHead round-trips the head we just wrote.
	head, err := e.ReadMVCCHead(context.Background(), logical)
	require.NoError(t, err)
	require.False(t, head.Version.IsZero())

	// WriteMVCCHead overwrites the head; ReadMVCCHead must reflect it.
	newV := MVCCVersion{CommitTimestamp: v.CommitTimestamp.Add(time.Minute), CommitSequence: v.CommitSequence + 1}
	require.NoError(t, e.WriteMVCCHead(context.Background(), logical, MVCCHead{Version: newV, Tombstoned: false, FloorVersion: newV}))
	head, err = e.ReadMVCCHead(context.Background(), logical)
	require.NoError(t, err)
	require.Equal(t, 0, head.Version.Compare(newV))

	// DeleteMVCCVersion removes the appended version.
	require.NoError(t, e.DeleteMVCCVersion(context.Background(), logical, v))
}

func TestBadgerEngine_IterateMVCCHeads_RespectsContextCancel(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// Stage some heads.
	for i := 0; i < 5; i++ {
		_, err := e.CreateNode(&Node{
			ID:         NodeID("test:n-" + string(rune('a'+i))),
			Labels:     []string{"L"},
			Properties: map[string]any{},
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the iteration starts

	err := e.IterateMVCCHeads(ctx, func(logicalKey []byte, head MVCCHead) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
