package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type txLifecycleControllerStub struct {
	mu             sync.Mutex
	enabled        bool
	err            error
	gracefulExpire bool
	hardExpire     bool
	acquireCount   int
	releaseCount   int
	registerCount  int
	evaluateCount  int
	pruneCount     int
	pauseCount     int
	resumeCount    int
	registeredInfo []SnapshotReaderInfo
}

func TestSchemaTransactionLifecycleAndRuntimeSnapshot(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	committed := engine.GetSchemaForNamespace("schema_lifetime")
	require.NoError(t, committed.AddPropertyIndex("retained", "Account", []string{"name"}))
	require.NoError(t, committed.BackfillPropertyIndex("Account", "name", map[NodeID]interface{}{"schema_lifetime:existing": "existing"}))
	require.NoError(t, committed.AddUniqueConstraint("retained_unique", "Account", "retainedUID"))
	require.NoError(t, committed.AddCompositeIndex("retained_composite", "Account", []string{"name", "retainedUID"}))
	require.NoError(t, committed.AddRangeIndex("retained_range", "Account", "age"))
	retainedProperty, _ := committed.GetPropertyIndex("Account", "name")
	retainedUnique := committed.uniqueConstraints["Account:retainedUID"]
	retainedComposite := committed.compositeIndexes["retained_composite"]
	retainedRange, _ := committed.GetRangeIndex("retained_range")
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.ErrorContains(t, tx.StageSchemaChanges(), "require a transaction schema view")
	require.NoError(t, tx.SetNamespace("schema_lifetime"))
	view, err := tx.Schema()
	require.NoError(t, err)
	require.NoError(t, tx.StageSchemaChanges())
	require.Nil(t, tx.schemaRuntime)
	require.NoError(t, view.AddPropertyIndex("created", "Account", []string{"id"}))
	require.NoError(t, view.BackfillPropertyIndex("Account", "id", map[NodeID]interface{}{"schema_lifetime:created": int64(1)}))
	require.NoError(t, view.AddUniqueConstraint("created_unique", "Account", "uid"))
	view.RegisterUniqueValue("Account", "uid", "created", "schema_lifetime:created")
	view.uniqueConstraints["Account:uid"].valuesCacheComplete = true
	require.NoError(t, view.AddCompositeIndex("created_composite", "Account", []string{"id", "uid"}))
	view.compositeIndexes["created_composite"].fullIndex["created"] = []NodeID{"schema_lifetime:created"}
	view.compositeIndexes["created_composite"].prefixIndex["created"] = []NodeID{"schema_lifetime:created"}
	require.NoError(t, view.AddRangeIndex("created_range", "Account", "score"))
	require.NoError(t, view.RangeIndexInsert("created_range", "schema_lifetime:created", float64(1)))
	require.NoError(t, tx.StageSchemaChanges())
	require.NoError(t, view.BackfillPropertyIndex("Account", "id", map[NodeID]interface{}{"schema_lifetime:later": int64(2)}))
	require.NoError(t, tx.Commit())
	require.Equal(t, []NodeID{"schema_lifetime:created"}, committed.PropertyIndexLookup("Account", "id", int64(1)))
	require.Empty(t, committed.PropertyIndexLookup("Account", "id", int64(2)))
	require.Equal(t, []NodeID{"schema_lifetime:created"}, committed.compositeIndexes["created_composite"].fullIndex["created"])
	require.Equal(t, []NodeID{"schema_lifetime:created"}, committed.compositeIndexes["created_composite"].prefixIndex["created"])
	require.Equal(t, float64(1), committed.rangeIndexes["created_range"].nodeValue["schema_lifetime:created"])
	_, found, exists, complete := committed.LookupUniqueConstraintValueForPlanning("Account", "uid", "created")
	require.True(t, found)
	require.True(t, exists)
	require.True(t, complete)
	actualProperty, _ := committed.GetPropertyIndex("Account", "name")
	require.Same(t, retainedProperty, actualProperty)
	require.Same(t, retainedUnique, committed.uniqueConstraints["Account:retainedUID"])
	require.Same(t, retainedComposite, committed.compositeIndexes["retained_composite"])
	require.Same(t, retainedRange, committed.rangeIndexes["retained_range"])
	require.Error(t, tx.StageSchemaChanges())
	_, err = tx.Schema()
	require.Error(t, err)
}

func TestSchemaTransactionRejectsAllEntityMutations(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("schema_mutations"))
	_, err = tx.Schema()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "schema_mutations:node"})
	require.ErrorContains(t, err, "ForbiddenDueToTransactionType")
	for _, mutation := range []func() error{
		func() error { return tx.UpdateNode(&Node{ID: "schema_mutations:node"}) },
		func() error { return tx.DeleteNode("schema_mutations:node") },
		func() error { return tx.CreateEdge(&Edge{ID: "schema_mutations:edge"}) },
		func() error { return tx.BulkCreateEdges([]*Edge{{ID: "schema_mutations:edge"}}) },
		func() error { return tx.UpdateEdge(&Edge{ID: "schema_mutations:edge"}) },
		func() error { return tx.DeleteEdge("schema_mutations:edge") },
	} {
		require.ErrorContains(t, mutation(), "ForbiddenDueToTransactionType")
	}
	require.NoError(t, tx.Rollback())
	_, err = tx.CreateNode(&Node{ID: "schema_mutations:node"})
	require.Error(t, err)
	_, err = tx.Schema()
	require.Error(t, err)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "schema_mutations:node"})
	require.NoError(t, err)
	_, err = tx.Schema()
	require.ErrorContains(t, err, "ForbiddenDueToTransactionType")
	_, err = tx.KnowledgePolicySchema()
	require.NoError(t, err)
	require.ErrorContains(t, tx.StageSchemaChanges(), "ForbiddenDueToTransactionType")
	require.NoError(t, tx.Rollback())
}

func TestSchemaWriteBarrierRejectsClosedEngine(t *testing.T) {
	engine := NewMemoryEngine()
	require.NoError(t, engine.Close())
	_, err := engine.beginSchemaWrite()
	require.ErrorIs(t, err, ErrStorageClosed)
}

func (s *txLifecycleControllerStub) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	s.mu.Lock()
	s.registerCount++
	s.registeredInfo = append(s.registeredInfo, info)
	s.mu.Unlock()
	return func() {}
}

func (s *txLifecycleControllerStub) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": s.enabled}
}

func (s *txLifecycleControllerStub) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	s.pruneCount++
	s.mu.Unlock()
	return nil
}

func (s *txLifecycleControllerStub) PauseLifecycle() {
	s.mu.Lock()
	s.pauseCount++
	s.mu.Unlock()
}

func (s *txLifecycleControllerStub) ResumeLifecycle() {
	s.mu.Lock()
	s.resumeCount++
	s.mu.Unlock()
}

func (s *txLifecycleControllerStub) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	s.acquireCount++
	s.registeredInfo = append(s.registeredInfo, info)
	return func() {
		s.mu.Lock()
		s.releaseCount++
		s.mu.Unlock()
	}, nil
}

func (s *txLifecycleControllerStub) EvaluateSnapshotReader(info SnapshotReaderInfo) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evaluateCount++
	s.registeredInfo = append(s.registeredInfo, info)
	return s.gracefulExpire, s.hardExpire
}

func (s *txLifecycleControllerStub) RunPruneNow(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}

func (s *txLifecycleControllerStub) StartLifecycle(ctx context.Context) {
	_ = ctx
}

func (s *txLifecycleControllerStub) StopLifecycle() {}

func (s *txLifecycleControllerStub) IsLifecycleEnabled() bool {
	return s.enabled
}

func (s *txLifecycleControllerStub) IsLifecycleRunning() bool {
	return false
}

func (s *txLifecycleControllerStub) ReaderRegistry() SnapshotReaderRegistry {
	return nil
}

func TestBeginTransaction_WithLifecycleAdmissionDoesNotDeadlock(t *testing.T) {
	// Reader admission is registered the first time the transaction's
	// namespace is pinned (on first prefixed write or SetNamespace),
	// not at BeginTransaction itself, because the namespace — and the
	// per-namespace MVCC counter to register against — is unknown at
	// begin time.
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	done := make(chan struct{})
	var tx *BadgerTransaction
	var err error
	go func() {
		tx, err = engine.BeginTransaction()
		if err == nil && tx != nil {
			err = tx.SetNamespace("test")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("BeginTransaction blocked with lifecycle admission enabled")
	}

	require.NoError(t, err)
	require.NotNil(t, tx)
	require.Equal(t, 1, controller.acquireCount)
	require.Len(t, controller.registeredInfo, 1)
	require.False(t, controller.registeredInfo[0].SnapshotVersion.IsZero())
	require.NoError(t, tx.Rollback())
	require.Equal(t, 1, controller.releaseCount)
}

func TestBeginTransaction_LifecycleAdmissionFailureReturnsError(t *testing.T) {
	// Admission failure now surfaces on the first pin attempt, since
	// admission cannot run until a namespace is bound.
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true, err: ErrMVCCResourcePressure}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	pinErr := tx.SetNamespace("test")
	require.ErrorIs(t, pinErr, ErrMVCCResourcePressure)
	require.Equal(t, 0, controller.releaseCount)
	require.NoError(t, tx.Rollback())
}

func TestTransaction_GracefulSnapshotExpirationCancelsWorkAndReleasesReader(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	// Pin the transaction so the snapshot reader is registered with the
	// lifecycle controller; expiration paths only fire for registered readers.
	require.NoError(t, tx.SetNamespace("nornic"))

	controller.mu.Lock()
	controller.gracefulExpire = true
	controller.mu.Unlock()

	_, err = tx.GetNode(NodeID("nornic:missing"))
	require.ErrorIs(t, err, ErrMVCCSnapshotGracefulCancel)
	require.Equal(t, TxStatusRolledBack, tx.Status)
	require.Equal(t, 1, controller.releaseCount)
	require.Equal(t, 1, controller.evaluateCount)
}

func TestTransaction_HardSnapshotExpirationFailsCommitAndReleasesReader(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	engine.SetLifecycleController(controller)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NoError(t, tx.SetNamespace("nornic"))

	controller.mu.Lock()
	controller.hardExpire = true
	controller.mu.Unlock()

	err = tx.Commit()
	require.ErrorIs(t, err, ErrMVCCSnapshotHardExpired)
	require.Equal(t, TxStatusRolledBack, tx.Status)
	require.Equal(t, 1, controller.releaseCount)
	require.Equal(t, 1, controller.evaluateCount)
}

func TestLifecycleWrappers_DelegateLifecycleControls(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	controller := &txLifecycleControllerStub{enabled: true}
	base.SetLifecycleController(controller)

	async := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = async.Close() })
	wal := NewWALEngine(base, nil)
	namespaced := NewNamespacedEngine(base, "tenant_a")

	async.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "async", Namespace: "override"})
	wal.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "wal"})
	namespaced.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "ns"})

	require.Equal(t, true, async.LifecycleStatus()["enabled"])
	require.Equal(t, true, wal.LifecycleStatus()["enabled"])
	require.Equal(t, "tenant_a", namespaced.LifecycleStatus()["namespace"])

	require.NoError(t, async.TriggerPruneNow(context.Background()))
	require.NoError(t, wal.TriggerPruneNow(context.Background()))
	require.NoError(t, namespaced.TriggerPruneNow(context.Background()))

	async.PauseLifecycle()
	wal.PauseLifecycle()
	namespaced.PauseLifecycle()
	async.ResumeLifecycle()
	wal.ResumeLifecycle()
	namespaced.ResumeLifecycle()

	controller.mu.Lock()
	defer controller.mu.Unlock()
	require.Equal(t, 3, controller.registerCount)
	require.Len(t, controller.registeredInfo, 3)
	require.Equal(t, "override", controller.registeredInfo[0].Namespace)
	require.Equal(t, "", controller.registeredInfo[1].Namespace)
	require.Equal(t, "tenant_a", controller.registeredInfo[2].Namespace)
	require.Equal(t, 3, controller.pruneCount)
	require.Equal(t, 3, controller.pauseCount)
	require.Equal(t, 3, controller.resumeCount)
}
