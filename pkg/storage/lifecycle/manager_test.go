package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagerRunPruneNow_RealEngineKeepsClosedVersions(t *testing.T) {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{
		RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100},
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	nodeID := storage.NodeID("lifecycle:n")
	_, err = engine.CreateNode(&storage.Node{ID: nodeID, Properties: map[string]any{"v": 0}})
	require.NoError(t, err)
	for version := 1; version <= 10; version++ {
		require.NoError(t, engine.UpdateNode(&storage.Node{ID: nodeID, Properties: map[string]any{"v": version}}))
	}
	var logical []byte
	require.NoError(t, engine.IterateMVCCHeads(context.Background(), func(key []byte, _ storage.MVCCHead) error {
		if key[0] == 0x0c {
			logical = bytes.Clone(key)
		}
		return nil
	}))
	require.NotEmpty(t, logical)
	countVersions := func() int {
		count := 0
		require.NoError(t, engine.IterateMVCCVersions(context.Background(), logical, func(_ storage.MVCCVersion, _ bool, _ int64) error {
			count++
			return nil
		}))
		return count
	}
	require.Equal(t, 10, countVersions())
	config := DefaultLifecycleConfig()
	config.Enabled = true
	config.MaxVersionsPerKey = 2
	manager := NewMVCCLifecycleManager(config, engine)
	engine.SetLifecycleController(manager)
	release := engine.RegisterSnapshotReader(storage.SnapshotReaderInfo{Namespace: "lifecycle"})
	deleted, err := manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, 10, countVersions())
	release()
	deleted, err = manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(8), deleted)
	require.Equal(t, 2, countVersions())
	deleted, err = manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.Equal(t, 1, countVersions())
}

type mockLifecycleEngine struct {
	mu          sync.Mutex
	heads       map[string]storage.MVCCHead
	versions    map[string][]mockVersion
	deleted     map[string][]storage.MVCCVersion
	freeSpace   int64
	deleteDelay time.Duration

	iterateHeadsErr    error
	iterateVersionsErr map[string]error
	deleteErrs         map[string]error
	readErrs           map[string]error
	writeErrs          map[string]error
}

type mockVersion struct {
	version    storage.MVCCVersion
	tombstoned bool
	sizeBytes  int64
}

func newMockLifecycleEngine() *mockLifecycleEngine {
	return &mockLifecycleEngine{
		heads:              make(map[string]storage.MVCCHead),
		versions:           make(map[string][]mockVersion),
		deleted:            make(map[string][]storage.MVCCVersion),
		freeSpace:          100 << 30,
		iterateVersionsErr: make(map[string]error),
		deleteErrs:         make(map[string]error),
		readErrs:           make(map[string]error),
		writeErrs:          make(map[string]error),
	}
}

func (m *mockLifecycleEngine) IterateMVCCHeads(ctx context.Context, yield func(logicalKey []byte, head storage.MVCCHead) error) error {
	if m.iterateHeadsErr != nil {
		return m.iterateHeadsErr
	}
	m.mu.Lock()
	entries := make([]struct {
		logicalKey string
		head       storage.MVCCHead
	}, 0, len(m.heads))
	for logicalKey, head := range m.heads {
		entries = append(entries, struct {
			logicalKey string
			head       storage.MVCCHead
		}{logicalKey: logicalKey, head: head})
	}
	m.mu.Unlock()
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := yield([]byte(entry.logicalKey), entry.head); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockLifecycleEngine) IterateMVCCVersions(ctx context.Context, logicalKey []byte, yield func(version storage.MVCCVersion, tombstoned bool, sizeBytes int64) error) error {
	if err := m.iterateVersionsErr[string(logicalKey)]; err != nil {
		return err
	}
	m.mu.Lock()
	versions := append([]mockVersion(nil), m.versions[string(logicalKey)]...)
	m.mu.Unlock()
	for _, version := range versions {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := yield(version.version, version.tombstoned, version.sizeBytes); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockLifecycleEngine) DeleteMVCCVersion(ctx context.Context, logicalKey []byte, version storage.MVCCVersion) error {
	if m.deleteDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.deleteDelay):
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(logicalKey)
	if err := m.deleteErrs[key]; err != nil {
		return err
	}
	m.deleted[key] = append(m.deleted[key], version)
	filtered := make([]mockVersion, 0, len(m.versions[key]))
	for _, existing := range m.versions[key] {
		if existing.version.Compare(version) != 0 {
			filtered = append(filtered, existing)
		}
	}
	m.versions[key] = filtered
	return nil
}

func (m *mockLifecycleEngine) WriteMVCCHead(ctx context.Context, logicalKey []byte, head storage.MVCCHead) error {
	if err := m.writeErrs[string(logicalKey)]; err != nil {
		return err
	}
	m.mu.Lock()
	m.heads[string(logicalKey)] = head
	m.mu.Unlock()
	return nil
}

func (m *mockLifecycleEngine) ReadMVCCHead(ctx context.Context, logicalKey []byte) (storage.MVCCHead, error) {
	if err := m.readErrs[string(logicalKey)]; err != nil {
		return storage.MVCCHead{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	head, ok := m.heads[string(logicalKey)]
	if !ok {
		return storage.MVCCHead{}, storage.ErrNotFound
	}
	return head, nil
}

func (m *mockLifecycleEngine) DataDirFreeSpace() (int64, error) {
	return m.freeSpace, nil
}

func TestReaderRegistry_RegisterDeregister(t *testing.T) {
	registry := NewReaderRegistry()
	base := time.Now().UTC()
	const readers = 16
	var wg sync.WaitGroup
	callbacks := make([]func(), 0, readers)
	var callbacksMu sync.Mutex
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, deregister := registry.Register(storage.SnapshotReaderInfo{
				SnapshotVersion: storage.MVCCVersion{CommitTimestamp: base.Add(time.Duration(i) * time.Second), CommitSequence: uint64(i + 1)},
				StartTime:       base.Add(-time.Duration(i) * time.Minute),
			})
			callbacksMu.Lock()
			callbacks = append(callbacks, deregister)
			callbacksMu.Unlock()
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(readers), registry.ActiveCount())
	oldest, ok := registry.OldestReaderVersion()
	require.True(t, ok)
	require.Equal(t, uint64(1), oldest.CommitSequence)
	for _, deregister := range callbacks {
		deregister()
	}
	require.Equal(t, int64(0), registry.ActiveCount())
}

func TestReaderRegistry_ReadersOlderThan(t *testing.T) {
	registry := NewReaderRegistry()
	_, oldDone := registry.Register(storage.SnapshotReaderInfo{ReaderID: "old", StartTime: time.Now().Add(-2 * time.Hour)})
	defer oldDone()
	_, newDone := registry.Register(storage.SnapshotReaderInfo{ReaderID: "new", StartTime: time.Now().Add(-2 * time.Minute)})
	defer newDone()
	older := registry.ReadersOlderThan(time.Hour)
	require.Len(t, older, 1)
	require.Equal(t, "old", older[0].ReaderID)
}

func TestComputeSafeFloor_Monotonic(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	previous := storage.MVCCVersion{CommitTimestamp: base.Add(5 * time.Second), CommitSequence: 5}
	oldest := storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Second), CommitSequence: 2}
	ttlBound := storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Second), CommitSequence: 3}
	maxBound := storage.MVCCVersion{CommitTimestamp: base.Add(4 * time.Second), CommitSequence: 4}
	computed := ComputeSafeFloor(oldest, ttlBound, maxBound, previous)
	require.Equal(t, 0, computed.Compare(previous))
}

func TestComputeSafeFloor_MinOfInputs(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	oldest := storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Second), CommitSequence: 2}
	ttlBound := storage.MVCCVersion{CommitTimestamp: base.Add(4 * time.Second), CommitSequence: 4}
	maxBound := storage.MVCCVersion{CommitTimestamp: base.Add(5 * time.Second), CommitSequence: 5}
	require.Greater(t, maxVersion().Compare(maxBound), 0)
	computed := ComputeSafeFloor(oldest, ttlBound, maxBound, storage.MVCCVersion{})
	require.Equal(t, oldest, computed)
	require.LessOrEqual(t, computed.Compare(ttlBound), 0)
	require.LessOrEqual(t, computed.Compare(maxBound), 0)
}

func TestPrunePlanner_CreatesPlan(t *testing.T) {
	engine := newMockLifecycleEngine()
	base := time.Unix(1_700_000_000, 0).UTC()
	logicalKey := string([]byte{0x0C}) + "tenant:key"
	engine.heads[logicalKey] = storage.MVCCHead{
		Version:      storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Minute), CommitSequence: 4},
		FloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.versions[logicalKey] = []mockVersion{
		{version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}, sizeBytes: 10},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, sizeBytes: 20},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, sizeBytes: 30},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Minute), CommitSequence: 4}, sizeBytes: 40},
	}
	planner := NewPrunePlanner(LifecycleConfig{MaxVersionsPerKey: 1, DebtSampleFraction: 1})
	plan, err := planner.Plan(context.Background(), engine, nil)
	require.NoError(t, err)
	require.Len(t, plan.Entries, 1)
	entry := plan.Entries[0]
	require.Equal(t, 0, entry.HeadVersion.Compare(engine.heads[logicalKey].Version))
	require.Equal(t, 0, entry.FloorVersion.Compare(engine.heads[logicalKey].FloorVersion))
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, entry.NewFloorVersion)
	require.Equal(t, int64(100), entry.DebtBytes)
	require.Len(t, entry.VersionsToDelete, 2)
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}, entry.VersionsToDelete[0])
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, entry.VersionsToDelete[1])
}

func TestPrunePlanner_MaxChainHardCapFallback(t *testing.T) {
	engine := newMockLifecycleEngine()
	base := time.Unix(1_700_000_000, 0).UTC()
	logicalKey := string([]byte{0x0C}) + "tenant:key"
	engine.heads[logicalKey] = storage.MVCCHead{
		Version:      storage.MVCCVersion{CommitTimestamp: base.Add(4 * time.Minute), CommitSequence: 5},
		FloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.versions[logicalKey] = []mockVersion{
		{version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}, sizeBytes: 10},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, sizeBytes: 10},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, sizeBytes: 10},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Minute), CommitSequence: 4}, sizeBytes: 10},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(4 * time.Minute), CommitSequence: 5}, sizeBytes: 10},
	}
	planner := NewPrunePlanner(LifecycleConfig{MaxVersionsPerKey: 100, MaxChainHardCap: 3, DebtSampleFraction: 1})
	plan, err := planner.Plan(context.Background(), engine, nil)
	require.NoError(t, err)
	require.Len(t, plan.Entries, 1)
	require.Len(t, plan.Entries[0].VersionsToDelete, 2)
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, plan.Entries[0].NewFloorVersion)
}

func TestPrunePlanner_PropagatesVersionIterationError(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	engine.heads[key] = storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: 1}}
	engine.iterateVersionsErr[key] = errors.New("boom")
	planner := NewPrunePlanner(LifecycleConfig{DebtSampleFraction: 1})
	_, err := planner.Plan(context.Background(), engine, nil)
	require.ErrorContains(t, err, "boom")
}

func TestPrunePlanner_PropagatesHeadIterationError(t *testing.T) {
	engine := newMockLifecycleEngine()
	engine.iterateHeadsErr = errors.New("head scan failed")
	planner := NewPrunePlanner(DefaultLifecycleConfig())
	_, err := planner.Plan(context.Background(), engine, nil)
	require.ErrorContains(t, err, "head scan failed")
}

func TestPruneApplier_FenceMismatch(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	entry := PrunePlanEntry{
		LogicalKey:       []byte(key),
		HeadVersion:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 0}},
		DebtBytes:        64,
	}
	engine.heads[key] = storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2}}
	applier := NewPruneApplier(DefaultLifecycleConfig(), NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entry}})
	require.Equal(t, 1, result.FenceMismatches)
	require.Empty(t, engine.deleted[key])
}

func TestPruneApplier_HotContentionCooldown(t *testing.T) {
	engine := newMockLifecycleEngine()
	config := DefaultLifecycleConfig()
	config.FenceRetryPerKeyLimit = 2
	config.FenceRetryCrossRunLimit = 0
	config.FenceRetryInitialDelay = 0
	config.FenceRetryMaxDelay = 0
	config.FenceRetryCooldown = time.Minute
	applier := NewPruneApplier(config, NewLifecycleMetrics())
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	entry := PrunePlanEntry{
		LogicalKey:      []byte(key),
		HeadVersion:     storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
		NewFloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.heads[key] = storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2}}
	plan := &PrunePlan{Entries: []PrunePlanEntry{entry}}
	first := applier.Apply(context.Background(), engine, plan)
	second := applier.Apply(context.Background(), engine, plan)
	third := applier.Apply(context.Background(), engine, plan)
	require.Equal(t, 1, first.FenceMismatches)
	require.Equal(t, 1, second.FenceMismatches)
	require.Equal(t, 1, second.HotContentionKeys)
	require.Equal(t, 0, third.FenceMismatches)
	require.False(t, applier.backoff.isEligible(key))
}

func TestPruneApplier_CrossRunRetryBudgetTriggersCooldown(t *testing.T) {
	engine := newMockLifecycleEngine()
	config := DefaultLifecycleConfig()
	config.FenceRetryPerKeyLimit = 100
	config.FenceRetryCrossRunLimit = 2
	config.FenceRetryInitialDelay = 0
	config.FenceRetryMaxDelay = 0
	config.FenceRetryCooldown = time.Minute
	applier := NewPruneApplier(config, NewLifecycleMetrics())
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	entry := PrunePlanEntry{
		LogicalKey:      []byte(key),
		HeadVersion:     storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
		NewFloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.heads[key] = storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2}}
	plan := &PrunePlan{Entries: []PrunePlanEntry{entry}}
	first := applier.Apply(context.Background(), engine, plan)
	second := applier.Apply(context.Background(), engine, plan)
	third := applier.Apply(context.Background(), engine, plan)
	require.Equal(t, 1, first.FenceMismatches)
	require.Equal(t, 1, second.FenceMismatches)
	require.Equal(t, 1, second.HotContentionKeys)
	require.Equal(t, 0, third.FenceMismatches)
	require.False(t, applier.backoff.isEligible(key))
}

func TestPruneApplier_FenceMatch(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	head := storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 3}}
	engine.heads[key] = head
	engine.versions[key] = []mockVersion{
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(-2 * time.Minute), CommitSequence: 1}, sizeBytes: 16},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 2}, sizeBytes: 16},
		{version: head.Version, sizeBytes: 16},
	}
	entry := PrunePlanEntry{
		LogicalKey:       []byte(key),
		HeadVersion:      head.Version,
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 2},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-2 * time.Minute), CommitSequence: 1}},
		DebtBytes:        16,
	}
	applier := NewPruneApplier(DefaultLifecycleConfig(), NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entry}})
	require.Equal(t, int64(1), result.VersionsDeleted)
	require.Len(t, engine.deleted[key], 1)
	require.Equal(t, 0, engine.heads[key].FloorVersion.Compare(entry.NewFloorVersion))
}

func TestPruneApplier_DeleteFailureDoesNotAdvanceFloor(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	head := storage.MVCCHead{
		Version:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 3},
		FloorVersion: storage.MVCCVersion{CommitTimestamp: base.Add(-3 * time.Minute), CommitSequence: 1},
	}
	engine.heads[key] = head
	engine.deleteErrs[key] = errors.New("delete failed")
	entry := PrunePlanEntry{
		LogicalKey:       []byte(key),
		HeadVersion:      head.Version,
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 2},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-2 * time.Minute), CommitSequence: 2}},
		DebtBytes:        32,
	}
	applier := NewPruneApplier(DefaultLifecycleConfig(), NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entry}})
	require.Equal(t, int64(0), result.VersionsDeleted)
	require.Equal(t, 0, result.KeysProcessed)
	require.Equal(t, int64(0), result.BytesFreed)
	require.Equal(t, 0, engine.heads[key].FloorVersion.Compare(head.FloorVersion))
	require.Empty(t, engine.deleted[key])
}

func TestPruneApplier_ReadHeadFailureSkipsEntry(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	engine.readErrs[key] = errors.New("cannot read head")
	applier := NewPruneApplier(DefaultLifecycleConfig(), NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{{LogicalKey: []byte(key)}}})
	require.Equal(t, 0, result.KeysProcessed)
	require.Equal(t, 0, result.FenceMismatches)
}

func TestPruneApplier_WriteHeadFailureDoesNotCountProgress(t *testing.T) {
	engine := newMockLifecycleEngine()
	key := string([]byte{0x0C}) + "tenant:key"
	base := time.Now().UTC()
	head := storage.MVCCHead{Version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2}}
	engine.heads[key] = head
	engine.writeErrs[key] = errors.New("write failed")
	entry := PrunePlanEntry{
		LogicalKey:       []byte(key),
		HeadVersion:      head.Version,
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1}},
		DebtBytes:        32,
	}
	applier := NewPruneApplier(DefaultLifecycleConfig(), NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entry}})
	require.Equal(t, int64(1), result.VersionsDeleted)
	require.Equal(t, 0, result.KeysProcessed)
	require.Equal(t, int64(0), result.BytesFreed)
}

func TestPruneApplier_RespectsIOBudget(t *testing.T) {
	engine := newMockLifecycleEngine()
	base := time.Now().UTC()
	keyA := string([]byte{0x0C}) + "tenant:key-a"
	keyB := string([]byte{0x0C}) + "tenant:key-b"
	entryA := PrunePlanEntry{
		LogicalKey:       []byte(keyA),
		HeadVersion:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2},
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1}},
		DebtBytes:        32,
	}
	entryB := PrunePlanEntry{
		LogicalKey:       []byte(keyB),
		HeadVersion:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2},
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1}},
		DebtBytes:        32,
	}
	engine.heads[keyA] = storage.MVCCHead{Version: entryA.HeadVersion}
	engine.heads[keyB] = storage.MVCCHead{Version: entryB.HeadVersion}
	config := DefaultLifecycleConfig()
	config.MaxIOBudgetBytesPerInterval = 32
	applier := NewPruneApplier(config, NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entryA, entryB}})
	require.Equal(t, 1, result.KeysProcessed)
	require.Equal(t, int64(32), result.BytesFreed)
	require.Len(t, engine.deleted[keyA], 1)
	require.Empty(t, engine.deleted[keyB])
}

func TestPruneApplier_RespectsRuntimeBudget(t *testing.T) {
	engine := newMockLifecycleEngine()
	engine.deleteDelay = 5 * time.Millisecond
	base := time.Now().UTC()
	keyA := string([]byte{0x0C}) + "tenant:key-a"
	keyB := string([]byte{0x0C}) + "tenant:key-b"
	entryA := PrunePlanEntry{
		LogicalKey:       []byte(keyA),
		HeadVersion:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2},
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1}},
		DebtBytes:        32,
	}
	entryB := PrunePlanEntry{
		LogicalKey:       []byte(keyB),
		HeadVersion:      storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 2},
		NewFloorVersion:  storage.MVCCVersion{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1},
		VersionsToDelete: []storage.MVCCVersion{{CommitTimestamp: base.Add(-time.Minute), CommitSequence: 1}},
		DebtBytes:        32,
	}
	engine.heads[keyA] = storage.MVCCHead{Version: entryA.HeadVersion}
	engine.heads[keyB] = storage.MVCCHead{Version: entryB.HeadVersion}
	config := DefaultLifecycleConfig()
	config.MaxRuntimePerCycle = time.Millisecond
	applier := NewPruneApplier(config, NewLifecycleMetrics())
	result := applier.Apply(context.Background(), engine, &PrunePlan{Entries: []PrunePlanEntry{entryA, entryB}})
	require.Equal(t, 1, result.KeysProcessed)
	require.Len(t, engine.deleted[keyA], 1)
	require.Empty(t, engine.deleted[keyB])
}

func TestBackoffQueue_HotContention(t *testing.T) {
	queue := newBackoffQueue(DefaultLifecycleConfig())
	key := "key-1"
	for i := 0; i < 3; i++ {
		queue.recordMismatch(key)
	}
	queue.markHotContention(key, time.Minute)
	require.False(t, queue.isEligible(key))
}

func TestPriorityScheduler_NamespaceBudgets(t *testing.T) {
	scheduler := NewPriorityScheduler(LifecycleConfig{NamespaceBudgets: map[string]NamespaceBudget{
		"a": {MaxBytesPerCycle: 50},
		"b": {MaxBytesPerCycle: 100},
	}})
	entries := []PrunePlanEntry{
		{CreatedAt: time.Now().Add(-time.Minute), LogicalKey: []byte{0x0C, 'a', ':', '1'}, DebtBytes: 60},
		{CreatedAt: time.Now().Add(-2 * time.Minute), LogicalKey: []byte{0x0C, 'b', ':', '1'}, DebtBytes: 40},
	}
	ordered := scheduler.Schedule(entries)
	require.Len(t, ordered, 1)
	require.Equal(t, "b", namespaceFromLogicalKey(ordered[0].LogicalKey))
	require.Equal(t, 1, scheduler.skipCounts[string([]byte{0x0C, 'a', ':', '1'})])
}

func TestPriorityScheduler_ScheduleEmergency(t *testing.T) {
	scheduler := NewPriorityScheduler(DefaultLifecycleConfig())
	entries := []PrunePlanEntry{
		{LogicalKey: []byte{0x0C, 'a', ':', '1'}, DebtBytes: 10, TombstoneDepth: 1},
		{LogicalKey: []byte{0x0C, 'b', ':', '1'}, DebtBytes: 50, TombstoneDepth: 0},
		{LogicalKey: []byte{0x0C, 'c', ':', '1'}, DebtBytes: 50, TombstoneDepth: 3},
	}
	ordered := scheduler.ScheduleEmergency(entries)
	require.Equal(t, "c", namespaceFromLogicalKey(ordered[0].LogicalKey))
	require.Equal(t, "b", namespaceFromLogicalKey(ordered[1].LogicalKey))
	require.Equal(t, "a", namespaceFromLogicalKey(ordered[2].LogicalKey))
}

func TestPressureController_BandTransitions(t *testing.T) {
	pinned := int64(0)
	controller := NewPressureController(PressureConfig{
		HighEnterBytes:      10,
		HighExitBytes:       5,
		CriticalEnterBytes:  20,
		CriticalExitBytes:   10,
		PressureEnterWindow: 0,
		PressureExitWindow:  0,
	}, func() int64 { return pinned }, func() int64 { return 100 })
	require.Equal(t, storage.PressureNormal, controller.Update())
	pinned = 12
	require.Equal(t, storage.PressureHigh, controller.Update())
	pinned = 25
	require.Equal(t, storage.PressureCritical, controller.Update())
	pinned = 8
	require.Equal(t, storage.PressureHigh, controller.Update())
	pinned = 4
	require.Equal(t, storage.PressureNormal, controller.Update())
}

func TestPressureController_SnapshotRejectionAndExpiration(t *testing.T) {
	pinned := int64(25)
	controller := NewPressureController(PressureConfig{
		HighEnterBytes:      10,
		HighExitBytes:       5,
		CriticalEnterBytes:  20,
		CriticalExitBytes:   10,
		PressureEnterWindow: 0,
		PressureExitWindow:  0,
	}, func() int64 { return pinned }, func() int64 { return 0 })
	require.Equal(t, storage.PressureCritical, controller.Update())
	require.True(t, controller.ShouldRejectLongSnapshot(31*time.Minute, time.Hour))
	graceful, hard := controller.ShouldExpireReader(storage.SnapshotReaderInfo{StartTime: time.Now().Add(-61 * time.Minute)}, time.Hour)
	require.True(t, graceful)
	require.True(t, hard)
}

func TestLifecycleMetrics_RecordPruneRun(t *testing.T) {
	metrics := NewLifecycleMetrics()
	metrics.UpdateDebt(256, 4)
	metrics.RecordPruneRun(ApplyResult{KeysProcessed: 2, VersionsDeleted: 3, BytesFreed: 128, FenceMismatches: 1}, 2*time.Second)
	status := metrics.ToMap(NewReaderRegistry())
	require.Equal(t, int64(128), status["mvcc_pruned_bytes_total"])
	require.Equal(t, int64(1), status["mvcc_prune_stale_plan_skips_total"])
	require.Equal(t, float64(2), status["mvcc_prune_run_duration_seconds"])
	rollups, ok := status["rollups"].(map[string]map[string]int64)
	require.True(t, ok)
	require.Equal(t, int64(1), rollups["10s"]["prune_runs"])
	require.Equal(t, int64(128), rollups["10s"]["bytes_freed"])
	require.Equal(t, int64(256), rollups["10s"]["compaction_debt_bytes_max"])
}

func TestDefaultLifecycleConfig(t *testing.T) {
	config := DefaultLifecycleConfig()
	require.Equal(t, 30*time.Second, config.CycleInterval)
	require.Equal(t, int64(5*1024*1024*1024), config.HighEnterBytes)
	require.Equal(t, int64(4*1024*1024*1024), config.HighExitBytes)
	require.Equal(t, int64(20*1024*1024*1024), config.CriticalEnterBytes)
	require.Equal(t, int64(16*1024*1024*1024), config.CriticalExitBytes)
	require.Equal(t, 1000, config.MaxChainHardCap)
	require.Equal(t, 20, config.FullScanEveryNCycles)
}

func TestManagerStatus(t *testing.T) {
	engine := newMockLifecycleEngine()
	manager := NewMVCCLifecycleManager(DefaultLifecycleConfig(), engine)
	status := manager.Status()
	require.Equal(t, false, status["enabled"])
	require.Equal(t, true, status["automatic"])
	require.Equal(t, "30s", status["cycle_interval"])
	require.Equal(t, storage.PressureNormal, status["pressure_band"])
	_, ok := status["last_run"]
	require.True(t, ok)
}

func TestManagerSetLifecycleSchedule(t *testing.T) {
	config := DefaultLifecycleConfig()
	config.Enabled = true
	manager := NewMVCCLifecycleManager(config, newMockLifecycleEngine())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.StartLifecycle(ctx)
	require.True(t, manager.IsLifecycleRunning())

	require.NoError(t, manager.SetLifecycleSchedule(0))
	require.False(t, manager.IsLifecycleRunning())
	status := manager.Status()
	require.Equal(t, false, status["automatic"])
	require.Equal(t, "0s", status["cycle_interval"])

	require.NoError(t, manager.SetLifecycleSchedule(2*time.Second))
	require.True(t, manager.IsLifecycleRunning())
	status = manager.Status()
	require.Equal(t, true, status["automatic"])
	require.Equal(t, "2s", status["cycle_interval"])
	manager.StopLifecycle()
}

func TestManagerAcquireSnapshotReaderRejectsUnderPressure(t *testing.T) {
	config := DefaultLifecycleConfig()
	config.Enabled = true
	config.HighEnterBytes = 10
	config.HighExitBytes = 5
	config.CriticalEnterBytes = 20
	config.CriticalExitBytes = 10
	config.PressureEnterWindow = 0
	config.PressureExitWindow = 0
	config.MaxSnapshotLifetime = time.Hour
	manager := NewMVCCLifecycleManager(config, newMockLifecycleEngine())
	manager.metrics.UpdatePinnedBytes(25)
	require.Equal(t, storage.PressureCritical, manager.pressure.Update())
	_, err := manager.AcquireSnapshotReader(storage.SnapshotReaderInfo{StartTime: time.Now().Add(-31 * time.Minute)})
	require.ErrorIs(t, err, storage.ErrMVCCResourcePressure)
}

func TestManagerEvaluateSnapshotReaderCountsExpirations(t *testing.T) {
	config := DefaultLifecycleConfig()
	config.Enabled = true
	config.HighEnterBytes = 10
	config.HighExitBytes = 5
	config.CriticalEnterBytes = 20
	config.CriticalExitBytes = 10
	config.PressureEnterWindow = 0
	config.PressureExitWindow = 0
	config.MaxSnapshotLifetime = time.Hour
	manager := NewMVCCLifecycleManager(config, newMockLifecycleEngine())

	manager.metrics.UpdatePinnedBytes(15)
	require.Equal(t, storage.PressureHigh, manager.pressure.Update())
	graceful, hard := manager.EvaluateSnapshotReader(storage.SnapshotReaderInfo{StartTime: time.Now().Add(-61 * time.Minute)})
	require.True(t, graceful)
	require.False(t, hard)
	status := manager.Status()
	require.Equal(t, int64(1), status["mvcc_snapshot_graceful_expirations_total"])
	require.Equal(t, int64(0), status["mvcc_snapshot_hard_expirations_total"])

	manager.metrics.UpdatePinnedBytes(25)
	require.Equal(t, storage.PressureCritical, manager.pressure.Update())
	graceful, hard = manager.EvaluateSnapshotReader(storage.SnapshotReaderInfo{StartTime: time.Now().Add(-61 * time.Minute)})
	require.True(t, graceful)
	require.True(t, hard)
	status = manager.Status()
	require.Equal(t, int64(1), status["mvcc_snapshot_graceful_expirations_total"])
	require.Equal(t, int64(1), status["mvcc_snapshot_hard_expirations_total"])
}

func TestManagerRunPruneNowPropagatesPlannerFailure(t *testing.T) {
	engine := newMockLifecycleEngine()
	engine.iterateHeadsErr = errors.New("scan failed")
	config := DefaultLifecycleConfig()
	config.Enabled = true
	manager := NewMVCCLifecycleManager(config, engine)
	_, err := manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.ErrorContains(t, err, "scan failed")
}

func TestManagerRunPruneNowUpdatesMetrics(t *testing.T) {
	engine := newMockLifecycleEngine()
	base := time.Unix(1_700_000_000, 0).UTC()
	tenantKey := string([]byte{0x0C}) + "tenant:key"
	otherKey := string([]byte{0x0C}) + "other:key"
	engine.heads[tenantKey] = storage.MVCCHead{
		Version:      storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Minute), CommitSequence: 4},
		FloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.versions[tenantKey] = []mockVersion{
		{version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}, sizeBytes: 8},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, sizeBytes: 8},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, sizeBytes: 8},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(3 * time.Minute), CommitSequence: 4}, sizeBytes: 8},
	}
	engine.heads[otherKey] = storage.MVCCHead{
		Version:      storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3},
		FloorVersion: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1},
	}
	engine.versions[otherKey] = []mockVersion{
		{version: storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}, sizeBytes: 4},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, sizeBytes: 4},
		{version: storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, sizeBytes: 4},
	}
	config := DefaultLifecycleConfig()
	config.Enabled = true
	config.MaxVersionsPerKey = 1
	config.DebtSampleFraction = 1
	manager := NewMVCCLifecycleManager(config, engine)
	deleted, err := manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(3), deleted)
	require.Len(t, engine.deleted[tenantKey], 2)
	require.Len(t, engine.deleted[otherKey], 1)
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Minute), CommitSequence: 3}, engine.heads[tenantKey].FloorVersion)
	require.Equal(t, storage.MVCCVersion{CommitTimestamp: base.Add(time.Minute), CommitSequence: 2}, engine.heads[otherKey].FloorVersion)
	status := manager.Status()
	require.Equal(t, deleted, status["last_run"].(map[string]interface{})["versions_deleted"])
	require.Equal(t, int64(2), status["mvcc_compaction_debt_keys"])
	require.Equal(t, int64(44), status["mvcc_pruned_bytes_total"])
	require.Equal(t, int64(44), status["mvcc_compaction_debt_bytes"])
	require.Equal(t, int64(2), status["mvcc_prune_run_keys_scanned_total"])
	require.Equal(t, int64(2), status["mvcc_floor_lag_versions"])
	topDebtKeys, ok := status["top_debt_keys"].([]storage.MVCCLifecycleDebtKey)
	require.True(t, ok)
	require.Len(t, topDebtKeys, 2)
	require.Equal(t, tenantKey, topDebtKeys[0].LogicalKey)
	require.Equal(t, int64(32), topDebtKeys[0].DebtBytes)
	require.Equal(t, 2, topDebtKeys[0].VersionsToDelete)
	perNamespace, ok := status["per_namespace"].(map[string]map[string]int64)
	require.True(t, ok)
	require.Equal(t, int64(32), perNamespace["tenant"]["compaction_debt_bytes"])
	require.Equal(t, int64(32), perNamespace["tenant"]["pruned_bytes_total"])
	require.Equal(t, int64(12), perNamespace["other"]["compaction_debt_bytes"])
	require.Equal(t, int64(12), perNamespace["other"]["pruned_bytes_total"])
}

func TestEmergencyController_ActivatesAndAdjustsBudget(t *testing.T) {
	controller := NewEmergencyController(EmergencyConfig{
		DebtGrowthSlopeThreshold: 1,
		MaxCPUShare:              0.8,
		MaxIOBudgetBytesPerCycle: 200,
		MaxRuntimePerCycle:       10 * time.Second,
	})
	controller.SetCritical(true)
	controller.RecordDebt(0)
	time.Sleep(5 * time.Millisecond)
	controller.RecordDebt(100)
	require.True(t, controller.Evaluate())
	updated := controller.AdjustCompactionBudget(LifecycleConfig{
		MaxCPUShare:                 0.2,
		MaxIOBudgetBytesPerInterval: 50,
		MaxRuntimePerCycle:          2 * time.Second,
		MaxSnapshotLifetime:         time.Hour,
	})
	require.Greater(t, updated.MaxCPUShare, 0.2)
	require.Greater(t, updated.MaxIOBudgetBytesPerInterval, int64(50))
	require.Greater(t, updated.MaxRuntimePerCycle, 2*time.Second)
	require.Equal(t, 30*time.Minute, updated.MaxSnapshotLifetime)
}
