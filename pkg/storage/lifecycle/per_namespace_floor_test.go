package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These tests assert the per-namespace floor invariants the lifecycle
// layer must uphold once MVCC counters are sharded by database:
//   - ReaderRegistry.OldestReaderVersionsByNamespace() filters by
//     SnapshotReaderInfo.Namespace and returns the minimum per group.
//   - PrunePlanner consults the per-namespace floor callback for each
//     head it visits, derived from the head's logical-key namespace.
//
// Without these invariants, the planner could prune a head whose
// namespace has an active reader pinning it (silent SI corruption) or
// over-conservatively retain versions for namespaces with no readers.

func TestReaderRegistry_OldestVersionsByNamespace_FiltersByNamespace(t *testing.T) {
	registry := NewReaderRegistry()

	t1 := time.Now()
	older := storage.MVCCVersion{CommitTimestamp: t1, CommitSequence: 5}
	newer := storage.MVCCVersion{CommitTimestamp: t1.Add(time.Second), CommitSequence: 10}
	otherTenant := storage.MVCCVersion{CommitTimestamp: t1, CommitSequence: 3}

	_, deregA1 := registry.Register(storage.SnapshotReaderInfo{
		ReaderID:        "tenant_a:tx1",
		Namespace:       "tenant_a",
		SnapshotVersion: newer,
		StartTime:       t1,
	})
	defer deregA1()
	_, deregA2 := registry.Register(storage.SnapshotReaderInfo{
		ReaderID:        "tenant_a:tx2",
		Namespace:       "tenant_a",
		SnapshotVersion: older,
		StartTime:       t1,
	})
	defer deregA2()
	_, deregB1 := registry.Register(storage.SnapshotReaderInfo{
		ReaderID:        "tenant_b:tx1",
		Namespace:       "tenant_b",
		SnapshotVersion: otherTenant,
		StartTime:       t1,
	})
	defer deregB1()

	floors := registry.OldestReaderVersionsByNamespace()
	require.Len(t, floors, 2,
		"must report exactly one floor per active namespace")

	floorA, ok := floors["tenant_a"]
	require.True(t, ok, "tenant_a floor must be present")
	require.Equal(t, older, floorA,
		"tenant_a floor must be the smaller of the two readers in tenant_a, NOT pulled from tenant_b")

	floorB, ok := floors["tenant_b"]
	require.True(t, ok, "tenant_b floor must be present")
	require.Equal(t, otherTenant, floorB,
		"tenant_b floor must come from a tenant_b reader, NOT mixed with tenant_a's lower CommitSequence")

	_, ok = floors["tenant_unused"]
	require.False(t, ok,
		"namespaces with no registered readers MUST NOT appear in the floor map (planner treats absence as 'no floor')")
}

func TestReaderRegistry_OldestVersionsByNamespace_DeregistrationDropsFloor(t *testing.T) {
	registry := NewReaderRegistry()
	t1 := time.Now()
	v := storage.MVCCVersion{CommitTimestamp: t1, CommitSequence: 7}

	_, dereg := registry.Register(storage.SnapshotReaderInfo{
		ReaderID:        "ephemeral",
		Namespace:       "ephemeral_ns",
		SnapshotVersion: v,
		StartTime:       t1,
	})

	require.Contains(t, registry.OldestReaderVersionsByNamespace(), "ephemeral_ns")
	dereg()
	require.NotContains(t, registry.OldestReaderVersionsByNamespace(), "ephemeral_ns",
		"deregistered readers must drop their namespace's floor entry")
}

func TestPrunePlanner_UsesPerNamespaceFloor(t *testing.T) {
	// Two heads in different namespaces share the same FloorVersion,
	// HeadVersion, and version chain. Only their logical-key
	// namespaces differ. The planner must consult the per-namespace
	// floor callback and produce different prune outcomes:
	//   - tenant_a (active reader at historicalVersion): historicalVersion
	//     must NOT be pruned.
	//   - tenant_b (no reader): historicalVersion MUST be pruned.
	planner := NewPrunePlanner(LifecycleConfig{
		MaxVersionsPerKey:  0,
		DebtSampleFraction: 1,
	})

	base := time.Unix(1_700_000_000, 0).UTC()
	keyA := append([]byte{0x0C}, "tenant_a:k1"...)
	keyB := append([]byte{0x0C}, "tenant_b:k1"...)

	headVersion := storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Second), CommitSequence: 5}
	historicalVersion := storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}

	engine := &floorTestEngine{
		heads: map[string]storage.MVCCHead{
			string(keyA): {Version: headVersion, FloorVersion: historicalVersion},
			string(keyB): {Version: headVersion, FloorVersion: historicalVersion},
		},
		versions: map[string][]floorTestVersion{
			string(keyA): {
				{version: historicalVersion, sizeBytes: 10},
				{version: headVersion, sizeBytes: 10},
			},
			string(keyB): {
				{version: historicalVersion, sizeBytes: 10},
				{version: headVersion, sizeBytes: 10},
			},
		},
	}

	// Sentinel "no floor" lets TTL/MaxVersions drive pruning alone.
	noFloor := storage.MVCCVersion{
		CommitTimestamp: time.Unix(0, 1<<62).UTC(),
		CommitSequence:  ^uint64(0),
	}
	floorByNS := func(ns string) storage.MVCCVersion {
		switch ns {
		case "tenant_a":
			return historicalVersion
		default:
			return noFloor
		}
	}

	plan, err := planner.Plan(context.Background(), engine, floorByNS)
	require.NoError(t, err)

	var aEntry, bEntry *PrunePlanEntry
	for i := range plan.Entries {
		entry := &plan.Entries[i]
		switch string(entry.LogicalKey) {
		case string(keyA):
			aEntry = entry
		case string(keyB):
			bEntry = entry
		}
	}

	// tenant_a's reader pins historicalVersion. The planner may produce
	// no entry at all for tenant_a (nothing to prune) or an entry with
	// an empty VersionsToDelete; either is acceptable. What is NOT
	// acceptable is including historicalVersion in tenant_a's
	// VersionsToDelete.
	if aEntry != nil {
		require.NotContains(t, aEntry.VersionsToDelete, historicalVersion,
			"tenant_a has an active reader pinning historicalVersion; the planner MUST spare it")
	}

	require.NotNil(t, bEntry,
		"tenant_b has no reader and a prunable historical version; the planner must produce an entry")
	require.Contains(t, bEntry.VersionsToDelete, historicalVersion,
		"tenant_b has no reader pinning historicalVersion; the planner MUST prune it")
}

func TestPrunePlanner_NilFloorCallbackTreatsAllNamespacesAsNoReader(t *testing.T) {
	planner := NewPrunePlanner(LifecycleConfig{
		MaxVersionsPerKey:  0,
		DebtSampleFraction: 1,
	})

	base := time.Unix(1_700_000_000, 0).UTC()
	key := append([]byte{0x0C}, "tenant_a:k1"...)
	headVersion := storage.MVCCVersion{CommitTimestamp: base.Add(2 * time.Second), CommitSequence: 5}
	historicalVersion := storage.MVCCVersion{CommitTimestamp: base, CommitSequence: 1}

	engine := &floorTestEngine{
		heads: map[string]storage.MVCCHead{
			string(key): {Version: headVersion, FloorVersion: historicalVersion},
		},
		versions: map[string][]floorTestVersion{
			string(key): {
				{version: historicalVersion, sizeBytes: 10},
				{version: headVersion, sizeBytes: 10},
			},
		},
	}

	plan, err := planner.Plan(context.Background(), engine, nil)
	require.NoError(t, err)
	require.Len(t, plan.Entries, 1)
	require.Contains(t, plan.Entries[0].VersionsToDelete, historicalVersion,
		"a nil floor callback must allow pruning of historical versions (no readers anywhere)")
}

// floorTestEngine is a minimal LifecycleStorageEngine that supplies a
// fixed set of heads and version chains for planner tests.
type floorTestEngine struct {
	heads    map[string]storage.MVCCHead
	versions map[string][]floorTestVersion
}

type floorTestVersion struct {
	version    storage.MVCCVersion
	tombstoned bool
	sizeBytes  int64
}

func (e *floorTestEngine) IterateMVCCHeads(ctx context.Context, yield func(logicalKey []byte, head storage.MVCCHead) error) error {
	for k, head := range e.heads {
		if err := yield([]byte(k), head); err != nil {
			return err
		}
	}
	return nil
}

func (e *floorTestEngine) IterateMVCCVersions(ctx context.Context, logicalKey []byte, yield func(version storage.MVCCVersion, tombstoned bool, sizeBytes int64) error) error {
	versions := e.versions[string(logicalKey)]
	for _, v := range versions {
		if err := yield(v.version, v.tombstoned, v.sizeBytes); err != nil {
			return err
		}
	}
	return nil
}

func (e *floorTestEngine) DeleteMVCCVersion(ctx context.Context, logicalKey []byte, version storage.MVCCVersion) error {
	return nil
}

func (e *floorTestEngine) WriteMVCCHead(ctx context.Context, logicalKey []byte, head storage.MVCCHead) error {
	return nil
}

func (e *floorTestEngine) ReadMVCCHead(ctx context.Context, logicalKey []byte) (storage.MVCCHead, error) {
	if h, ok := e.heads[string(logicalKey)]; ok {
		return h, nil
	}
	return storage.MVCCHead{}, storage.ErrNotFound
}

func (e *floorTestEngine) DataDirFreeSpace() (int64, error) {
	return 1 << 40, nil
}
