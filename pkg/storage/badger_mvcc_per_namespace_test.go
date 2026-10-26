package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// These tests assert the per-database MVCC counter and high-water clamp
// invariants. The version sequence and high-water mark are kept on
// namespaceMVCCState (one per active namespace); transactions are pinned
// to a single namespace at first prefixed write, and version allocations
// are routed through that namespace's state. Deeply asserting those
// invariants prevents two regressions at once: (1) a noisy tenant
// accelerating another tenant's version numbers, and (2) cross-namespace
// version collisions that would corrupt snapshot-isolation conflict
// detection.

func TestPerNamespaceMVCC_IndependentSequences(t *testing.T) {
	engine := newTestEngine(t)

	// Allocate three commits in tenant_a directly via the engine API.
	// Per-namespace counter must increment 1, 2, 3 in tenant_a regardless
	// of unrelated tenant_b activity.
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 3; i++ {
			_, err := engine.allocateMVCCVersion(txn, "tenant_a", time.Now())
			if err != nil {
				return err
			}
		}
		return nil
	}))
	stateA, err := engine.namespaceMVCC("tenant_a")
	require.NoError(t, err)
	require.Equal(t, uint64(3), stateA.seq.Load(), "tenant_a sequence after 3 commits must be exactly 3")

	// tenant_b starts fresh; its sequence must not have been advanced by
	// tenant_a's commits.
	stateB, err := engine.namespaceMVCC("tenant_b")
	require.NoError(t, err)
	require.Equal(t, uint64(0), stateB.seq.Load(), "tenant_b counter must remain at 0; tenant_a activity must not bleed across namespaces")

	// One commit in tenant_b moves only tenant_b's counter.
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		_, err := engine.allocateMVCCVersion(txn, "tenant_b", time.Now())
		return err
	}))
	require.Equal(t, uint64(1), stateB.seq.Load(), "tenant_b sequence after 1 commit must be 1")
	require.Equal(t, uint64(3), stateA.seq.Load(), "tenant_a sequence must be unchanged by tenant_b activity")
}

func TestPerNamespaceMVCC_ConcurrentTenantsDoNotInterleave(t *testing.T) {
	// A noisy tenant cannot accelerate another tenant's counter even
	// under heavy concurrent allocation. We allocate N commits each in
	// two namespaces from concurrent goroutines and assert that each
	// counter ends at exactly N.
	engine := newTestEngine(t)
	const allocsPerTenant = 200

	var wg sync.WaitGroup
	for _, ns := range []string{"tenant_x", "tenant_y"} {
		ns := ns
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < allocsPerTenant; i++ {
				err := engine.db.Update(func(txn *badger.Txn) error {
					_, err := engine.allocateMVCCVersion(txn, ns, time.Now())
					return err
				})
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	stateX, err := engine.namespaceMVCC("tenant_x")
	require.NoError(t, err)
	require.Equal(t, uint64(allocsPerTenant), stateX.seq.Load(),
		"tenant_x counter must equal exactly its allocation count even with concurrent tenant_y traffic")

	stateY, err := engine.namespaceMVCC("tenant_y")
	require.NoError(t, err)
	require.Equal(t, uint64(allocsPerTenant), stateY.seq.Load(),
		"tenant_y counter must equal exactly its allocation count even with concurrent tenant_x traffic")
}

func TestPerNamespaceMVCC_HighWaterIsPerNamespace(t *testing.T) {
	engine := newTestEngine(t)

	// Bump tenant_a's high-water 2s into the future; tenant_b's must
	// remain at zero. Reads in tenant_b must NOT be clamped by
	// tenant_a's clock skew.
	stateA, err := engine.namespaceMVCC("tenant_a")
	require.NoError(t, err)
	future := time.Now().Add(2 * time.Second).UnixNano()
	stateA.highWaterNanos.Store(future)

	stateB, err := engine.namespaceMVCC("tenant_b")
	require.NoError(t, err)
	require.Equal(t, int64(0), stateB.highWaterNanos.Load(),
		"tenant_b high-water must not be affected by tenant_a's high-water bump")

	readA := engine.currentMVCCReadVersion("tenant_a")
	require.GreaterOrEqual(t, readA.CommitTimestamp.UnixNano(), future,
		"tenant_a read must be clamped to its own high-water mark")

	readB := engine.currentMVCCReadVersion("tenant_b")
	require.Less(t, readB.CommitTimestamp.UnixNano(), future,
		"tenant_b read must NOT be clamped to tenant_a's high-water mark")
}

func TestPerNamespaceMVCC_BeginTransactionInOneNamespaceCannotObserveAnother(t *testing.T) {
	// Snapshot semantics: a transaction pinned to namespace A must not
	// see versions from namespace B. We commit a node in namespace_b
	// and confirm a transaction in namespace_a does not see it.
	engine := newTestEngine(t)

	// Seed a node in namespace_b.
	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx1.SetNamespace("namespace_b"))
	_, err = tx1.CreateNode(&Node{
		ID:         NodeID("namespace_b:visible"),
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(1)},
	})
	require.NoError(t, err)
	require.NoError(t, tx1.Commit())

	// Open a transaction in namespace_a.
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx2.SetNamespace("namespace_a"))

	// The node lives in namespace_b — namespace_a transaction must not
	// fetch it via its own prefix, and the cross-namespace lookup must
	// fail with the per-tx pin guard.
	_, err = tx2.GetNode(NodeID("namespace_a:visible"))
	require.ErrorIs(t, err, ErrNotFound,
		"transaction in namespace_a must not find a namespace_b node via its own prefix")
	require.NoError(t, tx2.Rollback())
}

func TestPerNamespaceMVCC_SaturationFallbackIsPerNamespace(t *testing.T) {
	// When tenant_a's sequence saturates, it must fall back to
	// timestamp-only ordering for tenant_a only — tenant_b's allocator
	// continues to use sequence ordering normally.
	engine := newTestEngine(t)

	stateA, err := engine.namespaceMVCC("tenant_a")
	require.NoError(t, err)
	stateA.seq.Store(maxMVCCCommitSequence)
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	stateA.highWaterNanos.Store(base.UnixNano())

	var saturated MVCCVersion
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		var allocErr error
		saturated, allocErr = engine.allocateMVCCVersion(txn, "tenant_a", base.Add(-time.Hour))
		return allocErr
	}))
	require.Equal(t, maxMVCCCommitSequence, saturated.CommitSequence,
		"saturated tenant_a must keep its sequence at max")
	require.Equal(t, base.Add(time.Nanosecond), saturated.CommitTimestamp,
		"saturated tenant_a must advance the high-water by 1ns rather than wrap")

	// The persisted per-namespace key must NOT have been rewritten
	// (saturation path skips persistence).
	persistedA, err := engine.loadPersistedNamespaceSequence("tenant_a")
	require.NoError(t, err)
	require.Equal(t, uint64(0), persistedA,
		"saturated allocator must not rewrite tenant_a's persisted sequence")

	// tenant_b still uses sequence ordering: its first commit increments
	// from 0 to 1 (or higher if seeded by the legacy global key — for a
	// fresh test engine, exactly 1).
	var fresh MVCCVersion
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		var allocErr error
		fresh, allocErr = engine.allocateMVCCVersion(txn, "tenant_b", time.Now())
		return allocErr
	}))
	require.Equal(t, uint64(1), fresh.CommitSequence,
		"tenant_b must allocate sequence 1 even while tenant_a is saturated")
}

func TestPerNamespaceMVCC_LegacyGlobalCounterSeedsNewNamespace(t *testing.T) {
	// Backward compatibility: an existing database with a value at the
	// legacy engine-global mvccSequenceKey must seed every newly-touched
	// namespace's starting sequence to the legacy value, so versions
	// previously emitted under the global counter remain strictly less
	// than anything new namespaces will emit. Without this seeding, a
	// reopened database could allocate a version that collides with a
	// pre-existing head's CommitSequence and silently corrupt SI.
	engine := newTestEngine(t)
	legacy := uint64(42)
	engine.mvccLegacyGlobalSeed = legacy

	state, err := engine.namespaceMVCC("fresh_namespace")
	require.NoError(t, err)
	require.Equal(t, legacy, state.seq.Load(),
		"a freshly-touched namespace in an upgraded database must start at the legacy global seed, not zero")

	// Allocating the first version in this namespace advances above
	// the seed.
	var v MVCCVersion
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		var allocErr error
		v, allocErr = engine.allocateMVCCVersion(txn, "fresh_namespace", time.Now())
		return allocErr
	}))
	require.Equal(t, legacy+1, v.CommitSequence,
		"first commit in upgraded namespace must be legacySeed+1, never colliding with pre-existing versions")
}

func TestEnsureNamespaceMVCC_RecoversSequenceFromExistingHeadsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{ID: NodeID("nornic:node-1"), Labels: []string{"L"}})
	require.NoError(t, err)

	persistedBeforeClose, err := engine.loadPersistedNamespaceSequence("nornic")
	require.NoError(t, err)
	require.Equal(t, uint64(0), persistedBeforeClose,
		"non-transactional writes do not persist the namespace sequence, so reopen must recover from MVCC heads")

	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer reopened.Close()

	require.NoError(t, reopened.EnsureNamespaceMVCC("nornic"))

	persistedAfterEnsure, err := reopened.loadPersistedNamespaceSequence("nornic")
	require.NoError(t, err)
	require.Equal(t, uint64(1), persistedAfterEnsure,
		"EnsureNamespaceMVCC must persist the recovered namespace floor so later restarts do not fall back to seq=0")

	tx, err := reopened.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("nornic"))
	require.Equal(t, uint64(1), tx.readTS.CommitSequence,
		"transaction snapshot must bind to the recovered namespace sequence instead of seq=0")

	node, err := tx.GetNode(NodeID("nornic:node-1"))
	require.NoError(t, err)
	node.Properties = map[string]any{"updated": true}
	require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	require.NoError(t, tx.Commit(),
		"updating a pre-reopen node must not conflict against a recovered namespace floor")
}
