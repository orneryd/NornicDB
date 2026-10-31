package multidb

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestReview1406SecondManagerMustNotStealPublicationOwner(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	first, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shared"}))
	view, err := first.GetStorage("shared")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"v": "small"}})
	require.NoError(t, err)
	before, _, _ := first.GetStorageSize(context.Background(), "shared")
	require.Positive(t, before)
	second, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	other, _, _ := second.GetStorageSize(context.Background(), "shared")
	require.Equal(t, before, other)
	require.NoError(t, view.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "n", Properties: map[string]any{"v": strings.Repeat("x", 4096)}}}))
	wantNodes, wantEdges, err := first.calculateStorageSizeFromEngine(context.Background(), view)
	require.NoError(t, err)
	tracked, _, _ := first.GetStorageSize(context.Background(), "shared")
	require.Equal(t, wantNodes+wantEdges, tracked, "a second read-only manager must not steal the first manager's accounting publication")
}

func TestManagedRuntimeSharedPolicyAndReservation(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	first, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "shared"}))
	left, err := first.GetStorage("shared")
	require.NoError(t, err)
	second, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	right, err := second.GetStorage("shared")
	require.NoError(t, err)
	require.NotSame(t, left, right, "each handle keeps its own engine route")
	require.Same(t, first.databases["shared"].managed.Load(), second.databases["shared"].managed.Load())
	require.NoError(t, second.SetDatabaseLimits(t.Context(), "shared", &Limits{Storage: StorageLimits{MaxNodes: 1}}))
	limits, err := first.GetDatabaseLimits("shared")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, err = left.CreateNode(&storage.Node{ID: "n"})
	require.NoError(t, err)
	_, err = right.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.NoError(t, first.SetDatabaseLimits(t.Context(), "shared", &Limits{Storage: StorageLimits{MaxNodes: 2}}))
	// The shared pending allowance is held between native admission and terminal
	// publication. A second manager must see it before its own public write.
	info := first.databases["shared"]
	require.NoError(t, info.reserveStorageUsage("shared", "pending", storageUsage{nodes: 1}, StorageLimits{MaxNodes: 2}))
	_, err = right.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	info.discardStorageUsage("pending")
	_, err = right.CreateNode(&storage.Node{ID: "allowed"})
	require.NoError(t, err)
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, int64(2))
	exactNodes, exactEdges, err := first.calculateStorageSizeFromEngine(context.Background(), storage.NewNamespacedEngine(base, "shared"))
	require.NoError(t, err)
	actual, _, _ := second.GetStorageSize(context.Background(), "shared")
	require.Equal(t, exactNodes+exactEdges, actual)
}

func TestManagedRuntimeWholeResetAndRecreate(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "lifecycle"}))
	view, err := manager.GetStorage("lifecycle")
	require.NoError(t, err)
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "lifecycle", &Limits{Storage: StorageLimits{MaxNodes: 1}}))
	_, err = view.CreateNode(&storage.Node{ID: "before"})
	require.NoError(t, err)
	_, _, _ = manager.GetStorageSize(context.Background(), "lifecycle")
	runtime := manager.databases["lifecycle"].managed.Load()
	_, _, err = base.DeleteByPrefix("lifecycle:")
	require.NoError(t, err)
	require.Same(t, runtime, manager.databases["lifecycle"].managed.Load())
	size, _, _ := manager.GetStorageSize(context.Background(), "lifecycle")
	require.Zero(t, size)
	limits, err := manager.GetDatabaseLimits("lifecycle")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, err = view.CreateNode(&storage.Node{ID: "after-reset"})
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "quota"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.NoError(t, manager.DropDatabase(t.Context(), "lifecycle"))
	_, err = view.CreateNode(&storage.Node{ID: "retired"})
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "lifecycle"}))
	next, err := manager.GetStorage("lifecycle")
	require.NoError(t, err)
	require.Same(t, runtime, manager.databases["lifecycle"].managed.Load(), "namespace lifetime keeps the canonical usage object")
	limits, err = manager.GetDatabaseLimits("lifecycle")
	require.NoError(t, err)
	require.Nil(t, limits)
	for _, id := range []storage.NodeID{"new-a", "new-b"} {
		_, err = next.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	exactNodes, exactEdges, err := manager.calculateStorageSizeFromEngine(context.Background(), storage.NewNamespacedEngine(base, "lifecycle"))
	require.NoError(t, err)
	actual, _, _ := manager.GetStorageSize(context.Background(), "lifecycle")
	require.Equal(t, exactNodes+exactEdges, actual)
}

type managedCatalogFailureEngine struct {
	storage.Engine
	reject            error
	rejectAt, updates int
	rejectDelete      bool
	acceptedError     bool
}

func (e *managedCatalogFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }

// Fault catalog writes at their actual authoritative submission boundary.
func (e *managedCatalogFailureEngine) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	update := false
	for _, op := range prepared.Operations() {
		if op.Type == storage.OpUpdateNode && op.Node != nil && op.Node.ID == "system:databases:metadata" {
			update = true
			break
		}
	}
	if update {
		if e.reject != nil {
			return nil, e.reject
		}
		e.updates++
		if e.rejectAt > 0 && e.updates == e.rejectAt {
			return nil, storage.ErrConflict
		}
		if e.acceptedError && e.updates == 1 {
			if err := prepared.Commit(nil); err != nil {
				return nil, err
			}
			receipt, resolve := storage.NewCommitReceipt(settle)
			if err := resolve.Resolve(storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict}); err != nil {
				return nil, err
			}
			return receipt, nil
		}
	}
	route, found := storage.EntityCommitSubmitterFor(e.Engine)
	if !found || route.Submitter == nil {
		return nil, storage.ErrInvalidData
	}
	return route.Submitter.SubmitEntityCommit(ctx, prepared, settle)
}
func (e *managedCatalogFailureEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	if e.rejectDelete {
		return 0, 0, storage.ErrConflict
	}
	return e.Engine.DeleteByPrefix(prefix)
}

func TestManagedRuntimeMetadataFailurePreservesPolicyAndRestoredDatabase(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	backing := &managedCatalogFailureEngine{Engine: base}
	first, err := NewDatabaseManager(t.Context(), backing, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "retained"}))
	left, err := first.GetStorage("retained")
	require.NoError(t, err)
	second, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	right, err := second.GetStorage("retained")
	require.NoError(t, err)
	require.NoError(t, first.SetDatabaseLimits(t.Context(), "retained", &Limits{Storage: StorageLimits{MaxNodes: 1}, Rate: RateLimits{MaxWritesPerSecond: 10}}))
	runtime := first.databases["retained"].managed.Load()
	_, oldRate := runtime.policy.rateSnapshot(writeRate)
	injected := fmt.Errorf("catalog persistence rejected")
	backing.reject = injected
	require.ErrorIs(t, first.SetDatabaseLimits(t.Context(), "retained", &Limits{Storage: StorageLimits{MaxNodes: 2}}), injected)
	limits, err := second.GetDatabaseLimits("retained")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, unchangedRate := runtime.policy.rateSnapshot(writeRate)
	require.Same(t, oldRate, unchangedRate, "failed persistence preserves the shared rate token owner")
	_, err = right.CreateNode(&storage.Node{ID: "before"})
	require.NoError(t, err)
	_, err = left.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.ErrorIs(t, first.DropDatabase(t.Context(), "retained"), injected)
	_, err = first.GetDatabase("retained")
	require.NoError(t, err, "metadata failure restores the live catalog record")
	require.False(t, runtime.retired.Load())
	limits, err = second.GetDatabaseLimits("retained")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	count, err := storage.NewNamespacedEngine(base, "retained").NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count, "a rejected catalog intent must preserve the existing data")
	require.NoError(t, left.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "before", Properties: map[string]interface{}{"survives": "after"}}}))
	retained, err := right.GetNode("before")
	require.NoError(t, err)
	require.Equal(t, "after", retained.Properties["survives"])
	_, err = right.CreateNode(&storage.Node{ID: "blocked-again"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	exactNodes, exactEdges, err := first.calculateStorageSizeFromEngine(context.Background(), storage.NewNamespacedEngine(base, "retained"))
	require.NoError(t, err)
	actual, _, _ := second.GetStorageSize(context.Background(), "retained")
	require.Equal(t, exactNodes+exactEdges, actual)
}
