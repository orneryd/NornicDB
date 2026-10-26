package multidb

import (
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
	first, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase("shared"))
	view, err := first.GetStorage("shared")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"v": "small"}})
	require.NoError(t, err)
	before, _, _ := first.GetStorageSize("shared")
	require.Positive(t, before)
	second, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	other, _, _ := second.GetStorageSize("shared")
	require.Equal(t, before, other)
	require.NoError(t, view.UpdateNode(&storage.Node{ID: "n", Properties: map[string]any{"v": strings.Repeat("x", 4096)}}))
	wantNodes, wantEdges, err := first.calculateStorageSizeFromEngine(view)
	require.NoError(t, err)
	tracked, _, _ := first.GetStorageSize("shared")
	require.Equal(t, wantNodes+wantEdges, tracked, "a second read-only manager must not steal the first manager's accounting publication")
}

func TestManagedRuntimeSharedPolicyAndReservation(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	first, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase("shared"))
	left, err := first.GetStorage("shared")
	require.NoError(t, err)
	second, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	right, err := second.GetStorage("shared")
	require.NoError(t, err)
	require.NotSame(t, left, right, "each handle keeps its own engine route")
	require.Same(t, first.databases["shared"].managed.Load(), second.databases["shared"].managed.Load())
	require.NoError(t, second.SetDatabaseLimits("shared", &Limits{Storage: StorageLimits{MaxNodes: 1}}))
	limits, err := first.GetDatabaseLimits("shared")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, err = left.CreateNode(&storage.Node{ID: "n"})
	require.NoError(t, err)
	_, err = right.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.NoError(t, first.SetDatabaseLimits("shared", &Limits{Storage: StorageLimits{MaxNodes: 2}}))
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
	exactNodes, exactEdges, err := first.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(base, "shared"))
	require.NoError(t, err)
	actual, _, _ := second.GetStorageSize("shared")
	require.Equal(t, exactNodes+exactEdges, actual)
}

func TestManagedRuntimeWholeResetAndRecreate(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	manager, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("lifecycle"))
	view, err := manager.GetStorage("lifecycle")
	require.NoError(t, err)
	require.NoError(t, manager.SetDatabaseLimits("lifecycle", &Limits{Storage: StorageLimits{MaxNodes: 1}}))
	_, err = view.CreateNode(&storage.Node{ID: "before"})
	require.NoError(t, err)
	_, _, _ = manager.GetStorageSize("lifecycle")
	runtime := manager.databases["lifecycle"].managed.Load()
	_, _, err = base.DeleteByPrefix("lifecycle:")
	require.NoError(t, err)
	require.Same(t, runtime, manager.databases["lifecycle"].managed.Load())
	size, _, _ := manager.GetStorageSize("lifecycle")
	require.Zero(t, size)
	limits, err := manager.GetDatabaseLimits("lifecycle")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, err = view.CreateNode(&storage.Node{ID: "after-reset"})
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "quota"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.NoError(t, manager.DropDatabase("lifecycle"))
	_, err = view.CreateNode(&storage.Node{ID: "retired"})
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.NoError(t, manager.CreateDatabase("lifecycle"))
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
	exactNodes, exactEdges, err := manager.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(base, "lifecycle"))
	require.NoError(t, err)
	actual, _, _ := manager.GetStorageSize("lifecycle")
	require.Equal(t, exactNodes+exactEdges, actual)
}

type managedCatalogFailureEngine struct {
	storage.Engine
	reject error
}

func (e *managedCatalogFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *managedCatalogFailureEngine) UpdateNode(node *storage.Node) error {
	if e.reject != nil && node.ID == "system:databases:metadata" {
		return e.reject
	}
	return e.Engine.UpdateNode(node)
}
func TestManagedRuntimeMetadataFailurePreservesPolicyAndRestoredDatabase(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	backing := &managedCatalogFailureEngine{Engine: base}
	first, err := NewDatabaseManager(backing, nil)
	require.NoError(t, err)
	require.NoError(t, first.CreateDatabase("retained"))
	left, err := first.GetStorage("retained")
	require.NoError(t, err)
	second, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	right, err := second.GetStorage("retained")
	require.NoError(t, err)
	require.NoError(t, first.SetDatabaseLimits("retained", &Limits{Storage: StorageLimits{MaxNodes: 1}, Rate: RateLimits{MaxWritesPerSecond: 10}}))
	runtime := first.databases["retained"].managed.Load()
	_, oldRate := runtime.policy.rateSnapshot(writeRate)
	injected := fmt.Errorf("catalog persistence rejected")
	backing.reject = injected
	require.ErrorIs(t, first.SetDatabaseLimits("retained", &Limits{Storage: StorageLimits{MaxNodes: 2}}), injected)
	limits, err := second.GetDatabaseLimits("retained")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	_, unchangedRate := runtime.policy.rateSnapshot(writeRate)
	require.Same(t, oldRate, unchangedRate, "failed persistence preserves the shared rate token owner")
	_, err = right.CreateNode(&storage.Node{ID: "before"})
	require.NoError(t, err)
	_, err = left.CreateNode(&storage.Node{ID: "blocked"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	require.ErrorIs(t, first.DropDatabase("retained"), injected)
	_, err = first.GetDatabase("retained")
	require.NoError(t, err, "metadata failure restores the live catalog record")
	require.False(t, runtime.retired.Load())
	limits, err = second.GetDatabaseLimits("retained")
	require.NoError(t, err)
	require.EqualValues(t, 1, limits.Storage.MaxNodes)
	count, err := storage.NewNamespacedEngine(base, "retained").NodeCount()
	require.NoError(t, err)
	require.Zero(t, count, "the existing physical-first DROP cleared storage before metadata failed")
	_, err = left.CreateNode(&storage.Node{ID: "after"})
	require.NoError(t, err)
	_, err = right.CreateNode(&storage.Node{ID: "blocked-again"})
	require.ErrorIs(t, err, ErrStorageLimitExceeded)
	exactNodes, exactEdges, err := first.calculateStorageSizeFromEngine(storage.NewNamespacedEngine(base, "retained"))
	require.NoError(t, err)
	actual, _, _ := second.GetStorageSize("retained")
	require.Equal(t, exactNodes+exactEdges, actual)
}
