package multidb

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStorageSizeReadModesPreserveInitializationError(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	injected := errors.New("stored-size scan unavailable")
	reader := &storageSizingErrorEngine{Engine: base, nodesErr: injected}
	info := &DatabaseInfo{Name: "test", Status: "online", Limits: &Limits{Storage: StorageLimits{MaxBytes: 1000}}, nodeSize: 11, edgeSize: 7, totalSize: 18}
	manager := &DatabaseManager{inner: reader, databases: map[string]*DatabaseInfo{"test": info}, engines: map[string]storage.Engine{"test": reader}, limitCheckers: make(map[string]*databaseLimitChecker)}
	total, nodes, edges := manager.GetStorageSize("test")
	require.EqualValues(t, 18, total)
	require.EqualValues(t, 11, nodes)
	require.EqualValues(t, 7, edges)
	require.False(t, info.accountingOwner().sizeInitialized, "reporting retains cached counters when lazy init fails")
	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked("test", false)
	manager.mu.Unlock()
	require.NoError(t, err)
	err = checker.CheckStorageLimits("create_node", &storage.Node{ID: "n"}, nil)
	require.ErrorIs(t, err, injected, "admission propagates the same actual scan failure")
	require.EqualValues(t, 18, info.accountingOwner().totalSize)
	missingTotal, missingNodes, missingEdges := manager.GetStorageSize("missing")
	require.Zero(t, missingTotal)
	require.Zero(t, missingNodes)
	require.Zero(t, missingEdges)
}
