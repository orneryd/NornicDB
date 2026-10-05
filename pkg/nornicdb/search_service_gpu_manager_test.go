package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// A search service created after the GPU manager is configured is handed that
// manager.
func TestGetOrCreateSearchServiceUsesConfiguredGPUManager(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	manager, err := gpu.NewManager(gpu.DefaultConfig())
	require.NoError(t, err)
	db.gpuManagerMu.Lock()
	db.gpuManager = manager
	db.gpuManagerMu.Unlock()

	svc, err := db.getOrCreateSearchService("gpu_db", nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
}
