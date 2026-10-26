package multidb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestManagedUsageTransitionPreservesAcceptedAllowances(t *testing.T) {
	for _, mode := range []storage.ManagedEntityUsageTransition{storage.ManagedEntityUsageEmpty, storage.ManagedEntityUsageRestored} {
		t.Run(map[storage.ManagedEntityUsageTransition]string{storage.ManagedEntityUsageEmpty: "empty", storage.ManagedEntityUsageRestored: "restored"}[mode], func(t *testing.T) {
			allowance := reservedUsage{nodes: 2, edges: 1, bytes: 70}
			info := &DatabaseInfo{usageNodes: 3, usageEdges: 1, nodeSize: 100, edgeSize: 20, totalSize: 120, usageInitialized: true, sizeInitialized: true, pending: map[string]reservedUsage{"accepted": allowance}, reserved: allowance}
			runtime := &managedDatabaseRuntime{accounting: info}
			runtime.ResetManagedEntityUsage(mode)
			require.Equal(t, allowance, info.reserved)
			require.Equal(t, map[string]reservedUsage{"accepted": allowance}, info.pending)
			require.True(t, info.sizeInitialized, "byte tracking remains sticky")
			if mode == storage.ManagedEntityUsageRestored {
				require.False(t, info.usageInitialized, "admission must recapture arbitrary restored contents")
				require.EqualValues(t, 3, info.usageNodes)
				require.EqualValues(t, 120, info.totalSize, "last-known reporting stays available until refresh")
			} else {
				require.True(t, info.usageInitialized)
				require.Zero(t, info.usageNodes)
				require.Zero(t, info.usageEdges)
				require.Zero(t, info.nodeSize)
				require.Zero(t, info.edgeSize)
				require.Zero(t, info.totalSize)
			}
		})
	}
}
