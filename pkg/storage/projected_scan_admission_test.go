package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedReadsOversizedProjectionScanRetainsExistingCoverage(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%t", snapshot), func(t *testing.T) {

			const count = labelScanPointLookups + 300
			b, ids := labelScanFixture(t, count)
			b.nodeCacheMaxEntries = labelScanPointLookups + 100
			for _, id := range ids {
				b.cacheDeleteNode(id, nodeCacheAllForms)
			}
			// Retain a node beyond the initial small-scan window. The wider
			// scan must fill its missing field even when new admission stops.
			last := ids[len(ids)-1]
			_, err := b.GetNodeProjected(t.Context(), last, []string{"big", "absent"})
			require.NoError(t, err)
			visited := 0
			err = labelTailScan(t, b, []string{"i"}, snapshot)(func(node *Node) error {
				require.Equal(t, ids[visited], node.ID)
				require.Contains(t, node.Properties, "i")
				require.NotContains(t, node.Properties, "big")
				visited++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, count, visited)
			b.nodeBodyCacheMu.RLock()
			defer b.nodeBodyCacheMu.RUnlock()
			require.LessOrEqual(t, len(b.nodeBodyCache), labelScanPointLookups+1,
				"a known oversized label must stop admitting new scan images")
			entry := b.nodeBodyCache[last]
			require.NotNil(t, entry)
			require.Contains(t, entry.coverage, "big")
			require.Contains(t, entry.coverage, "absent")
			require.Contains(t, entry.coverage, "i", "existing cache entries must still widen")
			require.Equal(t, "x", entry.node.Properties["big"])
		})
	}
}
