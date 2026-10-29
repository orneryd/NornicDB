package multidb

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationManagedIssuanceUsesCommonCursor(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	manager, err := NewDatabaseManager(t.Context(), storage.NewTracedEngine(base), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	work := &storage.CapabilityWork{Trace: true}
	end := storage.ObserveCapabilityWork(work)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "owner_tenant"}))
	issued, err := manager.GetStorage("owner_tenant")
	end()
	require.NoError(t, err)
	var visits uint64
	for site, count := range work.Sites() {
		if strings.HasPrefix(site, "provider:") && strings.Contains(site, "managedEntityOwnerFor") {
			visits += count
		}
	}
	require.Positive(t, visits, "public manager issuance must collect physical observer/registry/boundary references through the common cursor")
	_, err = issued.CreateNode(&storage.Node{ID: "n", Labels: []string{"Owner"}, Properties: map[string]any{"v": int64(7)}})
	require.NoError(t, err)
	node, err := issued.GetNode("n")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n"), node.ID)
	physical, err := base.GetStoredNode("owner_tenant:n")
	require.NoError(t, err)
	require.Equal(t, int64(7), physical.Properties["v"], "original managed namespace/publication path remains authoritative")
}
