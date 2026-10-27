package multidb

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A database's storage, as the manager hands it out, delivers the storage
// events of that database (sizeTrackingEngine forwards the subscription).
func TestSizeTrackedDatabaseDeliversEvents(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "tenant"}))
	engine, err := m.GetStorage("tenant")
	require.NoError(t, err)
	var mu sync.Mutex
	var created []storage.NodeID
	require.NoError(t, storage.SubscribeEvents(engine, storage.EventHandlers{
		NodeCreated: func(node *storage.Node) {
			mu.Lock()
			created = append(created, node.ID)
			mu.Unlock()
		},
	}))
	_, err = engine.CreateNode(&storage.Node{ID: "a"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(created) == 1 && created[0] == "a"
	}, 2*time.Second, 5*time.Millisecond, "the database's storage did not deliver its node event")
}
