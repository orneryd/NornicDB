package server

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// With the server, and so its GraphQL handler, subscribed to storage events,
// a node written straight to storage still reaches search through its
// storage event: the GraphQL subscription no longer replaces the search
// index's.
func TestServerStorageEventsStillReachSearch(t *testing.T) {
	server, _ := setupTestServer(t)
	defer stopTestServer(t, server)
	ctx := context.Background()
	_, err := server.db.GetStorage().CreateNode(&storage.Node{ID: "event-node", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "quokkalicious"}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		results, err := server.db.Search(ctx, "quokkalicious", nil, 10)
		return err == nil && len(results) == 1
	}, 5*time.Second, 25*time.Millisecond)

	require.NoError(t, server.db.GetStorage().DeleteNode("event-node"))
	require.Eventually(t, func() bool {
		results, err := server.db.Search(ctx, "quokkalicious", nil, 10)
		return err == nil && len(results) == 0
	}, 5*time.Second, 25*time.Millisecond)
}
