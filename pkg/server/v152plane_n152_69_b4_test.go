package server

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/stretchr/testify/require"
)

// This is an error-returning public router contract, not the diagnostic test
// that deliberately records today's silent-zero behavior. Bundled consumers
// include plugins/heimdall/plugin.go actionRepoMap/actionDBStats/actionStatus
// and conversation_turn.go privateConversationPrompt. Their error handling
// cannot observe failures discarded by this router.
func TestNOR287HeimdallStatsMustPropagateNativeCountFailure(t *testing.T) {
	server, _ := setupTestServer(t)
	manager, setFailure := nor287FaultedDatabaseManager(t, server)
	nor287SeedGraph(t, manager, "nor287_router_empty", 0, 0)
	nor287SeedGraph(t, manager, "nor287_router_graph", 2, 1)
	router := newHeimdallDBRouter(server.db, manager, nil)
	defer router.Close()
	ctx := heimdallAuthDisabled()
	t.Run("healthy empty", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_router_empty")
		require.NoError(t, err)
		require.Zero(t, stats.NodeCount)
		require.Zero(t, stats.RelationshipCount)
	})
	t.Run("healthy nonempty", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_router_graph")
		require.NoError(t, err)
		require.EqualValues(t, 2, stats.NodeCount)
		require.EqualValues(t, 1, stats.RelationshipCount)
	})
	for _, operation := range []storagefault.ReadOperation{storagefault.NodeCountRead, storagefault.EdgeCountRead} {
		t.Run(string(operation), func(t *testing.T) {
			setFailure(operation)
			defer setFailure("")
			stats, err := router.Stats(ctx, "nor287_router_graph")
			t.Logf("failed count operation=%s stats=%+v err=%v", operation, stats, err)
			require.ErrorIs(t, err, errNOR287CountReadFailure, "a failed native count must remain observable to error-aware callers")
		})
	}
	t.Run("healthy recovery", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_router_graph")
		require.NoError(t, err)
		require.EqualValues(t, 2, stats.NodeCount)
		require.EqualValues(t, 1, stats.RelationshipCount)
	})
}
