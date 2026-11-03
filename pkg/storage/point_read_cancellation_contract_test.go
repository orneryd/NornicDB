package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The family1 point reads and native transaction admission/staging run under
// the caller's context. These tests pin the behaviour that makes that contract
// observable: a request that is already cancelled reads nothing, admits no
// transaction and stages no write, while accepted work keeps the lifetime it
// captured and a healthy request sequence completes normally.

// pointReadFixture seeds one node pair and relationship through a healthy
// request so the cancelled calls below would otherwise resolve them. It returns
// the node and edge head versions a snapshot read needs.
func pointReadFixture(t *testing.T) (*MemoryEngine, MVCCVersion, MVCCVersion) {
	t.Helper()
	engine := newTestMemoryEngine(t)
	ctx := t.Context()
	_, err := engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"A"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))

	nodeHead, err := engine.GetNodeCurrentHead(context.Background(), "test:a")
	require.NoError(t, err)
	edgeHead, err := engine.GetEdgeCurrentHead(context.Background(), "test:e")
	require.NoError(t, err)
	return engine, nodeHead.Version, edgeHead.Version
}

func TestPointReadsHonorCancelledRequestContext(t *testing.T) {
	engine, version, _ := pointReadFixture(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	reads := []struct {
		name string
		read func() error
	}{
		{"GetNode", func() error { _, err := engine.GetNode(cancelled, "test:a"); return err }},
		{"GetNodeProjected", func() error { _, err := engine.GetNodeProjected(cancelled, "test:a", []string{"p"}); return err }},
		{"GetQueryNode", func() error { _, err := engine.GetQueryNode(cancelled, "test:a"); return err }},
		{"GetStoredNode", func() error { _, err := engine.GetStoredNode(cancelled, "test:a"); return err }},
		{"GetNodeWithoutEmbeddings", func() error { _, err := engine.GetNodeWithoutEmbeddings(cancelled, "test:a"); return err }},
		{"GetNodeLatestVisible", func() error { _, err := engine.GetNodeLatestVisible(cancelled, "test:a"); return err }},
		{"GetNodeVisibleAt", func() error { _, err := engine.GetNodeVisibleAt(cancelled, "test:a", version); return err }},
		{"GetEdge", func() error { _, err := engine.GetEdge(cancelled, "test:e"); return err }},
		{"GetStoredEdge", func() error { _, err := engine.GetStoredEdge(cancelled, "test:e"); return err }},
		{"GetEdgeBetween", func() error { _, err := engine.GetEdgeBetween(cancelled, "test:a", "test:b", "R"); return err }},
		{"GetEdgeBetweenUntyped", func() error { _, err := engine.GetEdgeBetween(cancelled, "test:a", "test:b", ""); return err }},
		{"GetEdgeLatestVisible", func() error { _, err := engine.GetEdgeLatestVisible(cancelled, "test:e"); return err }},
		{"GetEdgeVisibleAt", func() error { _, err := engine.GetEdgeVisibleAt(cancelled, "test:e", version); return err }},
		{"GetInDegree", func() error { _, err := engine.GetInDegree(cancelled, "test:b"); return err }},
		{"GetOutDegree", func() error { _, err := engine.GetOutDegree(cancelled, "test:a"); return err }},
		{"GetFirstNodeByLabel", func() error { _, err := engine.GetFirstNodeByLabel(cancelled, "A"); return err }},
	}
	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.read(), context.Canceled, "%s must not complete a read for a cancelled request", tc.name)
		})
	}
}

func TestWrapperPointReadsHonorCancelledRequestContext(t *testing.T) {
	engine, _, _ := pointReadFixture(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	namespaced := NewNamespacedEngine(engine, "test")
	wal := NewWALEngine(engine, nil)

	reads := map[string]func() error{
		"namespaced GetNode":        func() error { _, err := namespaced.GetNode(cancelled, "a"); return err },
		"namespaced GetEdgeBetween": func() error { _, err := namespaced.GetEdgeBetween(cancelled, "a", "b", "R"); return err },
		"namespaced GetQueryNode":   func() error { _, err := namespaced.GetQueryNode(cancelled, "a"); return err },
		"wal GetNode":               func() error { _, err := wal.GetNode(cancelled, "test:a"); return err },
		"wal GetEdgeBetween":        func() error { _, err := wal.GetEdgeBetween(cancelled, "test:a", "test:b", "R"); return err },
		"wal GetNodeProjected":      func() error { _, err := wal.GetNodeProjected(cancelled, "test:a", []string{"p"}); return err },
		"wal GetNodeLatestVisible":  func() error { _, err := wal.GetNodeLatestVisible(cancelled, "test:a"); return err },
		"wal GetOutDegree":          func() error { _, err := wal.GetOutDegree(cancelled, "test:a"); return err },
	}
	for name, read := range reads {
		require.ErrorIs(t, read(), context.Canceled, "%s must not complete a read for a cancelled request", name)
	}
}

func TestTransactionAdmissionHonorsCancelledRequestContext(t *testing.T) {
	engine, _, _ := pointReadFixture(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := engine.BeginTransaction(cancelled)
	require.ErrorIs(t, err, context.Canceled, "a cancelled request must not admit a transaction")
}

// TestAcceptedTransactionKeepsItsOwnLifetime pins the admission boundary: the
// request context admits the transaction, and the handle keeps the snapshot it
// captured, so cancelling that request afterwards changes no later statement.
func TestAcceptedTransactionKeepsItsOwnLifetime(t *testing.T) {
	engine, _, _ := pointReadFixture(t)
	admit, cancelAdmission := context.WithCancel(context.Background())
	tx, err := engine.BeginTransaction(admit)
	require.NoError(t, err)
	cancelAdmission()
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))

	node, err := tx.GetNode(t.Context(), "test:a")
	require.NoError(t, err, "accepted admission must outlive its request context")
	require.Equal(t, NodeID("test:a"), node.ID)
}

// TestTransactionStagingHonorsCancelledRequestContext pins that cancelled
// staging leaves nothing behind and a healthy retry of the same statement
// completes.
func TestTransactionStagingHonorsCancelledRequestContext(t *testing.T) {
	engine, _, _ := pointReadFixture(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = tx.CreateNode(cancelled, &Node{ID: "test:c", Labels: []string{"A"}})
	require.ErrorIs(t, err, context.Canceled)
	_, err = tx.GetNode(t.Context(), "test:c")
	require.ErrorIs(t, err, ErrNotFound, "cancelled staging must leave no write behind")

	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:c", Labels: []string{"A"}})
	require.NoError(t, err, "a healthy retry of the same statement must succeed")
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode(t.Context(), "test:c")
	require.NoError(t, err)
}

// TestSnapshotPointReadUsesItsOwnContext pins the other side of the same
// boundary: a snapshot read resolves only the version its own context owns, so
// one request's cancellation never shortens another request's snapshot read.
func TestSnapshotPointReadUsesItsOwnContext(t *testing.T) {
	engine, nodeVersion, edgeVersion := pointReadFixture(t)

	node, err := engine.GetNodeVisibleAt(t.Context(), "test:a", nodeVersion)
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), node.ID)
	edge, err := engine.GetEdgeVisibleAt(t.Context(), "test:e", edgeVersion)
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e"), edge.ID)
}
