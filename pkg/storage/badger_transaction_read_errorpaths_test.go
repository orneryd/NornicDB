package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ReadEdgeAndAdjacency_ErrorPaths(t *testing.T) {
	engine := newTestEngine(t)

	// Seed a minimal graph for read helpers.
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)

	// pinNamespaceFromIDLocked failure through public read methods.
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Node: "unprefixed"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "also-unprefixed"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "missingprefix"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	require.NoError(t, tx.Rollback())

	// ensureOperationAllowedLocked early-return path for closed tx.
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Node: "test:a"})
	require.ErrorIs(t, err, ErrTransactionClosed)
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:b"})
	require.ErrorIs(t, err, ErrTransactionClosed)
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.ErrorIs(t, err, ErrTransactionClosed)
}

func TestBadgerTransaction_GetCommittedEdgeLocked_PrimaryDecodeError(t *testing.T) {
	engine := newTestEngine(t)

	badEdgeID := EdgeID("test:bad-edge")
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(edgeKey(badEdgeID), []byte("not-a-valid-edge-encoding"))
	}))

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	tx.readTS = MVCCVersion{}

	_, err = tx.getCommittedEdgeLocked(t.Context(), badEdgeID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected format")
}

// getNodesByLabelLockedReadTSCase is one readTS/namespace combination of
// getNodesByLabelLocked: the transaction's namespace pin and snapshot version
// decide which read branch the helper takes.
type getNodesByLabelLockedReadTSCase struct {
	name      string
	nodeID    NodeID
	label     string
	namespace string // "" leaves the transaction unpinned
	// readTS builds the snapshot version pinned on the transaction once the
	// fixture is seeded; nil retains the version selected by BeginTransaction
	// and SetNamespace, which need not be zero.
	readTS func() MVCCVersion
}

// runGetNodesByLabelLockedReadTSCases is the shared fixture/read/assertion
// kernel for the getNodesByLabelLocked readTS branches: it seeds one labelled
// node, opens a transaction, applies the case's namespace pin and readTS, and
// requires the label scan to succeed with at least one node before rolling the
// transaction back.
func runGetNodesByLabelLockedReadTSCases(t *testing.T, cases []getNodesByLabelLockedReadTSCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestEngine(t)
			_, err := engine.CreateNode(t.Context(), &Node{ID: tc.nodeID, Labels: []string{tc.label}})
			require.NoError(t, err)

			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
			if tc.namespace != "" {
				require.NoError(t, tx.SetNamespace(tc.namespace))
			}
			if tc.readTS != nil {
				tx.readTS = tc.readTS()
			}

			nodes, err := tx.getNodesByLabelLocked(context.Background(), tc.label)
			require.NoError(t, err)
			require.NotEmpty(t, nodes)
		})
	}
}

func TestBadgerTransaction_GetNodesByLabelLocked_ReadTSBranches(t *testing.T) {
	runGetNodesByLabelLockedReadTSCases(t, []getNodesByLabelLockedReadTSCase{
		{name: "unpinned begin snapshot", nodeID: "test:n1", label: "Person"},
		{name: "pinned begin snapshot", nodeID: "test:n1", label: "Person", namespace: "test"},
	})
}

func TestBadgerTransaction_GetCommittedEdgeLocked_VisibleAtDecodeErrorPath(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer func() { _ = tx.Rollback() }()

	// Existing tests cover ErrNotVisibleAtSnapshot->ErrNotFound conversion.
	// Corrupt the edge MVCC head so visible-at returns a different error.
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		headKey := engine.mvccEdgeHeadKeyStringLookup("test:e1")
		if headKey == nil {
			return ErrNotFound
		}
		return txn.Set(headKey, []byte("corrupt-head"))
	}))

	// The original physical snapshot predates the injected corruption.
	_, err = tx.getCommittedEdgeLocked(t.Context(), "test:e1")
	require.NoError(t, err)

	corruptReader, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, corruptReader.SetNamespace("test"))
	defer func() { _ = corruptReader.Rollback() }()
	_, err = corruptReader.getCommittedEdgeLocked(t.Context(), "test:e1")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotFound)
}
