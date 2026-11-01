package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The wrapper's read exit keeps the two projections the callers depend on:
// adjacency reads return exactly what the read returned (unfiltered), while
// scans and type/MVCC reads drop records outside the wrapper's namespace. A
// failed read keeps its own raw error next to its own records.
func TestTransactionWrapperReadExitScopeContract(t *testing.T) {
	wrapper := &transactionStorageWrapper{namespace: "tenant", separator: ":"}
	records := []*storage.Edge{
		{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2"},
		nil,
		{ID: "other:e2", StartNode: "other:n1", EndNode: "other:n2"},
	}

	own, err := projectTransactionRead(context.Background(), wrapper, records, nil, readOwnAdjacency)
	require.NoError(t, err)
	require.Len(t, own, len(records), "an adjacency read returns every record it read")
	require.Equal(t, storage.EdgeID("e1"), own[0].ID)
	require.Nil(t, own[1])
	require.Equal(t, storage.EdgeID("other:e2"), own[2].ID, "adjacency records keep their identity")

	scanned, err := projectTransactionRead(context.Background(), wrapper, records, nil, readScanned)
	require.NoError(t, err)
	require.Len(t, scanned, 1, "a scan keeps only the wrapper's own namespace")
	require.Equal(t, storage.EdgeID("e1"), scanned[0].ID)

	userSpace, err := projectTransactionRead(context.Background(), wrapper, records, nil, readUserSpace)
	require.NoError(t, err)
	require.Equal(t, records[0], userSpace[0], "a user-space answer is not copied or retagged")
	require.Len(t, userSpace, len(records))

	readErr := errors.New("read failed")
	failed, err := projectTransactionRead(context.Background(), wrapper, records, readErr, readScanned)
	require.ErrorIs(t, err, readErr, "the read's own error is returned unchanged")
	require.Equal(t, records, failed, "the read's own result is returned unchanged")

	nodeRecords := []*storage.Node{
		{ID: "tenant:n1", Labels: []string{"Doc"}},
		nil,
		{ID: "other:n2", Labels: []string{"Doc"}},
	}
	ownNodes, err := projectTransactionRead(context.Background(), wrapper, nodeRecords, nil, readOwnAdjacency)
	require.NoError(t, err)
	require.Len(t, ownNodes, len(nodeRecords))
	require.Equal(t, storage.NodeID("n1"), ownNodes[0].ID)
	scannedNodes, err := projectTransactionRead(context.Background(), wrapper, nodeRecords, nil, readScanned)
	require.NoError(t, err)
	require.Len(t, scannedNodes, 1)
	require.Equal(t, storage.NodeID("n1"), scannedNodes[0].ID)
	require.Equal(t, "tenant", scannedNodes[0].RuntimeMetadata().Database)

	unscoped := &transactionStorageWrapper{separator: ":"}
	raw, err := projectTransactionRead(context.Background(), unscoped, nodeRecords, nil, readScanned)
	require.NoError(t, err)
	require.Equal(t, nodeRecords, raw, "a wrapper without a namespace returns the read's own records")
}

// A snapshot-visible read asks the immediate underlying engine for the exact
// version of the exact identifiers it names, and an underlying engine already
// scoped to the namespace is neither prefixed twice nor detagged.
func TestTransactionWrapperVisibleAtPreservesRequestAndScope(t *testing.T) {
	version := storage.MVCCVersion{CommitTimestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), CommitSequence: 11}
	engine := &recordingVisibilityEngine{
		MemoryEngine: newTestMemoryEngine(t),
		edges:        []*storage.Edge{{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "REL"}},
	}
	wrapper := &transactionStorageWrapper{underlying: engine, namespace: "tenant", separator: ":"}
	edges, err := wrapper.GetEdgesBetweenVisibleAt(context.Background(), "n1", "n2", version)
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("tenant:n1"), engine.node, "the read names the namespace-prefixed start node")
	require.Equal(t, storage.NodeID("tenant:n2"), engine.other, "the read names the namespace-prefixed end node")
	require.Equal(t, version, engine.version, "the exact version reaches the engine")
	require.Len(t, edges, 1)
	require.Equal(t, storage.EdgeID("e1"), edges[0].ID)

	scoped := &recordingVisibilityEngine{
		MemoryEngine: newTestMemoryEngine(t),
		namespace:    "tenant",
		edges:        []*storage.Edge{{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL"}},
	}
	scopedWrapper := &transactionStorageWrapper{underlying: scoped, namespace: "tenant", separator: ":"}
	outgoing, err := scopedWrapper.GetOutgoingEdgesVisibleAt(context.Background(), "n1", version)
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n1"), scoped.node, "an engine scoped to the namespace is not prefixed twice")
	require.Equal(t, version, scoped.version)
	require.Len(t, outgoing, 1)
	require.Equal(t, storage.EdgeID("e1"), outgoing[0].ID)
}
