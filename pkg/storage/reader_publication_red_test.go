// Author: #1683 lane (Claude), 2026-10-30 (NOR-148). Red overlay: the
// review's large-transaction reproduction on the reviewed head.
package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func captureRevision(t *testing.T, engine Engine) SnapshotReadRevision {
	t.Helper()
	snapshot, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), engine)
	require.NoError(t, err)
	defer snapshot.Close()
	rev, ok := snapshot.(QueryReadSnapshot).CapturedRevision()
	require.True(t, ok)
	return rev
}

// A transaction larger than one Badger batch is one reader publication: its
// intent, every batch and its follow-up commits are one step.
func TestReaderPublicationLargeTransactionIsOneStep(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	batches := countBatches(t)
	const n = 6000
	before := captureRevision(t, engine)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the create must have needed several Badger batches")
	created := captureRevision(t, engine)
	require.True(t, created.SingleReaderCommitAfter(before), "a multi-batch create must be one reader publication")

	batches.Store(0)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		node, err := tx.GetNode(largeCommitNodeID(i))
		require.NoError(t, err)
		node.Properties["pad"] = "fedcba9876543210-updated"
		require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the update must have needed several Badger batches")
	updated := captureRevision(t, engine)
	require.True(t, updated.SingleReaderCommitAfter(created), "a multi-batch update of existing nodes must be one reader publication")
}

// The public full-text test's shape: the in-memory engine keeps bodies inline,
// so one transaction growing 2,000 of them to about 6 KB needs several
// batches, and it is one reader publication.
func TestReaderPublicationLargeInlineUpdateIsOneStep(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	const n = 2000
	text := strings.Repeat("routine correspondence meeting attachment ", 48)
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:inline-%d", i)), Labels: []string{"Source"}, Properties: map[string]any{"text": text, "title": "Correspondence"}}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	batches := countBatches(t)
	before := captureRevision(t, engine.BadgerEngine)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		node, err := tx.GetNode(NodeID(fmt.Sprintf("test:inline-%d", i)))
		require.NoError(t, err)
		node.Properties["title"] = text + " " + text + " revised"
		require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the update must have needed several Badger batches")
	require.True(t, captureRevision(t, engine.BadgerEngine).SingleReaderCommitAfter(before), "a multi-batch update of inline bodies must be one reader publication")
}
