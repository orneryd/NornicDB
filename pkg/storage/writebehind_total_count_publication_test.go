package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindTotalCountsDuringPublicationCallbacks(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	_, err := engine.CreateNode(&Node{ID: "test:anchor", Labels: []string{"Anchor"}})
	require.NoError(t, err)
	type observation struct {
		kind                                   string
		nodes, prefixNodes, edges, prefixEdges int64
		errors                                 []error
		node, edge                             bool
	}
	observed := make(chan observation, 2)
	read := func(kind string) {
		got := observation{kind: kind}
		var err error
		got.nodes, err = engine.NodeCount()
		got.errors = append(got.errors, err)
		got.prefixNodes, err = engine.NodeCountByPrefix("test:")
		got.errors = append(got.errors, err)
		got.edges, err = engine.EdgeCount()
		got.errors = append(got.errors, err)
		got.prefixEdges, err = engine.EdgeCountByPrefix("test:")
		got.errors = append(got.errors, err)
		node, nodeErr := engine.GetNode("test:child")
		got.node = nodeErr == nil && node.Properties["body"] == "own write"
		edge, edgeErr := engine.GetEdge("test:link")
		got.edge = edgeErr == nil && edge.StartNode == "test:anchor" && edge.EndNode == "test:child"
		observed <- got
	}
	engine.OnNodeCreated(func(n *Node) {
		if n.ID == "test:child" {
			read("node")
		}
	})
	engine.OnEdgeCreated(func(e *Edge) {
		if e.ID == "test:link" {
			read("edge")
		}
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:child", Labels: []string{"Child"}, Properties: map[string]interface{}{"body": "own write"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:link", StartNode: "test:anchor", EndNode: "test:child", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())
	for range 2 {
		select {
		case got := <-observed:
			for _, err := range got.errors {
				require.NoError(t, err, got.kind)
			}
			assert.Equal(t, int64(2), got.nodes, got.kind+" NodeCount")
			assert.Equal(t, int64(2), got.prefixNodes, got.kind+" NodeCountByPrefix")
			assert.Equal(t, int64(1), got.edges, got.kind+" EdgeCount")
			assert.Equal(t, int64(1), got.prefixEdges, got.kind+" EdgeCountByPrefix")
			require.True(t, got.node, "callback retains acknowledged node body")
			require.True(t, got.edge, "callback retains acknowledged relationship body")
		default:
			t.Fatal("missing publication callback")
		}
	}
}

func TestWriteBehindTotalCountsIgnoreUpdates(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]interface{}{"body": "updated"}}, ClientNodeEdits))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]interface{}{"body": "updated"}}))
	require.NoError(t, tx.Commit())
	nodes, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(2), nodes, "updates do not create nodes")
	nodes, err = engine.NodeCountByPrefix("test:")
	require.NoError(t, err)
	assert.Equal(t, int64(2), nodes, "prefix updates do not create nodes")
	edges, err := engine.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), edges, "updates do not create relationships")
	edges, err = engine.EdgeCountByPrefix("test:")
	require.NoError(t, err)
	assert.Equal(t, int64(1), edges, "prefix updates do not create relationships")
}

// Arbitrary uncached ID prefixes fall back to a durable scan. A callback's first
// read must not add the already-published generation over that scan.
func TestWriteBehindTotalCountsColdPrefixesAndMutationLifecycle(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"test:a", "test:b", "other:z"} {
		_, err = engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:old", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:obsolete", StartNode: "test:a", EndNode: "test:a", Type: "LINK"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "other:old", StartNode: "other:z", EndNode: "other:z", Type: "LINK"}))
	type observation struct {
		counts  []int64
		errs    []error
		visible bool
	}
	read := func() observation {
		got := observation{}
		readers := []func() (int64, error){engine.NodeCount, engine.EdgeCount,
			func() (int64, error) { return engine.NodeCountByPrefix("test:") }, func() (int64, error) { return engine.EdgeCountByPrefix("test:") },
			func() (int64, error) { return engine.NodeCountByPrefix("other:") }, func() (int64, error) { return engine.EdgeCountByPrefix("other:") },
			func() (int64, error) { return engine.NodeCountByPrefix("test:c") }, func() (int64, error) { return engine.EdgeCountByPrefix("test:new") }}
		for _, read := range readers {
			value, err := read()
			got.counts = append(got.counts, value)
			got.errs = append(got.errs, err)
		}
		child, childErr := engine.GetNode("test:c")
		_, goneErr := engine.GetNode("test:b")
		edges, edgeErr := engine.GetOutgoingEdges("test:a")
		got.visible = childErr == nil && child.Properties["body"] == "new" && goneErr != nil && edgeErr == nil && len(edges) == 1 && edges[0].ID == "test:new"
		return got
	}
	check := func(got observation) {
		for _, err := range got.errs {
			require.NoError(t, err)
		}
		require.Equal(t, []int64{3, 2, 2, 1, 1, 1, 1, 1}, got.counts)
		require.True(t, got.visible, "body and adjacency overlay remain live")
	}
	observed := make(chan observation, 8)
	engine.OnNodeCreated(func(n *Node) {
		if n.ID == "test:c" {
			observed <- read()
		}
	})
	engine.OnNodeUpdated(func(n *Node) {
		if n.ID == "test:a" {
			observed <- read()
		}
	})
	engine.OnNodeDeleted(func(NodeID) { observed <- read() })
	engine.OnEdgeCreated(func(e *Edge) {
		if e.ID == "test:new" {
			observed <- read()
		}
	})
	engine.OnEdgeDeleted(func(EdgeID) { observed <- read() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]interface{}{"body": "updated"}}, ClientNodeEdits))
	require.NoError(t, tx.DeleteEdge("test:obsolete"))
	require.NoError(t, tx.DeleteGraphNode("test:b", CascadeEdges))
	_, err = tx.CreateNode(&Node{ID: "test:c", Properties: map[string]interface{}{"body": "new"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:c", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	// Do not warm partial-prefix caches here: their first read occurs in callback.
	require.NoError(t, engine.FlushWriteBehind())
	require.GreaterOrEqual(t, len(observed), 4)
	for len(observed) > 0 {
		check(<-observed)
	}
	check(read())
	noop, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, noop.SetImplicit(true))
	require.NoError(t, noop.SetNamespace("test"))
	require.NoError(t, noop.Commit())
	require.NoError(t, engine.FlushWriteBehind())
	check(read())
	require.Empty(t, observed, "empty commits do not republish callbacks")
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	check(read())
}

func TestWriteBehindTotalCountsRecoveryPublication(t *testing.T) {
	// Other WAL tests may leave the process flag disabled; this crash fixture
	// requires durable ACK capture, independently of test execution order.
	defer config.WithWALEnabled()()
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	_, err := engine.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	// BeginTransaction flushes even an empty buffer, leaving a worker wakeup
	// that can consume a later ACK. Stop just the flusher before staging this
	// crash fixture; WAL capture and the engine's commit path remain unchanged.
	engine.writeBehind.Close()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:b"})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "fixture must recover a genuinely buffered ACK")
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	found := false
	for _, entry := range entries {
		if entry.Operation != OpBufferedCommit {
			continue
		}
		payload, err := decompressWALBufferedCommitPayload(entry.Data)
		require.NoError(t, err)
		var data WALBufferedCommitData
		require.NoError(t, decodeRecoveryPayload(payload, &data))
		if data.TxID == tx.ID {
			found = true
		}
	}
	require.True(t, found, "buffered ACK must have an actual recoverable WAL payload before crash")
	marker, err := engine.observeBufferedCommitReceipt(bufferedReceiptRead{txID: tx.ID})
	require.NoError(t, err)
	require.False(t, marker.present, "crash fixture must not be durably replayed")
	crashClose(t, engine, wal)
	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal2 := recovered.bufferedCommitWAL
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })
	observed := make(chan []int64, 2)
	read := func() {
		nodes, err1 := recovered.NodeCount()
		edges, err2 := recovered.EdgeCount()
		nsnodes, err3 := recovered.NodeCountByPrefix("test:")
		nsedges, err4 := recovered.EdgeCountByPrefix("test:")
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			observed <- nil
			return
		}
		observed <- []int64{nodes, edges, nsnodes, nsedges}
	}
	recovered.OnNodeCreated(func(n *Node) {
		if n.ID == "test:b" {
			read()
		}
	})
	recovered.OnEdgeCreated(func(e *Edge) {
		if e.ID == "test:ab" {
			read()
		}
	})
	count, err := RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Len(t, observed, 2)
	for len(observed) > 0 {
		require.Equal(t, []int64{2, 1, 2, 1}, <-observed)
	}
	count, err = RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	require.Zero(t, count, "recovery marker prevents double publication")
	read()
	require.Equal(t, []int64{2, 1, 2, 1}, <-observed)
}

func TestWriteBehindTotalCountsMixedStagingAPIs(t *testing.T) {
	buffer := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return nil })
	defer buffer.Close()
	buffer.AppendNode(&Node{ID: "test:raw"})
	buffer.AppendEdge(&Edge{ID: "test:rawedge", StartNode: "test:raw", EndNode: "test:raw", Type: "LINK"})
	require.Equal(t, int64(1), buffer.NodeCountDeltaForPrefix(""))
	require.Equal(t, int64(1), buffer.EdgeCountDelta())
	require.Equal(t, int64(1), buffer.NodeCountDeltaForPrefix("test:"))
	require.Equal(t, int64(1), buffer.EdgeCountDeltaForPrefix("test:"))
	buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:typed"}}, {Type: OpCreateEdge, Edge: &Edge{ID: "test:typededge", StartNode: "test:typed", EndNode: "test:typed", Type: "LINK"}}}})
	require.Equal(t, int64(2), buffer.NodeCountDeltaForPrefix(""), "transaction records must not hide legacy staged nodes")
	require.Equal(t, int64(2), buffer.EdgeCountDelta(), "transaction records must not hide legacy staged edges")
	require.Equal(t, int64(2), buffer.NodeCountDeltaForPrefix("test:"))
	require.Equal(t, int64(2), buffer.EdgeCountDeltaForPrefix("test:"))
	buffer.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: "test:raw", EdgesDeleted: 1, DeletedEdgeIDs: []EdgeID{"test:rawedge"}}}})
	require.Equal(t, int64(1), buffer.NodeCountDeltaForPrefix(""))
	require.Equal(t, int64(1), buffer.EdgeCountDelta())
	require.Equal(t, int64(0), buffer.NodeCountDeltaForPrefix("test:raw"))
	require.Equal(t, int64(0), buffer.EdgeCountDeltaForPrefix("test:rawedge"))
}
