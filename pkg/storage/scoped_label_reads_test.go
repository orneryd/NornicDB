package storage

// Label reads in one database skip the other databases' label-index entries
// before reading their nodes (#851): the label index is shared by every
// database, and a scan in a small database decoded a large one's nodes.

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sortedIDs(nodes []*Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	return ids
}

// serverStack is the server's storage stack: Badger -> WAL -> Async.
func serverStack(t *testing.T) (*BadgerEngine, *AsyncEngine) {
	t.Helper()
	dir := t.TempDir()
	badger, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	wal, err := NewWAL(dir+"/wal", nil)
	require.NoError(t, err)
	async := NewAsyncEngine(NewWALEngine(badger, wal), nil)
	t.Cleanup(func() {
		_ = async.Close()
		_ = wal.Close()
		_ = badger.Close()
	})
	return badger, async
}

func TestScopedLabelReadsReturnOneDatabase(t *testing.T) {
	_, async := serverStack(t)
	a := NewNamespacedEngine(async, "a")
	b := NewNamespacedEngine(async, "b")
	for i := 0; i < 3; i++ {
		_, err := a.CreateNode(&Node{ID: NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"Person"}, Properties: map[string]any{"age": int64(i)}})
		require.NoError(t, err)
		_, err = b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"Person"}, Properties: map[string]any{"age": int64(10 + i)}})
		require.NoError(t, err)
	}
	require.NoError(t, async.Flush())
	// Unflushed writes in both databases: a new node in each, and b's p0
	// deleted.
	_, err := a.CreateNode(&Node{ID: "p9", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "p9", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, b.DeleteNode("p0"))

	want := []string{"a:p0", "a:p1", "a:p2", "a:p9"}
	nodes, err := async.GetNodesByLabelInScope("a:", "Person")
	require.NoError(t, err)
	require.Equal(t, want, sortedIDs(nodes), "async")
	var streamed []*Node
	require.NoError(t, async.StreamNodesByLabelProjectedInScope("a:", "Person", []string{"age"}, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Equal(t, want, sortedIDs(streamed))
	first, err := async.GetFirstNodeByLabelInScope("b:", "Person")
	require.NoError(t, err)
	require.Contains(t, []NodeID{"b:p1", "b:p2", "b:p9"}, first.ID)

	// Through the database wrapper: each database sees its own nodes.
	nodes, err = b.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2", "p9"}, sortedIDs(nodes))
	first, err = a.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	require.Contains(t, []NodeID{"p0", "p1", "p2", "p9"}, first.ID)

	// The unscoped reads still return every database.
	nodes, err = async.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 7)
}

func TestScopedLabelReadsVisibleAtAndInTransactions(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"a:p1", "a:p2", "b:p1"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	version := engine.currentMVCCReadVersion("a")
	nodes, err := engine.GetNodesByLabelVisibleAtInScope("a:", "Person", version)
	require.NoError(t, err)
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(nodes))
	nodes, err = NewNamespacedEngine(engine, "b").GetNodesByLabelVisibleAt("Person", engine.currentMVCCReadVersion("b"))
	require.NoError(t, err)
	require.Equal(t, []string{"p1"}, sortedIDs(nodes))

	// A transaction pinned to a database scans only that database's label
	// entries, from the snapshot and, without a read version, the engine.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.Equal(t, "", tx.labelScanScopeLocked(), "unpinned")
	require.NoError(t, tx.SetNamespace("a"))
	var streamed []*Node
	require.NoError(t, tx.StreamNodesByLabelProjected("Person", nil, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(streamed))
	nodes, err = tx.getNodesByLabelLocked("Person")
	require.NoError(t, err)
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(nodes))
	tx.readTS = MVCCVersion{}
	nodes, err = tx.getNodesByLabelLocked("Person")
	require.NoError(t, err)
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(nodes))
	tx.snapshotTx.Discard()
	tx.snapshotTx = nil
	streamed = nil
	require.NoError(t, tx.StreamNodesByLabelProjected("Person", []string{"x"}, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(streamed))
	tx.readTS = engine.currentMVCCReadVersion("a")
	streamed = nil
	require.NoError(t, tx.StreamNodesByLabelProjected("Person", []string{"y"}, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Equal(t, []string{"a:p1", "a:p2"}, sortedIDs(streamed), "MVCC view at the read version")
}

// The cost of a label read follows its own database: 1,000 nodes in database
// a are read in about the same time with 50,000 nodes of the label in b.
func TestScopedLabelReadCostIgnoresOtherDatabases(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	a := NewNamespacedEngine(engine, "a")
	b := NewNamespacedEngine(engine, "b")
	for i := 0; i < 1000; i++ {
		_, err := a.CreateNode(&Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Person"}, Properties: map[string]any{"age": int64(i)}})
		require.NoError(t, err)
	}
	scan := func() time.Duration {
		engine.nodeCacheMu.Lock()
		engine.nodeCache = map[NodeID]*Node{}
		engine.nodeCacheMu.Unlock()
		start := time.Now()
		for r := 0; r < 5; r++ {
			seen := 0
			require.NoError(t, a.StreamNodesByLabelProjected("Person", []string{"age"}, func(*Node) error { seen++; return nil }))
			require.Equal(t, 1000, seen)
			nodes, err := a.GetNodesByLabel("Person")
			require.NoError(t, err)
			require.Len(t, nodes, 1000)
		}
		return time.Since(start)
	}
	alone := scan()
	for i := 0; i < 50_000; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Person"}, Properties: map[string]any{"age": int64(i)}})
		require.NoError(t, err)
	}
	shared := scan()
	require.Less(t, shared, 5*alone+200*time.Millisecond, "alone %s, with 50,000 :Person in another database %s", alone, shared)
}
