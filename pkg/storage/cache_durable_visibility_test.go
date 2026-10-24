package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Observe the public result before either callback and after each possible
// release order. The hook runs after durability and oracle publication.
func runDurableCacheSchedule(t *testing.T, b *BadgerEngine, newerFirst bool, older, newer func() error, check func()) {
	t.Helper()
	entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	resume := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	release := func(i int) { once[i].Do(func() { close(resume[i]) }) }
	var phase atomic.Int32
	restore := setCommitTailHook(func() {
		i := int(phase.Add(1)) - 1
		if i < 2 {
			close(entered[i])
			<-resume[i]
		}
	})
	defer restore()
	defer release(0)
	defer release(1)
	done := [2]chan error{make(chan error, 1), make(chan error, 1)}
	wait := func(i int) {
		t.Helper()
		select {
		case <-entered[i]:
		case <-time.After(5 * time.Second):
			t.Fatal("write did not reach durable cache tail")
		}
	}
	go func() { done[0] <- older() }()
	wait(0)
	go func() { done[1] <- newer() }()
	wait(1)
	check()
	first := 0
	if newerFirst {
		first = 1
	}
	release(first)
	require.NoError(t, <-done[first])
	check()
	release(1 - first)
	require.NoError(t, <-done[1-first])
	check()
}

func TestDurableCacheNodeMutationOrders(t *testing.T) {
	for _, queryOnly := range []bool{false, true} {
		for _, newerFirst := range []bool{false, true} {
			for _, shape := range []string{"update-update", "delete-create", "create-delete"} {
				t.Run(fmt.Sprintf("%s/query-only=%t/newer-first=%t", shape, queryOnly, newerFirst), func(t *testing.T) {
					path := t.TempDir()
					b, err := NewBadgerEngine(path)
					require.NoError(t, err)
					initial := &Node{ID: "test:durable-mutations", Labels: []string{"DurableNode"}, Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{{1, 0}}}
					if shape != "create-delete" {
						_, err = b.CreateNode(initial)
						require.NoError(t, err)
					}
					// Reopen through the public owner so the selected read cache is the only
					// warm payload. No private deletion manufactures the query-only state.
					if queryOnly {
						require.NoError(t, b.Close())
						b, err = NewBadgerEngine(path)
						require.NoError(t, err)
					}
					defer b.Close()
					if shape != "create-delete" {
						read := b.GetNode
						if queryOnly {
							read = b.GetQueryNode
						}
						_, err = read(initial.ID)
						require.NoError(t, err)
					}
					update := func(v int64) func() error {
						return func() error {
							n := copyNode(initial)
							n.Properties["v"] = v
							n.ChunkEmbeddings = [][]float32{{float32(v), 1}}
							return b.UpdateNode(n)
						}
					}
					older, newer := update(1), update(2)
					if shape == "delete-create" {
						older = func() error { return b.DeleteNode(initial.ID) }
						newer = func() error {
							n := copyNode(initial)
							n.Properties["v"] = int64(2)
							n.ChunkEmbeddings = [][]float32{{2, 1}}
							_, err := b.CreateNode(n)
							return err
						}
					}
					if shape == "create-delete" {
						older = func() error { _, err := b.CreateNode(initial); return err }
						newer = func() error { return b.DeleteNode(initial.ID) }
					}
					check := func() {
						for _, read := range []func(NodeID) (*Node, error){b.GetNode, b.GetQueryNode, b.GetNodeWithoutEmbeddings} {
							n, err := read(initial.ID)
							if shape == "create-delete" {
								require.ErrorIs(t, err, ErrNotFound)
								continue
							}
							require.NoError(t, err)
							require.Equal(t, int64(2), n.Properties["v"])
						}
						expected := 0
						if shape != "create-delete" {
							expected = 1
						}
						nodes, err := b.GetNodesByLabel("DurableNode")
						require.NoError(t, err)
						require.Len(t, nodes, expected)
						nodes, err = b.AllNodes()
						require.NoError(t, err)
						require.Len(t, nodes, expected)
						seen := 0
						err = b.StreamNodesByLabelProjected("DurableNode", []string{"v"}, func(n *Node) error { seen++; require.Equal(t, int64(2), n.Properties["v"]); return nil })
						require.NoError(t, err)
						require.Equal(t, expected, seen)
					}
					runDurableCacheSchedule(t, b, newerFirst, older, newer, check)
				})
			}
		}
	}
}

func TestDurableCacheEdgeMutationOrders(t *testing.T) {
	for _, newerFirst := range []bool{false, true} {
		for _, shape := range []string{"update-delete", "delete-create", "update-update", "node-delete"} {
			t.Run(fmt.Sprintf("%s/newer-first=%t", shape, newerFirst), func(t *testing.T) {
				b := NewMemoryEngine()
				defer b.Close()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"DurableEndpoint"}, ChunkEmbeddings: [][]float32{{1, 0}}})
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "DURABLE", Properties: map[string]any{"v": int64(0)}}
				require.NoError(t, b.CreateEdge(edge))
				_, err := b.GetOutgoingEdges("test:a")
				require.NoError(t, err)
				_, err = b.GetIncomingEdges("test:b")
				require.NoError(t, err)
				_, err = b.GetEdgesByType(edge.Type)
				require.NoError(t, err)
				update := func(v int64) func() error {
					return func() error { e := copyEdge(edge); e.Properties["v"] = v; return b.UpdateEdge(e) }
				}
				older, newer := update(1), func() error { return b.DeleteEdge(edge.ID) }
				exists := false
				if shape == "delete-create" {
					older = func() error { return b.DeleteEdge(edge.ID) }
					newer = func() error { e := copyEdge(edge); e.Properties["v"] = int64(2); return b.CreateEdge(e) }
					exists = true
				}
				if shape == "update-update" {
					newer = update(2)
					exists = true
				}
				if shape == "node-delete" {
					newer = func() error { return b.DeleteNode("test:a") }
				}
				check := func() {
					n, err := b.GetEdge(edge.ID)
					if exists {
						require.NoError(t, err)
						require.Equal(t, int64(2), n.Properties["v"])
					} else {
						require.ErrorIs(t, err, ErrNotFound)
					}
					want := 0
					if exists {
						want = 1
					}
					out, err := b.GetOutgoingEdges("test:a")
					require.NoError(t, err)
					require.Len(t, out, want)
					in, err := b.GetIncomingEdges("test:b")
					require.NoError(t, err)
					require.Len(t, in, want)
					out, in, err = b.GetAdjacentEdges("test:a")
					require.NoError(t, err)
					require.Len(t, out, want)
					require.Empty(t, in)
					all, err := b.GetEdgesByType(edge.Type)
					require.NoError(t, err)
					require.Len(t, all, want)
					for _, e := range all {
						require.Equal(t, int64(2), e.Properties["v"])
					}
				}
				runDurableCacheSchedule(t, b.BadgerEngine, newerFirst, older, newer, check)
			})
		}
	}
}

// An assigned earlier timestamp holds publication behind a newer durable
// delete. A public read in that interval may see the old node, but its fill
// must not become a current cache entry when the delete is then published.
func TestDurableCacheRejectsFillFromUnpublishedView(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	n := &Node{ID: "test:publication-hole", ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := b.CreateNode(n)
	require.NoError(t, err)
	held, err := b.db.oracle.assign()
	require.NoError(t, err)
	var finish sync.Once
	release := func() { finish.Do(func() { b.db.oracle.finish(held) }) }
	defer release()
	entered, resume := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	continueTail := func() { unblock.Do(func() { close(resume) }) }
	restore := setCommitTailHook(func() { close(entered); <-resume })
	defer restore()
	defer continueTail()
	done := make(chan error, 1)
	go func() { done <- b.DeleteNode(n.ID) }()
	require.Eventually(t, func() bool { return b.nodeCacheCommitTs.Load() > held }, 5*time.Second, time.Millisecond)
	// The durable delete is not yet published; this old view is valid now.
	_, err = b.GetNode(n.ID)
	require.NoError(t, err)
	release()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not publish")
	}
	_, err = b.GetNode(n.ID)
	require.ErrorIs(t, err, ErrNotFound)
	continueTail()
	require.NoError(t, <-done)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(nodeKey(n.ID))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}
