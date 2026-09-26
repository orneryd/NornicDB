package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAsyncEngineLabelScanDuringUpdates: while nodes are updated (as the
// embedding worker writes them) and flushed, a label read sees every node
// that has the label. A node updated between the label read's cache pass and
// its store pass was dropped before (a count one batch short); its cached
// version is used now (#683).
func TestAsyncEngineLabelScanDuringUpdates(t *testing.T) {
	inner := NewNamespacedEngine(NewMemoryEngine(), "race")
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Millisecond})
	t.Cleanup(func() { _ = ae.Close() })

	const count = 400
	for i := 0; i < count; i++ {
		_, err := ae.CreateNode(&Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"i": int64(i)}})
		require.NoError(t, err)
	}
	require.NoError(t, ae.Flush())

	var stop atomic.Bool
	var wg sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; !stop.Load(); round++ {
				for i := worker; i < count && !stop.Load(); i += 2 {
					node := &Node{ID: NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"i": int64(i), "round": int64(round)}}
					_ = ae.UpdateNode(node)
				}
			}
		}(worker)
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for reads := 0; time.Now().Before(deadline); reads++ {
		nodes, err := ae.GetNodesByLabel("Doc")
		require.NoError(t, err)
		require.Len(t, nodes, count, "read %d", reads)
		counted, err := ae.NodeCountByLabel("Doc")
		require.NoError(t, err)
		require.EqualValues(t, count, counted, "read %d", reads)
	}
}

// TestAsyncEngineEdgeReadsDuringUpdates: the edge reads (outgoing, incoming,
// adjacent) see every relationship while relationships are updated and
// flushed; one updated between a read's cache pass and its store pass was
// dropped before.
func TestAsyncEngineEdgeReadsDuringUpdates(t *testing.T) {
	inner := NewNamespacedEngine(NewMemoryEngine(), "race")
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Millisecond})
	t.Cleanup(func() { _ = ae.Close() })

	const count = 200
	_, err := ae.CreateNode(&Node{ID: "hub", Labels: []string{"Hub"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: "sink", Labels: []string{"Sink"}})
	require.NoError(t, err)
	edge := func(i, round int) *Edge {
		return &Edge{ID: EdgeID(fmt.Sprintf("e%d", i)), StartNode: "hub", EndNode: "sink", Type: "R", Properties: map[string]interface{}{"round": int64(round)}}
	}
	for i := 0; i < count; i++ {
		require.NoError(t, ae.CreateEdge(edge(i, 0)))
	}
	require.NoError(t, ae.Flush())

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 1; !stop.Load(); round++ {
			for i := 0; i < count && !stop.Load(); i++ {
				_ = ae.UpdateEdge(edge(i, round))
			}
		}
	}()
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for reads := 0; time.Now().Before(deadline); reads++ {
		outgoing, err := ae.GetOutgoingEdges("hub")
		require.NoError(t, err)
		require.Len(t, outgoing, count, "outgoing read %d", reads)
		incoming, err := ae.GetIncomingEdges("sink")
		require.NoError(t, err)
		require.Len(t, incoming, count, "incoming read %d", reads)
		out, _, err := ae.GetAdjacentEdges("hub")
		require.NoError(t, err)
		require.Len(t, out, count, "adjacent read %d", reads)
	}
}
