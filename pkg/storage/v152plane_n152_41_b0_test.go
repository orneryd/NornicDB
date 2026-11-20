package storage

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise the actual public listing. Observing its held read lock starts
// Close only after discovery has passed ensureOpen and is reading counters.
// The large node-counter population keeps the independent edge-counter read
// behind that admitted traversal. No sleeps or production hooks are used.
func TestNOR77NamespaceListingKeepsAdmittedCounterStateDuringClose(t *testing.T) {
	engine := createTestBadgerEngine(t)
	const namespaces = 250000
	engine.namespaceCountsMu.Lock()
	for i := 0; i < namespaces; i++ {
		engine.namespaceNodeCounts[fmt.Sprintf("nor77_%06d:", i)] = 1
	}
	engine.namespaceEdgeCounts["nor77_edge_only:"] = 1
	engine.namespaceCountsMu.Unlock()

	done := make(chan []string, 1)
	go func() { done <- engine.ListNamespaces() }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if !engine.namespaceCountsMu.TryLock() {
			break
		}
		engine.namespaceCountsMu.Unlock()
		select {
		case <-done:
			t.Fatal("fixture did not observe the public reader's admitted counter lock")
		case <-deadline.C:
			t.Fatal("public namespace discovery did not acquire its counter lock")
		default:
			runtime.Gosched()
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	var got []string
	select {
	case got = <-done:
	case <-deadline.C:
		t.Fatal("admitted namespace discovery hung during Close")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-deadline.C:
		t.Fatal("Close did not complete after namespace discovery")
	}
	require.Equal(t, namespaces+1, len(got), "an admitted listing must not combine the old node counters with released edge counters")
	require.True(t, slices.Contains(got, "nor77_edge_only"), "the admitted edge-only namespace must remain present")
	require.Empty(t, engine.ListNamespaces(), "new discovery after close follows the existing nil result contract")
	require.NoError(t, engine.Close(), "repeated close remains supported")
}

func TestNOR77NamespaceListingLifecycleControls(t *testing.T) {
	engine := createTestBadgerEngine(t)
	require.Empty(t, engine.ListNamespaces())
	_, err := engine.CreateNode(t.Context(), &Node{ID: "nor77_nodes:n", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "nor77_edges:n", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "nor77_edges:e", StartNode: "nor77_edges:n", EndNode: "nor77_edges:n", Type: "SELF"}))
	require.ElementsMatch(t, []string{"nor77_nodes", "nor77_edges"}, engine.ListNamespaces())
	require.NoError(t, engine.Close())
	require.Empty(t, engine.ListNamespaces())
	require.NoError(t, engine.Close())
}
