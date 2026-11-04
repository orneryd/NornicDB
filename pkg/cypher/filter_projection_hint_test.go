package cypher

// NornicDB #1682: a whole-node scan that carries an equality filter tells the
// engine which properties that filter reads
// (storage.StreamNodesOptions.PropertyFilterProjection), so the engine can
// reject a non-matching node before it decodes the rest of the body. A
// projected scan already reads what the filter needs, so it carries no hint.

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// filterHintScan is one node stream a statement ran: the projection the scan
// carried and the properties it named for its equality filter.
type filterHintScan struct {
	projection       []string
	filterProjection []string
}

// filterHintProbeEngine records the node streams a statement runs.
type filterHintProbeEngine struct {
	*storage.MemoryEngine
	mu    sync.Mutex
	scans []filterHintScan
}

func (e *filterHintProbeEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	e.mu.Lock()
	e.scans = append(e.scans, filterHintScan{
		projection:       append([]string(nil), opts.Projection...),
		filterProjection: sortedFilterHintNames(opts.PropertyFilterProjection),
	})
	e.mu.Unlock()
	return e.MemoryEngine.StreamNodesWithOptions(ctx, opts, fn)
}

func (e *filterHintProbeEngine) take() []filterHintScan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.scans
	e.scans = nil
	return out
}

func sortedFilterHintNames(names []string) []string {
	if names == nil {
		return nil
	}
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}

func TestFilterProjectionHintIsSentOnlyForWholeNodeScans(t *testing.T) {
	probe := &filterHintProbeEngine{MemoryEngine: newTestMemoryEngine(t)}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(probe, "h1682"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (:Code {id: 'c1', name: 'one'}), ({id: 'c1', name: 'unlabeled'}), ({id: 'c2', name: 'two'})`, nil)
	require.NoError(t, err)
	probe.take()

	// A label-less MATCH row is the whole node, so the scan is told which
	// properties its equality filter reads.
	nodes, err := exec.collectNodesWithStreamingProjection(ctx, nil, nil, "a", "a.id = 'c1'", -1, nil)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	scans := probe.take()
	require.Len(t, scans, 1)
	require.Nil(t, scans[0].projection, "the row is the whole node")
	require.Equal(t, []string{"id"}, scans[0].filterProjection)
	for _, node := range nodes {
		require.Equal(t, "c1", node.Properties["id"])
		require.NotEmpty(t, node.Properties["name"], "a kept row carries the properties the filter never read")
	}

	// The projection already names the filter's property: no hint, and the row
	// is still the projection.
	nodes, err = exec.collectNodesWithStreamingProjection(ctx, nil, nil, "a", "a.id = 'c1'", -1, []string{"name"})
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	scans = probe.take()
	require.Len(t, scans, 1)
	require.Equal(t, []string{"id", "name"}, scans[0].projection)
	require.Empty(t, scans[0].filterProjection, "a projected scan already reads what the filter tests")
	for _, node := range nodes {
		require.Equal(t, "c1", node.Properties["id"])
		require.NotEmpty(t, node.Properties["name"])
	}
}
