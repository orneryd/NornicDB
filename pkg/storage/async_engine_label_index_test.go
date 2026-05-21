package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// indexedLabelIDs returns the NodeID set tracked by labelIndex for the given
// (case-insensitive) label, copied under the read lock for assertion.
func indexedLabelIDs(ae *AsyncEngine, label string) []NodeID {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	set := ae.labelIndex[strings.ToLower(label)]
	out := make([]NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func mustCreateNodeWithLabels(t *testing.T, ae *AsyncEngine, id string, labels ...string) NodeID {
	t.Helper()
	n := &Node{ID: NodeID(prefixTestID(id)), Labels: labels}
	_, err := ae.CreateNode(n)
	require.NoError(t, err)
	return n.ID
}

func nodeIDsOf(nodes []*Node) []NodeID {
	out := make([]NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

func TestAsyncEngine_LabelIndex_FlushEvictsEntries(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNodeWithLabels(t, async, "lix-flush-a", "Star")
	b := mustCreateNodeWithLabels(t, async, "lix-flush-b", "Star")
	mustCreateNodeWithLabels(t, async, "lix-flush-c", "Planet")

	// Pre-flush, the inverted index points at the cached creates.
	assert.ElementsMatch(t, []NodeID{a, b}, indexedLabelIDs(async, "Star"))

	require.NoError(t, async.Flush())

	// Post-flush, labelIndex must NOT retain stale IDs. Without this fix
	// the entries leaked across every flush, and label-scoped reads paid
	// an O(flushed_history) cost dereferencing dead IDs against a now-
	// empty nodeCache.
	assert.Empty(t, indexedLabelIDs(async, "Star"),
		"labelIndex must be cleared for nodes successfully flushed to the underlying engine")
	assert.Empty(t, indexedLabelIDs(async, "Planet"))

	// Engine-side reads still surface the nodes (sanity).
	got, err := async.GetNodesByLabel("Star")
	require.NoError(t, err)
	assert.ElementsMatch(t, []NodeID{a, b}, nodeIDsOf(got))
}

func TestAsyncEngine_LabelIndex_GetNodesByLabelMergesCacheAndEngine(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	// Engine-only node: created directly on the inner engine, never seen
	// by the async cache.
	engineOnly := NodeID(prefixTestID("lix-merge-engine"))
	_, err := engine.CreateNode(&Node{ID: engineOnly, Labels: []string{"Star"}})
	require.NoError(t, err)

	// Cache-only node: queued through the async cache, not flushed.
	cacheOnly := mustCreateNodeWithLabels(t, async, "lix-merge-cache", "Star")

	// Override: exists in engine, then updated via async — async value wins.
	overridden := NodeID(prefixTestID("lix-merge-override"))
	_, err = engine.CreateNode(&Node{ID: overridden, Labels: []string{"Star"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, async.UpdateNode(&Node{ID: overridden, Labels: []string{"Star"}, Properties: map[string]any{"v": 2}}))

	got, err := async.GetNodesByLabel("Star")
	require.NoError(t, err)
	assert.ElementsMatch(t, []NodeID{engineOnly, cacheOnly, overridden}, nodeIDsOf(got))

	for _, n := range got {
		if n.ID == overridden {
			assert.Equal(t, 2, n.Properties["v"], "cache override must win over engine value")
		}
	}
}

func TestAsyncEngine_LabelIndex_DeletedNodeFiltered(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNodeWithLabels(t, async, "lix-del-a", "Star")
	b := mustCreateNodeWithLabels(t, async, "lix-del-b", "Star")

	require.NoError(t, async.DeleteNode(a))

	got, err := async.GetNodesByLabel("Star")
	require.NoError(t, err)
	assert.ElementsMatch(t, []NodeID{b}, nodeIDsOf(got))
	assert.NotContains(t, indexedLabelIDs(async, "Star"), a,
		"deleted node should be removed from labelIndex")
}

func TestAsyncEngine_LabelIndex_RebuildOnLabelChange(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNodeWithLabels(t, async, "lix-rebuild-a", "Star")
	assert.ElementsMatch(t, []NodeID{a}, indexedLabelIDs(async, "Star"))
	assert.Empty(t, indexedLabelIDs(async, "Planet"))

	require.NoError(t, async.UpdateNode(&Node{ID: a, Labels: []string{"Planet"}}))
	assert.Empty(t, indexedLabelIDs(async, "Star"),
		"old label must be cleared after a label-changing update")
	assert.ElementsMatch(t, []NodeID{a}, indexedLabelIDs(async, "Planet"))

	got, err := async.GetNodesByLabel("Star")
	require.NoError(t, err)
	assert.Empty(t, got)

	got, err = async.GetNodesByLabel("Planet")
	require.NoError(t, err)
	assert.ElementsMatch(t, []NodeID{a}, nodeIDsOf(got))
}

func TestAsyncEngine_LabelIndex_CaseInsensitive(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNodeWithLabels(t, async, "lix-case-a", "Star")

	for _, q := range []string{"Star", "STAR", "star", "sTaR"} {
		got, err := async.GetNodesByLabel(q)
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{a}, nodeIDsOf(got), "label query %q", q)
	}
}
