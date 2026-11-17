package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Checkpoint scans must preserve every stored image without admitting cold
// bodies into the query cache. Existing hot bodies remain detached from the
// stream callback, and ordinary node streams retain their admission policy.

const (
	snapshotAdmissionPlainNodes = 6
	snapshotAdmissionChunked    = NodeID("app:chunked")
	snapshotAdmissionNamed      = NodeID("app:named")
)

// assertSnapshotAdmissionPayload checks one streamed image against the exact
// payload the population was written with.
func assertSnapshotAdmissionPayload(t *testing.T, node *Node) {
	t.Helper()
	require.NotNil(t, node)
	require.Equal(t, []string{"Doc"}, node.Labels)
	require.Equal(t, "original", node.Properties["state"])
	switch node.ID {
	case snapshotAdmissionChunked:
		require.Equal(t, map[string]any{"state": "original"}, node.Properties)
		require.Equal(t, [][]float32{{1, 2, 3}, {4, 5, 6}}, node.ChunkEmbeddings)
		require.Equal(t, "original", node.EmbedMeta["status"])
		require.EqualValues(t, 2, node.EmbedMeta["chunk_count"])
	case snapshotAdmissionNamed:
		require.Equal(t, map[string]any{"state": "original"}, node.Properties)
		require.Equal(t, map[string][]float32{"title": {0.5, 0.25, 0.125}}, node.NamedEmbeddings)
	default:
		require.True(t, strings.HasPrefix(string(node.ID), "app:plain-"), "unexpected node %q", node.ID)
		var index int64
		_, err := fmt.Sscanf(string(node.ID), "app:plain-%d", &index)
		require.NoError(t, err)
		require.EqualValues(t, index, node.Properties["index"])
		require.Empty(t, node.ChunkEmbeddings)
		require.Empty(t, node.NamedEmbeddings)
	}
}

// writeSnapshotAdmissionPopulation stores the population both tests pin: plain
// documents whose bodies are cacheable, one document whose chunk vectors live
// in their sidecar, and one whose named vectors stay in its body.
func writeSnapshotAdmissionPopulation(t *testing.T, ctx context.Context, engine *BadgerEngine) {
	t.Helper()
	for index := 0; index < snapshotAdmissionPlainNodes; index++ {
		_, err := engine.CreateNode(ctx, &Node{
			ID:         NodeID(fmt.Sprintf("app:plain-%d", index)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"state": "original", "index": int64(index)},
		})
		require.NoError(t, err)
	}
	_, err := engine.CreateNode(ctx, &Node{ID: snapshotAdmissionChunked, Labels: []string{"Doc"}, Properties: map[string]any{"state": "original"}})
	require.NoError(t, err)
	chunked, err := engine.GetNode(ctx, snapshotAdmissionChunked)
	require.NoError(t, err)
	chunked.ChunkEmbeddings = [][]float32{{1, 2, 3}, {4, 5, 6}}
	chunked.EmbedMeta = map[string]any{"status": "original", "chunk_count": int64(2)}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(ctx, chunked))
	_, err = engine.CreateNode(ctx, &Node{
		ID:              snapshotAdmissionNamed,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"state": "original"},
		NamedEmbeddings: map[string][]float32{"title": {0.5, 0.25, 0.125}},
	})
	require.NoError(t, err)
	// The pin reads committed rows; a buffered node would be visited from the
	// overlay instead of the scan this regression measures.
	require.NoError(t, engine.FlushWriteBehind(ctx))
}

// clearSnapshotBodyCache puts the version-keyed body cache in the state a
// freshly opened engine has, so a read below decodes instead of taking an entry
// an earlier write happened to publish.
func clearSnapshotBodyCache(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	engine.nodeBodyCacheMu.Lock()
	clear(engine.nodeBodyCache)
	engine.nodeBodyCacheLRU.Init()
	engine.nodeBodyCacheBytes = 0
	engine.nodeBodyCacheMu.Unlock()
}

// clearSnapshotWholeNodeCaches removes the whole-node images writes publish, so
// the next point read reaches the body the cache admission decision is about.
func clearSnapshotWholeNodeCaches(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	engine.nodeCacheMu.Lock()
	engine.resetNodeReadCachesLocked()
	engine.nodeCacheMu.Unlock()
}

// snapshotBodyCacheLen reports the number of retained whole-node bodies.
func snapshotBodyCacheLen(engine *BadgerEngine) int {
	engine.nodeBodyCacheMu.RLock()
	defer engine.nodeBodyCacheMu.RUnlock()
	return len(engine.nodeBodyCache)
}

func TestStoredSnapshotNodeStreamDoesNotAdmitColdBodies(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	writeSnapshotAdmissionPopulation(t, ctx, engine)
	clearSnapshotWholeNodeCaches(t, engine)
	clearSnapshotBodyCache(t, engine)

	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.view.Close()

	streamed := map[NodeID]struct{}{}
	require.NoError(t, source.StreamNodes(ctx, func(node *Node) error {
		assertSnapshotAdmissionPayload(t, node)
		streamed[node.ID] = struct{}{}
		return nil
	}))
	require.Len(t, streamed, snapshotAdmissionPlainNodes+2, "the pin streams its complete stored population")

	require.Zero(t, snapshotBodyCacheLen(engine), "a whole-store snapshot stream must not admit the cold bodies it serializes once")
}

func TestStoredSnapshotNodeStreamKeepsHotBodiesAndAdmitsNoColdBody(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	writeSnapshotAdmissionPopulation(t, ctx, engine)
	// The query read below is the test's one admission: a whole-node point read
	// keeps its image in the node cache and drops the body, so the query image
	// is how a hot body is admitted. A cold body cache keeps creation out of
	// the count.
	clearSnapshotWholeNodeCaches(t, engine)
	clearSnapshotBodyCache(t, engine)
	hot, err := engine.GetQueryNode(ctx, snapshotAdmissionChunked)
	require.NoError(t, err)
	require.Equal(t, "original", hot.Properties["state"])
	require.Empty(t, hot.ChunkEmbeddings, "a query image carries no stored vectors")
	engine.nodeBodyCacheMu.RLock()
	hotEntry, hotCached := engine.nodeBodyCache[snapshotAdmissionChunked]
	engine.nodeBodyCacheMu.RUnlock()
	require.True(t, hotCached, "an ordinary query read admits the body it decodes")
	hotVersion := hotEntry.itemVersion

	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.view.Close()

	streamed := 0
	require.NoError(t, source.StreamNodes(ctx, func(node *Node) error {
		streamed++
		assertSnapshotAdmissionPayload(t, node)
		if node.ID != snapshotAdmissionChunked {
			return nil
		}
		// A stream callback owns its image: mutating it must not reach the
		// body the cache keeps.
		node.Properties["state"] = "mutated"
		node.Labels[0] = "Mutated"
		node.ChunkEmbeddings[0][0] = 99
		node.EmbedMeta["status"] = "mutated"
		return nil
	}))
	require.Equal(t, snapshotAdmissionPlainNodes+2, streamed, "the greater population is a complete stream")

	engine.nodeBodyCacheMu.RLock()
	entry, retained := engine.nodeBodyCache[snapshotAdmissionChunked]
	retainedEntries := len(engine.nodeBodyCache)
	engine.nodeBodyCacheMu.RUnlock()
	require.True(t, retained, "a greater snapshot population must not evict a hot body")
	require.Equal(t, hotVersion, entry.itemVersion)
	require.Equal(t, "original", entry.node.Properties["state"])
	require.Equal(t, []string{"Doc"}, entry.node.Labels)
	require.Empty(t, entry.node.ChunkEmbeddings, "the body cache holds bodies, not vectors")
	require.Equal(t, 1, retainedEntries, "the snapshot stream admits no cold body")

	// The retained hot body still serves a caller after the snapshot stream read
	// and re-hydrated it, unchanged by the callback.
	clearSnapshotWholeNodeCaches(t, engine)
	after, err := engine.GetNode(ctx, snapshotAdmissionChunked)
	require.NoError(t, err)
	assertSnapshotAdmissionPayload(t, after)

	// An ordinary whole-node stream keeps its admission: the body cache is what
	// a repeated scan reads instead of decoding. The named-vector body is never
	// cacheable, so the six plain bodies and the chunked sidecar body count.
	require.NoError(t, engine.StreamNodesWithOptions(ctx, fullNodeStreamOptions(""), func(*Node) error {
		return nil
	}))
	require.Equal(t, snapshotAdmissionPlainNodes+1, snapshotBodyCacheLen(engine), "an ordinary node stream keeps admitting its bodies")
}
