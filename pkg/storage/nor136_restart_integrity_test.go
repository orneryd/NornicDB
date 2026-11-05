package storage

// NOR-136 native storage acceptance tests, covering the consolidated NOR-138
// restart/count integrity and NOR-143 stale-image, delete/resurrection and
// UNIQUE-label-scoped slices.
//
// These are pure pkg/storage tests. They drive the storage engine's own
// producers (write-behind transactions and direct engine writes), an explicit
// flush, a graceful restart and a kill-9-style crash boundary. No Cypher,
// Execute, Bolt or HTTP route is exercised here, so no parser matrix is
// invented: the canonical public-route suites own that surface.
//
// Every restart/recovery assertion is stated on independent read surfaces:
//   - primary bodies through GetStoredNode/GetStoredEdge (storedRead),
//   - the stored label index through GetStoredNodesByLabel,
//   - the node/edge cache maps, and the public GetNode/GetEdge reads,
//   - the persisted per-label counters through NodeCountByLabelInNamespace.
//
// The independent full recount is the production primary-body scan
// collectNodeLabelCountsByPrefix, which decodes raw node bodies under the node
// keyspace and never reads the derived counters or the label index. A counter
// is accepted only when it equals that recount, so a negative, duplicate or
// omitted count adjustment fails even when the derived counter itself happens
// to be self-consistent.
//
// Fixtures reuse the canonical write-behind owners: newWriteBehindTestEngine,
// bufferedWrite/bufferedCreate and crashClose. Production code, hooks and
// checkers are not modified for these tests.

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nor136Config is one write-behind / LowMemory fixture combination.
type nor136Config struct {
	writeBehind bool
	lowMemory   bool
}

func (c nor136Config) name() string {
	return fmt.Sprintf("writebehind=%s/lowmemory=%s", boolString(c.writeBehind), boolString(c.lowMemory))
}

// nor136Configs is the requested matrix: write-behind enabled and disabled,
// each with LowMemory on and off.
func nor136Configs() []nor136Config {
	return []nor136Config{
		{writeBehind: true, lowMemory: true},
		{writeBehind: true, lowMemory: false},
		{writeBehind: false, lowMemory: true},
		{writeBehind: false, lowMemory: false},
	}
}

// nor136Open creates an engine on dir through the canonical write-behind
// fixture. manualDrain stops the background flusher so an acknowledged
// generation stays undrained until the test explicitly flushes, crashes or
// closes: an assertion can therefore never pass only because a timer moved
// the write to disk first.
func nor136Open(t *testing.T, dir string, cfg nor136Config, wal bool) *BadgerEngine {
	t.Helper()
	return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{
		dataDir:     dir,
		synchronous: !cfg.writeBehind,
		lowMemory:   cfg.lowMemory,
		wal:         wal,
		manualDrain: cfg.writeBehind,
	})
}

func nor136Pending(engine *BadgerEngine) int {
	if engine == nil || engine.writeBehind == nil {
		return 0
	}
	return engine.writeBehind.PendingOps()
}

// nor136AssertAcked proves the write took the configured route: with
// write-behind enabled it is acknowledged through the buffer and stays in the
// undrained generation; with write-behind disabled it commits directly and
// leaves no buffer behind.
func nor136AssertAcked(t *testing.T, engine *BadgerEngine, cfg nor136Config, tx *BadgerTransaction, before int) {
	t.Helper()
	if cfg.writeBehind {
		require.True(t, tx.AcknowledgedBuffered(), "the write must be acknowledged through the write-behind buffer")
		after := nor136Pending(engine)
		require.Greater(t, after, before, "the acknowledged write must remain in the undrained generation")
		return
	}
	require.False(t, tx.AcknowledgedBuffered(), "a synchronous write must not report a buffered acknowledgement")
	require.Zero(t, nor136Pending(engine), "write-behind disabled leaves no pending generation")
}

// nor136NodeWant is the exact state one node must show on a read surface.
type nor136NodeWant struct {
	labels []string
	props  map[string]any
	absent []string // property names that must be gone (renames/removals)
}

func nor136AssertWant(t *testing.T, node *Node, want nor136NodeWant, surface string) {
	t.Helper()
	require.NotNil(t, node)
	require.ElementsMatch(t, want.labels, node.Labels, "%s labels of %s", surface, node.ID)
	for name, value := range want.props {
		require.Equal(t, value, node.Properties[name], "%s property %s of %s", surface, name, node.ID)
	}
	for _, name := range want.absent {
		require.NotContains(t, node.Properties, name, "%s property %s of %s must be gone", surface, name, node.ID)
	}
}

func nor136AssertStoredNode(t *testing.T, engine *BadgerEngine, id NodeID, want nor136NodeWant) {
	t.Helper()
	node, err := engine.GetStoredNode(context.Background(), id)
	require.NoError(t, err, "stored body of %s", id)
	nor136AssertWant(t, node, want, "stored")
}

func nor136AssertVisibleNode(t *testing.T, engine *BadgerEngine, id NodeID, want nor136NodeWant) {
	t.Helper()
	node, err := engine.GetNode(context.Background(), id)
	require.NoError(t, err, "public read of %s", id)
	nor136AssertWant(t, node, want, "public")
}

func nor136CachedNode(engine *BadgerEngine, id NodeID) (*Node, bool) {
	engine.nodeCacheMu.RLock()
	defer engine.nodeCacheMu.RUnlock()
	cached, ok := engine.nodeCache[id]
	if !ok || cached == nil {
		return nil, false
	}
	return copyNode(cached), true
}

func nor136CachedEdge(engine *BadgerEngine, id EdgeID) (*Edge, bool) {
	engine.edgeCacheMu.RLock()
	defer engine.edgeCacheMu.RUnlock()
	cached, ok := engine.edgeCache[id]
	if !ok || cached == nil {
		return nil, false
	}
	return copyEdge(cached), true
}

// nor136AssertCacheAgrees requires the public read and, when resident, the
// cache image to carry exactly the stored labels and properties of id.
func nor136AssertCacheAgrees(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	stored, err := engine.GetStoredNode(context.Background(), id)
	require.NoError(t, err)
	visible, err := engine.GetNode(context.Background(), id)
	require.NoError(t, err)
	require.ElementsMatch(t, stored.Labels, visible.Labels, "public read labels of %s disagree with the stored body", id)
	require.Equal(t, stored.Properties, visible.Properties, "public read properties of %s disagree with the stored body", id)
	if cached, ok := nor136CachedNode(engine, id); ok {
		require.Equal(t, stored.ID, cached.ID)
		require.ElementsMatch(t, stored.Labels, cached.Labels, "cache labels of %s disagree with the stored body", id)
		require.Equal(t, stored.Properties, cached.Properties, "cache properties of %s disagree with the stored body", id)
	}
}

// nor136PrimaryRecount is the independent full recount: it decodes every raw
// node body under the test namespace's node keyspace.
func nor136PrimaryRecount(t *testing.T, engine *BadgerEngine) (int64, map[namespaceLabel]int64) {
	t.Helper()
	prefix := append([]byte{prefixNode}, []byte("test:")...)
	total, counts, err := engine.collectNodeLabelCountsByPrefix(prefix)
	require.NoError(t, err)
	return total, counts
}

// nor136AssertStoredCounts requires the persisted per-label counters, the
// persisted node total and the primary bodies to agree exactly.
func nor136AssertStoredCounts(t *testing.T, engine *BadgerEngine, want map[string]int64) {
	t.Helper()
	ctx := context.Background()
	total, counts := nor136PrimaryRecount(t, engine)
	labels := make([]string, 0, len(want))
	var wantTotal int64
	for label, count := range want {
		require.GreaterOrEqual(t, count, int64(0), "expected %s count", label)
		wantTotal += count
		labels = append(labels, label)
	}
	sort.Strings(labels)
	require.Equal(t, wantTotal, total, "primary bodies hold a different node total")
	for key := range counts {
		_, known := want[key.label]
		require.True(t, known, "primary bodies hold unexpected label %q", key.label)
	}
	for _, label := range labels {
		body := counts[namespaceLabel{namespace: "test", label: label}]
		require.Equal(t, want[label], body, "primary-body recount of label %q", label)
		count, err := engine.NodeCountByLabelInNamespace(ctx, "test", label)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, int64(0), "persisted %s count underflowed", label)
		require.Equal(t, body, count, "persisted counter for label %q disagrees with the primary-body recount", label)
	}
	count, err := engine.NodeCount(ctx)
	require.NoError(t, err)
	require.Equal(t, wantTotal, count, "persisted node total disagrees with the primary-body recount")
}

// nor136AssertLiveCounters checks the read-your-writes counters while an
// acknowledged generation is still unapplied.
func nor136AssertLiveCounters(t *testing.T, engine *BadgerEngine, want map[string]int64) {
	t.Helper()
	for label, count := range want {
		got, err := engine.NodeCountByLabelInNamespace(context.Background(), "test", label)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, int64(0), "live %s count underflowed", label)
		require.Equal(t, count, got, "live %s count", label)
	}
}

// nor136AssertNoNegativeCounters rejects the recorded NOR-138 underflow
// symptom at any intermediate point of an unflushed generation.
func nor136AssertNoNegativeCounters(t *testing.T, engine *BadgerEngine, labels ...string) {
	t.Helper()
	for _, label := range labels {
		count, err := engine.NodeCountByLabelInNamespace(context.Background(), "test", label)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, int64(0), "live %s count underflowed", label)
	}
}

// nor136AssertLabelIndex requires the stored label index to hold exactly the
// expected node IDs per label.
func nor136AssertLabelIndex(t *testing.T, engine *BadgerEngine, want map[string][]NodeID) {
	t.Helper()
	labels := make([]string, 0, len(want))
	for label := range want {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		nodes, err := engine.GetStoredNodesByLabel(context.Background(), label)
		require.NoError(t, err)
		ids := make([]NodeID, 0, len(nodes))
		for _, node := range nodes {
			ids = append(ids, node.ID)
			require.Contains(t, node.Labels, label, "stored body %s is indexed under %q without carrying it", node.ID, label)
		}
		require.ElementsMatch(t, want[label], ids, "stored label index of %q", label)
	}
}

// nor136AssertEdges requires exactly the expected relationship population.
func nor136AssertEdges(t *testing.T, engine *BadgerEngine, want map[EdgeID][2]NodeID) {
	t.Helper()
	edges, err := engine.AllEdges(context.Background())
	require.NoError(t, err)
	require.Len(t, edges, len(want), "stored relationship population")
	got := make(map[EdgeID]*Edge, len(edges))
	for _, edge := range edges {
		got[edge.ID] = edge
	}
	for id, endpoints := range want {
		edge, ok := got[id]
		require.True(t, ok, "relationship %s is missing", id)
		require.Equal(t, endpoints[0], edge.StartNode, "relationship %s start", id)
		require.Equal(t, endpoints[1], edge.EndNode, "relationship %s end", id)
		require.Equal(t, "LINK", edge.Type)
		stored, err := engine.GetStoredEdge(context.Background(), id)
		require.NoError(t, err)
		require.Equal(t, endpoints[0], stored.StartNode, "stored relationship %s start", id)
		require.Equal(t, endpoints[1], stored.EndNode, "stored relationship %s end", id)
	}
}

// nor136AssertAbsent requires a deleted node or relationship to be gone from
// its primary record, every read surface and the cache.
func nor136AssertAbsent(t *testing.T, engine *BadgerEngine, nodes []NodeID, edges []EdgeID) {
	t.Helper()
	ctx := context.Background()
	for _, id := range nodes {
		_, err := engine.GetStoredNode(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, "node %s was resurrected in its primary body", id)
		_, err = engine.GetNode(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, "node %s is readable after deletion", id)
		_, cached := nor136CachedNode(engine, id)
		require.False(t, cached, "deleted node %s is still cached", id)
	}
	for _, id := range edges {
		_, err := engine.GetStoredEdge(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, "relationship %s was resurrected in its primary body", id)
		_, err = engine.GetEdge(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, "relationship %s is readable after deletion", id)
		_, cached := nor136CachedEdge(engine, id)
		require.False(t, cached, "deleted relationship %s is still cached", id)
	}
}

// nor136AssertRestartedState is the acceptance oracle after a flush/restart or
// crash recovery: exact bodies, index, counts, cache, relationship endpoints
// and absence of every deleted entity.
func nor136AssertRestartedState(
	t *testing.T,
	engine *BadgerEngine,
	nodes map[NodeID]nor136NodeWant,
	counts map[string]int64,
	edges map[EdgeID][2]NodeID,
	absentNodes []NodeID,
	absentEdges []EdgeID,
) {
	t.Helper()
	ctx := context.Background()
	index := make(map[string][]NodeID, len(counts))
	for label := range counts {
		index[label] = nil
	}
	for id, want := range nodes {
		nor136AssertStoredNode(t, engine, id, want)
		nor136AssertVisibleNode(t, engine, id, want)
		nor136AssertCacheAgrees(t, engine, id)
		for _, label := range want.labels {
			if _, ok := index[label]; !ok {
				index[label] = nil
			}
			index[label] = append(index[label], id)
		}
	}
	nor136AssertStoredCounts(t, engine, counts)
	nor136AssertLabelIndex(t, engine, index)
	nor136AssertEdges(t, engine, edges)
	nor136AssertAbsent(t, engine, absentNodes, absentEdges)
	total, err := engine.NodeCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len(nodes), total, "node total")
	edgeCount, err := engine.EdgeCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len(edges), edgeCount, "relationship total")
}

// nor136Restart closes a flushed engine and opens it again, proving the flushed
// generation does not replay from the WAL.
func nor136Restart(t *testing.T, engine *BadgerEngine, dir string, cfg nor136Config) *BadgerEngine {
	t.Helper()
	require.NoError(t, engine.Close())
	reopened := nor136Open(t, dir, cfg, cfg.writeBehind)
	if cfg.writeBehind {
		replayed, err := RecoverBufferedCommits(reopened, reopened.bufferedCommitWAL)
		require.NoError(t, err)
		require.Zero(t, replayed, "a flushed and closed generation must not replay")
	}
	return reopened
}

// TestNOR136MixedMutationRestartIntegrity drives acknowledged updates,
// property renames, a relabel, a node delete with cascade, an edge delete and
// new node/edge writes through an unflushed generation, then asserts exact
// state and per-label counts after an explicit flush/restart and after a crash
// with WAL replay.
func TestNOR136MixedMutationRestartIntegrity(t *testing.T) {
	ctx := context.Background()
	base := []struct {
		id    NodeID
		props map[string]any
	}{
		{id: "test:doc-a", props: map[string]any{"id": "a", "peer": "peer-a", "counter": int64(1)}},
		{id: "test:doc-b", props: map[string]any{"id": "b", "counter": int64(1)}},
		{id: "test:doc-c", props: map[string]any{"id": "c"}},
		{id: "test:doc-d", props: map[string]any{"id": "d", "old": "value"}},
	}
	want := map[NodeID]nor136NodeWant{
		"test:doc-a": {labels: []string{"Doc"}, props: map[string]any{"id": "a", "peer": "peer-a", "counter": int64(2), "note": "updated"}},
		"test:doc-b": {labels: []string{"Parked"}, props: map[string]any{"id": "b", "counter": int64(2)}},
		"test:doc-d": {labels: []string{"Doc"}, props: map[string]any{"id": "d", "renamed": "value"}, absent: []string{"old"}},
		"test:doc-e": {labels: []string{"Doc"}, props: map[string]any{"id": "e"}},
	}
	wantCounts := map[string]int64{"Doc": 3, "Parked": 1}
	wantEdges := map[EdgeID][2]NodeID{"test:rel-ae": {"test:doc-a", "test:doc-e"}}
	absentNodes := []NodeID{"test:doc-c"}
	absentEdges := []EdgeID{"test:rel-ab", "test:rel-cd"}

	for _, cfg := range nor136Configs() {
		routes := []string{"flush"}
		if cfg.writeBehind {
			routes = append(routes, "crash")
		}
		for _, route := range routes {
			t.Run(cfg.name()+"/"+route, func(t *testing.T) {
				dir := t.TempDir()
				engine := nor136Open(t, dir, cfg, route == "crash")

				// Durable base graph: the later mixed generation cannot be
				// confused with these writes.
				for _, seeded := range base {
					bufferedCreate(t, engine, &Node{ID: seeded.id, Labels: []string{"Doc"}, Properties: seeded.props})
				}
				bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					if err := tx.CreateEdge(ctx, &Edge{ID: "test:rel-ab", Type: "LINK", StartNode: "test:doc-a", EndNode: "test:doc-b"}); err != nil {
						return err
					}
					return tx.CreateEdge(ctx, &Edge{ID: "test:rel-cd", Type: "LINK", StartNode: "test:doc-c", EndNode: "test:doc-d"})
				})
				require.NoError(t, engine.FlushWriteBehind(ctx))
				require.Zero(t, nor136Pending(engine))
				nor136AssertStoredCounts(t, engine, map[string]int64{"Doc": 4, "Parked": 0})

				mixed := []struct {
					name  string
					write func(*BadgerTransaction) error
				}{
					{"update", func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
							After:  &Node{ID: "test:doc-a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "a", "peer": "peer-a", "counter": int64(2), "note": "updated"}},
							Policy: ClientNodeEdits,
						})
					}},
					{"property rename", func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
							After:  &Node{ID: "test:doc-d", Labels: []string{"Doc"}, Properties: map[string]any{"id": "d", "renamed": "value"}},
							Policy: ClientNodeEdits,
						})
					}},
					{"relabel", func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
							After:  &Node{ID: "test:doc-b", Labels: []string{"Parked"}, Properties: map[string]any{"id": "b", "counter": int64(2)}},
							Policy: ClientNodeEdits,
						})
					}},
					{"node delete", func(tx *BadgerTransaction) error {
						return tx.DeleteGraphNode(ctx, "test:doc-c", CascadeEdges)
					}},
					{"edge delete", func(tx *BadgerTransaction) error {
						return tx.DeleteEdge(ctx, "test:rel-ab")
					}},
					{"node create", func(tx *BadgerTransaction) error {
						_, err := tx.CreateNode(ctx, &Node{ID: "test:doc-e", Labels: []string{"Doc"}, Properties: map[string]any{"id": "e"}})
						return err
					}},
					{"edge create", func(tx *BadgerTransaction) error {
						return tx.CreateEdge(ctx, &Edge{ID: "test:rel-ae", Type: "LINK", StartNode: "test:doc-a", EndNode: "test:doc-e"})
					}},
				}
				for _, step := range mixed {
					before := nor136Pending(engine)
					tx := bufferedWrite(t, engine, step.write)
					nor136AssertAcked(t, engine, cfg, tx, before)
					// The unapplied generation must never publish a negative
					// count, the recorded NOR-138 underflow symptom.
					nor136AssertNoNegativeCounters(t, engine, "Doc", "Parked")
				}
				for id, nodeWant := range want {
					nor136AssertVisibleNode(t, engine, id, nodeWant)
				}
				_, err := engine.GetNode(ctx, "test:doc-c")
				require.ErrorIs(t, err, ErrNotFound)
				_, err = engine.GetEdge(ctx, "test:rel-ab")
				require.ErrorIs(t, err, ErrNotFound)
				_, err = engine.GetEdge(ctx, "test:rel-cd")
				require.ErrorIs(t, err, ErrNotFound)
				nor136AssertLiveCounters(t, engine, wantCounts)

				switch route {
				case "flush":
					require.NoError(t, engine.FlushWriteBehind(ctx))
					require.Zero(t, nor136Pending(engine))
					nor136AssertRestartedState(t, engine, want, wantCounts, wantEdges, absentNodes, absentEdges)
					reopened := nor136Restart(t, engine, dir, cfg)
					nor136AssertRestartedState(t, reopened, want, wantCounts, wantEdges, absentNodes, absentEdges)
				case "crash":
					require.Greater(t, nor136Pending(engine), 0, "the mixed generation must still be undrained at the crash boundary")
					crashClose(t, engine, engine.bufferedCommitWAL)
					reopened := nor136Open(t, dir, cfg, true)
					replayed, err := RecoverBufferedCommits(reopened, reopened.bufferedCommitWAL)
					require.NoError(t, err)
					require.Greater(t, replayed, 0, "the acknowledged mixed generation must replay after the crash")
					require.Zero(t, nor136Pending(reopened))
					nor136AssertRestartedState(t, reopened, want, wantCounts, wantEdges, absentNodes, absentEdges)
				}
			})
		}
	}
}

// nor136ImageVariant is one ordering of the later image over an older
// acknowledged buffered image.
type nor136ImageVariant struct {
	name          string
	bufferedLater bool
}

// TestNOR143OlderBufferedImageCannotOverwriteLaterImage reproduces the
// reported order: an acknowledged buffered peer image is followed by a newer
// relabel (buffered variant) or a newer direct write (direct variant) without
// flushing in between. The later image must win while the peer property
// survives, and bodies, label index, counters, cache and public reads must
// agree after flush and reopen.
func TestNOR143OlderBufferedImageCannotOverwriteLaterImage(t *testing.T) {
	ctx := context.Background()
	for _, cfg := range nor136Configs() {
		variants := []nor136ImageVariant{{name: "later-direct-relabel"}}
		if cfg.writeBehind {
			variants = append(variants, nor136ImageVariant{name: "later-buffered-relabel", bufferedLater: true})
		}
		for _, variant := range variants {
			t.Run(cfg.name()+"/"+variant.name, func(t *testing.T) {
				dir := t.TempDir()
				engine := nor136Open(t, dir, cfg, false)
				bufferedCreate(t, engine, &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "doc", "stage": "base"}})
				require.NoError(t, engine.FlushWriteBehind(ctx))
				require.Zero(t, nor136Pending(engine))

				// Older peer image, acknowledged into the undrained buffer.
				before := nor136Pending(engine)
				older := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
						After:  &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "doc", "peer": "peer-value", "stage": "buffered"}},
						Policy: ClientNodeEdits,
					})
				})
				nor136AssertAcked(t, engine, cfg, older, before)
				// The acknowledged peer property is already readable and the
				// older generation is not preflushed.
				nor136AssertVisibleNode(t, engine, "test:doc", nor136NodeWant{
					labels: []string{"Doc"},
					props:  map[string]any{"id": "doc", "peer": "peer-value", "stage": "buffered"},
				})

				want := nor136NodeWant{
					labels: []string{"Parked"},
					props:  map[string]any{"id": "doc", "peer": "peer-value"},
				}
				if variant.bufferedLater {
					before = nor136Pending(engine)
					newer := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
							After:  &Node{ID: "test:doc", Labels: []string{"Parked"}, Properties: map[string]any{"id": "doc", "peer": "peer-value", "stage": "buffered-newer"}},
							Policy: ClientNodeEdits,
						})
					})
					nor136AssertAcked(t, engine, cfg, newer, before)
					want.props["stage"] = "buffered-newer"
				} else {
					current, err := engine.GetNode(ctx, "test:doc")
					require.NoError(t, err)
					// The direct writer orders after the acknowledged buffered
					// commits and keeps the peer property it read.
					require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{
						Before: current,
						After:  &Node{ID: "test:doc", Labels: []string{"Parked"}, Properties: map[string]any{"id": "doc", "peer": "peer-value", "stage": "direct"}},
						Policy: ClientNodeEdits,
					}))
					want.props["stage"] = "direct"
				}

				require.NoError(t, engine.FlushWriteBehind(ctx))
				require.Zero(t, nor136Pending(engine))
				wantCounts := map[string]int64{"Doc": 0, "Parked": 1}
				nodes := map[NodeID]nor136NodeWant{"test:doc": want}
				nor136AssertRestartedState(t, engine, nodes, wantCounts, map[EdgeID][2]NodeID{}, nil, nil)
				reopened := nor136Restart(t, engine, dir, cfg)
				nor136AssertRestartedState(t, reopened, nodes, wantCounts, map[EdgeID][2]NodeID{}, nil, nil)
			})
		}
	}
}

// TestNOR143DeletedEntitiesCannotResurrect acknowledges an older node image,
// an older relationship image, a new node and a relationship to a later
// deleted endpoint, then deletes the endpoint and relationship directly. After
// flush/restart and after crash recovery no deleted node or relationship may
// reappear, no relationship may reference the deleted endpoint, and counts,
// index, cache and bodies must agree.
func TestNOR143DeletedEntitiesCannotResurrect(t *testing.T) {
	ctx := context.Background()
	want := map[NodeID]nor136NodeWant{
		"test:p":   {labels: []string{"Doc"}, props: map[string]any{"id": "p"}},
		"test:g":   {labels: []string{"Doc"}, props: map[string]any{"id": "g"}},
		"test:new": {labels: []string{"Doc"}, props: map[string]any{"id": "new"}},
	}
	wantCounts := map[string]int64{"Doc": 3}
	absentNodes := []NodeID{"test:q"}
	absentEdges := []EdgeID{"test:e-pq", "test:e-qg", "test:e-nq", "test:e-after-delete"}

	for _, cfg := range nor136Configs() {
		routes := []string{"flush"}
		if cfg.writeBehind {
			routes = append(routes, "crash")
		}
		for _, route := range routes {
			t.Run(cfg.name()+"/"+route, func(t *testing.T) {
				dir := t.TempDir()
				engine := nor136Open(t, dir, cfg, route == "crash")

				bufferedCreate(t, engine, &Node{ID: "test:p", Labels: []string{"Doc"}, Properties: map[string]any{"id": "p"}})
				bufferedCreate(t, engine, &Node{ID: "test:q", Labels: []string{"Doc"}, Properties: map[string]any{"id": "q", "peer": "q-peer"}})
				bufferedCreate(t, engine, &Node{ID: "test:g", Labels: []string{"Doc"}, Properties: map[string]any{"id": "g"}})
				bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					if err := tx.CreateEdge(ctx, &Edge{ID: "test:e-pq", Type: "LINK", StartNode: "test:p", EndNode: "test:q"}); err != nil {
						return err
					}
					return tx.CreateEdge(ctx, &Edge{ID: "test:e-qg", Type: "LINK", StartNode: "test:q", EndNode: "test:g"})
				})
				require.NoError(t, engine.FlushWriteBehind(ctx))
				require.Zero(t, nor136Pending(engine))

				// Older acknowledged images that must not outlive the later
				// direct deletes.
				before := nor136Pending(engine)
				olderNode := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
						After:  &Node{ID: "test:q", Labels: []string{"Doc"}, Properties: map[string]any{"id": "q", "peer": "late"}},
						Policy: ClientNodeEdits,
					})
				})
				nor136AssertAcked(t, engine, cfg, olderNode, before)
				before = nor136Pending(engine)
				olderEdge := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					return tx.UpdateEdge(ctx, EdgeUpdateRequest{
						After: &Edge{ID: "test:e-pq", Type: "LINK", StartNode: "test:p", EndNode: "test:q", Properties: map[string]any{"weight": int64(2)}},
					})
				})
				nor136AssertAcked(t, engine, cfg, olderEdge, before)
				bufferedCreate(t, engine, &Node{ID: "test:new", Labels: []string{"Doc"}, Properties: map[string]any{"id": "new"}})
				bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					return tx.CreateEdge(ctx, &Edge{ID: "test:e-nq", Type: "LINK", StartNode: "test:new", EndNode: "test:q"})
				})

				// Later direct deletes: the relationship first, then the node
				// whose cascade must take every incident relationship with it,
				// including the acknowledged buffered one above.
				require.NoError(t, engine.DeleteEdge(ctx, "test:e-pq"))
				require.NoError(t, engine.DeleteNode(ctx, "test:q"))
				// A new relationship to the deleted endpoint is rejected outright.
				require.Error(t, engine.CreateEdge(ctx, &Edge{ID: "test:e-after-delete", Type: "LINK", StartNode: "test:p", EndNode: "test:q"}),
					"a relationship to a deleted endpoint must be rejected")
				nor136AssertAbsent(t, engine, absentNodes, absentEdges)
				edges, err := engine.AllEdges(ctx)
				require.NoError(t, err)
				for _, edge := range edges {
					require.NotEqual(t, NodeID("test:q"), edge.StartNode, "relationship %s still starts at the deleted node", edge.ID)
					require.NotEqual(t, NodeID("test:q"), edge.EndNode, "relationship %s still ends at the deleted node", edge.ID)
				}
				nor136AssertLiveCounters(t, engine, wantCounts)

				if route == "flush" {
					require.NoError(t, engine.FlushWriteBehind(ctx))
					require.Zero(t, nor136Pending(engine))
					nor136AssertRestartedState(t, engine, want, wantCounts, map[EdgeID][2]NodeID{}, absentNodes, absentEdges)
					reopened := nor136Restart(t, engine, dir, cfg)
					nor136AssertRestartedState(t, reopened, want, wantCounts, map[EdgeID][2]NodeID{}, absentNodes, absentEdges)
					return
				}

				// Leave an acknowledged generation pending so recovery has a
				// real replay to perform next to the deleted entities.
				before = nor136Pending(engine)
				survivor := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(ctx, NodeUpdateRequest{
						After:  &Node{ID: "test:p", Labels: []string{"Doc"}, Properties: map[string]any{"id": "p", "peer": "survivor"}},
						Policy: ClientNodeEdits,
					})
				})
				nor136AssertAcked(t, engine, cfg, survivor, before)
				require.Greater(t, nor136Pending(engine), 0)
				crashClose(t, engine, engine.bufferedCommitWAL)
				reopened := nor136Open(t, dir, cfg, true)
				replayed, err := RecoverBufferedCommits(reopened, reopened.bufferedCommitWAL)
				require.NoError(t, err)
				require.Greater(t, replayed, 0, "the acknowledged survivor write must replay after the crash")
				crashWant := map[NodeID]nor136NodeWant{
					"test:p":   {labels: []string{"Doc"}, props: map[string]any{"id": "p", "peer": "survivor"}},
					"test:g":   want["test:g"],
					"test:new": want["test:new"],
				}
				nor136AssertRestartedState(t, reopened, crashWant, wantCounts, map[EdgeID][2]NodeID{}, absentNodes, absentEdges)
			})
		}
	}
}

// TestNOR143UniqueLabelScopedHolderTransitions covers the UNIQUE controls:
// an actual constrained-label holder rejects the duplicate key, the sole
// holder relabeled away from the constrained label releases the key, and a
// new holder of the constrained label is enforced again. Neo4j uniqueness is
// label-scoped, so an unconstrained label may hold the same key.
//
// The preflush=on route is the flushed control. A write-behind-on
// configuration also runs preflush=off: the replacement for the released key
// is created while the relabel is still an acknowledged, undrained generation,
// so a release that so far exists only in the buffer must be honoured without
// an intervening flush.
func TestNOR143UniqueLabelScopedHolderTransitions(t *testing.T) {
	ctx := context.Background()
	for _, cfg := range nor136Configs() {
		preflushes := []bool{true}
		if cfg.writeBehind {
			preflushes = append(preflushes, false)
		}
		for _, preflush := range preflushes {
			t.Run(fmt.Sprintf("%s/preflush=%s", cfg.name(), boolString(preflush)), func(t *testing.T) {
				dir := t.TempDir()
				engine := nor136Open(t, dir, cfg, false)
				schema := engine.GetSchemaForNamespace("test")
				require.NoError(t, schema.AddConstraint(Constraint{
					Name:       "nor136_doc_id_unique",
					Type:       ConstraintUnique,
					Label:      "Doc",
					Properties: []string{"id"},
				}))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(ctx, engine, schema))

				// The sole constrained-label holder is acknowledged through the
				// configured producer.
				before := nor136Pending(engine)
				holder := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					_, err := tx.CreateNode(ctx, &Node{ID: "test:holder", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k", "peer": "holder"}})
					return err
				})
				nor136AssertAcked(t, engine, cfg, holder, before)

				// An actual constrained-label holder rejects the duplicate key.
				_, err := engine.CreateNode(ctx, &Node{ID: "test:duplicate-direct", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
				relatedViolation(t, err, ConstraintUnique, "k")
				_, err = engine.GetNode(ctx, "test:duplicate-direct")
				require.ErrorIs(t, err, ErrNotFound, "a rejected duplicate publishes no visible body")
				_, err = engine.GetStoredNode(ctx, "test:duplicate-direct")
				require.ErrorIs(t, err, ErrNotFound, "a rejected duplicate publishes no stored body")

				// The sole holder leaves the constrained label.
				before = nor136Pending(engine)
				relabel := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					current, err := tx.GetNode(ctx, "test:holder")
					if err != nil {
						return err
					}
					after := *current
					after.Labels = []string{"Parked"}
					after.Properties = maps.Clone(current.Properties)
					return tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: current, After: &after, Policy: ClientNodeEdits})
				})
				nor136AssertAcked(t, engine, cfg, relabel, before)
				// The acknowledged relabel is already live: the logical holder
				// has left the constrained label and no Doc holder remains.
				nor136AssertLiveCounters(t, engine, map[string]int64{"Doc": 0, "Parked": 1})
				if preflush {
					require.NoError(t, engine.FlushWriteBehind(ctx))
					require.Zero(t, nor136Pending(engine))
					// With no Doc holder left, the label-scoped contract
					// releases the key: bodies, index and counters agree on
					// zero Doc holders.
					nor136AssertStoredCounts(t, engine, map[string]int64{"Doc": 0, "Parked": 1})
					nor136AssertLabelIndex(t, engine, map[string][]NodeID{"Doc": nil, "Parked": {"test:holder"}})
				} else {
					// The release is a genuinely unflushed, acknowledged
					// generation: the relabel operation is still pending and
					// the logical holder has already left the constrained
					// label, with the live Doc count above at zero.
					require.NotZero(t, nor136Pending(engine), "the released relabel must still be undrained")
					nor136AssertVisibleNode(t, engine, "test:holder", nor136NodeWant{
						labels: []string{"Parked"},
						props:  map[string]any{"id": "k", "peer": "holder"},
					})
				}

				// A new Doc may take the released key; on the preflush=off
				// route no flush happens between the relabel and this write.
				before = nor136Pending(engine)
				replacement := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
					_, err := tx.CreateNode(ctx, &Node{ID: "test:replacement", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k", "peer": "replacement"}})
					return err
				})
				nor136AssertAcked(t, engine, cfg, replacement, before)
				// It is now the actual holder, so the duplicate is rejected again.
				_, err = engine.CreateNode(ctx, &Node{ID: "test:duplicate-again", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
				relatedViolation(t, err, ConstraintUnique, "k")
				// The constraint is label-scoped: an unconstrained label may carry
				// the same key.
				_, err = engine.CreateNode(ctx, &Node{ID: "test:parked-peer", Labels: []string{"Parked"}, Properties: map[string]any{"id": "k"}})
				require.NoError(t, err, "the UNIQUE constraint is scoped to the Doc label")
				require.NoError(t, engine.FlushWriteBehind(ctx))

				nodes := map[NodeID]nor136NodeWant{
					"test:holder":      {labels: []string{"Parked"}, props: map[string]any{"id": "k", "peer": "holder"}},
					"test:replacement": {labels: []string{"Doc"}, props: map[string]any{"id": "k", "peer": "replacement"}},
					"test:parked-peer": {labels: []string{"Parked"}, props: map[string]any{"id": "k"}},
				}
				counts := map[string]int64{"Doc": 1, "Parked": 2}
				absentNodes := []NodeID{"test:duplicate-direct", "test:duplicate-again"}
				nor136AssertRestartedState(t, engine, nodes, counts, map[EdgeID][2]NodeID{}, absentNodes, nil)

				reopened := nor136Restart(t, engine, dir, cfg)
				nor136AssertRestartedState(t, reopened, nodes, counts, map[EdgeID][2]NodeID{}, absentNodes, nil)
				// The restored constraint still rejects the actual holder's key
				// and still permits the unconstrained label's key.
				_, err = reopened.CreateNode(ctx, &Node{ID: "test:duplicate-after-restart", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
				relatedViolation(t, err, ConstraintUnique, "k")
				_, err = reopened.CreateNode(ctx, &Node{ID: "test:parked-peer-after-restart", Labels: []string{"Parked"}, Properties: map[string]any{"id": "k"}})
				require.NoError(t, err, "the restored UNIQUE constraint remains label-scoped")
			})
		}
	}
}

func nor136StressIncrement(engine *BadgerEngine, id NodeID) error {
	return engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
		ctx := context.Background()
		current, err := tx.GetNode(ctx, id)
		if err != nil {
			return err
		}
		after := *current
		after.Properties = maps.Clone(current.Properties)
		counter, ok := after.Properties["counter"].(int64)
		if !ok {
			return fmt.Errorf("counter of %s decoded as %T", id, after.Properties["counter"])
		}
		after.Properties["counter"] = counter + 1
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: current, After: &after, Policy: ClientNodeEdits})
	})
}

func nor136StressRelabel(engine *BadgerEngine, id NodeID, label string) error {
	return engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
		ctx := context.Background()
		current, err := tx.GetNode(ctx, id)
		if err != nil {
			return err
		}
		after := *current
		after.Labels = []string{label}
		after.Properties = maps.Clone(current.Properties)
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: current, After: &after, Policy: ClientNodeEdits})
	})
}

func nor136StressRename(engine *BadgerEngine, id NodeID, token string) error {
	return engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
		ctx := context.Background()
		current, err := tx.GetNode(ctx, id)
		if err != nil {
			return err
		}
		after := *current
		after.Properties = maps.Clone(current.Properties)
		delete(after.Properties, "token")
		after.Properties["renamed"] = token
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: current, After: &after, Policy: ClientNodeEdits})
	})
}

// TestNOR136ConcurrentMixedStressRestartRecount runs a bounded deterministic
// concurrent workload (three disjoint workers, thirteen acknowledged
// operations each) and requires the flush/restart and crash-recovery outcomes
// to match the exact model and the independent primary-body recount. Seeds and
// run counts are recorded, and every run is required to execute.
func TestNOR136ConcurrentMixedStressRestartRecount(t *testing.T) {
	seeds := []int64{136, 138, 143}
	runs := 0
	for _, seed := range seeds {
		lowMemory := seed%2 == 0
		for _, route := range []string{"flush", "crash"} {
			t.Run(fmt.Sprintf("seed=%d/route=%s/lowmemory=%s", seed, route, boolString(lowMemory)), func(t *testing.T) {
				nor136StressRun(t, nor136Config{writeBehind: true, lowMemory: lowMemory}, seed, route)
				runs++
			})
		}
		t.Run(fmt.Sprintf("seed=%d/route=flush/writebehind=off", seed), func(t *testing.T) {
			nor136StressRun(t, nor136Config{writeBehind: false, lowMemory: lowMemory}, seed, "flush")
			runs++
		})
	}
	require.Equal(t, len(seeds)*3, runs, "every bounded stress run must execute")
	t.Logf("NOR-136 concurrent stress: seeds=%v runs=%d writebehind-on=2/seed writebehind-off=1/seed workers=3 steps=13 acknowledged-operations-per-run=39", seeds, runs)
}

func nor136StressRun(t *testing.T, cfg nor136Config, seed int64, route string) {
	t.Helper()
	const (
		workers      = 3
		steps        = 13
		innerCounter = 1000
	)
	ctx := context.Background()
	dir := t.TempDir()
	engine := nor136Open(t, dir, cfg, route == "crash")
	prefix := fmt.Sprintf("test:nor136stress-%d-", seed)
	nodeID := func(w, slot int) NodeID { return NodeID(fmt.Sprintf("%snode-%d-%d", prefix, w, slot)) }
	hubID := NodeID(prefix + "hub")
	edgeID := func(w int, kind string) EdgeID { return EdgeID(fmt.Sprintf("%sedge-%d-%s", prefix, w, kind)) }
	transientID := func(w int) NodeID { return NodeID(fmt.Sprintf("%stransient-%d", prefix, w)) }
	tokens := make([]string, workers)
	for w := range tokens {
		rng := rand.New(rand.NewSource(seed*innerCounter + int64(w)))
		tokens[w] = fmt.Sprintf("renamed-%d-%d-%d", seed, w, rng.Intn(innerCounter))
	}

	// Durable base graph before the concurrent phase.
	bufferedCreate(t, engine, &Node{ID: hubID, Labels: []string{"Doc"}, Properties: map[string]any{"id": "hub"}})
	for w := 0; w < workers; w++ {
		for slot := 0; slot < 2; slot++ {
			bufferedCreate(t, engine, &Node{
				ID:         nodeID(w, slot),
				Labels:     []string{"Doc"},
				Properties: map[string]any{"id": fmt.Sprintf("node-%d-%d", w, slot), "counter": int64(0), "token": "base"},
			})
		}
		bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
			if err := tx.CreateEdge(ctx, &Edge{ID: edgeID(w, "old"), Type: "LINK", StartNode: nodeID(w, 1), EndNode: nodeID(w, 0)}); err != nil {
				return err
			}
			return tx.CreateEdge(ctx, &Edge{ID: edgeID(w, "hub"), Type: "LINK", StartNode: nodeID(w, 0), EndNode: hubID})
		})
	}
	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, nor136Pending(engine))
	nor136AssertStoredCounts(t, engine, map[string]int64{"Doc": 1 + 2*workers})

	// Concurrent acknowledged mutations. Workers own disjoint nodes, so the
	// exact final model does not depend on commit interleaving.
	var wg sync.WaitGroup
	var acknowledged atomic.Int64
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for step := 0; step < steps; step++ {
				var err error
				switch step {
				case 0, 4, 9:
					err = nor136StressIncrement(engine, nodeID(w, 0))
				case 1, 5:
					err = nor136StressIncrement(engine, nodeID(w, 1))
				case 2:
					err = nor136StressRelabel(engine, nodeID(w, 0), "Parked")
				case 3:
					err = nor136StressRename(engine, nodeID(w, 1), tokens[w])
				case 6:
					err = engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
						return tx.DeleteEdge(ctx, edgeID(w, "old"))
					})
				case 7:
					err = engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
						_, createErr := tx.CreateNode(ctx, &Node{ID: transientID(w), Labels: []string{"Doc"}, Properties: map[string]any{"id": "transient"}})
						return createErr
					})
				case 8:
					err = nor136StressRelabel(engine, nodeID(w, 0), "Doc")
				case 10:
					err = engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
						return tx.DeleteGraphNode(ctx, transientID(w), CascadeEdges)
					})
				case 11:
					err = nor136StressRelabel(engine, nodeID(w, 1), "Parked")
				case 12:
					err = engine.inBulkTransaction(context.Background(), "test", func(tx *BadgerTransaction) error {
						return tx.CreateEdge(ctx, &Edge{ID: edgeID(w, "new"), Type: "LINK", StartNode: nodeID(w, 1), EndNode: hubID})
					})
				}
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d step %d: %w", w, step, err):
					default:
					}
					return
				}
				acknowledged.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.EqualValues(t, workers*steps, acknowledged.Load(), "every worker operation must be acknowledged")
	if cfg.writeBehind {
		require.Greater(t, nor136Pending(engine), 0, "the concurrent generation must still be undrained")
	}

	want := make(map[NodeID]nor136NodeWant, 1+2*workers)
	want[hubID] = nor136NodeWant{labels: []string{"Doc"}, props: map[string]any{"id": "hub"}}
	wantEdges := make(map[EdgeID][2]NodeID, 2*workers)
	absentNodes := make([]NodeID, 0, workers)
	absentEdges := make([]EdgeID, 0, workers)
	for w := 0; w < workers; w++ {
		want[nodeID(w, 0)] = nor136NodeWant{
			labels: []string{"Doc"},
			props:  map[string]any{"id": fmt.Sprintf("node-%d-0", w), "counter": int64(3), "token": "base"},
		}
		want[nodeID(w, 1)] = nor136NodeWant{
			labels: []string{"Parked"},
			props:  map[string]any{"id": fmt.Sprintf("node-%d-1", w), "counter": int64(2), "renamed": tokens[w]},
			absent: []string{"token"},
		}
		wantEdges[edgeID(w, "hub")] = [2]NodeID{nodeID(w, 0), hubID}
		wantEdges[edgeID(w, "new")] = [2]NodeID{nodeID(w, 1), hubID}
		absentNodes = append(absentNodes, transientID(w))
		absentEdges = append(absentEdges, edgeID(w, "old"))
	}
	wantCounts := map[string]int64{"Doc": 1 + workers, "Parked": workers}

	switch route {
	case "flush":
		require.NoError(t, engine.FlushWriteBehind(ctx))
		require.Zero(t, nor136Pending(engine))
		nor136AssertRestartedState(t, engine, want, wantCounts, wantEdges, absentNodes, absentEdges)
		reopened := nor136Restart(t, engine, dir, cfg)
		nor136AssertRestartedState(t, reopened, want, wantCounts, wantEdges, absentNodes, absentEdges)
	case "crash":
		crashClose(t, engine, engine.bufferedCommitWAL)
		reopened := nor136Open(t, dir, cfg, true)
		replayed, err := RecoverBufferedCommits(reopened, reopened.bufferedCommitWAL)
		require.NoError(t, err)
		require.Greater(t, replayed, 0, "the acknowledged concurrent generation must replay after the crash")
		require.Zero(t, nor136Pending(reopened))
		nor136AssertRestartedState(t, reopened, want, wantCounts, wantEdges, absentNodes, absentEdges)
	}
	t.Logf("seed=%d route=%s writebehind=%s lowmemory=%s workers=%d steps=%d nodes=%d edges=%d",
		seed, route, boolString(cfg.writeBehind), boolString(cfg.lowMemory), workers, steps, len(want), len(wantEdges))
}
