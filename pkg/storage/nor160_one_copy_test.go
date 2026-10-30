//go:build nornic_copycount

// NOR-160 lane (Claude), exactly one structural copy per delivery, 30 October 2026.
// Run with -tags nornic_copycount: the test build counts every node copy.
package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

var nor160OneCopyIDs = []NodeID{"test:a", "test:b"}

// nor160OneCopyRoute is one public label-stream route. want is how many
// structural copies a delivered node may take: one when its source retains
// it, none when the read decoded it for this caller.
type nor160OneCopyRoute struct {
	name  string
	setup func(*testing.T) (read func(*testing.T, func(*Node) error), update func(*testing.T, NodeID), want func(NodeID) int, strip bool)
}

func nor160BufferedEngine(t *testing.T) *BadgerEngine {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		for _, id := range nor160OneCopyIDs {
			if _, err := writer.CreateNode(nor160OneCopyNode(id)); err != nil {
				return err
			}
		}
		return nil
	})
	require.Positive(t, engine.writeBehind.PendingOps(), "the nodes must still be buffered")
	return engine
}

func nor160CommittedEngine(t *testing.T) *BadgerEngine {
	engine := createTestBadgerEngine(t)
	for _, id := range nor160OneCopyIDs {
		_, err := engine.CreateNode(nor160OneCopyNode(id))
		require.NoError(t, err)
	}
	return engine
}

func nor160EngineUpdate(engine *BadgerEngine) func(*testing.T, NodeID) {
	return func(t *testing.T, id NodeID) {
		node := nor160OneCopyNode(id)
		node.Properties["key"] = "updated"
		require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: node}))
	}
}

func nor160Retained(NodeID) int { return 1 }

// nor160NativeWant: the native label stream copies a node-cache image once
// and hands over a decoded node without copying it.
func nor160NativeWant(engine *BadgerEngine) func(NodeID) int {
	engine.nodeCacheMu.RLock()
	cached := make(map[NodeID]bool, len(engine.nodeCache))
	for id := range engine.nodeCache {
		cached[id] = true
	}
	engine.nodeCacheMu.RUnlock()
	return func(id NodeID) int {
		if cached[id] {
			return 1
		}
		return 0
	}
}

func nor160Tx(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

var nor160OneCopyRoutes = []nor160OneCopyRoute{
	{"native/buffered/strip", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160BufferedEngine(t)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{QueryNodes: true, Label: "Held", StripEmbeddings: true}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, true
	}},
	{"native/committed/strip", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160CommittedEngine(t)
		for _, id := range nor160OneCopyIDs {
			_, err := engine.GetNode(id) // warm the node cache where it admits reads
			require.NoError(t, err)
		}
		want := nor160NativeWant(engine)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{QueryNodes: true, Label: "Held", StripEmbeddings: true}, visit))
		}, nor160EngineUpdate(engine), want, true
	}},
	{"transaction/buffered/projected", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160BufferedEngine(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, tx.StreamNodesByLabelProjected("Held", []string{"nested"}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, false
	}},
	{"transaction/buffered/query-nodes", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160BufferedEngine(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{QueryNodes: true, Label: "Held"}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, false
	}},
	{"transaction/buffered/query-nodes/strip", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160BufferedEngine(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{QueryNodes: true, Label: "Held", StripEmbeddings: true}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, true
	}},
	{"transaction/snapshot/projected-replay", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160CommittedEngine(t)
		tx := nor160Tx(t, engine)
		// The pinned snapshot retains the population for replay: the first
		// delivery and every replay copy that retained image once.
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, tx.StreamNodesByLabelProjected("Held", []string{"nested"}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, false
	}},
	{"transaction/snapshot/query-nodes-replay", func(t *testing.T) (func(*testing.T, func(*Node) error), func(*testing.T, NodeID), func(NodeID) int, bool) {
		engine := nor160CommittedEngine(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T, visit func(*Node) error) {
			require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{QueryNodes: true, Label: "Held", StripEmbeddings: true}, visit))
		}, nor160EngineUpdate(engine), nor160Retained, true
	}},
}

// nor160CopyViolations reports every delivered node whose structural copy
// count differs from its source's contract.
func nor160CopyViolations(delivered []*Node, copies map[NodeID]int, want func(NodeID) int) []string {
	if len(delivered) == 0 {
		return []string{"nothing delivered"}
	}
	var out []string
	for _, node := range delivered {
		if got := copies[node.ID]; got != want(node.ID) {
			out = append(out, fmt.Sprintf("%s copied %d times, want %d", node.ID, got, want(node.ID)))
		}
	}
	return out
}

func nor160Collect(t *testing.T, read func(*testing.T, func(*Node) error), consume func(*Node) *Node) ([]*Node, map[NodeID]int) {
	delivered, copies, _ := nor160CollectVectors(t, read, consume)
	return delivered, copies
}

// nor160CollectVectors also reports how often each node's vectors were copied.
func nor160CollectVectors(t *testing.T, read func(*testing.T, func(*Node) error), consume func(*Node) *Node) ([]*Node, map[NodeID]int, map[NodeID]int) {
	var delivered []*Node
	var copies map[NodeID]int
	vectors := CountVectorCopies(func() {
		copies, _ = CountStructuralCopies(func() {
			read(t, func(node *Node) error {
				delivered = append(delivered, consume(node))
				return nil
			})
		})
	})
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].ID < delivered[j].ID })
	return delivered, copies, vectors
}

func nor160Keep(node *Node) *Node { return node }

func TestNOR160OneCopyLabelStreams(t *testing.T) {
	for _, route := range nor160OneCopyRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, _, want, strip := route.setup(t)
			for pass := 0; pass < 2; pass++ {
				delivered, copies, vectors := nor160CollectVectors(t, read, nor160Keep)
				require.Len(t, delivered, len(nor160OneCopyIDs))
				require.Empty(t, nor160CopyViolations(delivered, copies, want), "pass %d", pass)
				if strip {
					// Stripping happens in the source's one copy: no vector is
					// copied and then dropped.
					require.Empty(t, vectors, "pass %d: vectors copied for a stripped read", pass)
				}
				for _, node := range delivered {
					require.Contains(t, node.Properties, "nested", "the delivery must carry a mutable value")
					if strip {
						require.Nil(t, node.ChunkEmbeddings)
						require.Nil(t, node.NamedEmbeddings)
					}
				}
				// Caller to store: changing the delivery changes no later read.
				image := readownership.Capture(delivered)
				readownership.Mutate(delivered)
				again, _ := nor160Collect(t, read, nor160Keep)
				require.Equal(t, image, readownership.Capture(again), "a caller's change reached a later read, pass %d", pass)
			}
		})
	}
}

func TestNOR160OneCopyLabelStreamsSourceChanges(t *testing.T) {
	for _, route := range nor160OneCopyRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, update, _, _ := route.setup(t)
			delivered, _ := nor160Collect(t, read, nor160Keep)
			require.NotEmpty(t, delivered)
			image := readownership.Capture(delivered)
			// Store to caller: a later write never reaches a delivered value.
			for _, id := range nor160OneCopyIDs {
				update(t, id)
			}
			require.Equal(t, image, readownership.Capture(delivered), "a later write changed a delivered node")
		})
	}
}

// Planted control: the counter must see a consumer's second copy of an owned
// delivery on every route (one copy more than the same read on a fresh
// fixture without it, so a route that under-copies cannot mask it), and on
// every route that copies correctly the plant must turn the one-copy check red.
func TestNOR160OneCopyControlSecondCopy(t *testing.T) {
	for _, route := range nor160OneCopyRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, _, want, _ := route.setup(t)
			kept, baseline := nor160Collect(t, read, nor160Keep)
			planted, _, _, _ := route.setup(t)
			delivered, copies := nor160Collect(t, planted, copyNode)
			if len(nor160CopyViolations(kept, baseline, want)) == 0 {
				// On a route that copies correctly, the plant turns the check red.
				// A route that is already wrong is red in TestNOR160OneCopyLabelStreams.
				require.NotEmpty(t, nor160CopyViolations(delivered, copies, want), "the planted second copy was not detected")
			}
			for _, node := range delivered {
				require.Equal(t, baseline[node.ID]+1, copies[node.ID], "%s: the counter missed the planted copy", node.ID)
			}
		})
	}
}

// Relationship reads: after the read has warmed its caches, every delivered
// relationship is copied exactly once per read, whether a cache, the
// write-behind buffer or a cache fill made that copy; never copied and
// dropped, never copied twice.
type nor160EdgeRoute struct {
	name  string
	setup func(*testing.T) (read func(*testing.T) []*Edge, update func(*testing.T))
}

func nor160EdgeFixtureEdge(id EdgeID, end NodeID) *Edge {
	return &Edge{ID: id, StartNode: "test:a", EndNode: end, Type: "T", Properties: nor160OwnershipProperties()}
}

// nor160WriteBehindEdges flushes test:e1 (a->b), reads it so its body is cached
// at its record version, then buffers test:e2 (a->c), which drops a's cached
// adjacency list: the next adjacency read takes the indexed reader.
func nor160WriteBehindEdges(t *testing.T) *BadgerEngine {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
			if _, err := writer.CreateNode(&Node{ID: id, Labels: []string{"Held"}}); err != nil {
				return err
			}
		}
		return writer.CreateEdge(nor160EdgeFixtureEdge("test:e1", "test:b"))
	})
	require.NoError(t, engine.FlushWriteBehind())
	_, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		return writer.CreateEdge(nor160EdgeFixtureEdge("test:e2", "test:c"))
	})
	return engine
}

func nor160CommittedEdges(t *testing.T) *BadgerEngine {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(nor160EdgeFixtureEdge("test:e1", "test:b")))
	require.NoError(t, engine.CreateEdge(nor160EdgeFixtureEdge("test:e2", "test:c")))
	return engine
}

func nor160EdgeUpdate(engine *BadgerEngine) func(*testing.T) {
	return func(t *testing.T) {
		edge := nor160EdgeFixtureEdge("test:e1", "test:b")
		edge.Properties["key"] = "updated"
		require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: edge}))
	}
}

// nor160Edges unwraps a read; a read error fails the test through panic.
func nor160Edges(edges []*Edge, err error) []*Edge {
	if err != nil {
		panic(err)
	}
	return edges
}

var nor160EdgeRoutes = []nor160EdgeRoute{
	{"engine/write-behind/indexed-adjacency", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160WriteBehindEdges(t)
		return func(t *testing.T) []*Edge { return nor160Edges(engine.GetOutgoingEdges("test:a")) }, nor160EdgeUpdate(engine)
	}},
	{"transaction/write-behind/outgoing", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160WriteBehindEdges(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T) []*Edge {
			return nor160Edges(tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"}))
		}, nor160EdgeUpdate(engine)
	}},
	{"engine/committed/adjacency", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160CommittedEdges(t)
		return func(t *testing.T) []*Edge { return nor160Edges(engine.GetOutgoingEdges("test:a")) }, nor160EdgeUpdate(engine)
	}},
	{"engine/committed/between", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160CommittedEdges(t)
		return func(t *testing.T) []*Edge { return nor160Edges(engine.GetEdgesBetween("test:a", "test:b")) }, nor160EdgeUpdate(engine)
	}},
	{"engine/committed/by-type", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160CommittedEdges(t)
		return func(t *testing.T) []*Edge { return nor160Edges(engine.GetEdgesByType("T")) }, nor160EdgeUpdate(engine)
	}},
	{"engine/committed/point", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160CommittedEdges(t)
		return func(t *testing.T) []*Edge {
			edge, err := engine.GetEdge("test:e1")
			return nor160Edges([]*Edge{edge}, err)
		}, nor160EdgeUpdate(engine)
	}},
	{"transaction/committed/outgoing", func(t *testing.T) (func(*testing.T) []*Edge, func(*testing.T)) {
		engine := nor160CommittedEdges(t)
		tx := nor160Tx(t, engine)
		return func(t *testing.T) []*Edge {
			return nor160Edges(tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"}))
		}, nor160EdgeUpdate(engine)
	}},
}

func nor160CountEdges(t *testing.T, read func(*testing.T) []*Edge, consume func(*Edge) *Edge) ([]*Edge, map[EdgeID]int) {
	var delivered []*Edge
	_, copies := CountStructuralCopies(func() {
		for _, edge := range read(t) {
			delivered = append(delivered, consume(edge))
		}
	})
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].ID < delivered[j].ID })
	return delivered, copies
}

func nor160EdgeKeep(edge *Edge) *Edge { return edge }

func nor160EdgeCopyViolations(delivered []*Edge, copies map[EdgeID]int) []string {
	if len(delivered) == 0 {
		return []string{"nothing delivered"}
	}
	var out []string
	for _, edge := range delivered {
		if copies[edge.ID] != 1 {
			out = append(out, fmt.Sprintf("%s copied %d times, want 1", edge.ID, copies[edge.ID]))
		}
	}
	return out
}

func TestNOR160OneCopyRelationshipReads(t *testing.T) {
	for _, route := range nor160EdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, _ := route.setup(t)
			read(t) // warm every cache this read fills
			for pass := 0; pass < 2; pass++ {
				delivered, copies := nor160CountEdges(t, read, nor160EdgeKeep)
				require.Empty(t, nor160EdgeCopyViolations(delivered, copies), "pass %d", pass)
				require.Contains(t, delivered[0].Properties, "nested", "the delivery must carry a mutable value")
				image := readownership.Capture(delivered)
				readownership.Mutate(delivered)
				again, _ := nor160CountEdges(t, read, nor160EdgeKeep)
				require.Equal(t, image, readownership.Capture(again), "a caller's change reached a later read, pass %d", pass)
			}
		})
	}
}

func TestNOR160OneCopyRelationshipReadsSourceChanges(t *testing.T) {
	for _, route := range nor160EdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, update := route.setup(t)
			delivered := read(t)
			require.NotEmpty(t, delivered)
			image := readownership.Capture(delivered)
			update(t)
			require.Equal(t, image, readownership.Capture(delivered), "a later write changed a delivered relationship")
		})
	}
}

// Planted control: the counter sees a consumer's second copy of a delivered
// relationship on every route (one copy more than the same read without it).
func TestNOR160OneCopyRelationshipControlSecondCopy(t *testing.T) {
	for _, route := range nor160EdgeRoutes {
		t.Run(route.name, func(t *testing.T) {
			read, _ := route.setup(t)
			read(t)
			_, baseline := nor160CountEdges(t, read, nor160EdgeKeep)
			delivered, copies := nor160CountEdges(t, read, copyEdge)
			for _, edge := range delivered {
				require.Equal(t, baseline[edge.ID]+1, copies[edge.ID], "%s: the counter missed the planted copy", edge.ID)
			}
		})
	}
}
