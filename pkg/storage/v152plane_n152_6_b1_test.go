package storage

// NOR-178 closure-completeness matrix.
//
// The parent proof (nor178_snapshot_drop_test.go) establishes the case for one
// snapshot opened directly on a BadgerEngine. This file fills the ticket's
// wrapper/lifecycle matrix on the current release, with no production change:
// it exercises the public OpenReadSnapshot capability
// (pkg/storage/read_snapshot.go:241) as it resolves through the wrappers the
// server actually composes, each of which forwards to the one capture owner:
//
//   - WALEngine.OpenReadSnapshot forwards to the engine the WAL writes through
//     (read_snapshot.go:263),
//   - TracedEngine.OpenReadSnapshot forwards to its decorated engine
//     (read_snapshot.go:268),
//   - NamespacedEngine.OpenReadSnapshot scopes the capture to its namespace and
//     forwards to its inner engine (read_snapshot.go:274).
//
// Rows are the ticket's test matrix:
//
//	1 historical: old snapshot, DROP, recreate, iterate -> the old view
//	2 no recreate: old snapshot, DROP, iterate -> the same established view
//	3 fresh snapshot after recreate -> New body only
//	4 other namespace: DROP one database while reading the other -> unaffected
//	5 wrappers/lifecycle: WAL, tracing and namespaced compositions; overlapping
//	  readers; Close retires the reader and later reads fail
//
// Every DROP goes through the real managed prefix-drop path
// (BadgerEngine.DeleteByPrefix -> deleteByPrefixHeld ->
// managedBadgerDB.DropPrefix with retainPrefixSnapshots), reached through the
// existing pr1667DropPrefix helper, plus one row through
// NamespacedEngine.DeleteByPrefix, which is the database DROP call itself.
//
// The assertions state the contract the original reproduction demands: an
// admitted snapshot keeps the bodies it was opened on for its whole lifetime,
// and a fresh snapshot sees only post-DROP state. A current-release failure
// keeps that assertion rather than weakening it.

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

const (
	// nor178SubjectPrefix is the database the ticket drops; nor178PeerPrefix
	// is the independent database that must be unaffected.
	nor178SubjectPrefix = "d7"
	nor178PeerPrefix    = "peer"
)

// nor178SnapshotStack is one wrapper composition over one managed Badger
// engine. scoped records whether the composition's snapshot is scoped to
// nor178SubjectPrefix, which decides the IDs the view reports.
type nor178SnapshotStack struct {
	name string
	// engine is the composition's public view: the value a consumer would be
	// given, and the value passed to OpenReadSnapshot.
	engine Engine
	// scoped is true when the composition is a namespaced view of the subject
	// database, so its snapshot reports user IDs and hides every other
	// namespace.
	scoped bool
	// source names the forwarding chain a failure traverses.
	source string
}

// nor178SnapshotStackFactory builds one composition over the engine the
// subtest owns.
type nor178SnapshotStackFactory struct {
	name  string
	build func(*testing.T, *BadgerEngine) nor178SnapshotStack
}

func nor178WALSnapshotOwner(t *testing.T, inner Engine) Engine {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	owner := NewWALEngine(inner, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return owner
}

// nor178SnapshotStackFactories is the wrapper matrix. Each composition exists
// in current code or current tests:
//
//   - namespaced over WAL over Badger is the database view the manager hands
//     out (pkg/multidb/manager.go:824 over a WAL-backed inner engine),
//   - namespaced over TracedEngine is a cypher executor over a namespaced
//     storage view (pkg/cypher/typed_count_namespace_test.go:114),
//   - TracedEngine over a namespaced view is the executor's tracing decorator
//     over a database view (pkg/server/storage_membership_test.go:48),
//   - namespaced over WAL over TracedEngine is an existing composition matrix
//     (pkg/storage/nor56_chunk_visibility_test.go:67).
//
// All of them forward to the same BadgerEngine capture owner, so all of them
// must survive the same DROP with the same retained view.
var nor178SnapshotStackFactories = []nor178SnapshotStackFactory{
	{
		name: "badger",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "badger",
				engine: base,
				source: "BadgerEngine.OpenReadSnapshot captures the managed read view",
			}
		},
	},
	{
		name: "wal",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "wal",
				engine: nor178WALSnapshotOwner(t, base),
				source: "WALEngine.OpenReadSnapshot -> engine",
			}
		},
	},
	{
		name: "traced",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "traced",
				engine: NewTracedEngine(base),
				source: "TracedEngine.OpenReadSnapshot -> Engine",
			}
		},
	},
	{
		name: "namespaced",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "namespaced",
				engine: NewNamespacedEngine(base, nor178SubjectPrefix),
				scoped: true,
				source: "NamespacedEngine.OpenReadSnapshot -> inner engine",
			}
		},
	},
	{
		name: "namespaced-wal",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "namespaced-wal",
				engine: NewNamespacedEngine(nor178WALSnapshotOwner(t, base), nor178SubjectPrefix),
				scoped: true,
				source: "NamespacedEngine -> WALEngine -> BadgerEngine",
			}
		},
	},
	{
		name: "namespaced-traced",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "namespaced-traced",
				engine: NewNamespacedEngine(NewTracedEngine(base), nor178SubjectPrefix),
				scoped: true,
				source: "NamespacedEngine -> TracedEngine -> BadgerEngine",
			}
		},
	},
	{
		name: "traced-namespaced",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "traced-namespaced",
				engine: NewTracedEngine(NewNamespacedEngine(base, nor178SubjectPrefix)),
				scoped: true,
				source: "TracedEngine -> NamespacedEngine -> BadgerEngine",
			}
		},
	},
	{
		name: "namespaced-wal-traced",
		build: func(t *testing.T, base *BadgerEngine) nor178SnapshotStack {
			return nor178SnapshotStack{
				name:   "namespaced-wal-traced",
				engine: NewNamespacedEngine(nor178WALSnapshotOwner(t, NewTracedEngine(base)), nor178SubjectPrefix),
				scoped: true,
				source: "NamespacedEngine -> WALEngine -> TracedEngine -> BadgerEngine",
			}
		},
	},
}

// nor178StoredNodes is the stored population's node bodies, stated once in the
// stored ID space. Every expectation below derives from it, so a scoped and an
// unscoped expectation cannot drift apart.
func nor178StoredNodes() map[NodeID]string {
	return map[NodeID]string{
		"d7:n":   "Old",
		"d7:m":   "Old",
		"peer:n": "Peer",
		"peer:m": "Peer",
	}
}

// nor178StoredEdges is the stored population's relationships.
func nor178StoredEdges() map[EdgeID]string {
	return map[EdgeID]string{
		"d7:r":   "EdgeOld",
		"peer:r": "PeerEdge",
	}
}

// nor178DroppedNodes is the stored node population after DROP DATABASE d7 with
// no recreate: the subject database is empty, the peer database is untouched.
func nor178DroppedNodes() map[NodeID]string {
	nodes := nor178StoredNodes()
	delete(nodes, "d7:n")
	delete(nodes, "d7:m")
	return nodes
}

// nor178RecreatedNodes is the stored node population after DROP DATABASE d7 and
// recreating the subject node with the New body.
func nor178RecreatedNodes() map[NodeID]string {
	return map[NodeID]string{
		"d7:n":   "New",
		"peer:n": "Peer",
		"peer:m": "Peer",
	}
}

// nor178UpdatedNodes is the stored node population after the subject node was
// updated once before the DROP, so two overlapping readers were admitted on
// different bodies.
func nor178UpdatedNodes() map[NodeID]string {
	nodes := nor178StoredNodes()
	nodes["d7:n"] = "Updated"
	return nodes
}

// nor178PeerOnlyEdges is the stored relationship population after the subject
// database was dropped and only the node was recreated.
func nor178PeerOnlyEdges() map[EdgeID]string {
	edges := nor178StoredEdges()
	delete(edges, "d7:r")
	return edges
}

// nor178Seed writes the stored population directly on the managed engine, so
// every stack reads exactly the same committed state.
func nor178Seed(t *testing.T, base *BadgerEngine) {
	t.Helper()
	for _, node := range []struct {
		id   NodeID
		text string
	}{
		{"d7:n", "Old"},
		{"d7:m", "Old"},
		{"peer:n", "Peer"},
		{"peer:m", "Peer"},
	} {
		_, err := base.CreateNode(t.Context(), &Node{ID: node.id, Labels: []string{"Doc"}, Properties: map[string]any{"text": node.text}})
		require.NoError(t, err, "seed node %s", node.id)
	}
	for _, edge := range []struct {
		id         EdgeID
		start, end NodeID
		note       string
	}{
		{"d7:r", "d7:n", "d7:m", "EdgeOld"},
		{"peer:r", "peer:n", "peer:m", "PeerEdge"},
	} {
		require.NoError(t, base.CreateEdge(t.Context(), &Edge{ID: edge.id, StartNode: edge.start, EndNode: edge.end, Type: "REL", Properties: map[string]any{"note": edge.note}}), "seed edge %s", edge.id)
	}
}

// nor178RecreateSubject recreates the dropped subject node with the New body.
func nor178RecreateSubject(t *testing.T, base *BadgerEngine) {
	t.Helper()
	_, err := base.CreateNode(t.Context(), &Node{ID: "d7:n", Labels: []string{"Replacement"}, Properties: map[string]any{"text": "New"}})
	require.NoError(t, err, "recreate d7:n")
}

// nor178Open opens a public read snapshot of engine. Close runs at cleanup and
// is idempotent, so a test may close the view itself to assert retirement.
func nor178Open(t *testing.T, engine any, source string) ReadSnapshotView {
	t.Helper()
	view, err := OpenReadSnapshot(t.Context(), engine)
	require.NoError(t, err, "OpenReadSnapshot through %s", source)
	t.Cleanup(view.Close)
	return view
}

// node and edge translate a stored ID into the ID this stack's snapshot view
// reports, and report whether the view can see it at all.
func (s nor178SnapshotStack) node(id NodeID) (NodeID, bool) {
	if !s.scoped {
		return id, true
	}
	scoped, ok := strings.CutPrefix(string(id), nor178SubjectPrefix+":")
	return NodeID(scoped), ok
}

func (s nor178SnapshotStack) edge(id EdgeID) (EdgeID, bool) {
	if !s.scoped {
		return id, true
	}
	scoped, ok := strings.CutPrefix(string(id), nor178SubjectPrefix+":")
	return EdgeID(scoped), ok
}

// nodes derives what this stack's snapshot must report for a stored node
// population.
func (s nor178SnapshotStack) nodes(stored map[NodeID]string) map[NodeID]string {
	viewed := make(map[NodeID]string, len(stored))
	for id, text := range stored {
		if visible, ok := s.node(id); ok {
			viewed[visible] = text
		}
	}
	return viewed
}

// edges derives what this stack's snapshot must report for a stored
// relationship population.
func (s nor178SnapshotStack) edges(stored map[EdgeID]string) map[EdgeID]string {
	viewed := make(map[EdgeID]string, len(stored))
	for id, note := range stored {
		if visible, ok := s.edge(id); ok {
			viewed[visible] = note
		}
	}
	return viewed
}

// requireRetired asserts the snapshot stopped serving reads after Close.
func requireRetired(t *testing.T, view ReadSnapshotView, when string) {
	t.Helper()
	require.ErrorIs(t, view.IterateNodes(func(*Node) bool { return true }), ErrStorageClosed,
		"a closed snapshot must fail node iteration (%s)", when)
	require.ErrorIs(t, view.StreamEdges(t.Context(), func(*Edge) error { return nil }), ErrStorageClosed,
		"a closed snapshot must fail edge streaming (%s)", when)
}

// TestNOR178SnapshotMatrixAcrossWrapperCompositions covers matrix rows 1, 2, 3
// and 5 for every wrapper composition: the capability resolves through the
// wrapper, the admitted view keeps the Old bodies across DROP with and without
// a recreate, a fresh snapshot sees the New body only, and Close retires the
// reader. Row 4 rides along: a scoped view never sees the peer database and an
// unscoped view keeps the peer bodies across the subject database's DROP.
func TestNOR178SnapshotMatrixAcrossWrapperCompositions(t *testing.T) {
	for _, factory := range nor178SnapshotStackFactories {
		t.Run(factory.name, func(t *testing.T) {
			base := newReadSnapshotTestEngine(t, false)
			nor178Seed(t, base)
			stack := factory.build(t, base)

			require.True(t, SupportsReadSnapshot(t.Context(), stack.engine),
				"the public capability must resolve through %s", stack.source)

			// Row 1/2 baseline: the view the snapshot is admitted on.
			old := nor178Open(t, stack.engine, stack.source)
			require.Equal(t, stack.nodes(nor178StoredNodes()), snapshotNodeText(t, old),
				"matrix row 1/2: the admitted view is the stored population (%s)", stack.source)
			require.Equal(t, stack.edges(nor178StoredEdges()), snapshotEdgeNotes(t, old),
				"matrix row 1/2: the admitted view carries the stored relationships (%s)", stack.source)

			// Row 2: DROP with no recreate. The admitted view must not lose bodies.
			pr1667DropPrefix(t, base, nor178SubjectPrefix+":")
			require.Equal(t, stack.nodes(nor178StoredNodes()), snapshotNodeText(t, old),
				"matrix row 2: DROP must not drop bodies from an admitted snapshot (%s)", stack.source)
			require.Equal(t, stack.edges(nor178StoredEdges()), snapshotEdgeNotes(t, old),
				"matrix row 2: DROP must not drop relationships from an admitted snapshot (%s)", stack.source)

			// Row 2 control: the DROP really removed the database's live state.
			dropped := nor178Open(t, stack.engine, stack.source)
			require.Equal(t, stack.nodes(nor178DroppedNodes()), snapshotNodeText(t, dropped),
				"matrix row 2: a fresh snapshot after DROP sees the database empty and the peer intact (%s)", stack.source)
			require.Equal(t, stack.edges(nor178PeerOnlyEdges()), snapshotEdgeNotes(t, dropped),
				"matrix row 2: a fresh snapshot after DROP sees the dropped relationships gone (%s)", stack.source)

			// Row 1: recreate, then iterate the admitted snapshot again.
			nor178RecreateSubject(t, base)
			require.Equal(t, stack.nodes(nor178StoredNodes()), snapshotNodeText(t, old),
				"matrix row 1: recreation must not replace or omit the old body (%s)", stack.source)
			require.Equal(t, stack.edges(nor178StoredEdges()), snapshotEdgeNotes(t, old),
				"matrix row 1: recreation must not replace or omit the old relationship (%s)", stack.source)

			// Row 3: a new snapshot sees the recreated body only.
			current := nor178Open(t, stack.engine, stack.source)
			require.Equal(t, stack.nodes(nor178RecreatedNodes()), snapshotNodeText(t, current),
				"matrix row 3: a fresh snapshot after recreation sees the New body only (%s)", stack.source)
			require.Equal(t, stack.edges(nor178PeerOnlyEdges()), snapshotEdgeNotes(t, current),
				"matrix row 3: a fresh snapshot after recreation sees no dropped relationship (%s)", stack.source)

			// Row 5: Close retires the admitted reader, and is idempotent.
			old.Close()
			requireRetired(t, old, "matrix row 5 after DROP and recreation through "+stack.source)
			require.NotPanics(t, old.Close, "Close must be idempotent (%s)", stack.source)
			require.Equal(t, stack.nodes(nor178RecreatedNodes()), snapshotNodeText(t, current),
				"matrix row 5: retiring one reader must not disturb another (%s)", stack.source)
		})
	}
}

// TestNOR178PeerNamespaceSnapshotAcrossOtherDatabaseDrop covers matrix row 4
// through the database views themselves: two namespaces share one managed
// engine, and one is dropped and recreated while admitted snapshots of both are
// open.
func TestNOR178PeerNamespaceSnapshotAcrossOtherDatabaseDrop(t *testing.T) {
	base := newReadSnapshotTestEngine(t, false)
	nor178Seed(t, base)
	subject := NewNamespacedEngine(base, nor178SubjectPrefix)
	peer := NewNamespacedEngine(base, nor178PeerPrefix)

	oldSubject := nor178Open(t, subject, "the subject database view")
	oldPeer := nor178Open(t, peer, "the peer database view")

	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, oldSubject))
	require.Equal(t, map[EdgeID]string{"r": "EdgeOld"}, snapshotEdgeNotes(t, oldSubject))
	require.Equal(t, map[NodeID]string{"n": "Peer", "m": "Peer"}, snapshotNodeText(t, oldPeer),
		"row 4: the peer namespace's admitted view is the peer population")
	require.Equal(t, map[EdgeID]string{"r": "PeerEdge"}, snapshotEdgeNotes(t, oldPeer))

	pr1667DropPrefix(t, base, nor178SubjectPrefix+":")

	require.Equal(t, map[NodeID]string{"n": "Peer", "m": "Peer"}, snapshotNodeText(t, oldPeer),
		"row 4: another database's DROP must not change the peer namespace's admitted view")
	require.Equal(t, map[EdgeID]string{"r": "PeerEdge"}, snapshotEdgeNotes(t, oldPeer),
		"row 4: another database's DROP must not change the peer namespace's admitted relationships")
	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, oldSubject),
		"row 4: the dropped database's admitted view keeps its own bodies")

	nor178RecreateSubject(t, base)

	require.Equal(t, map[NodeID]string{"n": "Peer", "m": "Peer"}, snapshotNodeText(t, oldPeer),
		"row 4: recreation in another database must not change the peer namespace's admitted view")
	require.Equal(t, map[NodeID]string{"n": "Peer", "m": "Peer"}, snapshotNodeText(t, nor178Open(t, peer, "a fresh peer database view")),
		"row 4: a fresh peer snapshot sees the peer namespace only")
	require.Equal(t, map[NodeID]string{"n": "New"}, snapshotNodeText(t, nor178Open(t, subject, "a fresh subject database view")),
		"row 3: a fresh subject snapshot sees the New body only")
	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, oldSubject),
		"row 1: the dropped database's admitted view keeps its bodies across recreation")
}

// TestNOR178OverlappingSnapshotReadersAcrossDrop covers matrix row 5's
// overlapping reader lifetimes: two snapshots admitted on different bodies
// before the DROP each keep their own point, and retiring one does not retire
// the other.
func TestNOR178OverlappingSnapshotReadersAcrossDrop(t *testing.T) {
	for _, factory := range nor178SnapshotStackFactories {
		t.Run(factory.name, func(t *testing.T) {
			base := newReadSnapshotTestEngine(t, false)
			nor178Seed(t, base)
			stack := factory.build(t, base)

			first := nor178Open(t, stack.engine, stack.source)
			require.Equal(t, stack.nodes(nor178StoredNodes()), snapshotNodeText(t, first),
				"the first admitted view is the stored population (%s)", stack.source)

			// A second reader is admitted after one update, so it must keep a
			// different point than the first for the rest of its lifetime.
			require.NoError(t, base.UpdateNode(t.Context(), NodeUpdateRequest{
				After: &Node{ID: "d7:n", Labels: []string{"Doc"}, Properties: map[string]any{"text": "Updated"}},
			}))
			second := nor178Open(t, stack.engine, stack.source)
			require.Equal(t, stack.nodes(nor178UpdatedNodes()), snapshotNodeText(t, second),
				"the second admitted view is the later point (%s)", stack.source)

			pr1667DropPrefix(t, base, nor178SubjectPrefix+":")
			nor178RecreateSubject(t, base)

			require.Equal(t, stack.nodes(nor178StoredNodes()), snapshotNodeText(t, first),
				"row 5: the older overlapping reader keeps its point across DROP and recreation (%s)", stack.source)
			require.Equal(t, stack.nodes(nor178UpdatedNodes()), snapshotNodeText(t, second),
				"row 5: the newer overlapping reader keeps its own point across DROP and recreation (%s)", stack.source)
			require.Equal(t, stack.edges(nor178StoredEdges()), snapshotEdgeNotes(t, second),
				"row 5: the overlapping reader keeps its relationships across DROP and recreation (%s)", stack.source)

			first.Close()
			requireRetired(t, first, "the retired overlapping reader through "+stack.source)
			require.Equal(t, stack.nodes(nor178UpdatedNodes()), snapshotNodeText(t, second),
				"row 5: retiring one overlapping reader must not retire the other (%s)", stack.source)

			second.Close()
			requireRetired(t, second, "the last overlapping reader through "+stack.source)
		})
	}
}

// TestNOR178DropThroughNamespacedEngineKeepsAdmittedSnapshot covers matrix row
// 5 through the database DROP call itself: NamespacedEngine.DeleteByPrefix
// translates the namespace and delegates to the same managed prefix drop, while
// a scoped snapshot of that database is admitted.
func TestNOR178DropThroughNamespacedEngineKeepsAdmittedSnapshot(t *testing.T) {
	base := newReadSnapshotTestEngine(t, false)
	nor178Seed(t, base)
	subject := NewNamespacedEngine(base, nor178SubjectPrefix)
	peer := NewNamespacedEngine(base, nor178PeerPrefix)

	old := nor178Open(t, subject, "the subject database view")
	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, old))

	// The public database DROP: the namespaced view scopes the empty user
	// prefix to this namespace and delegates to the managed prefix drop.
	nodesDeleted, edgesDeleted, err := subject.DeleteByPrefix("")
	require.NoError(t, err, "DROP DATABASE %s through the namespaced engine", nor178SubjectPrefix)
	require.Greater(t, nodesDeleted, int64(0), "DROP DATABASE must delete the namespace's nodes")
	require.Greater(t, edgesDeleted, int64(0), "DROP DATABASE must delete the namespace's relationships")

	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, old),
		"row 5: DROP DATABASE through the namespaced engine must keep the admitted view")
	require.Equal(t, map[EdgeID]string{"r": "EdgeOld"}, snapshotEdgeNotes(t, old),
		"row 5: DROP DATABASE through the namespaced engine must keep the admitted relationships")

	nor178RecreateSubject(t, base)
	require.Equal(t, map[NodeID]string{"n": "Old", "m": "Old"}, snapshotNodeText(t, old),
		"row 1: recreation after that DROP must not replace or omit the old body")
	require.Equal(t, map[NodeID]string{"n": "New"}, snapshotNodeText(t, nor178Open(t, subject, "a fresh subject database view")),
		"row 3: a fresh snapshot of the recreated database sees the New body only")
	require.Equal(t, map[NodeID]string{"n": "Peer", "m": "Peer"}, snapshotNodeText(t, nor178Open(t, peer, "a fresh peer database view")),
		"row 4: the peer database is unaffected by that DROP")
}
