package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scanGuardEngine wraps the server-wide inner engine of a multi-database
// setup and records every whole-server scan (AllNodes / AllEdges) plus every
// edge and node that reaches it through a typed or labelled read. Constraint
// creation in one database must not read any other database's records.
type scanGuardEngine struct {
	Engine
	inner *MemoryEngine

	mu              sync.Mutex
	allEdgesCalls   int
	allNodesCalls   int
	edgesByType     int
	visitedEdgeIDs  []EdgeID
	visitedNodeIDs  []NodeID
	loadedNodeLists int
}

func newScanGuardEngine(inner *MemoryEngine) *scanGuardEngine {
	return &scanGuardEngine{Engine: inner, inner: inner}
}

func (g *scanGuardEngine) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.allEdgesCalls, g.allNodesCalls, g.edgesByType, g.loadedNodeLists = 0, 0, 0, 0
	g.visitedEdgeIDs, g.visitedNodeIDs = nil, nil
}

func (g *scanGuardEngine) recordEdges(edges ...*Edge) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, edge := range edges {
		if edge != nil {
			g.visitedEdgeIDs = append(g.visitedEdgeIDs, edge.ID)
		}
	}
}

func (g *scanGuardEngine) recordNodes(nodes ...*Node) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, node := range nodes {
		if node != nil {
			g.visitedNodeIDs = append(g.visitedNodeIDs, node.ID)
		}
	}
}

func (g *scanGuardEngine) AllEdges() ([]*Edge, error) {
	g.mu.Lock()
	g.allEdgesCalls++
	g.mu.Unlock()
	edges, err := g.inner.AllEdges()
	g.recordEdges(edges...)
	return edges, err
}

func (g *scanGuardEngine) AllNodes() ([]*Node, error) {
	g.mu.Lock()
	g.allNodesCalls++
	g.mu.Unlock()
	nodes, err := g.inner.AllNodes()
	g.recordNodes(nodes...)
	return nodes, err
}

func (g *scanGuardEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	g.mu.Lock()
	g.edgesByType++
	g.mu.Unlock()
	edges, err := g.inner.GetEdgesByType(edgeType)
	g.recordEdges(edges...)
	return edges, err
}

func (g *scanGuardEngine) GetNodesByLabel(label string) ([]*Node, error) {
	g.mu.Lock()
	g.loadedNodeLists++
	g.mu.Unlock()
	nodes, err := g.inner.GetNodesByLabel(label)
	g.recordNodes(nodes...)
	return nodes, err
}

func (g *scanGuardEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	return g.inner.GetSchemaForNamespace(namespace)
}

// ScopedLabelNodeReader forwarding, recording what the scoped reads return.

func (g *scanGuardEngine) GetNodesByLabelInScope(scope, label string) ([]*Node, error) {
	nodes, err := g.inner.GetNodesByLabelInScope(scope, label)
	g.recordNodes(nodes...)
	return nodes, err
}

func (g *scanGuardEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	node, err := g.inner.GetFirstNodeByLabelInScope(scope, label)
	g.recordNodes(node)
	return node, err
}

func (g *scanGuardEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return g.inner.StreamNodesByLabelProjectedInScope(scope, label, properties, func(node *Node) error {
		g.recordNodes(node)
		return visit(node)
	})
}

func (g *scanGuardEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return g.StreamNodesByLabelProjectedInScope("", label, properties, visit)
}

func (g *scanGuardEngine) GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error) {
	return g.inner.GetNodesByLabelVisibleAtInScope(scope, label, version)
}

// Edge-type streaming forwarding (EdgeTypeStreamer / ScopedEdgeTypeStreamer).

func (g *scanGuardEngine) StreamEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	return g.inner.StreamEdgesByTypeInScope(ctx, scope, edgeType, func(edge *Edge) error {
		g.recordEdges(edge)
		return visit(edge)
	})
}

func (g *scanGuardEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return g.StreamEdgesByTypeInScope(ctx, "", edgeType, visit)
}

func (g *scanGuardEngine) foreignEdges(prefix string) []EdgeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []EdgeID
	for _, id := range g.visitedEdgeIDs {
		if strings.HasPrefix(string(id), prefix) {
			out = append(out, id)
		}
	}
	return out
}

func (g *scanGuardEngine) foreignNodes(prefix string) []NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []NodeID
	for _, id := range g.visitedNodeIDs {
		if strings.HasPrefix(string(id), prefix) {
			out = append(out, id)
		}
	}
	return out
}

// populateOtherDatabase fills database "b" with nodes and edges, most of an
// unrelated type and some of the type the constraints in "a" target.
func populateOtherDatabase(t testing.TB, inner Engine, nodes, otherEdges, sameTypeEdges int) {
	t.Helper()
	nsB := NewNamespacedEngine(inner, "b")
	batch := make([]*Node, 0, nodes)
	for i := 0; i < nodes; i++ {
		batch = append(batch, &Node{
			ID:         NodeID(fmt.Sprintf("n%d", i)),
			Labels:     []string{"Person", "User"},
			Properties: map[string]any{"email": "dup@example.com", "uid": i},
		})
	}
	require.NoError(t, nsB.BulkCreateNodes(batch))
	edges := make([]*Edge, 0, otherEdges+sameTypeEdges)
	for i := 0; i < otherEdges+sameTypeEdges; i++ {
		edgeType := "OTHER"
		if i < sameTypeEdges {
			edgeType = "REL"
		}
		edges = append(edges, &Edge{
			ID:         EdgeID(fmt.Sprintf("e%d", i)),
			StartNode:  NodeID(fmt.Sprintf("n%d", i%nodes)),
			EndNode:    "n0",
			Type:       edgeType,
			Properties: map[string]any{"k": "dup", "from": "2020-01-01T00:00:00Z", "to": "2030-01-01T00:00:00Z", "v": "nope"},
		})
	}
	require.NoError(t, nsB.BulkCreateEdges(edges))
}

func relationshipConstraintKinds() []Constraint {
	return []Constraint{
		{Name: "u", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"}},
		{Name: "ex", Type: ConstraintExists, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"}},
		{Name: "key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"}},
		{Name: "tmp", Type: ConstraintTemporal, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k", "from", "to"}},
		{Name: "dom", Type: ConstraintDomain, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"v"}, AllowedValues: []interface{}{"ok"}},
		{Name: "card", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "REL", Direction: "INCOMING", MaxCount: 1},
		{Name: "allow", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL", PolicyMode: "ALLOWED", SourceLabel: "A", TargetLabel: "A"},
		{Name: "deny", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "REL", PolicyMode: "DISALLOWED", SourceLabel: "Person", TargetLabel: "Person"},
	}
}

// TestConstraintCreation_DoesNotScanOtherDatabases is the regression for the
// constraint-DDL slowdown on a server holding large databases: creating a
// relationship constraint scanned AllEdges of every database, and the UNIQUE
// value refresh scanned AllNodes of every database.
func TestConstraintCreation_DoesNotScanOtherDatabases(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	populateOtherDatabase(t, inner, 50, 200, 20)

	guard := newScanGuardEngine(inner)
	nsA := NewNamespacedEngine(guard, "a")
	for _, id := range []NodeID{"x", "y"} {
		_, err := nsA.CreateNode(&Node{ID: id, Labels: []string{"A"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, nsA.CreateEdge(&Edge{ID: "r1", StartNode: "x", EndNode: "y", Type: "REL",
		Properties: map[string]any{"k": "one", "from": "2020-01-01T00:00:00Z", "to": "2021-01-01T00:00:00Z", "v": "ok"}}))

	for _, c := range relationshipConstraintKinds() {
		t.Run(string(c.Type)+"_"+c.Name, func(t *testing.T) {
			guard.reset()
			require.NoError(t, ValidateConstraintOnCreationForEngine(nsA, c))
			require.Zero(t, guard.allEdgesCalls, "relationship constraint creation must not scan AllEdges")
			require.Zero(t, guard.allNodesCalls, "relationship constraint creation must not scan AllNodes")
			require.Zero(t, guard.edgesByType, "relationship constraint creation must stream, not load GetEdgesByType")
			require.Empty(t, guard.foreignEdges("b:"), "edges of database b must not be read")
		})
	}

	t.Run("relationship property type", func(t *testing.T) {
		guard.reset()
		require.NoError(t, ValidatePropertyTypeConstraintOnCreationForEngine(nsA, PropertyTypeConstraint{
			Name: "ptc", EntityType: ConstraintEntityRelationship, Label: "REL", Property: "k", ExpectedType: PropertyTypeString,
		}))
		require.Zero(t, guard.allEdgesCalls)
		require.Zero(t, guard.edgesByType)
		require.Empty(t, guard.foreignEdges("b:"))
	})

	t.Run("unique node constraint refresh", func(t *testing.T) {
		schema := nsA.GetSchema()
		require.NoError(t, schema.AddConstraint(Constraint{Name: "uniq_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
		guard.reset()
		require.NoError(t, RefreshUniqueConstraintValuesForEngine(nsA, schema))
		require.Zero(t, guard.allNodesCalls, "UNIQUE refresh must not scan AllNodes")
		require.Empty(t, guard.foreignNodes("b:"), "nodes of database b must not be read")
	})
}

// seedRelDatabase creates nodes x (label A), y (label A), p, q (label
// Person) and the given edges in database ns.
func seedRelDatabase(t *testing.T, ns *NamespacedEngine, edges ...*Edge) {
	t.Helper()
	for id, labels := range map[NodeID][]string{"x": {"A"}, "y": {"A"}, "p": {"Person"}, "q": {"Person"}} {
		_, err := ns.CreateNode(&Node{ID: id, Labels: labels, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	for _, edge := range edges {
		require.NoError(t, ns.CreateEdge(edge))
	}
}

func relEdge(id, start, end, edgeType string, props map[string]any) *Edge {
	return &Edge{ID: EdgeID(id), StartNode: NodeID(start), EndNode: NodeID(end), Type: edgeType, Properties: props}
}

// violatingEdges returns, per constraint name in relationshipConstraintKinds,
// edges of the given type that violate it.
func violatingEdges(edgeType string) map[string][]*Edge {
	period := func(k string) map[string]any {
		return map[string]any{"k": k, "from": "2020-01-01T00:00:00Z", "to": "2030-01-01T00:00:00Z", "v": "ok"}
	}
	return map[string][]*Edge{
		"u":     {relEdge("v1", "x", "y", edgeType, period("same")), relEdge("v2", "y", "x", edgeType, period("same"))},
		"ex":    {relEdge("v1", "x", "y", edgeType, map[string]any{"v": "ok"})},
		"key":   {relEdge("v1", "x", "y", edgeType, period("same")), relEdge("v2", "y", "x", edgeType, period("same"))},
		"tmp":   {relEdge("v1", "x", "y", edgeType, period("same")), relEdge("v2", "y", "x", edgeType, period("same"))},
		"dom":   {relEdge("v1", "x", "y", edgeType, map[string]any{"v": "bad"})},
		"card":  {relEdge("v1", "x", "y", edgeType, period("a")), relEdge("v2", "p", "y", edgeType, period("b"))},
		"allow": {relEdge("v1", "p", "q", edgeType, period("a"))},
		"deny":  {relEdge("v1", "p", "q", edgeType, period("a"))},
	}
}

// TestRelationshipConstraintCreation_StreamsOnlyTypeAndDatabase checks that
// every relationship constraint kind still finds violations among the
// existing edges of the constrained type in the constraint's database, and
// ignores violating edges of other types and of other databases.
func TestRelationshipConstraintCreation_StreamsOnlyTypeAndDatabase(t *testing.T) {
	for _, c := range relationshipConstraintKinds() {
		t.Run(c.Name, func(t *testing.T) {
			inner := NewMemoryEngine()
			t.Cleanup(func() { _ = inner.Close() })

			// Database b holds violating REL edges; database a holds
			// violating edges of another type only.
			seedRelDatabase(t, NewNamespacedEngine(inner, "b"), violatingEdges("REL")[c.Name]...)
			nsA := NewNamespacedEngine(inner, "a")
			seedRelDatabase(t, nsA, violatingEdges("OTHER")[c.Name]...)
			require.NoError(t, ValidateConstraintOnCreationForEngine(nsA, c),
				"violations of other types / other databases must be ignored")

			// The same violation in database a's REL edges is reported.
			nsC := NewNamespacedEngine(inner, "c")
			seedRelDatabase(t, nsC, violatingEdges("REL")[c.Name]...)
			err := ValidateConstraintOnCreationForEngine(nsC, c)
			require.Error(t, err)
			var cve *ConstraintViolationError
			require.ErrorAs(t, err, &cve)
			require.Equal(t, "REL", cve.Label)
			// User-facing IDs: the namespace prefix never leaks into messages.
			require.NotContains(t, err.Error(), "c:")
		})
	}
}

func TestRelationshipKeyCreation_ReportsMissingPropertyBeforeDuplicate(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ns := NewNamespacedEngine(inner, "a")
	// A duplicate key first in index order, a missing key property later:
	// the missing property wins, as with the former existence-then-unique scans.
	seedRelDatabase(t, ns,
		relEdge("e1", "x", "y", "REL", map[string]any{"k": "dup"}),
		relEdge("e2", "y", "x", "REL", map[string]any{"k": "dup"}),
		relEdge("e3", "x", "x", "REL", map[string]any{}),
	)
	err := ValidateConstraintOnCreationForEngine(ns, Constraint{Name: "key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required property")
}

func TestRelationshipPropertyTypeCreation_StreamsOnlyTypeAndDatabase(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ptc := PropertyTypeConstraint{Name: "ptc", EntityType: ConstraintEntityRelationship, Label: "REL", Property: "k", ExpectedType: PropertyTypeString}
	bad := map[string]any{"k": 42}
	seedRelDatabase(t, NewNamespacedEngine(inner, "b"), relEdge("e1", "x", "y", "REL", bad))
	nsA := NewNamespacedEngine(inner, "a")
	seedRelDatabase(t, nsA, relEdge("e1", "x", "y", "OTHER", bad), relEdge("e2", "x", "y", "REL", map[string]any{"k": "s"}))
	require.NoError(t, ValidatePropertyTypeConstraintOnCreationForEngine(nsA, ptc))
	require.NoError(t, nsA.CreateEdge(relEdge("e3", "y", "x", "REL", bad)))
	require.Error(t, ValidatePropertyTypeConstraintOnCreationForEngine(nsA, ptc))
}

func TestBadgerValidateRelationshipConstraint_StreamsType(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	for _, id := range []NodeID{"t:x", "t:y"} {
		_, err := e.CreateNode(&Node{ID: id, Labels: []string{"A"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(relEdge("t:o1", "t:x", "t:y", "OTHER", map[string]any{"k": "dup"})))
	require.NoError(t, e.CreateEdge(relEdge("t:o2", "t:y", "t:x", "OTHER", map[string]any{})))
	require.NoError(t, e.CreateEdge(relEdge("t:r1", "t:x", "t:y", "REL", map[string]any{"k": "dup"})))

	unique := RelationshipConstraint{Name: "u", Type: ConstraintUnique, RelType: "REL", Properties: []string{"k"}}
	exists := RelationshipConstraint{Name: "e", Type: ConstraintExists, RelType: "REL", Properties: []string{"k"}}
	require.NoError(t, e.ValidateRelationshipConstraint(unique))
	require.NoError(t, e.ValidateRelationshipConstraint(exists))

	require.NoError(t, e.CreateEdge(relEdge("t:r2", "t:y", "t:x", "REL", map[string]any{"k": "dup"})))
	require.ErrorContains(t, e.ValidateRelationshipConstraint(unique), "r1")
	require.NoError(t, e.CreateEdge(relEdge("t:r3", "t:y", "t:y", "REL", map[string]any{})))
	require.ErrorContains(t, e.ValidateRelationshipConstraint(exists), "r3")
}

func collectEdgeIDs(t *testing.T, engine Engine, edgeType string) []string {
	t.Helper()
	var ids []string
	require.NoError(t, StreamEdgesByType(context.Background(), engine, edgeType, func(edge *Edge) error {
		ids = append(ids, string(edge.ID))
		return nil
	}))
	sort.Strings(ids)
	return ids
}

func TestStreamEdgesByType_NamespacedReturnsUserFacingEdgesOfOneDatabase(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	seedRelDatabase(t, NewNamespacedEngine(inner, "b"), relEdge("e1", "x", "y", "REL", nil))
	nsA := NewNamespacedEngine(inner, "a")
	seedRelDatabase(t, nsA, relEdge("e1", "x", "y", "REL", nil), relEdge("e2", "p", "q", "OTHER", nil))

	var got []*Edge
	require.NoError(t, StreamEdgesByType(context.Background(), nsA, "REL", func(edge *Edge) error {
		got = append(got, edge)
		return nil
	}))
	require.Len(t, got, 1)
	require.Equal(t, EdgeID("e1"), got[0].ID)
	require.Equal(t, NodeID("x"), got[0].StartNode)
	require.Equal(t, NodeID("y"), got[0].EndNode)
	require.Equal(t, []string{"a:e1", "b:e1"}, collectEdgeIDs(t, inner, "REL"))
	require.Empty(t, collectEdgeIDs(t, nsA, "MISSING"))
}

func TestStreamEdgesByType_StopsOnVisitorErrorAndContext(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	seedRelDatabase(t, NewNamespacedEngine(e, "a"), relEdge("e1", "x", "y", "REL", nil), relEdge("e2", "y", "x", "REL", nil))

	stop := errors.New("stop")
	visits := 0
	err := StreamEdgesByType(context.Background(), e, "REL", func(*Edge) error { visits++; return stop })
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, visits)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, StreamEdgesByType(ctx, e, "REL", func(*Edge) error { return nil }), context.Canceled)
	require.ErrorIs(t, StreamEdgesByType(ctx, e, "REL", nil), ErrInvalidData)
}

func TestStreamEdgesByType_AsyncEngineMergesPendingWrites(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	for _, id := range []NodeID{"a:x", "a:y"} {
		_, err := ae.CreateNode(&Node{ID: id, Labels: []string{"A"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, ae.CreateEdge(relEdge("a:flushed", "a:x", "a:y", "REL", map[string]any{"k": "old"})))
	require.NoError(t, ae.CreateEdge(relEdge("a:gone", "a:x", "a:y", "REL", nil)))
	require.NoError(t, ae.Flush())

	// Unflushed: a create, an update and a delete.
	require.NoError(t, ae.CreateEdge(relEdge("a:pending", "a:y", "a:x", "REL", nil)))
	require.NoError(t, ae.CreateEdge(relEdge("a:other", "a:y", "a:x", "OTHER", nil)))
	require.NoError(t, ae.UpdateEdge(relEdge("a:flushed", "a:x", "a:y", "REL", map[string]any{"k": "new"})))
	require.NoError(t, ae.DeleteEdge("a:gone"))

	seen := map[EdgeID]*Edge{}
	require.NoError(t, StreamEdgesByType(context.Background(), ae, "REL", func(edge *Edge) error {
		_, dup := seen[edge.ID]
		require.False(t, dup, "edge %s visited twice", edge.ID)
		seen[edge.ID] = edge
		return nil
	}))
	require.Len(t, seen, 2)
	require.Contains(t, seen, EdgeID("a:pending"), "pending (unflushed) edge must be streamed")
	require.NotContains(t, seen, EdgeID("a:gone"), "pending delete must hide the flushed edge")
	require.Equal(t, "new", seen["a:flushed"].Properties["k"], "pending update must shadow the flushed edge")

	// The same through a namespaced view over the async engine.
	require.Equal(t, []string{"flushed", "pending"}, collectEdgeIDs(t, NewNamespacedEngine(ae, "a"), "REL"))
	require.Empty(t, collectEdgeIDs(t, NewNamespacedEngine(ae, "b"), "REL"))
}

func TestStreamEdgesByType_TransactionMergesPendingWrites(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	seedRelDatabase(t, NewNamespacedEngine(e, "a"),
		relEdge("keep", "x", "y", "REL", map[string]any{"k": "old"}),
		relEdge("drop", "x", "y", "REL", nil),
	)
	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.CreateEdge(relEdge("a:new", "a:y", "a:x", "REL", nil)))
	require.NoError(t, tx.UpdateEdge(relEdge("a:keep", "a:x", "a:y", "REL", map[string]any{"k": "new"})))
	require.NoError(t, tx.DeleteEdge("a:drop"))

	seen := map[EdgeID]*Edge{}
	require.NoError(t, tx.StreamEdgesByType(context.Background(), "REL", func(edge *Edge) error {
		seen[edge.ID] = edge
		return nil
	}))
	require.Len(t, seen, 2)
	require.Contains(t, seen, EdgeID("a:new"))
	require.Equal(t, "new", seen["a:keep"].Properties["k"])

	sliced, err := tx.GetEdgesByType("REL")
	require.NoError(t, err)
	require.Len(t, sliced, len(seen), "stream and GetEdgesByType must agree")
}

// edgeTypeOnlyEngine hides every optional interface of the wrapped engine.
type edgeTypeOnlyEngine struct {
	Engine
	allEdges, allNodes, byType, byLabel int
}

func (e *edgeTypeOnlyEngine) AllEdges() ([]*Edge, error) { e.allEdges++; return e.Engine.AllEdges() }
func (e *edgeTypeOnlyEngine) AllNodes() ([]*Node, error) { e.allNodes++; return e.Engine.AllNodes() }
func (e *edgeTypeOnlyEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	e.byType++
	return e.Engine.GetEdgesByType(edgeType)
}
func (e *edgeTypeOnlyEngine) GetNodesByLabel(label string) ([]*Node, error) {
	e.byLabel++
	return e.Engine.GetNodesByLabel(label)
}

func TestConstraintCreation_FallbackForEnginesWithoutStreaming(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	plain := &edgeTypeOnlyEngine{Engine: inner}
	seedRelDatabase(t, NewNamespacedEngine(inner, "a"),
		relEdge("e1", "x", "y", "REL", map[string]any{"k": "dup"}),
		relEdge("e2", "y", "x", "REL", map[string]any{"k": "dup"}),
	)

	err := ValidateConstraintOnCreationForEngine(plain, Constraint{Name: "u", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"}})
	require.Error(t, err)
	require.Equal(t, 1, plain.byType, "fallback reads the one relationship type")
	require.Zero(t, plain.allEdges, "fallback must never scan AllEdges")

	schema := inner.GetSchema()
	require.NoError(t, schema.AddConstraint(Constraint{Name: "uq", Type: ConstraintUnique, Label: "A", Properties: []string{"k"}}))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(plain, schema))
	require.Equal(t, 1, plain.byLabel, "fallback reads the constrained label")
	require.Zero(t, plain.allNodes, "fallback must never scan AllNodes")
}

// legacyRefreshUniqueValues is the former AllNodes-based rebuild, kept here
// as the reference the streamed rebuild must match.
func legacyRefreshUniqueValues(t *testing.T, engine Engine, schema *SchemaManager) map[string]map[interface{}]NodeID {
	t.Helper()
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	out := map[string]map[interface{}]NodeID{}
	schema.mu.RLock()
	defer schema.mu.RUnlock()
	for key := range schema.uniqueConstraints {
		out[key] = map[interface{}]NodeID{}
	}
	for _, node := range nodes {
		for _, label := range node.Labels {
			for prop, value := range node.Properties {
				values, ok := out[label+":"+prop]
				if !ok {
					continue
				}
				if valueKey, ok := indexValueKey(value); ok {
					values[valueKey] = EnsureNodeIDDatabasePrefixForEngine(engine, node.ID)
				}
			}
		}
	}
	return out
}

func uniqueCacheSnapshot(schema *SchemaManager) map[string]map[interface{}]NodeID {
	schema.mu.RLock()
	defer schema.mu.RUnlock()
	out := map[string]map[interface{}]NodeID{}
	for key, uc := range schema.uniqueConstraints {
		uc.mu.RLock()
		values := make(map[interface{}]NodeID, len(uc.values))
		for k, v := range uc.values {
			values[k] = v
		}
		if !uc.valuesCacheComplete {
			panic("unique cache " + key + " not marked complete")
		}
		uc.mu.RUnlock()
		out[key] = values
	}
	return out
}

func TestRefreshUniqueConstraintValues_StreamedMatchesFullScan(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	populateOtherDatabase(t, inner, 20, 0, 0)
	nsA := NewNamespacedEngine(inner, "a")
	for i, n := range []*Node{
		{ID: "u1", Labels: []string{"User", "Admin"}, Properties: map[string]any{"email": "a@x", "uid": int64(1), "name": "a"}},
		{ID: "u2", Labels: []string{"User"}, Properties: map[string]any{"email": "b@x", "uid": int64(2)}},
		{ID: "u3", Labels: []string{"User"}, Properties: map[string]any{"name": "no email"}},
		{ID: "d1", Labels: []string{"Admin"}, Properties: map[string]any{"uid": int64(3), "email": "a@x"}},
		{ID: "o1", Labels: []string{"Other"}, Properties: map[string]any{"email": "a@x"}},
	} {
		_, err := nsA.CreateNode(n)
		require.NoError(t, err, "node %d", i)
	}
	schema := nsA.GetSchema()
	for _, c := range []Constraint{
		{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}},
		{Name: "u_uid", Type: ConstraintUnique, Label: "User", Properties: []string{"uid"}},
		{Name: "a_uid", Type: ConstraintUnique, Label: "Admin", Properties: []string{"uid"}},
	} {
		require.NoError(t, schema.AddConstraint(c))
	}

	require.NoError(t, RefreshUniqueConstraintValuesForEngine(nsA, schema))
	got := uniqueCacheSnapshot(schema)
	require.Equal(t, legacyRefreshUniqueValues(t, nsA, schema), got)
	require.Equal(t, NodeID("a:u1"), got["User:email"]["a@x"])
	require.Len(t, got["Admin:uid"], 2)

	// A duplicate among constrained values is still reported.
	nsDup := NewNamespacedEngine(inner, "dup")
	for _, id := range []NodeID{"d1", "d2"} {
		_, err := nsDup.CreateNode(&Node{ID: id, Labels: []string{"User"}, Properties: map[string]any{"email": "same@x"}})
		require.NoError(t, err)
	}
	dupSchema := nsDup.GetSchema()
	require.NoError(t, dupSchema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
	require.ErrorContains(t, RefreshUniqueConstraintValuesForEngine(nsDup, dupSchema), "refresh unique constraint values")
}

func TestStreamEdgesByType_CompositeStreamsEachConstituent(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	nsA := NewNamespacedEngine(inner, "a")
	nsB := NewNamespacedEngine(inner, "b")
	seedRelDatabase(t, nsA, relEdge("ea", "x", "y", "REL", nil))
	seedRelDatabase(t, nsB, relEdge("eb", "x", "y", "REL", nil), relEdge("ob", "x", "y", "OTHER", nil))
	composite := NewCompositeEngine(
		map[string]Engine{"a": nsA, "b": nsB},
		map[string]string{"a": "a", "b": "b"},
		map[string]string{"a": "read", "b": "read"},
	)
	require.Equal(t, []string{"ea", "eb"}, collectEdgeIDs(t, composite, "REL"))
	sliced, err := composite.GetEdgesByType("REL")
	require.NoError(t, err)
	require.Len(t, sliced, 2)
}
