// The retained store owns the graph a caller reads. A value a public APOC
// storage read hands back belongs to that caller: mutating it must leave the
// retained graph, and every later read of it, exactly as it was. A value a
// public write accepts from a caller stays the caller's: retaining it must not
// let the caller change the graph behind the store's back.
//
// These are canonical-owner proofs: each test calls the public method a backend
// consumer calls and then mutates what it received or supplied.
package storage

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// ownershipGraph is one small graph whose properties hold every mutable shape a
// property bag can carry: a scalar, a list and a map, plus labels and a
// relationship with its own list.
//
//	alice -KNOWS-> bob
//	alice -MANAGES-> acme
type ownershipGraph struct {
	store            *InMemoryStorage
	alice, bob, acme int64
	knows, manages   int64
}

func newOwnershipGraph(t *testing.T) *ownershipGraph {
	t.Helper()
	ctx := t.Context()
	store := NewInMemoryStorage()
	alice, err := store.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name":    "Alice",
		"tags":    []interface{}{"first", "second"},
		"profile": map[string]interface{}{"city": "Berlin"},
	})
	require.NoError(t, err)
	bob, err := store.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	acme, err := store.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "Acme"})
	require.NoError(t, err)
	knows, err := store.CreateRelationship(ctx, alice.ID, bob.ID, "KNOWS", map[string]interface{}{
		"since": int64(2020),
		"notes": []interface{}{"met at work"},
	})
	require.NoError(t, err)
	manages, err := store.CreateRelationship(ctx, alice.ID, acme.ID, "MANAGES", map[string]interface{}{
		"since": int64(2021),
	})
	require.NoError(t, err)
	return &ownershipGraph{
		store:   store,
		alice:   alice.ID,
		bob:     bob.ID,
		acme:    acme.ID,
		knows:   knows.ID,
		manages: manages.ID,
	}
}

// mutateEveryShape changes every mutable part of a node value in place: its
// identity, its scalar property, its list, its map and its labels.
func mutateEveryShape(node *Node) {
	node.ID = -1
	node.Properties["name"] = "Mallory"
	node.Properties["injected"] = true
	node.Properties["tags"].([]interface{})[0] = "mutated"
	node.Properties["profile"].(map[string]interface{})["city"] = "Mutated"
	node.Labels[0] = "Mutated"
}

// requireAliceUnchanged requires the retained Alice value to carry exactly the
// state the store was given, whatever a caller did with a value it received.
func (g *ownershipGraph) requireAliceUnchanged(t *testing.T) *Node {
	t.Helper()
	retained, err := g.store.GetNode(t.Context(), g.alice)
	require.NoError(t, err)
	require.Equal(t, g.alice, retained.ID, "a caller cannot renumber a retained node")
	require.Equal(t, []string{"Person"}, retained.Labels, "a caller cannot relabel a retained node")
	require.NotNil(t, retained.Properties, "a caller cannot clear a retained property bag")
	require.Equal(t, "Alice", retained.Properties["name"], "a caller cannot rewrite a retained property")
	require.NotContains(t, retained.Properties, "injected", "a caller cannot add a retained property")
	require.Equal(t, []interface{}{"first", "second"}, retained.Properties["tags"],
		"a caller cannot rewrite a retained list")
	require.Equal(t, map[string]interface{}{"city": "Berlin"}, retained.Properties["profile"],
		"a caller cannot rewrite a retained map")
	return retained
}

// requireKnowsUnchanged requires the retained relationship to carry exactly the
// state the store was given.
func (g *ownershipGraph) requireKnowsUnchanged(t *testing.T) *Relationship {
	t.Helper()
	retained, err := g.store.GetRelationship(t.Context(), g.knows)
	require.NoError(t, err)
	require.Equal(t, g.knows, retained.ID, "a caller cannot renumber a retained relationship")
	require.Equal(t, "KNOWS", retained.Type, "a caller cannot retype a retained relationship")
	require.Equal(t, g.alice, retained.StartNode, "a caller cannot rewire a retained relationship")
	require.Equal(t, g.bob, retained.EndNode, "a caller cannot rewire a retained relationship")
	require.NotNil(t, retained.Properties, "a caller cannot clear a retained relationship property bag")
	require.Equal(t, int64(2020), retained.Properties["since"],
		"a caller cannot rewrite a retained relationship property")
	require.Equal(t, []interface{}{"met at work"}, retained.Properties["notes"],
		"a caller cannot rewrite a retained relationship list")
	return retained
}

// TestGetNodeReturnsCallerOwnedValues: a node read hands out the caller's own
// value, down to its identity, labels, properties and the containers in them.
func TestGetNodeReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	read, err := graph.store.GetNode(t.Context(), graph.alice)
	require.NoError(t, err)

	mutateEveryShape(read)
	delete(read.Properties, "profile")
	read.Labels = nil

	retained := graph.requireAliceUnchanged(t)
	require.NotSame(t, read, retained, "two reads of one node are independent values")
}

// TestGetNodeKeepsANilPropertyBagOwnedByTheStore: an entity without properties
// stays without properties, and assigning a bag on the read value does not
// install one.
func TestGetNodeKeepsANilPropertyBagOwnedByTheStore(t *testing.T) {
	ctx := t.Context()
	store := NewInMemoryStorage()
	created, err := store.CreateNode(ctx, nil, nil)
	require.NoError(t, err)
	createdID := created.ID

	read, err := store.GetNode(ctx, createdID)
	require.NoError(t, err)
	require.Nil(t, read.Properties, "the fixture starts with a nil property bag")
	read.Properties = map[string]interface{}{"injected": true}
	read.Labels = []string{"Injected"}

	retained, err := store.GetNode(ctx, createdID)
	require.NoError(t, err)
	require.Nil(t, retained.Properties, "a caller cannot install a retained property bag")
	require.Nil(t, retained.Labels, "a caller cannot install retained labels")
}

// TestGetRelationshipReturnsCallerOwnedValues: a relationship read hands out the
// caller's own value.
func TestGetRelationshipReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	read, err := graph.store.GetRelationship(t.Context(), graph.knows)
	require.NoError(t, err)

	read.ID = -1
	read.Type = "Mutated"
	read.StartNode = graph.bob
	read.EndNode = graph.alice
	read.Properties["since"] = int64(1999)
	read.Properties["notes"].([]interface{})[0] = "mutated"
	read.Properties["injected"] = true

	retained := graph.requireKnowsUnchanged(t)
	require.NotContains(t, retained.Properties, "injected",
		"a caller cannot add a retained relationship property")
	require.NotSame(t, read, retained, "two reads of one relationship are independent values")
}

// TestCreateNodeKeepsRetainedStateAwayFromTheCaller: the node a create returns
// is the caller's value, and the labels and properties the caller supplied stay
// the caller's too.
func TestCreateNodeKeepsRetainedStateAwayFromTheCaller(t *testing.T) {
	ctx := t.Context()
	store := NewInMemoryStorage()

	labels := []string{"Person"}
	properties := map[string]interface{}{
		"name":    "Alice",
		"tags":    []interface{}{"first", "second"},
		"profile": map[string]interface{}{"city": "Berlin"},
	}
	created, err := store.CreateNode(ctx, labels, properties)
	require.NoError(t, err)
	createdID := created.ID

	mutateEveryShape(created)
	labels[0] = "Mutated"
	properties["name"] = "Supplied"
	properties["tags"].([]interface{})[0] = "mutated"
	properties["profile"].(map[string]interface{})["city"] = "Mutated"

	retained, err := store.GetNode(ctx, createdID)
	require.NoError(t, err)
	require.Equal(t, []string{"Person"}, retained.Labels,
		"the store keeps its own labels, not the slice a caller supplied")
	require.NotNil(t, retained.Properties, "the store keeps a property bag")
	require.Equal(t, "Alice", retained.Properties["name"],
		"the store keeps its own properties, not the map a caller supplied")
	require.Equal(t, []interface{}{"first", "second"}, retained.Properties["tags"],
		"the store keeps its own list, not the list a caller supplied")
	require.Equal(t, map[string]interface{}{"city": "Berlin"}, retained.Properties["profile"],
		"the store keeps its own map, not the map a caller supplied")
}

// TestCreateRelationshipKeepsRetainedStateAwayFromTheCaller is the same
// requirement for a created relationship.
func TestCreateRelationshipKeepsRetainedStateAwayFromTheCaller(t *testing.T) {
	graph := newOwnershipGraph(t)
	ctx := t.Context()

	properties := map[string]interface{}{
		"since": int64(2020),
		"notes": []interface{}{"met at work"},
	}
	created, err := graph.store.CreateRelationship(ctx, graph.bob, graph.acme, "KNOWS", properties)
	require.NoError(t, err)
	createdID := created.ID

	created.Type = "Mutated"
	created.Properties["since"] = int64(1999)
	created.Properties["notes"].([]interface{})[0] = "mutated"
	properties["since"] = int64(1998)
	properties["notes"].([]interface{})[0] = "supplied"

	retained, err := graph.store.GetRelationship(ctx, createdID)
	require.NoError(t, err)
	require.Equal(t, "KNOWS", retained.Type, "the store keeps the type it was given")
	require.NotNil(t, retained.Properties, "the store keeps a property bag")
	require.Equal(t, int64(2020), retained.Properties["since"],
		"the store keeps its own properties, not the map a caller supplied")
	require.Equal(t, []interface{}{"met at work"}, retained.Properties["notes"],
		"the store keeps its own list, not the list a caller supplied")
}

// TestAllNodesReturnsCallerOwnedValues: a whole-graph node read hands out one
// caller-owned value per node and a collection of its own.
func TestAllNodesReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	nodes, err := graph.store.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 3)

	var alice *Node
	for index, node := range nodes {
		if node.ID == graph.alice {
			alice = node
			nodes[index] = nil
		}
	}
	require.NotNil(t, alice)
	mutateEveryShape(alice)

	graph.requireAliceUnchanged(t)
	again, err := graph.store.AllNodes(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{graph.alice, graph.bob, graph.acme}, sortedNodeIDs(again),
		"every node still appears exactly once")
}

// TestAllRelationshipsReturnsCallerOwnedValues is the same requirement for the
// whole-graph relationship read.
func TestAllRelationshipsReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	rels, err := graph.store.AllRelationships(t.Context())
	require.NoError(t, err)
	require.Len(t, rels, 2)

	for index, rel := range rels {
		if rel.ID == graph.knows {
			rel.Type = "Mutated"
			rel.Properties["since"] = int64(1999)
			rel.Properties["notes"].([]interface{})[0] = "mutated"
			rels[index] = nil
		}
	}

	graph.requireKnowsUnchanged(t)
	again, err := graph.store.AllRelationships(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{graph.knows, graph.manages}, sortedRelationshipIDs(again),
		"every relationship still appears exactly once")
}

// TestGetNodeRelationshipsReturnsCallerOwnedValues: a neighborhood relationship
// read hands out caller-owned values and a collection of its own.
func TestGetNodeRelationshipsReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	rels, err := graph.store.GetNodeRelationships(t.Context(), graph.alice, "", DirectionOutgoing)
	require.NoError(t, err)
	require.Len(t, rels, 2)

	for index, rel := range rels {
		if rel.ID == graph.knows {
			rel.Type = "Mutated"
			rel.Properties["since"] = int64(1999)
			rel.Properties["notes"].([]interface{})[0] = "mutated"
			rels[index] = nil
		}
	}

	graph.requireKnowsUnchanged(t)
	again, err := graph.store.GetNodeRelationships(t.Context(), graph.alice, "", DirectionOutgoing)
	require.NoError(t, err)
	require.Equal(t, []int64{graph.knows, graph.manages}, relationshipIDs(again),
		"the neighborhood keeps its order and its entries")
}

// TestGetNodeNeighborsReturnsCallerOwnedValues: a neighbor read hands out
// caller-owned node values.
func TestGetNodeNeighborsReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	neighbors, err := graph.store.GetNodeNeighbors(t.Context(), graph.alice, "", DirectionOutgoing)
	require.NoError(t, err)
	require.Len(t, neighbors, 2)

	for index, neighbor := range neighbors {
		if neighbor.ID == graph.bob {
			neighbor.Properties["name"] = "Mallory"
			neighbor.Properties["injected"] = true
			neighbor.Labels[0] = "Mutated"
			neighbors[index] = nil
		}
	}

	retained, err := graph.store.GetNode(t.Context(), graph.bob)
	require.NoError(t, err)
	require.Equal(t, "Bob", retained.Properties["name"], "a caller cannot rewrite a retained neighbor")
	require.NotContains(t, retained.Properties, "injected", "a caller cannot add a retained neighbor property")
	require.Equal(t, []string{"Person"}, retained.Labels, "a caller cannot relabel a retained neighbor")
	again, err := graph.store.GetNodeNeighbors(t.Context(), graph.alice, "", DirectionOutgoing)
	require.NoError(t, err)
	require.Equal(t, []int64{graph.bob, graph.acme}, nodeIDs(again), "the neighborhood keeps its entries")
}

// TestFindShortestPathReturnsCallerOwnedValues: a path read hands out
// caller-owned nodes, relationships and collections.
func TestFindShortestPathReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	path, err := graph.store.FindShortestPath(t.Context(), graph.alice, graph.bob, "", 2)
	require.NoError(t, err)
	require.Len(t, path.Nodes, 2)
	require.Len(t, path.Relationships, 1)

	mutateEveryShape(path.Nodes[0])
	path.Relationships[0].Type = "Mutated"
	path.Relationships[0].Properties["since"] = int64(1999)
	path.Nodes[1] = nil
	path.Relationships[0] = nil

	graph.requireAliceUnchanged(t)
	graph.requireKnowsUnchanged(t)
}

// TestFindAllPathsReturnsCallerOwnedValues is the same requirement for the
// all-paths read.
func TestFindAllPathsReturnsCallerOwnedValues(t *testing.T) {
	graph := newOwnershipGraph(t)
	paths, err := graph.store.FindAllPaths(t.Context(), graph.alice, graph.bob, "", 2)
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	require.NotEmpty(t, paths[0].Nodes)
	require.NotEmpty(t, paths[0].Relationships)

	mutateEveryShape(paths[0].Nodes[0])
	paths[0].Relationships[0].Type = "Mutated"
	paths[0].Relationships[0].Properties["since"] = int64(1999)

	graph.requireAliceUnchanged(t)
	graph.requireKnowsUnchanged(t)
}

// TestTraversalsHandTheVisitorCallerOwnedValues: a traversal exit is a read
// exit, so mutating the node a visitor receives leaves the retained graph
// exactly as it was.
func TestTraversalsHandTheVisitorCallerOwnedValues(t *testing.T) {
	for name, traverse := range map[string]func(*InMemoryStorage, func(*Node) bool) error{
		"BFS": func(store *InMemoryStorage, visitor func(*Node) bool) error {
			return store.BFS(context.Background(), 1, "", 2, visitor)
		},
		"DFS": func(store *InMemoryStorage, visitor func(*Node) bool) error {
			return store.DFS(context.Background(), 1, "", 2, visitor)
		},
	} {
		t.Run(name, func(t *testing.T) {
			graph := newOwnershipGraph(t)
			visited := 0
			require.NoError(t, traverse(graph.store, func(node *Node) bool {
				visited++
				if node.ID == graph.alice {
					mutateEveryShape(node)
				}
				return true
			}))
			require.NotZero(t, visited, "%s reaches the graph", name)
			graph.requireAliceUnchanged(t)
		})
	}
}

// TestUpdateNodeKeepsRetainedStateAwayFromTheCaller: the properties an explicit
// update accepts stay the caller's, so retaining their containers must not let
// the caller change the graph afterwards.
func TestUpdateNodeKeepsRetainedStateAwayFromTheCaller(t *testing.T) {
	graph := newOwnershipGraph(t)
	ctx := t.Context()
	update := map[string]interface{}{
		"profile": map[string]interface{}{"city": "Paris"},
		"tags":    []interface{}{"updated"},
	}
	require.NoError(t, graph.store.UpdateNode(ctx, graph.bob, PropertyUpdate{Set: update}))

	update["profile"].(map[string]interface{})["city"] = "Mutated"
	update["tags"].([]interface{})[0] = "mutated"

	retained, err := graph.store.GetNode(ctx, graph.bob)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"city": "Paris"}, retained.Properties["profile"],
		"the store keeps the map an explicit update accepted")
	require.Equal(t, []interface{}{"updated"}, retained.Properties["tags"],
		"the store keeps the list an explicit update accepted")
}

// TestUpdateNodeLabelsKeepsRetainedStateAwayFromTheCaller: the label slice an
// explicit label update accepts stays the caller's.
func TestUpdateNodeLabelsKeepsRetainedStateAwayFromTheCaller(t *testing.T) {
	graph := newOwnershipGraph(t)
	ctx := t.Context()
	labels := []string{"Person", "Employee"}
	require.NoError(t, graph.store.UpdateNodeLabels(ctx, graph.bob, labels))

	labels[0] = "Mutated"

	retained, err := graph.store.GetNode(ctx, graph.bob)
	require.NoError(t, err)
	require.Equal(t, []string{"Person", "Employee"}, retained.Labels,
		"the store keeps the labels an explicit update accepted")
}

// TestOwnershipControlsKeepOrdinaryResults is the control set: isolation must
// not change the values an ordinary caller already receives. An explicit update
// still reaches the retained graph, a nil update leaves it alone, traversal
// reads keep their order and their entries, and a missing entity keeps its own
// documented error.
func TestOwnershipControlsKeepOrdinaryResults(t *testing.T) {
	graph := newOwnershipGraph(t)
	ctx := t.Context()

	require.NoError(t, graph.store.UpdateNode(ctx, graph.bob, PropertyUpdate{Set: map[string]interface{}{"age": int64(25)}}))
	bob, err := graph.store.GetNode(ctx, graph.bob)
	require.NoError(t, err)
	require.Equal(t, "Bob", bob.Properties["name"], "an explicit update merges into the retained value")
	require.Equal(t, int64(25), bob.Properties["age"])

	require.NoError(t, graph.store.UpdateNode(ctx, graph.bob, PropertyUpdate{}))
	bob, err = graph.store.GetNode(ctx, graph.bob)
	require.NoError(t, err)
	require.Equal(t, int64(25), bob.Properties["age"], "a zero update changes nothing")

	require.NoError(t, graph.store.AddLabels(ctx, graph.bob, []string{"Employee", "Employee"}))
	bob, err = graph.store.GetNode(ctx, graph.bob)
	require.NoError(t, err)
	require.Equal(t, []string{"Person", "Employee"}, bob.Labels, "a label already present is not repeated")

	rels, err := graph.store.GetNodeRelationships(ctx, graph.alice, "", DirectionOutgoing)
	require.NoError(t, err)
	require.Equal(t, []int64{graph.knows, graph.manages}, relationshipIDs(rels),
		"a traversal read keeps its order and returns each relationship once")

	_, err = graph.store.GetNode(ctx, int64(9999))
	require.ErrorIs(t, err, ErrNodeNotFound, "a missing node keeps its documented error")
	_, err = graph.store.GetRelationship(ctx, int64(9999))
	require.ErrorIs(t, err, ErrRelationshipNotFound, "a missing relationship keeps its documented error")
}

func nodeIDs(nodes []*Node) []int64 {
	ids := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		if node != nil {
			ids = append(ids, node.ID)
		}
	}
	return ids
}

func sortedNodeIDs(nodes []*Node) []int64 {
	ids := nodeIDs(nodes)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func relationshipIDs(rels []*Relationship) []int64 {
	ids := make([]int64, 0, len(rels))
	for _, rel := range rels {
		if rel != nil {
			ids = append(ids, rel.ID)
		}
	}
	return ids
}

func sortedRelationshipIDs(rels []*Relationship) []int64 {
	ids := relationshipIDs(rels)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
