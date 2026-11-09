// The warmup cache owns the graph it retains. A value a cached getter hands
// back belongs to that caller: mutating it must leave the cache, and every
// later read of it, exactly as it was. A value a caller supplies to a warmup
// helper that retains it stays the caller's: retaining it must not let the
// caller change the cache behind the warmup's back.
//
// These are canonical-owner proofs: each test warms the cache through the
// public helper, then mutates what it received or supplied.
package warmup

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// ownershipWarmupFixture is one small graph whose properties hold every mutable
// shape a property bag can carry, bound to a warmup instance of its own.
//
//	alice(1) -WORKS_AT-> acme(3)
//	bob(2)   -WORKS_AT-> acme(3)
//	alice(1) -KNOWS->    bob(2)
type ownershipWarmupFixture struct {
	functions    *Functions
	backend      *storage.InMemoryStorage
	alice, bob   int64
	acme         int64
	worksAtAlice int64
	knows        int64
}

func newOwnershipWarmupFixture(t *testing.T) *ownershipWarmupFixture {
	t.Helper()
	ctx := t.Context()
	backend := storage.NewInMemoryStorage()
	alice, err := backend.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name":    "Alice",
		"tags":    []interface{}{"first", "second"},
		"profile": map[string]interface{}{"city": "Berlin"},
	})
	require.NoError(t, err)
	bob, err := backend.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	acme, err := backend.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "Acme"})
	require.NoError(t, err)
	worksAtAlice, err := backend.CreateRelationship(ctx, alice.ID, acme.ID, "WORKS_AT", map[string]interface{}{
		"since": int64(2020),
		"roles": []interface{}{"engineer"},
	})
	require.NoError(t, err)
	_, err = backend.CreateRelationship(ctx, bob.ID, acme.ID, "WORKS_AT", map[string]interface{}{"since": int64(2021)})
	require.NoError(t, err)
	knows, err := backend.CreateRelationship(ctx, alice.ID, bob.ID, "KNOWS", map[string]interface{}{"since": int64(2019)})
	require.NoError(t, err)
	return &ownershipWarmupFixture{
		functions:    New(backend),
		backend:      backend,
		alice:        alice.ID,
		bob:          bob.ID,
		acme:         acme.ID,
		worksAtAlice: worksAtAlice.ID,
		knows:        knows.ID,
	}
}

// warm runs the full warmup and requires the cache to hold the whole fixture
// graph: three nodes and three relationships.
func (f *ownershipWarmupFixture) warm(t *testing.T) {
	t.Helper()
	result, err := f.functions.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, result["nodesLoaded"], "the warmup retains every node")
	require.Equal(t, 3, result["relationshipsLoaded"], "the warmup retains every relationship")
}

// TestGetCachedNodeReturnsCallerOwnedValues: a cached node read hands out the
// caller's own value, and the hit counter still counts every read.
func TestGetCachedNodeReturnsCallerOwnedValues(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	cached, ok := fixture.functions.GetCachedNode(fixture.alice)
	require.True(t, ok)
	cached.ID = -1
	cached.Properties["name"] = "Mallory"
	cached.Properties["injected"] = true
	cached.Properties["tags"].([]interface{})[0] = "mutated"
	cached.Properties["profile"].(map[string]interface{})["city"] = "Mutated"
	delete(cached.Properties, "profile")
	cached.Labels[0] = "Mutated"

	again, ok := fixture.functions.GetCachedNode(fixture.alice)
	require.True(t, ok)
	require.Equal(t, fixture.alice, again.ID, "a caller cannot renumber a cached node")
	require.Equal(t, []string{"Person"}, again.Labels, "a caller cannot relabel a cached node")
	require.Equal(t, "Alice", again.Properties["name"], "a caller cannot rewrite a cached property")
	require.NotContains(t, again.Properties, "injected", "a caller cannot add a cached property")
	require.Equal(t, []interface{}{"first", "second"}, again.Properties["tags"],
		"a caller cannot rewrite a cached list")
	require.Equal(t, map[string]interface{}{"city": "Berlin"}, again.Properties["profile"],
		"a caller cannot rewrite a cached map")
	require.NotSame(t, cached, again, "two cached reads of one node are independent values")
	require.Equal(t, int64(2), fixture.functions.Stats()["cacheHits"], "both reads remain cache hits")
}

// TestGetCachedRelationshipReturnsCallerOwnedValues: a cached relationship read
// hands out the caller's own value.
func TestGetCachedRelationshipReturnsCallerOwnedValues(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	cached, ok := fixture.functions.GetCachedRelationship(fixture.worksAtAlice)
	require.True(t, ok)
	cached.Type = "Mutated"
	cached.StartNode = fixture.bob
	cached.Properties["since"] = int64(1999)
	cached.Properties["roles"].([]interface{})[0] = "mutated"

	again, ok := fixture.functions.GetCachedRelationship(fixture.worksAtAlice)
	require.True(t, ok)
	require.Equal(t, "WORKS_AT", again.Type, "a caller cannot retype a cached relationship")
	require.Equal(t, fixture.alice, again.StartNode, "a caller cannot rewire a cached relationship")
	require.Equal(t, int64(2020), again.Properties["since"],
		"a caller cannot rewrite a cached relationship property")
	require.Equal(t, []interface{}{"engineer"}, again.Properties["roles"],
		"a caller cannot rewrite a cached relationship list")
	require.NotSame(t, cached, again, "two cached reads of one relationship are independent values")
}

// TestGetNodesByLabelReturnsCallerOwnedValues: a label index read hands out
// caller-owned node values and a collection of its own, and the index keeps its
// order across reads.
func TestGetNodesByLabelReturnsCallerOwnedValues(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	nodes, ok := fixture.functions.GetNodesByLabel("Person")
	require.True(t, ok)
	require.Len(t, nodes, 2)
	order := cachedNodeIDs(nodes)

	nodes[0].Properties["name"] = "Mallory"
	nodes[0].Labels[0] = "Mutated"
	nodes[1] = nil

	again, ok := fixture.functions.GetNodesByLabel("Person")
	require.True(t, ok)
	require.Len(t, again, 2, "the label index keeps every entry")
	require.Equal(t, order, cachedNodeIDs(again), "the label index keeps its order")
	for _, node := range again {
		require.NotNil(t, node)
		require.NotEqual(t, "Mallory", node.Properties["name"], "a caller cannot rewrite a cached label entry")
		require.Equal(t, []string{"Person"}, node.Labels, "a caller cannot relabel a cached label entry")
	}
}

// TestGetRelationshipsByType is the same requirement
// for the relationship type index.
func TestGetRelationshipsByType(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	missing, found := fixture.functions.GetRelationshipsByType("NonExistent")
	require.False(t, found)
	require.Nil(t, missing)

	rels, ok := fixture.functions.GetRelationshipsByType("WORKS_AT")
	require.True(t, ok)
	require.Len(t, rels, 2)
	order := cachedRelationshipIDs(rels)

	rels[0].Type = "Mutated"
	rels[0].Properties["since"] = int64(1999)
	rels[1] = nil

	again, ok := fixture.functions.GetRelationshipsByType("WORKS_AT")
	require.True(t, ok)
	require.Len(t, again, 2, "the type index keeps every entry")
	require.Equal(t, order, cachedRelationshipIDs(again), "the type index keeps its order")
	for _, rel := range again {
		require.NotNil(t, rel)
		require.Equal(t, "WORKS_AT", rel.Type, "a caller cannot retype a cached type entry")
		require.NotEqual(t, int64(1999), rel.Properties["since"],
			"a caller cannot rewrite a cached type entry property")
	}
}

// TestPathRetainsCallerOwnedCopiesOfItsInputs: the values a caller supplies to
// the path helper are retained by the cache, so they must be the cache's own.
func TestPathRetainsCallerOwnedCopiesOfItsInputs(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	node := &Node{
		ID:     int64(42),
		Labels: []string{"Synthetic"},
		Properties: map[string]interface{}{
			"name": "Supplied",
			"tags": []interface{}{"first"},
		},
	}
	rel := &Relationship{
		ID:         int64(43),
		Type:       "SUPPLIED",
		StartNode:  int64(42),
		EndNode:    int64(42),
		Properties: map[string]interface{}{"since": int64(2024)},
	}
	result := fixture.functions.Path([]*Node{node}, []*Relationship{rel})
	require.Equal(t, 1, result["nodesLoaded"])
	require.Equal(t, 1, result["relationshipsLoaded"])

	node.Properties["name"] = "Mutated"
	node.Properties["tags"].([]interface{})[0] = "mutated"
	node.Labels[0] = "Mutated"
	node.ID = -1
	rel.Type = "Mutated"
	rel.Properties["since"] = int64(1999)
	rel.ID = -1

	cachedNode, ok := fixture.functions.GetCachedNode(int64(42))
	require.True(t, ok, "the path helper retains the node it was given")
	require.Equal(t, int64(42), cachedNode.ID, "a caller cannot renumber a retained path node")
	require.Equal(t, []string{"Synthetic"}, cachedNode.Labels, "a caller cannot relabel a retained path node")
	require.Equal(t, "Supplied", cachedNode.Properties["name"],
		"a caller cannot rewrite a retained path node property")
	require.Equal(t, []interface{}{"first"}, cachedNode.Properties["tags"],
		"a caller cannot rewrite a retained path node list")
	cachedRel, ok := fixture.functions.GetCachedRelationship(int64(43))
	require.True(t, ok, "the path helper retains the relationship it was given")
	require.Equal(t, int64(43), cachedRel.ID, "a caller cannot renumber a retained path relationship")
	require.Equal(t, "SUPPLIED", cachedRel.Type, "a caller cannot retype a retained path relationship")
	require.Equal(t, int64(2024), cachedRel.Properties["since"],
		"a caller cannot rewrite a retained path relationship property")
}

// TestSubgraphRetainsCallerOwnedCopyOfItsStartNode: the start node a caller
// supplies to the subgraph helper is retained by the cache, so it must be the
// cache's own value.
func TestSubgraphRetainsCallerOwnedCopyOfItsStartNode(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	start, err := fixture.backend.GetNode(t.Context(), fixture.alice)
	require.NoError(t, err)

	result, err := fixture.functions.Subgraph(context.Background(), start, 1)
	require.NoError(t, err)
	require.Equal(t, 3, result["nodesLoaded"], "the subgraph retains the start node and its neighbors")
	require.Equal(t, 2, result["relationshipsLoaded"], "the subgraph retains the relationships it read")

	start.Properties["name"] = "Mutated"
	start.Properties["injected"] = true
	start.Properties["tags"].([]interface{})[0] = "mutated"
	start.Labels[0] = "Mutated"
	start.ID = -1

	cached, ok := fixture.functions.GetCachedNode(fixture.alice)
	require.True(t, ok, "the subgraph helper retains its start node")
	require.Equal(t, fixture.alice, cached.ID, "a caller cannot renumber a retained subgraph node")
	require.Equal(t, []string{"Person"}, cached.Labels, "a caller cannot relabel a retained subgraph node")
	require.Equal(t, "Alice", cached.Properties["name"],
		"a caller cannot rewrite a retained subgraph node property")
	require.NotContains(t, cached.Properties, "injected",
		"a caller cannot add a retained subgraph node property")
	require.Equal(t, []interface{}{"first", "second"}, cached.Properties["tags"],
		"a caller cannot rewrite a retained subgraph node list")
}

// TestWarmupRunKeepsSeeingExplicitBackendUpdates is the control: a fresh
// warmup reflects an explicit backend update exactly as it always did.
func TestWarmupRunKeepsSeeingExplicitBackendUpdates(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	fixture.warm(t)

	require.NoError(t, fixture.backend.UpdateNode(t.Context(), fixture.alice, storage.PropertyUpdate{
		Set: map[string]interface{}{
			"name": "Alicia",
			"age":  int64(31),
		},
	}))
	_, err := fixture.functions.Run(t.Context())
	require.NoError(t, err)

	cached, ok := fixture.functions.GetCachedNode(fixture.alice)
	require.True(t, ok)
	require.Equal(t, "Alicia", cached.Properties["name"],
		"a fresh warmup reads the value an explicit update left in the backend")
	require.Equal(t, int64(31), cached.Properties["age"])
}

func cachedNodeIDs(nodes []*Node) []int64 {
	ids := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		if node != nil {
			ids = append(ids, node.ID)
		}
	}
	return ids
}

func cachedRelationshipIDs(rels []*Relationship) []int64 {
	ids := make([]int64, 0, len(rels))
	for _, rel := range rels {
		if rel != nil {
			ids = append(ids, rel.ID)
		}
	}
	return ids
}
