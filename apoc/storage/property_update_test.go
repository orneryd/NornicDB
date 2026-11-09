package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// propertyUpdateFixture owns one node and one relationship carrying the same
// property bag, plus one entity of each kind whose bag is absent, so every patch
// proof runs against both APOC storage entity kinds.
type propertyUpdateFixture struct {
	store    *InMemoryStorage
	node     int64
	rel      int64
	bareNode int64
	bareRel  int64
}

func newPropertyUpdateFixture(t *testing.T) *propertyUpdateFixture {
	t.Helper()
	ctx := t.Context()
	store := NewInMemoryStorage()
	properties := func() map[string]interface{} {
		return map[string]interface{}{
			"name": "Alice",
			"age":  int64(30),
			"tags": []interface{}{"first"},
		}
	}
	alice, err := store.CreateNode(ctx, []string{"Person"}, properties())
	require.NoError(t, err)
	bob, err := store.CreateNode(ctx, nil, nil)
	require.NoError(t, err)
	knows, err := store.CreateRelationship(ctx, alice.ID, bob.ID, "KNOWS", properties())
	require.NoError(t, err)
	bare, err := store.CreateRelationship(ctx, alice.ID, bob.ID, "BARE", nil)
	require.NoError(t, err)
	return &propertyUpdateFixture{
		store:    store,
		node:     alice.ID,
		rel:      knows.ID,
		bareNode: bob.ID,
		bareRel:  bare.ID,
	}
}

// propertyUpdateEntity is one APOC storage entity kind: the identity a patch
// targets, the entity whose bag is absent, the public update call a caller makes,
// how its retained bag is read back and the error a missing entity reports.
type propertyUpdateEntity struct {
	name     string
	id       func(graph *propertyUpdateFixture) int64
	bareID   func(graph *propertyUpdateFixture) int64
	update   func(graph *propertyUpdateFixture, ctx context.Context, id int64, update PropertyUpdate) error
	read     func(t *testing.T, graph *propertyUpdateFixture, ctx context.Context, id int64) map[string]interface{}
	notFound error
}

func propertyUpdateEntities() []propertyUpdateEntity {
	return []propertyUpdateEntity{
		{
			name:   "node",
			id:     func(graph *propertyUpdateFixture) int64 { return graph.node },
			bareID: func(graph *propertyUpdateFixture) int64 { return graph.bareNode },
			update: func(graph *propertyUpdateFixture, ctx context.Context, id int64, update PropertyUpdate) error {
				return graph.store.UpdateNode(ctx, id, update)
			},
			read: func(t *testing.T, graph *propertyUpdateFixture, ctx context.Context, id int64) map[string]interface{} {
				node, err := graph.store.GetNode(ctx, id)
				require.NoError(t, err)
				return node.Properties
			},
			notFound: ErrNodeNotFound,
		},
		{
			name:   "relationship",
			id:     func(graph *propertyUpdateFixture) int64 { return graph.rel },
			bareID: func(graph *propertyUpdateFixture) int64 { return graph.bareRel },
			update: func(graph *propertyUpdateFixture, ctx context.Context, id int64, update PropertyUpdate) error {
				return graph.store.UpdateRelationship(ctx, id, update)
			},
			read: func(t *testing.T, graph *propertyUpdateFixture, ctx context.Context, id int64) map[string]interface{} {
				rel, err := graph.store.GetRelationship(ctx, id)
				require.NoError(t, err)
				return rel.Properties
			},
			notFound: ErrRelationshipNotFound,
		},
	}
}

// TestPropertyUpdateSetsOnlySuppliedProperties: a patch sets the properties it
// names and keeps every other property the bag already held.
func TestPropertyUpdateSetsOnlySuppliedProperties(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			require.NoError(t, entity.update(graph, ctx, entity.id(graph), PropertyUpdate{
				Set: map[string]interface{}{"age": int64(31), "city": "Berlin"},
			}))
			require.Equal(t, map[string]interface{}{
				"name": "Alice",
				"age":  int64(31),
				"tags": []interface{}{"first"},
				"city": "Berlin",
			}, entity.read(t, graph, ctx, entity.id(graph)),
				"an update sets its own properties and keeps the rest")
		})
	}
}

// TestPropertyUpdateRemovesNamedProperties: a removal-only patch deletes exactly
// the named property and keeps every other property.
func TestPropertyUpdateRemovesNamedProperties(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			require.NoError(t, entity.update(graph, ctx, entity.id(graph), PropertyUpdate{
				Remove: []string{"name"},
			}))
			require.Equal(t, map[string]interface{}{
				"age":  int64(30),
				"tags": []interface{}{"first"},
			}, entity.read(t, graph, ctx, entity.id(graph)),
				"a removal-only update deletes the named property and keeps the rest")
		})
	}
}

// TestPropertyUpdateAppliesRemovalsBeforeSet: the removals run first, so one
// update can rename a property or replace the value stored under a removed name.
func TestPropertyUpdateAppliesRemovalsBeforeSet(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name+"/renamed", func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			id := entity.id(graph)
			require.NoError(t, entity.update(graph, ctx, id, PropertyUpdate{
				Remove: []string{"name"},
				Set:    map[string]interface{}{"fullName": "Alice"},
			}))
			require.Equal(t, map[string]interface{}{
				"fullName": "Alice",
				"age":      int64(30),
				"tags":     []interface{}{"first"},
			}, entity.read(t, graph, ctx, id), "a rename carries the value to its new name")
		})

		t.Run(entity.name+"/same name", func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			id := entity.id(graph)
			require.NoError(t, entity.update(graph, ctx, id, PropertyUpdate{
				Remove: []string{"age"},
				Set:    map[string]interface{}{"age": int64(31)},
			}))
			bag := entity.read(t, graph, ctx, id)
			require.Equal(t, int64(31), bag["age"],
				"a name removed and set by one update keeps the value that update set")
			require.Equal(t, "Alice", bag["name"])
		})
	}
}

// TestPropertyUpdateZeroUpdateChangesNothing: a patch that sets and removes
// nothing still admits its caller and resolves its entity, and it leaves an
// absent property bag absent.
func TestPropertyUpdateZeroUpdateChangesNothing(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			id := entity.id(graph)
			require.NoError(t, entity.update(graph, ctx, id, PropertyUpdate{}))
			require.Equal(t, map[string]interface{}{
				"name": "Alice",
				"age":  int64(30),
				"tags": []interface{}{"first"},
			}, entity.read(t, graph, ctx, id), "a zero update changes no property")

			bare := entity.bareID(graph)
			require.Nil(t, entity.read(t, graph, ctx, bare))
			require.NoError(t, entity.update(graph, ctx, bare, PropertyUpdate{}))
			require.Nil(t, entity.read(t, graph, ctx, bare), "a zero update leaves an absent bag absent")
			require.NoError(t, entity.update(graph, ctx, bare, PropertyUpdate{Remove: []string{"name"}}))
			require.Nil(t, entity.read(t, graph, ctx, bare), "a removal-only update leaves an absent bag absent")
		})
	}
}

// TestPropertyUpdateMissingEntityStillReportsNotFound: a missing entity keeps
// its own documented error, for a patch and for a zero update alike.
func TestPropertyUpdateMissingEntityStillReportsNotFound(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			require.ErrorIs(t, entity.update(graph, ctx, int64(9999), PropertyUpdate{
				Set: map[string]interface{}{"name": "Nobody"},
			}), entity.notFound)
			require.ErrorIs(t, entity.update(graph, ctx, int64(9999), PropertyUpdate{}), entity.notFound,
				"a zero update still resolves its entity")
		})
	}
}

// TestPropertyUpdateRefusesAStoppedCaller: the caller decides admission, so a
// caller that already stopped is refused before any state is read or written.
func TestPropertyUpdateRefusesAStoppedCaller(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			id := entity.id(graph)
			stopped, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, entity.update(graph, stopped, id, PropertyUpdate{
				Remove: []string{"name"},
				Set:    map[string]interface{}{"age": int64(31)},
			}), context.Canceled, "a caller that already stopped is refused")
			require.Equal(t, map[string]interface{}{
				"name": "Alice",
				"age":  int64(30),
				"tags": []interface{}{"first"},
			}, entity.read(t, graph, ctx, id), "a refused update changes nothing")
			require.ErrorIs(t, entity.update(graph, stopped, id, PropertyUpdate{}), context.Canceled,
				"a zero update still checks its caller")
		})
	}
}

// TestPropertyUpdateOwnsTheValuesItRetains: the values a patch sets stay the
// caller's, so retaining them must not let the caller change the graph after.
func TestPropertyUpdateOwnsTheValuesItRetains(t *testing.T) {
	for _, entity := range propertyUpdateEntities() {
		t.Run(entity.name, func(t *testing.T) {
			graph := newPropertyUpdateFixture(t)
			ctx := t.Context()
			id := entity.id(graph)
			values := map[string]interface{}{
				"profile": map[string]interface{}{"city": "Paris"},
				"tags":    []interface{}{"updated"},
			}
			require.NoError(t, entity.update(graph, ctx, id, PropertyUpdate{Set: values}))

			values["profile"].(map[string]interface{})["city"] = "Mutated"
			values["tags"].([]interface{})[0] = "mutated"

			bag := entity.read(t, graph, ctx, id)
			require.Equal(t, map[string]interface{}{"city": "Paris"}, bag["profile"],
				"the store keeps the map a patch set")
			require.Equal(t, []interface{}{"updated"}, bag["tags"],
				"the store keeps the list a patch set")
		})
	}
}
