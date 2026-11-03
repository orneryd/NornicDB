package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This capability observer delegates every result to the real native stored
// reader. It only terminates the request after the first real row was prepared.
// Embedding NamespacedEngine deliberately preserves StoredEntityReader without
// promoting the native scoped capability that would bypass this observer.
type schemaCreationCancellationReader struct {
	*NamespacedEngine
	cancel     context.CancelFunc
	rows       int
	supplied   context.Context
	firstError error
	afterFirst func()
}

func (r *schemaCreationCancellationReader) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	r.supplied = ctx
	return r.NamespacedEngine.StreamStoredNodesByLabelProjected(ctx, label, properties, func(node *Node) error {
		r.rows++
		err := visit(node)
		if r.rows == 1 {
			r.firstError = err
			if r.cancel != nil {
				r.cancel()
			}
			if r.afterFirst != nil {
				r.afterFirst()
			}
		}
		return err
	})
}

func TestReplaceDefinitionDurableCancellationDuringUniquePreparation(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	target := NewNamespacedEngine(engine, "test")
	for _, id := range []NodeID{"a", "b"} {
		_, err := target.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Item"}, Properties: map[string]any{"code": "duplicate"}})
		require.NoError(t, err)
	}
	schema := target.GetSchema()
	before := schemaDefinitionForTest(t, schema)
	definition := &SchemaDefinition{Constraints: []Constraint{{Name: "item_code_unique", Type: ConstraintUnique, Label: "Item", Properties: []string{"code"}, OwnedIndex: "item_code_unique"}}}
	// Preserve the actual native durable writer and count only accepted writes.
	persisted := 0
	nativePersist := schema.persist
	require.NotNil(t, nativePersist)
	schema.SetPersister(func(def *SchemaDefinition) error { persisted++; return nativePersist(def) })

	// A live request still reports the actual stored-data violation.
	err := schema.ReplaceDefinitionDurable(context.Background(), target, definition)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	require.Zero(t, persisted)
	require.Equal(t, before, schemaDefinitionForTest(t, schema))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &schemaCreationCancellationReader{NamespacedEngine: target, cancel: cancel}
	err = schema.ReplaceDefinitionDurable(ctx, observer, definition)
	require.Positive(t, observer.rows, "observer must see real native rows")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotNil(t, observer.supplied)
	require.NoError(t, observer.firstError, "first real row prepared successfully before cancellation")
	t.Logf("native preparation rows=%d request=%v supplied=%v result=%v", observer.rows, ctx.Err(), observer.supplied.Err(), err)
	// Nonfatal so unchanged-state and healthy-retry controls still execute on RED.
	assert.ErrorIs(t, err, context.Canceled, "preparation must stop before testing a later duplicate")
	assert.Zero(t, persisted)
	assert.Equal(t, before, schemaDefinitionForTest(t, schema))
	require.NoError(t, engine.loadPersistedSchemas())
	target = NewNamespacedEngine(engine, "test")
	require.Equal(t, before, schemaDefinitionForTest(t, target.GetSchema()), "native durable definition remains unchanged")
	for _, id := range []NodeID{"a", "b"} {
		node, readErr := target.GetNode(t.Context(), id)
		require.NoError(t, readErr)
		require.Equal(t, "duplicate", node.Properties["code"])
	}

	node, err := target.GetNode(t.Context(), "b")
	require.NoError(t, err)
	node.Properties["code"] = "distinct"
	require.NoError(t, target.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.NoError(t, target.GetSchema().ReplaceDefinitionDurable(context.Background(), target, definition))
	require.NoError(t, engine.loadPersistedSchemas())
	reloaded := NewNamespacedEngine(engine, "test").GetSchema()
	require.Len(t, schemaDefinitionForTest(t, reloaded).Constraints, 1)
	for _, value := range []string{"duplicate", "distinct"} {
		_, found, exists, complete, lookupErr := reloaded.LookupUniqueConstraintValue(context.Background(), "Item", "code", value)
		require.NoError(t, lookupErr)
		require.True(t, found)
		require.True(t, exists)
		require.True(t, complete)
	}
	t.Log("unchanged live/durable schema, original rows, native duplicate precedence, and healthy durable retry controls passed")
}

func TestUniquePreparationCancellationPreservesPublishedCaches(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	target := NewNamespacedEngine(engine, "test")
	for _, id := range []NodeID{"a", "b"} {
		_, err := target.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Item"}, Properties: map[string]any{"code": string(id)}})
		require.NoError(t, err)
	}
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("unique_code", "Item", "code"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), target, schema))
	before := uniqueCacheSnapshot(schema)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &schemaCreationCancellationReader{NamespacedEngine: target, cancel: cancel}
	require.ErrorIs(t, RefreshUniqueConstraintValuesForEngine(ctx, observer, schema), context.Canceled)
	require.Positive(t, observer.rows)
	require.Equal(t, before, uniqueCacheSnapshot(schema), "failed refresh must preserve the complete previous cache")
	_, found, exists, complete, err := schema.LookupUniqueConstraintValue(context.Background(), "Item", "code", "b")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, exists)
	require.True(t, complete)
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), target, schema))
	require.Equal(t, before, uniqueCacheSnapshot(schema))
}

func TestApplyUniqueValueCancellationKeepsCurrentClaim(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("unique_code", "Item", "code"))
	require.NoError(t, schema.ApplyUniqueValue(context.Background(), "Item", "code", "value", "old", ValidatedUniqueClaim))
	constraint := schema.uniqueConstraints[nodePropertyKey{label: "Item", property: "code"}]
	constraint.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	restore := setAdmissionWaitObserver(func(owner any, supplied context.Context) {
		if owner == &constraint.mu && supplied == ctx {
			waits++
			cancel()
		}
	})
	defer restore()
	err := schema.ApplyUniqueValue(ctx, "Item", "code", "value", "new", ValidatedUniqueClaim)
	constraint.mu.Unlock()
	require.Equal(t, 1, waits, "canceled at the actual held claim admission boundary")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, NodeID("old"), constraint.values["value"])
	require.ErrorIs(t, schema.ApplyUniqueValue(ctx, "Item", "code", "value", "old", RetiredUniqueClaim), context.Canceled)
	require.Equal(t, NodeID("old"), constraint.values["value"])
	require.NoError(t, schema.ApplyUniqueValue(context.Background(), "Item", "code", "value", "new", ValidatedUniqueClaim))
	require.Equal(t, NodeID("new"), constraint.values["value"])
}

func TestRefreshSelectedUniqueConstraintsDeduplicatesAdmission(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	target := NewNamespacedEngine(engine, "test")
	_, err := target.CreateNode(t.Context(), &Node{ID: "a", Labels: []string{"Item"}, Properties: map[string]any{"code": "a"}})
	require.NoError(t, err)
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("unique_code", "Item", "code"))
	selected := Constraint{Name: "unique_code", Type: ConstraintUnique, Label: "Item", Properties: []string{"code"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, RefreshUniqueConstraintValuesForConstraintsForEngine(ctx, target, schema, []Constraint{selected, selected}))
	_, found, exists, complete, err := schema.LookupUniqueConstraintValue(context.Background(), "Item", "code", "a")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, exists)
	require.True(t, complete)
}

func TestRefreshUniqueConstraintsRejectsReplacedSelection(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	target := NewNamespacedEngine(engine, "test")
	_, err := target.CreateNode(t.Context(), &Node{ID: "a", Labels: []string{"Item"}, Properties: map[string]any{"code": "a"}})
	require.NoError(t, err)
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("old_code", "Item", "code"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), target, schema))
	old := schema.uniqueConstraints[nodePropertyKey{label: "Item", property: "code"}]
	observer := &schemaCreationCancellationReader{NamespacedEngine: target, afterFirst: func() {
		require.NoError(t, schema.DropConstraint("old_code"))
		require.NoError(t, schema.AddUniqueConstraint("new_code", "Item", "code"))
	}}
	err = RefreshUniqueConstraintValuesForEngine(context.Background(), observer, schema)
	assert.ErrorIs(t, err, ErrConflict, "a replaced captured constraint must not publish stale success")
	require.Equal(t, 1, observer.rows)
	require.NotSame(t, old, schema.uniqueConstraints[nodePropertyKey{label: "Item", property: "code"}])
	_, found, exists, complete, lookupErr := schema.LookupUniqueConstraintValue(context.Background(), "Item", "code", "a")
	require.NoError(t, lookupErr)
	require.False(t, found)
	require.True(t, exists)
	require.False(t, complete)
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), target, schema))
	_, found, exists, complete, lookupErr = schema.LookupUniqueConstraintValue(context.Background(), "Item", "code", "a")
	require.NoError(t, lookupErr)
	require.True(t, found)
	require.True(t, exists)
	require.True(t, complete)
}
