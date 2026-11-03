package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

const durableDefinitionNamespace = "nornic"

// TestReplaceDefinitionDurablePublishesTheDefinitionItPersisted pins the
// accepted replacement's contract: one durable write carrying the definition
// that is then published, every family and the token order included, with the
// derived runtime caches rebuilt from the stored entities the definition
// constrains.
func TestReplaceDefinitionDurablePublishesTheDefinitionItPersisted(t *testing.T) {
	engine := newTestMemoryEngine(t)
	target := NewNamespacedEngine(engine, durableDefinitionNamespace)
	_, err := target.CreateNode(t.Context(), &Node{
		ID: "u1", Labels: []string{"Person"},
		Properties: map[string]any{"name": "Alice", "email": "alice@example.com"},
	})
	require.NoError(t, err)
	_, err = target.CreateNode(t.Context(), &Node{
		ID: "u2", Labels: []string{"Person"},
		Properties: map[string]any{"name": "Bob", "email": "bob@example.com"},
	})
	require.NoError(t, err)

	schema := engine.GetSchema()
	var persisted []*SchemaDefinition
	schema.SetPersister(func(def *SchemaDefinition) error {
		persisted = append(persisted, def)
		return nil
	})

	require.NoError(t, schema.ReplaceDefinitionDurable(t.Context(), target, &SchemaDefinition{
		LabelTokens: []string{"Person"},
		Constraints: []Constraint{{
			Name:       "person_email_unique",
			Type:       ConstraintUnique,
			Label:      "Person",
			Properties: []string{"email"},
			OwnedIndex: "person_email_unique",
		}},
		PropertyIndexes: []SchemaPropertyIndexDef{{
			Name: "person_name_idx", Label: "Person", Properties: []string{"name"},
		}},
	}))

	require.Len(t, persisted, 1, "one accepted replacement is one durable schema write")
	require.Equal(t, schemaDefinitionForTest(t, schema), persisted[0],
		"the durable definition must be the definition the replacement published")
	require.Equal(t, []string{"Person"}, schemaDefinitionForTest(t, schema).LabelTokens,
		"the replacement's token order must be published with it")

	// The derived UNIQUE values were rebuilt from the stored nodes.
	holder, found, exists, complete, err := schema.LookupUniqueConstraintValue(
		context.Background(), "Person", "email", "alice@example.com",
	)
	require.NoError(t, err)
	require.True(t, exists, "the replacement's UNIQUE constraint must be published")
	require.True(t, complete, "an accepted replacement's value cache is rebuilt from storage")
	require.True(t, found)
	require.Equal(t, NodeID(durableDefinitionNamespace+":u1"), holder)
	require.Error(t, schema.CheckUniqueConstraint(context.Background(), "Person", "email", "alice@example.com", "someone-else"),
		"the rebuilt UNIQUE cache must reject a duplicate of a stored value")

	// The equality index contents were rebuilt from the stored nodes too.
	// The stream reads each node's own identity, so an index entry keeps the
	// user-visible ID the stored entity carries.
	indexed, err := schema.PropertyIndexLookup(context.Background(), "Person", "name", "Alice")
	require.NoError(t, err)
	require.Contains(t, indexed, NodeID("u1"))
}

// TestReplaceDefinitionDurableRejectsUnsatisfiedDefinitionBeforeWriting pins
// the acceptance order: a definition the stored entities cannot satisfy is
// rejected before the persister is called and before anything is published.
func TestReplaceDefinitionDurableRejectsUnsatisfiedDefinitionBeforeWriting(t *testing.T) {
	engine := newTestMemoryEngine(t)
	target := NewNamespacedEngine(engine, durableDefinitionNamespace)
	for _, id := range []string{"d1", "d2"} {
		_, err := target.CreateNode(t.Context(), &Node{
			ID: NodeID(id), Labels: []string{"Person"},
			Properties: map[string]any{"email": "shared@example.com"},
		})
		require.NoError(t, err)
	}

	schema := engine.GetSchema()
	before := schemaDefinitionForTest(t, schema)
	attempts := 0
	schema.SetPersister(func(*SchemaDefinition) error {
		attempts++
		return nil
	})

	err := schema.ReplaceDefinitionDurable(t.Context(), target, &SchemaDefinition{
		Constraints: []Constraint{{
			Name:       "person_email_unique",
			Type:       ConstraintUnique,
			Label:      "Person",
			Properties: []string{"email"},
			OwnedIndex: "person_email_unique",
		}},
	})
	require.Error(t, err, "stored entities that violate the definition must reject it")
	require.Zero(t, attempts, "a rejected definition must be rejected before it is persisted")
	require.Equal(t, before, schemaDefinitionForTest(t, schema),
		"a rejected definition must leave the published schema unchanged")
}

// TestReplaceDefinitionDurableRejectedWriteKeepsTheOldCaches pins the other
// half of the replacement: when the persister refuses the accepted definition,
// the published families and the previous runtime index and UNIQUE caches keep
// serving and enforcing the definition they already held.
func TestReplaceDefinitionDurableRejectedWriteKeepsTheOldCaches(t *testing.T) {
	engine := newTestMemoryEngine(t)
	target := NewNamespacedEngine(engine, durableDefinitionNamespace)
	schema := engine.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("legacy_key_unique", "Legacy", "key"))
	_, err := target.CreateNode(t.Context(), &Node{
		ID: "legacy-1", Labels: []string{"Legacy"}, Properties: map[string]any{"key": "k1"},
	})
	require.NoError(t, err)
	before := schemaDefinitionForTest(t, schema)

	rejection := errors.New("schema persistence rejected")
	schema.SetPersister(func(*SchemaDefinition) error { return rejection })

	err = schema.ReplaceDefinitionDurable(t.Context(), target, &SchemaDefinition{
		Constraints: []Constraint{{
			Name:       "person_email_unique",
			Type:       ConstraintUnique,
			Label:      "Person",
			Properties: []string{"email"},
			OwnedIndex: "person_email_unique",
		}},
	})
	require.ErrorIs(t, err, rejection)
	require.Equal(t, before, schemaDefinitionForTest(t, schema),
		"a rejected replacement must leave the published schema unchanged")

	holder, found, exists, _, err := schema.LookupUniqueConstraintValue(
		context.Background(), "Legacy", "key", "k1",
	)
	require.NoError(t, err)
	require.True(t, exists, "a rejected replacement must not remove the published constraint")
	require.True(t, found, "the previous UNIQUE cache must keep the published value")
	require.Equal(t, NodeID(durableDefinitionNamespace+":legacy-1"), holder)

	_, err = target.CreateNode(t.Context(), &Node{
		ID: "legacy-2", Labels: []string{"Legacy"}, Properties: map[string]any{"key": "k1"},
	})
	require.Error(t, err, "a rejected replacement must leave the previous UNIQUE constraint enforced")
}

func TestReplaceDefinitionDurableRequiresContextAndEngine(t *testing.T) {
	engine := newTestMemoryEngine(t)
	target := NewNamespacedEngine(engine, durableDefinitionNamespace)
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("before", "Person", "id"))
	before := schemaDefinitionForTest(t, schema)
	attempts := 0
	schema.SetPersister(func(*SchemaDefinition) error { attempts++; return nil })
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		engine Engine
	}{
		{name: "missing engine", ctx: context.Background(), engine: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, schema.ReplaceDefinitionDurable(tc.ctx, tc.engine, &SchemaDefinition{}))
			require.Zero(t, attempts)
			require.Equal(t, before, schemaDefinitionForTest(t, schema))
		})
	}
}
