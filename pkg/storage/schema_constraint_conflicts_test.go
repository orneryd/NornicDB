package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// AddConstraint rejects a constraint that conflicts with an existing one,
// accepts an equivalent one silently under IF NOT EXISTS, and reports
// whether anything was added (#823: a no-op persists nothing).
func TestAddConstraint_ConflictsAndEquivalents(t *testing.T) {
	domain := Constraint{Name: "d", Type: ConstraintDomain, Label: "Doc", Properties: []string{"status"}, AllowedValues: []interface{}{"a", "b"}}
	cardinality := Constraint{Name: "c", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "HAS", Direction: "OUTGOING", MaxCount: 2}
	policy := Constraint{Name: "p", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINKS", SourceLabel: "A", TargetLabel: "B", PolicyMode: "ALLOWED"}
	unique := Constraint{Name: "u", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}

	for _, tc := range []struct {
		name     string
		existing []Constraint
		add      Constraint
		silent   bool
		wantErr  string
	}{
		{name: "same name, different allowed values", existing: []Constraint{domain},
			add: Constraint{Name: "d", Type: ConstraintDomain, Label: "Doc", Properties: []string{"status"}, AllowedValues: []interface{}{"c"}}, wantErr: "d"},
		{name: "same name, different max count", existing: []Constraint{cardinality},
			add: Constraint{Name: "c", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "HAS", Direction: "OUTGOING", MaxCount: 3}, wantErr: "c"},
		{name: "same name, different schema", existing: []Constraint{unique},
			add: Constraint{Name: "u", Type: ConstraintUnique, Label: "Doc", Properties: []string{"other"}}, wantErr: "u"},
		{name: "opposite policy for the same endpoints", existing: []Constraint{policy},
			add: Constraint{Name: "p2", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINKS", SourceLabel: "A", TargetLabel: "B", PolicyMode: "DISALLOWED"}, wantErr: "p"},
		{name: "other name, different allowed values", existing: []Constraint{domain},
			add: Constraint{Name: "d2", Type: ConstraintDomain, Label: "Doc", Properties: []string{"status"}, AllowedValues: []interface{}{"c"}}, wantErr: "d"},
		{name: "other name, different max count", existing: []Constraint{cardinality},
			add: Constraint{Name: "c2", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "HAS", Direction: "OUTGOING", MaxCount: 5}, wantErr: "c"},
		{name: "other name, equivalent", existing: []Constraint{unique},
			add: Constraint{Name: "u2", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}, wantErr: "u"},
		{name: "other name, equivalent, if not exists", existing: []Constraint{unique},
			add: Constraint{Name: "u2", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}, silent: true},
		{name: "unique against node key", existing: []Constraint{{Name: "k", Type: ConstraintNodeKey, Label: "Doc", Properties: []string{"id"}}},
			add: Constraint{Name: "u2", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}, wantErr: "k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewSchemaManager()
			for _, c := range tc.existing {
				require.NoError(t, sm.AddConstraint(c))
			}
			before := len(sm.GetAllConstraints())
			err := sm.AddConstraint(tc.add, tc.silent)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
			require.Len(t, sm.GetAllConstraints(), before, "a rejected or equivalent constraint adds nothing")
		})
	}
}

func TestAddConstraint_NameTakenByIndex(t *testing.T) {
	for name, setup := range map[string]func(*SchemaManager) error{
		"property": func(schema *SchemaManager) error { return schema.AddPropertyIndex("taken", "Doc", []string{"id"}) },
		"composite": func(schema *SchemaManager) error {
			return schema.AddPropertyIndex("taken", "Doc", []string{"id", "tenant"})
		},
		"range": func(schema *SchemaManager) error {
			return schema.AddRangeIndexForEntity("taken", "Doc", []string{"id"}, ConstraintEntityNode)
		},
		"fulltext": func(schema *SchemaManager) error {
			return schema.AddFulltextIndex("taken", []string{"Doc"}, []string{"id"})
		},
		"vector": func(schema *SchemaManager) error {
			return schema.AddVectorIndexForEntity("taken", "Doc", "id", 3, "cosine", ConstraintEntityNode)
		},
		"text": func(schema *SchemaManager) error {
			return schema.AddTypedIndexForEntity(IndexKindText, "taken", "Doc", []string{"id"}, ConstraintEntityNode)
		},
		"point": func(schema *SchemaManager) error {
			return schema.AddTypedIndexForEntity(IndexKindPoint, "taken", "Doc", []string{"id"}, ConstraintEntityNode)
		},
		"lookup": func(schema *SchemaManager) error {
			if err := schema.DropIndex(defaultLookupIndexes()[ConstraintEntityNode]); err != nil {
				return err
			}
			return schema.AddLookupIndex("taken", ConstraintEntityNode)
		},
	} {
		t.Run(name, func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, setup(schema))
			before := schema.GetIndexes()
			persisted := 0
			schema.SetPersister(func(*SchemaDefinition) error { persisted++; return nil })
			for _, guarded := range []bool{false, true} {
				err := schema.AddConstraint(Constraint{Name: "taken", Type: ConstraintUnique, Label: "Other", Properties: []string{"id"}}, guarded)
				require.Error(t, err)
				var classified *schemaAdmissionError
				require.ErrorAs(t, err, &classified)
				require.Equal(t, "Neo.ClientError.Schema.IndexWithNameAlreadyExists", classified.BoltErrorCode())
				require.Empty(t, schema.GetAllConstraints())
				require.ElementsMatch(t, before, schema.GetIndexes())
			}
			require.Zero(t, persisted)
		})
	}
}

// A constraint name taken by a constraint contract cannot name another
// constraint.
func TestAddConstraint_NameTakenByContract(t *testing.T) {
	sm := NewSchemaManager()
	sm.constraintContracts["taken"] = ConstraintContract{Name: "taken", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Doc"}
	require.Error(t, sm.AddConstraint(Constraint{Name: "taken", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}))
	require.Error(t, sm.AddPropertyTypeConstraint("taken", "Doc", "id", PropertyTypeString))
	require.Empty(t, sm.GetAllConstraints())
}

// A CREATE CONSTRAINT ... IF NOT EXISTS that finds the constraint present
// neither persists nor snapshots the schema (#823); a real addition
// persists once.
func TestConstraintAdders_IfNotExistsNoOpPersistsNothing(t *testing.T) {
	sm := NewSchemaManager()
	persisted := 0
	sm.SetPersister(func(*SchemaDefinition) error { persisted++; return nil })

	require.NoError(t, sm.AddUniqueConstraint("u", "Doc", "id", true))
	require.NoError(t, sm.AddConstraint(Constraint{Name: "k", Type: ConstraintNodeKey, Label: "Item", Properties: []string{"sku"}}, true))
	require.NoError(t, sm.AddPropertyTypeConstraintWithOptions("t", "Doc", "n", PropertyTypeInteger, PropertyTypeConstraintOptions{IfNotExists: true}))
	require.Equal(t, 3, persisted)

	for i := 0; i < 3; i++ {
		require.NoError(t, sm.AddUniqueConstraint("u", "Doc", "id", true))
		require.NoError(t, sm.AddConstraint(Constraint{Name: "k", Type: ConstraintNodeKey, Label: "Item", Properties: []string{"sku"}}, true))
		require.NoError(t, sm.AddPropertyTypeConstraintWithOptions("t", "Doc", "n", PropertyTypeInteger, PropertyTypeConstraintOptions{IfNotExists: true}))
	}
	require.Equal(t, 3, persisted)
	require.Len(t, sm.GetAllConstraints(), 2)
}

func TestSameConstraintSchema(t *testing.T) {
	base := Constraint{Type: ConstraintUnique, Label: "Doc", Properties: []string{"a", "b"}}
	require.True(t, sameConstraintSchema(base, Constraint{Type: ConstraintNodeKey, Label: "Doc", Properties: []string{"b", "a"}}))
	require.False(t, sameConstraintSchema(base, Constraint{Type: ConstraintUnique, Label: "Doc", Properties: []string{"a", "a"}}))
	require.False(t, sameConstraintSchema(base, Constraint{Type: ConstraintUnique, Label: "Doc", Properties: []string{"a"}}))
	require.False(t, sameConstraintSchema(base, Constraint{Type: ConstraintUnique, Label: "Other", Properties: []string{"a", "b"}}))
	require.False(t, sameConstraintSchema(base, Constraint{Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "Doc", Properties: []string{"a", "b"}}))
	policy := Constraint{Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "L", SourceLabel: "A", TargetLabel: "B", PolicyMode: "ALLOWED"}
	require.True(t, sameConstraintSchema(policy, policy))
	other := policy
	other.PolicyMode = "DISALLOWED"
	require.False(t, sameConstraintSchema(policy, other))
	require.False(t, sameConstraintSchema(policy, Constraint{Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "L"}))
	card := Constraint{Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "L", Direction: "OUTGOING", MaxCount: 1}
	inbound := card
	inbound.Direction = "INCOMING"
	require.False(t, sameConstraintSchema(card, inbound))
	require.True(t, sameConstraintSchema(card, Constraint{Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "L", Direction: "OUTGOING", MaxCount: 5}))
}
