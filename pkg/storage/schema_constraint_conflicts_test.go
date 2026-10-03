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

// A constraint name taken by a constraint contract cannot name another
// constraint.
func TestAddConstraint_NameTakenByContract(t *testing.T) {
	sm := NewSchemaManager()
	sm.constraintContracts["taken"] = ConstraintContract{Name: "taken", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Doc"}
	require.Error(t, sm.AddConstraint(Constraint{Name: "taken", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"}}))
	require.Error(t, sm.AddPropertyTypeConstraint("taken", "Doc", "id", PropertyTypeString))
	require.Empty(t, sm.GetAllConstraints())
}
