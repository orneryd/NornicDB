package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// compareValues coverage — exercises all type combination branches
// ============================================================================

func TestCompareValues_IntTypes(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"int==int equal", int(42), int(42), true},
		{"int==int not equal", int(1), int(2), false},
		{"int==int64 equal", int(10), int64(10), true},
		{"int==int64 not equal", int(10), int64(11), false},
		{"int==float64 equal", int(5), float64(5.0), true},
		{"int==float64 not equal", int(5), float64(5.1), false},
		{"int64==int equal", int64(99), int(99), true},
		{"int64==int not equal", int64(99), int(100), false},
		{"int64==int64 equal", int64(7), int64(7), true},
		{"int64==int64 not equal", int64(7), int64(8), false},
		{"int64==float64 equal", int64(3), float64(3.0), true},
		{"int64==float64 not equal", int64(3), float64(3.5), false},
		{"float64==int equal", float64(12.0), int(12), true},
		{"float64==int not equal", float64(12.5), int(12), false},
		{"float64==int64 equal", float64(100.0), int64(100), true},
		{"float64==int64 not equal", float64(100.1), int64(100), false},
		{"float64==float64 equal", float64(3.14), float64(3.14), true},
		{"float64==float64 not equal", float64(3.14), float64(2.71), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompareValues_StringBool(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"string equal", "hello", "hello", true},
		{"string not equal", "hello", "world", false},
		{"string vs int", "42", int(42), false},
		{"bool true==true", true, true, true},
		{"bool true==false", true, false, false},
		{"bool false==false", false, false, true},
		{"bool vs string", true, "true", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompareValues_NilAndUncomparable(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"nil==nil", nil, nil, true},
		{"nil vs string", nil, "x", false},
		{"string vs nil", "x", nil, false},
		{"slice equal", []int{1, 2}, []int{1, 2}, true},
		{"slice not equal", []int{1, 2}, []int{3, 4}, false},
		{"slice vs nil", []int{1}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompareValues_NumericConstraintValue_WidenedTypes(t *testing.T) {
	// Exercise the numericConstraintValue path with less common types
	// (int8, int16, int32, uint, uint8, etc.) that go through the
	// leading numericConstraintValue check before the switch.
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"int8 vs int8", int8(5), int8(5), true},
		{"int8 vs int8 ne", int8(5), int8(6), false},
		{"int16 vs int32", int16(100), int32(100), true},
		{"uint8 vs int64", uint8(255), int64(255), true},
		{"uint8 vs int64 ne", uint8(255), int64(256), false},
		{"uint vs float64", uint(42), float64(42.0), true},
		{"float32 vs float64", float32(1.5), float64(1.5), true},
		{"uint64 vs int", uint64(77), int(77), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}

// ============================================================================
// validatePolicyForEdgesWithPrefixInTxn — full path coverage
// ============================================================================

func TestValidatePolicyForEdgesWithPrefix_DisallowedViolation(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	ns := "test"
	sm := eng.GetSchemaForNamespace(ns)

	// Create nodes
	src := &Node{ID: "test:src", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
	tgt := &Node{ID: "test:tgt", Labels: []string{"Company"}, Properties: map[string]any{"name": "Acme"}}
	_, err = eng.CreateNode(src)
	require.NoError(t, err)
	_, err = eng.CreateNode(tgt)
	require.NoError(t, err)

	// Create edge
	err = eng.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:src", EndNode: "test:tgt",
		Type: "WORKS_AT", Properties: map[string]any{},
	})
	require.NoError(t, err)

	// Add a DISALLOWED policy: Person cannot WORKS_AT Company
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:        "no_person_works_at_company",
		Type:        ConstraintPolicy,
		Label:       "WORKS_AT",
		EntityType:  ConstraintEntityRelationship,
		PolicyMode:  "DISALLOWED",
		SourceLabel: "Person",
		TargetLabel: "Company",
		Properties:  []string{},
	}))

	// Simulate label change validation — changing src labels triggers the check.
	// The node still has Person label and there's already an edge WORKS_AT → Company.
	err = eng.withView(func(txn *badger.Txn) error {
		outPrefix := eng.outgoingIndexPrefixString(src.ID)
		if outPrefix == nil {
			t.Fatal("expected non-nil outgoing prefix")
		}
		return eng.validatePolicyForEdgesWithPrefixInTxn(txn, outPrefix, src, true, sm, ns)
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintPolicy, cve.Type)
	require.Contains(t, cve.Message, "DISALLOWED")
	require.Contains(t, cve.Message, "Person")
	require.Contains(t, cve.Message, "Company")
}

func TestValidatePolicyForEdgesWithPrefix_AllowedViolation(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	ns := "test"
	sm := eng.GetSchemaForNamespace(ns)

	// Create nodes — src is a Robot, tgt is a Company
	src := &Node{ID: "test:robot", Labels: []string{"Robot"}, Properties: map[string]any{}}
	tgt := &Node{ID: "test:corp", Labels: []string{"Company"}, Properties: map[string]any{}}
	_, err = eng.CreateNode(src)
	require.NoError(t, err)
	_, err = eng.CreateNode(tgt)
	require.NoError(t, err)

	// Create edge: Robot -[:WORKS_AT]-> Company
	err = eng.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:robot", EndNode: "test:corp",
		Type: "WORKS_AT", Properties: map[string]any{},
	})
	require.NoError(t, err)

	// ALLOWED policy only permits Person → Company, not Robot → Company
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:        "only_person_works_at_company",
		Type:        ConstraintPolicy,
		Label:       "WORKS_AT",
		EntityType:  ConstraintEntityRelationship,
		PolicyMode:  "ALLOWED",
		SourceLabel: "Person",
		TargetLabel: "Company",
		Properties:  []string{},
	}))

	err = eng.withView(func(txn *badger.Txn) error {
		outPrefix := eng.outgoingIndexPrefixString(src.ID)
		if outPrefix == nil {
			t.Fatal("expected non-nil outgoing prefix")
		}
		return eng.validatePolicyForEdgesWithPrefixInTxn(txn, outPrefix, src, true, sm, ns)
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintPolicy, cve.Type)
	require.Contains(t, cve.Message, "ALLOWED")
}

func TestValidatePolicyForEdgesWithPrefix_NoPolicies_NoError(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	ns := "test"
	sm := eng.GetSchemaForNamespace(ns)

	src := &Node{ID: "test:a", Labels: []string{"X"}, Properties: map[string]any{}}
	tgt := &Node{ID: "test:b", Labels: []string{"Y"}, Properties: map[string]any{}}
	_, err = eng.CreateNode(src)
	require.NoError(t, err)
	_, err = eng.CreateNode(tgt)
	require.NoError(t, err)

	err = eng.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:b",
		Type: "LIKES", Properties: map[string]any{},
	})
	require.NoError(t, err)

	// No policies — validation should pass.
	err = eng.withView(func(txn *badger.Txn) error {
		outPrefix := eng.outgoingIndexPrefixString(src.ID)
		if outPrefix == nil {
			return nil
		}
		return eng.validatePolicyForEdgesWithPrefixInTxn(txn, outPrefix, src, true, sm, ns)
	})
	require.NoError(t, err)
}

func TestValidatePolicyForEdgesWithPrefix_AllowedMatch_NoError(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	ns := "test"
	sm := eng.GetSchemaForNamespace(ns)

	// Person → Company is ALLOWED
	src := &Node{ID: "test:person", Labels: []string{"Person"}, Properties: map[string]any{}}
	tgt := &Node{ID: "test:company", Labels: []string{"Company"}, Properties: map[string]any{}}
	_, err = eng.CreateNode(src)
	require.NoError(t, err)
	_, err = eng.CreateNode(tgt)
	require.NoError(t, err)

	err = eng.CreateEdge(&Edge{
		ID: "test:e4", StartNode: "test:person", EndNode: "test:company",
		Type: "WORKS_AT", Properties: map[string]any{},
	})
	require.NoError(t, err)

	require.NoError(t, sm.AddConstraint(Constraint{
		Name:        "person_works_at_company_ok",
		Type:        ConstraintPolicy,
		Label:       "WORKS_AT",
		EntityType:  ConstraintEntityRelationship,
		PolicyMode:  "ALLOWED",
		SourceLabel: "Person",
		TargetLabel: "Company",
		Properties:  []string{},
	}))

	err = eng.withView(func(txn *badger.Txn) error {
		outPrefix := eng.outgoingIndexPrefixString(src.ID)
		if outPrefix == nil {
			t.Fatal("expected non-nil outgoing prefix")
		}
		return eng.validatePolicyForEdgesWithPrefixInTxn(txn, outPrefix, src, true, sm, ns)
	})
	require.NoError(t, err)
}

func TestValidatePolicyForEdgesWithPrefix_IncomingDirection(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	ns := "test"
	sm := eng.GetSchemaForNamespace(ns)

	// src → tgt edge. We test from tgt's perspective (incoming).
	src := &Node{ID: "test:emp", Labels: []string{"Employee"}, Properties: map[string]any{}}
	tgt := &Node{ID: "test:dept", Labels: []string{"Department"}, Properties: map[string]any{}}
	_, err = eng.CreateNode(src)
	require.NoError(t, err)
	_, err = eng.CreateNode(tgt)
	require.NoError(t, err)

	err = eng.CreateEdge(&Edge{
		ID: "test:e5", StartNode: "test:emp", EndNode: "test:dept",
		Type: "BELONGS_TO", Properties: map[string]any{},
	})
	require.NoError(t, err)

	// DISALLOWED: Employee -[:BELONGS_TO]-> Department
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:        "no_emp_belongs_dept",
		Type:        ConstraintPolicy,
		Label:       "BELONGS_TO",
		EntityType:  ConstraintEntityRelationship,
		PolicyMode:  "DISALLOWED",
		SourceLabel: "Employee",
		TargetLabel: "Department",
		Properties:  []string{},
	}))

	// Validate from tgt (incoming direction)
	err = eng.withView(func(txn *badger.Txn) error {
		inPrefix := eng.incomingIndexPrefixString(tgt.ID)
		if inPrefix == nil {
			t.Fatal("expected non-nil incoming prefix")
		}
		return eng.validatePolicyForEdgesWithPrefixInTxn(txn, inPrefix, tgt, false, sm, ns)
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Contains(t, cve.Message, "DISALLOWED")
	require.Contains(t, cve.Message, "Employee")
	require.Contains(t, cve.Message, "Department")
}

// ============================================================================
// validateEdgeConstraintsInTxn — domain, exists, cardinality paths
// ============================================================================

func TestValidateEdgeConstraintsInTxn_NilArgs(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	// nil edge, nil schema → no error (early return)
	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, nil, nil, "", "")
	})
	require.NoError(t, err)

	// nil schema → no error
	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, &Edge{Type: "X"}, nil, "", "")
	})
	require.NoError(t, err)

	// empty type → no error
	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, &Edge{}, NewSchemaManager(), "", "")
	})
	require.NoError(t, err)
}

func TestValidateEdgeConstraintsInTxn_ExistsViolation(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	sm := eng.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "rel_exists_weight",
		Type:       ConstraintExists,
		Label:      "KNOWS",
		EntityType: ConstraintEntityRelationship,
		Properties: []string{"weight"},
	}))

	// Edge missing required property "weight"
	edge := &Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "KNOWS", Properties: map[string]any{},
	}

	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, edge, sm, "test", "")
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintExists, cve.Type)
}

func TestValidateEdgeConstraintsInTxn_DomainViolation(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	sm := eng.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:          "rel_domain_status",
		Type:          ConstraintDomain,
		Label:         "RATED",
		EntityType:    ConstraintEntityRelationship,
		Properties:    []string{"status"},
		AllowedValues: []interface{}{"good", "bad", "neutral"},
	}))

	// Edge has invalid domain value
	edge := &Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:b",
		Type: "RATED", Properties: map[string]any{"status": "invalid_value"},
	}

	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, edge, sm, "test", "")
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, ConstraintDomain, cve.Type)
	require.Contains(t, cve.Message, "not in allowed values")
}

func TestValidateEdgeConstraintsInTxn_DomainPass(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	sm := eng.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:          "rel_domain_status_ok",
		Type:          ConstraintDomain,
		Label:         "RATED",
		EntityType:    ConstraintEntityRelationship,
		Properties:    []string{"status"},
		AllowedValues: []interface{}{"good", "bad", "neutral"},
	}))

	// Edge has valid domain value
	edge := &Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:b",
		Type: "RATED", Properties: map[string]any{"status": "good"},
	}

	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, edge, sm, "test", "")
	})
	require.NoError(t, err)
}

func TestValidateEdgeConstraintsInTxn_ExistsPass(t *testing.T) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer eng.Close()

	sm := eng.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "rel_exists_weight_ok",
		Type:       ConstraintExists,
		Label:      "KNOWS",
		EntityType: ConstraintEntityRelationship,
		Properties: []string{"weight"},
	}))

	// Edge has required property
	edge := &Edge{
		ID: "test:e4", StartNode: "test:a", EndNode: "test:b",
		Type: "KNOWS", Properties: map[string]any{"weight": 0.9},
	}

	err = eng.withView(func(txn *badger.Txn) error {
		return eng.validateEdgeConstraintsInTxn(txn, edge, sm, "test", "")
	})
	require.NoError(t, err)
}
