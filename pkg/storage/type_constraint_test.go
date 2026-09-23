// Package storage - Property type constraint tests.
package storage

import (
	"testing"
	"time"
)

type typedTemporalProperty string

func (value typedTemporalProperty) TemporalPropertyKind() string { return string(value) }

// TestValidatePropertyType tests property type validation.
func TestValidatePropertyType(t *testing.T) {
	tests := []struct {
		name         string
		value        interface{}
		expectedType PropertyType
		shouldFail   bool
	}{
		// STRING tests
		{"string valid", "hello", PropertyTypeString, false},
		{"string invalid int", 42, PropertyTypeString, true},

		// INTEGER tests
		{"int valid", 42, PropertyTypeInteger, false},
		{"int64 valid", int64(42), PropertyTypeInteger, false},
		{"integer float64 whole number valid", float64(42), PropertyTypeInteger, false},
		{"integer float64 fractional invalid", 42.5, PropertyTypeInteger, true},
		{"integer float32 whole number valid", float32(7), PropertyTypeInteger, false},
		{"integer float32 fractional invalid", float32(7.5), PropertyTypeInteger, true},
		{"int invalid string", "42", PropertyTypeInteger, true},

		// FLOAT tests
		{"float64 valid", 3.14, PropertyTypeFloat, false},
		{"float32 valid", float32(3.14), PropertyTypeFloat, false},
		{"float invalid int", 42, PropertyTypeFloat, true},

		// BOOLEAN tests
		{"bool true valid", true, PropertyTypeBoolean, false},
		{"bool false valid", false, PropertyTypeBoolean, false},
		{"bool invalid int", 1, PropertyTypeBoolean, true},

		// NULL tests
		{"null string", nil, PropertyTypeString, false},
		{"null integer", nil, PropertyTypeInteger, false},
		{"null float", nil, PropertyTypeFloat, false},
		{"null boolean", nil, PropertyTypeBoolean, false},

		// Temporal type tests (Neo4j semantics)
		{"date valid string", "2025-11-27", PropertyTypeDate, false},
		{"date valid time", time.Now(), PropertyTypeDate, false},
		{"date invalid string", "2025-11-27T10:30:00Z", PropertyTypeDate, true},
		{"zoned datetime valid RFC3339", "2025-11-27T10:30:00Z", PropertyTypeZonedDateTime, false},
		{"zoned datetime valid offset", "2025-11-27T10:30:00+02:00", PropertyTypeZonedDateTime, false},
		{"zoned datetime invalid local format", "2025-11-27T10:30:00", PropertyTypeZonedDateTime, true},
		{"legacy datetime alias accepts zoned string", "2025-11-27T10:30:00Z", PropertyTypeDateTime, false},
		{"local datetime valid", "2025-11-27T10:30:00", PropertyTypeLocalDateTime, false},
		{"local datetime valid spaced format", "2025-11-27 10:30:00", PropertyTypeLocalDateTime, false},
		{"local datetime invalid zoned format", "2025-11-27T10:30:00Z", PropertyTypeLocalDateTime, true},
		{"legacy datetime alias accepts time.Time", time.Now(), PropertyTypeDateTime, false},
		{"typed date value", typedTemporalProperty("date"), PropertyTypeDate, false},
		{"typed zoned datetime value", typedTemporalProperty("zoned-date-time"), PropertyTypeZonedDateTime, false},
		{"typed local datetime value", typedTemporalProperty("local-date-time"), PropertyTypeLocalDateTime, false},
		{"typed local datetime is not zoned", typedTemporalProperty("local-date-time"), PropertyTypeZonedDateTime, true},
		{"unknown type", "hello", PropertyType("WEIRD"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePropertyType(tt.value, tt.expectedType)

			if tt.shouldFail && err == nil {
				t.Errorf("Expected error for %v as %s, got nil", tt.value, tt.expectedType)
			}

			if !tt.shouldFail && err != nil {
				t.Errorf("Expected no error for %v as %s, got %v", tt.value, tt.expectedType, err)
			}
		})
	}
}

// TestBadgerEngine_PropertyTypeConstraintValidation tests type constraint validation on existing data.
func TestBadgerEngine_PropertyTypeConstraintValidation(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Insert node with correct type
	tx1, _ := engine.BeginTransaction()
	tx1.CreateNode(&Node{
		ID:     NodeID(prefixTestID("user-1")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"age": 30, // INTEGER
		},
	})
	tx1.Commit()

	// Insert node with WRONG type
	tx2, _ := engine.BeginTransaction()
	tx2.CreateNode(&Node{
		ID:     NodeID(prefixTestID("user-2")),
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"age": "thirty", // STRING (wrong!)
		},
	})
	tx2.Commit()

	// Try to create type constraint (should fail - existing data violates)
	err := engine.ValidatePropertyTypeConstraintOnCreation(PropertyTypeConstraint{
		Label:        "User",
		Property:     "age",
		ExpectedType: PropertyTypeInteger,
	})

	if err == nil {
		t.Fatal("Expected type constraint validation error, got nil")
	}
}

// TestBadgerEngine_PropertyTypeConstraintSuccess tests successful type constraint creation.
func TestBadgerEngine_PropertyTypeConstraintSuccess(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	// Insert nodes with correct types
	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID:     "user-1",
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"name": "Alice", // STRING
			"age":  25,      // INTEGER
		},
	})
	tx.CreateNode(&Node{
		ID:     "user-2",
		Labels: []string{"User"},
		Properties: map[string]interface{}{
			"name": "Bob",
			"age":  30,
		},
	})
	tx.Commit()

	// Validate STRING type constraint (should succeed)
	err := engine.ValidatePropertyTypeConstraintOnCreation(PropertyTypeConstraint{
		Label:        "User",
		Property:     "name",
		ExpectedType: PropertyTypeString,
	})

	if err != nil {
		t.Errorf("Type constraint should succeed with valid data: %v", err)
	}

	// Validate INTEGER type constraint (should succeed)
	err = engine.ValidatePropertyTypeConstraintOnCreation(PropertyTypeConstraint{
		Label:        "User",
		Property:     "age",
		ExpectedType: PropertyTypeInteger,
	})

	if err != nil {
		t.Errorf("Type constraint should succeed with valid data: %v", err)
	}
}

// TestBadgerEngine_MixedNumericTypes tests that int and int64 are treated as INTEGER.
func TestBadgerEngine_MixedNumericTypes(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID:     "node-1",
		Labels: []string{"Node"},
		Properties: map[string]interface{}{
			"count": 42, // int
		},
	})
	tx.CreateNode(&Node{
		ID:     "node-2",
		Labels: []string{"Node"},
		Properties: map[string]interface{}{
			"count": int64(100), // int64
		},
	})
	tx.Commit()

	// Both should satisfy INTEGER constraint
	err := engine.ValidatePropertyTypeConstraintOnCreation(PropertyTypeConstraint{
		Label:        "Node",
		Property:     "count",
		ExpectedType: PropertyTypeInteger,
	})

	if err != nil {
		t.Errorf("Both int and int64 should satisfy INTEGER constraint: %v", err)
	}
}

// TestBadgerEngine_PropertyTypeConstraintWithNulls tests type constraints allow NULL values.
func TestBadgerEngine_PropertyTypeConstraintWithNulls(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	tx, _ := engine.BeginTransaction()
	tx.CreateNode(&Node{
		ID:     "person-1",
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"nickname": "Bob",
		},
	})
	tx.CreateNode(&Node{
		ID:         "person-2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{
			// nickname is NULL
		},
	})
	tx.Commit()

	// STRING type constraint should allow NULL
	err := engine.ValidatePropertyTypeConstraintOnCreation(PropertyTypeConstraint{
		Label:        "Person",
		Property:     "nickname",
		ExpectedType: PropertyTypeString,
	})

	if err != nil {
		t.Errorf("Type constraints should allow NULL values: %v", err)
	}
}
