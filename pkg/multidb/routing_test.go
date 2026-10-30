package multidb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelRouting(t *testing.T) {
	routing := NewLabelRouting()

	// Set up routing rules
	routing.SetLabelRouting("Person", []string{"db1", "db2"})
	routing.SetLabelRouting("Order", []string{"db3"})

	// Test routing with matching label
	queryInfo := &QueryInfo{
		Labels: []string{"Person"},
	}
	result := routing.RouteQuery(queryInfo)
	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "db1")
	assert.Contains(t, result, "db2")

	// Test routing with multiple labels
	queryInfo = &QueryInfo{
		Labels: []string{"Person", "Order"},
	}
	result = routing.RouteQuery(queryInfo)
	// Should return all constituents for all labels
	assert.GreaterOrEqual(t, len(result), 1)

	// Test routing with no labels (full scan)
	queryInfo = &QueryInfo{
		Labels: []string{},
	}
	result = routing.RouteQuery(queryInfo)
	assert.Nil(t, result) // nil means all constituents

	// Test routing with unknown label (full scan)
	queryInfo = &QueryInfo{
		Labels: []string{"Unknown"},
	}
	result = routing.RouteQuery(queryInfo)
	assert.Nil(t, result) // nil means all constituents
}

func TestLabelRouting_RouteWrite(t *testing.T) {
	routing := NewLabelRouting()

	// Set up routing rules
	routing.SetLabelRouting("Person", []string{"db1", "db2"})

	// Test write routing
	result := routing.RouteWrite("create", []string{"Person"}, nil)
	assert.NotEmpty(t, result)
	assert.Contains(t, []string{"db1", "db2"}, result)

	// Test write routing with no labels
	result = routing.RouteWrite("create", []string{}, nil)
	assert.Empty(t, result) // Empty means full scan
}

func TestPropertyRouting(t *testing.T) {
	routing := NewPropertyRouting("database_id")

	// Set up routing rules
	routing.SetPropertyRouting("db_a", "db1")
	routing.SetPropertyRouting("db_b", "db2")
	routing.SetDefaultConstituent("db3")

	// Test routing with matching property
	queryInfo := &QueryInfo{
		Properties: map[string]interface{}{
			"database_id": "db_a",
		},
	}
	result := routing.RouteQuery(queryInfo)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "db1", result[0])

	// Test routing with default
	queryInfo = &QueryInfo{
		Properties: map[string]interface{}{
			"database_id": "unknown",
		},
	}
	result = routing.RouteQuery(queryInfo)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "db3", result[0])

	// Test routing with no properties
	queryInfo = &QueryInfo{
		Properties: nil,
	}
	result = routing.RouteQuery(queryInfo)
	assert.Nil(t, result) // nil means full scan

	// Test routing with property not in map and no default
	routing2 := NewPropertyRouting("database_id")
	routing2.SetPropertyRouting("db_a", "db1")
	queryInfo = &QueryInfo{
		Properties: map[string]interface{}{
			"database_id": "unknown",
		},
	}
	result = routing2.RouteQuery(queryInfo)
	assert.Nil(t, result) // nil means full scan when no default
}

func TestPropertyRouting_RouteWrite(t *testing.T) {
	routing := NewPropertyRouting("database_id")

	// Set up routing rules
	routing.SetPropertyRouting("db_a", "db1")
	routing.SetDefaultConstituent("db2")

	// Test write routing with matching property
	result := routing.RouteWrite("create", []string{}, map[string]interface{}{
		"database_id": "db_a",
	})
	assert.Equal(t, "db1", result)

	// Test write routing with default
	result = routing.RouteWrite("create", []string{}, map[string]interface{}{
		"database_id": "unknown",
	})
	assert.Equal(t, "db2", result)

	// Test write routing with no properties
	result = routing.RouteWrite("create", []string{}, nil)
	assert.Equal(t, "db2", result) // Uses default
}

func TestFullScanRouting(t *testing.T) {
	routing := NewFullScanRouting()

	// Test query routing (always returns nil = all constituents)
	queryInfo := &QueryInfo{
		Labels: []string{"Person"},
	}
	result := routing.RouteQuery(queryInfo)
	assert.Nil(t, result)

	// Test write routing (always returns empty = default)
	result2 := routing.RouteWrite("create", []string{"Person"}, nil)
	assert.Empty(t, result2)
}

func TestCompositeRouting(t *testing.T) {
	composite := NewCompositeRouting()

	// Add label routing strategy
	labelRouting := NewLabelRouting()
	labelRouting.SetLabelRouting("Person", []string{"db1"})
	composite.AddStrategy(labelRouting)

	// Add property routing strategy
	propertyRouting := NewPropertyRouting("database_id")
	propertyRouting.SetPropertyRouting("db_a", "db2")
	composite.AddStrategy(propertyRouting)

	// Test with label (should use label routing)
	queryInfo := &QueryInfo{
		Labels: []string{"Person"},
	}
	result := composite.RouteQuery(queryInfo)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "db1", result[0])

	// Test with property (should use property routing)
	queryInfo = &QueryInfo{
		Properties: map[string]interface{}{
			"database_id": "db_a",
		},
	}
	result = composite.RouteQuery(queryInfo)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "db2", result[0])

	// Test with no match (should return nil)
	queryInfo = &QueryInfo{
		Labels: []string{"Unknown"},
	}
	result = composite.RouteQuery(queryInfo)
	assert.Nil(t, result)
}

func TestCompositeRouting_RouteWrite(t *testing.T) {
	composite := NewCompositeRouting()

	// Add label routing strategy
	labelRouting := NewLabelRouting()
	labelRouting.SetLabelRouting("Person", []string{"db1"})
	composite.AddStrategy(labelRouting)

	// Test write routing
	result := composite.RouteWrite("create", []string{"Person"}, nil)
	assert.Equal(t, "db1", result)

	// Test with no match
	result = composite.RouteWrite("create", []string{"Unknown"}, nil)
	assert.Empty(t, result)
}
