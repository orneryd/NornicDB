package cypher

import (
	"testing"
)

// ========================================
// Advanced Spatial Functions Tests
// ========================================

func TestPolygonFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Test creating a polygon from a list of points
	expr := `polygon([
		point({x: 0.0, y: 0.0}),
		point({x: 4.0, y: 0.0}),
		point({x: 4.0, y: 3.0}),
		point({x: 0.0, y: 3.0})
	])`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	polygonMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("polygon() should return map, got %T", result)
	}
	
	if polygonMap["type"] != "polygon" {
		t.Errorf("polygon type = %v, want 'polygon'", polygonMap["type"])
	}
	
	points, ok := polygonMap["points"].([]interface{})
	if !ok {
		t.Fatal("polygon should have points list")
	}
	
	if len(points) != 4 {
		t.Errorf("polygon should have 4 points, got %d", len(points))
	}
}

func TestPolygonFunctionInvalid(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with too few points (less than 3)
	expr := `polygon([point({x: 0.0, y: 0.0}), point({x: 1.0, y: 1.0})])`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != nil {
		t.Error("polygon with less than 3 points should return nil")
	}
}

func TestLineStringFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Test creating a lineString from a list of points
	expr := `lineString([
		point({x: 0.0, y: 0.0}),
		point({x: 1.0, y: 1.0}),
		point({x: 2.0, y: 0.0})
	])`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	lineMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("lineString() should return map, got %T", result)
	}
	
	if lineMap["type"] != "linestring" {
		t.Errorf("lineString type = %v, want 'linestring'", lineMap["type"])
	}
	
	points, ok := lineMap["points"].([]interface{})
	if !ok {
		t.Fatal("lineString should have points list")
	}
	
	if len(points) != 3 {
		t.Errorf("lineString should have 3 points, got %d", len(points))
	}
}

func TestLineStringFunctionInvalid(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with too few points (less than 2)
	expr := `lineString([point({x: 0.0, y: 0.0})])`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != nil {
		t.Error("lineString with less than 2 points should return nil")
	}
}

func TestPointIntersectsPolygon(t *testing.T) {
	e := setupTestExecutor(t)

	// Create a square polygon
	polygonExpr := `polygon([
		point({x: 0.0, y: 0.0}),
		point({x: 4.0, y: 0.0}),
		point({x: 4.0, y: 3.0}),
		point({x: 0.0, y: 3.0})
	])`
	polygon := e.evaluateExpressionWithContext(polygonExpr, nil, nil)
	
	tests := []struct {
		name     string
		point    string
		expected bool
	}{
		{
			name:     "point inside polygon",
			point:    "point({x: 2.0, y: 1.5})",
			expected: true,
		},
		{
			name:     "point outside polygon",
			point:    "point({x: 5.0, y: 5.0})",
			expected: false,
		},
		{
			name:     "point on corner",
			point:    "point({x: 0.0, y: 0.0})",
			expected: true,
		},
		{
			name:     "point on edge",
			point:    "point({x: 2.0, y: 0.0})",
			expected: true,
		},
		{
			name:     "point just outside",
			point:    "point({x: -0.1, y: 1.0})",
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			point := e.evaluateExpressionWithContext(tt.point, nil, nil)
			
			// Create a node with the polygon for testing
			polygonMap := polygon.(map[string]interface{})
			pointMap := point.(map[string]interface{})
			
			// Build the point.intersects expression manually
			result := e.evaluateExpressionWithContext("", nil, nil)
			
			// Test using the helper function directly
			points := extractPolygonPoints(polygonMap)
			px, py, _ := getXY(pointMap)
			result = pointInPolygon(px, py, points)
			
			if result != tt.expected {
				t.Errorf("point.intersects(%s, polygon) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

func TestPointIntersectsFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Test point.intersects with a square polygon
	// Point inside polygon
	expr := `point.intersects(
		point({x: 2.0, y: 1.5}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 4.0, y: 0.0}),
			point({x: 4.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		])
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point.intersects should return true for point inside polygon")
	}

	// Point outside polygon
	expr = `point.intersects(
		point({x: 5.0, y: 5.0}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 4.0, y: 0.0}),
			point({x: 4.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		])
	)`
	result = e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point.intersects should return false for point outside polygon")
	}
}

func TestPointContainsFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Test point.contains with a square polygon
	// Point inside polygon
	expr := `point.contains(
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 4.0, y: 0.0}),
			point({x: 4.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		]),
		point({x: 2.0, y: 1.5})
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point.contains should return true for point inside polygon")
	}

	// Point outside polygon
	expr = `point.contains(
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 4.0, y: 0.0}),
			point({x: 4.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		]),
		point({x: 5.0, y: 5.0})
	)`
	result = e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point.contains should return false for point outside polygon")
	}
}

func TestPointIntersectsWithLatLon(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with geographic coordinates (latitude/longitude)
	// Create a polygon around a region (simplified NYC area)
	expr := `point.intersects(
		point({latitude: 40.7128, longitude: -74.0060}),
		polygon([
			point({latitude: 40.5, longitude: -74.5}),
			point({latitude: 40.5, longitude: -73.5}),
			point({latitude: 41.0, longitude: -73.5}),
			point({latitude: 41.0, longitude: -74.5})
		])
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point.intersects should work with lat/lon coordinates")
	}
}

func TestPointContainsWithLatLon(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with geographic coordinates (latitude/longitude)
	expr := `point.contains(
		polygon([
			point({latitude: 40.5, longitude: -74.5}),
			point({latitude: 40.5, longitude: -73.5}),
			point({latitude: 41.0, longitude: -73.5}),
			point({latitude: 41.0, longitude: -74.5})
		]),
		point({latitude: 40.7128, longitude: -74.0060})
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point.contains should work with lat/lon coordinates")
	}
}

func TestComplexPolygonShape(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with a more complex polygon (L-shape)
	expr := `point.intersects(
		point({x: 1.5, y: 1.5}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 3.0, y: 0.0}),
			point({x: 3.0, y: 1.0}),
			point({x: 1.0, y: 1.0}),
			point({x: 1.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		])
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point should be outside L-shaped polygon")
	}

	// Point inside the L-shape
	expr = `point.intersects(
		point({x: 0.5, y: 0.5}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 3.0, y: 0.0}),
			point({x: 3.0, y: 1.0}),
			point({x: 1.0, y: 1.0}),
			point({x: 1.0, y: 3.0}),
			point({x: 0.0, y: 3.0})
		])
	)`
	result = e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point should be inside L-shaped polygon")
	}
}

func TestTrianglePolygon(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with a triangle (minimum polygon)
	expr := `point.intersects(
		point({x: 1.0, y: 1.0}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 3.0, y: 0.0}),
			point({x: 1.5, y: 2.0})
		])
	)`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != true {
		t.Error("point should be inside triangle")
	}

	// Point outside triangle
	expr = `point.intersects(
		point({x: 3.0, y: 3.0}),
		polygon([
			point({x: 0.0, y: 0.0}),
			point({x: 3.0, y: 0.0}),
			point({x: 1.5, y: 2.0})
		])
	)`
	result = e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point should be outside triangle")
	}
}

func TestEdgeCases(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with invalid polygon (no points)
	expr := `point.intersects(point({x: 1.0, y: 1.0}), polygon([]))`
	result := e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point.intersects should return false for empty polygon")
	}

	// Test with invalid point format
	expr = `point.intersects(point({z: 1.0}), polygon([
		point({x: 0.0, y: 0.0}),
		point({x: 1.0, y: 0.0}),
		point({x: 1.0, y: 1.0})
	]))`
	result = e.evaluateExpressionWithContext(expr, nil, nil)
	
	if result != false {
		t.Error("point.intersects should return false for invalid point format")
	}
}
