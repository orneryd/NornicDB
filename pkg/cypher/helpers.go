package cypher

import (
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ========================================
// Keyword Detection Functions
// ========================================

// isWordBoundary checks if a character is a word boundary (not alphanumeric or underscore)
// In Cypher, ':' precedes labels so it's NOT a valid left boundary for keywords
func isWordBoundary(r rune) bool {
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
}

// isLeftKeywordBoundary checks if a character can precede a keyword
// Colon is excluded because it precedes labels in Cypher (e.g., :Label)
func isLeftKeywordBoundary(r rune) bool {
	if r == ':' {
		return false // Colon precedes labels, not keywords
	}
	return isWordBoundary(r)
}

// findKeywordIndex finds a keyword at word boundaries in the string.
// This prevents matching substrings like "RemoveReturn" when searching for "RETURN".
// Also prevents matching labels like ":Return" as keywords.
// Returns -1 if not found.
func findKeywordIndex(s, keyword string) int {
	upper := strings.ToUpper(s)
	keywordUpper := strings.ToUpper(keyword)
	keyLen := len(keywordUpper)

	// Build a map of positions that are inside string literals
	inStringLiteral := makeStringLiteralMask(s)

	idx := 0
	for {
		pos := strings.Index(upper[idx:], keywordUpper)
		if pos == -1 {
			return -1
		}
		absolutePos := idx + pos

		// Skip if this position is inside a string literal
		if absolutePos < len(inStringLiteral) && inStringLiteral[absolutePos] {
			idx = absolutePos + 1
			if idx >= len(upper) {
				return -1
			}
			continue
		}

		// Check left boundary (before keyword) - use special boundary check
		// that excludes ':' to avoid matching labels
		leftOK := absolutePos == 0 || isLeftKeywordBoundary(rune(upper[absolutePos-1]))

		// Check right boundary (after keyword)
		endPos := absolutePos + keyLen
		rightOK := endPos >= len(upper) || isWordBoundary(rune(upper[endPos]))

		if leftOK && rightOK {
			return absolutePos
		}

		// Move past this occurrence and continue searching
		idx = absolutePos + 1
		if idx >= len(upper) {
			return -1
		}
	}
}

// containsKeywordOutsideStrings checks if a keyword exists in the string but NOT inside
// string literals. This prevents matching keywords inside user data content.
func containsKeywordOutsideStrings(s, keyword string) bool {
	return findKeywordIndex(s, keyword) != -1
}

// containsOutsideStrings checks if a substring exists in the string but NOT inside
// string literals. Unlike containsKeywordOutsideStrings, this does exact substring matching
// without word boundary checks (useful for symbols like -> and <-).
func containsOutsideStrings(s, substr string) bool {
	mask := makeStringLiteralMask(s)
	idx := 0
	for {
		pos := strings.Index(s[idx:], substr)
		if pos == -1 {
			return false
		}
		absolutePos := idx + pos

		// Check if this position is outside string literals
		insideString := false
		for i := 0; i < len(substr); i++ {
			checkPos := absolutePos + i
			if checkPos < len(mask) && mask[checkPos] {
				insideString = true
				break
			}
		}

		if !insideString {
			return true
		}

		// Move past this occurrence and continue searching
		idx = absolutePos + 1
		if idx >= len(s) {
			return false
		}
	}
}

// makeStringLiteralMask creates a boolean slice where true means that position
// is inside a string literal (single or double quoted).
// This is used to skip string contents when searching for keywords.
func makeStringLiteralMask(s string) []bool {
	mask := make([]bool, len(s))
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]

		// Check for escaped quotes
		if c == '\\' && i+1 < len(s) {
			// Skip the escaped character
			if inString {
				mask[i] = true
				mask[i+1] = true
			}
			i++
			continue
		}

		// Handle string boundaries
		if c == '\'' || c == '"' {
			if !inString {
				// Starting a string
				inString = true
				stringChar = c
				mask[i] = true // The quote itself is part of the string
			} else if c == stringChar {
				// Ending the string
				mask[i] = true // The closing quote is part of the string
				inString = false
				stringChar = 0
			} else {
				// Different quote type inside string, just mark as inside
				mask[i] = true
			}
			continue
		}

		// Mark positions inside strings
		if inString {
			mask[i] = true
		}
	}

	return mask
}

// ========================================
// Type Conversion Functions
// ========================================

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// toFloat64Slice converts a value to a slice of float64
func toFloat64Slice(v interface{}) ([]float64, bool) {
	switch val := v.(type) {
	case []float64:
		return val, true
	case []interface{}:
		result := make([]float64, len(val))
		for i, item := range val {
			if f, ok := toFloat64(item); ok {
				result[i] = f
			} else {
				return nil, false
			}
		}
		return result, true
	}
	return nil, false
}

// ========================================
// Math Helper Functions
// ========================================

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ========================================
// Vector Similarity Functions
// ========================================

// cosineSimilarity calculates cosine similarity between two vectors
func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// euclideanSimilarity calculates similarity based on Euclidean distance
func euclideanSimilarity(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return 1.0 / (1.0 + math.Sqrt(sum))
}

// ========================================
// Spatial Helper Functions
// ========================================

// getXY extracts x, y coordinates from a map
func getXY(m map[string]interface{}) (float64, float64, bool) {
	x, okX := toFloat64(m["x"])
	y, okY := toFloat64(m["y"])
	return x, y, okX && okY
}

// getLatLon extracts latitude, longitude from a map
func getLatLon(m map[string]interface{}) (float64, float64, bool) {
	lat, okLat := toFloat64(m["latitude"])
	if !okLat {
		lat, okLat = toFloat64(m["lat"])
	}
	lon, okLon := toFloat64(m["longitude"])
	if !okLon {
		lon, okLon = toFloat64(m["lon"])
	}
	return lat, lon, okLat && okLon
}

// haversineDistance calculates the distance between two lat/lon points in meters
func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadius = 6371000 // meters

	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	deltaLat := (lat2 - lat1) * math.Pi / 180
	deltaLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// pointInPolygon uses the ray casting algorithm to determine if a point is inside a polygon.
// The polygon is defined by a list of point maps (with x,y or latitude,longitude coordinates).
// Returns true if the point is inside or on the boundary of the polygon.
func pointInPolygon(px, py float64, polygonPoints []interface{}) bool {
	if len(polygonPoints) < 3 {
		return false // A valid polygon needs at least 3 points
	}

	// Extract coordinates from polygon points
	coords := make([][2]float64, 0, len(polygonPoints))
	for _, p := range polygonPoints {
		pm, ok := p.(map[string]interface{})
		if !ok {
			return false
		}

		// Try x/y coordinates
		x, y, hasXY := getXY(pm)
		if hasXY {
			coords = append(coords, [2]float64{x, y})
			continue
		}

		// Try lat/lon coordinates
		lat, lon, hasLatLon := getLatLon(pm)
		if hasLatLon {
			coords = append(coords, [2]float64{lat, lon})
			continue
		}

		return false // Invalid point format
	}

	if len(coords) < 3 {
		return false
	}

	// Ray casting algorithm: count how many times a ray from the point
	// to infinity crosses the polygon edges. Odd = inside, Even = outside.
	inside := false
	j := len(coords) - 1

	for i := 0; i < len(coords); i++ {
		xi, yi := coords[i][0], coords[i][1]
		xj, yj := coords[j][0], coords[j][1]

		// Check if point is on a horizontal edge
		if yi == py && yj == py && ((xi <= px && px <= xj) || (xj <= px && px <= xi)) {
			return true // On boundary
		}

		// Ray casting check
		if ((yi > py) != (yj > py)) &&
			(px < (xj-xi)*(py-yi)/(yj-yi)+xi) {
			inside = !inside
		}

		j = i
	}

	return inside
}

// extractPolygonPoints extracts point list from a polygon/lineString map
func extractPolygonPoints(geom map[string]interface{}) []interface{} {
	if points, ok := geom["points"]; ok {
		if pointList, ok := points.([]interface{}); ok {
			return pointList
		}
	}
	// Try "coordinates" as alternative (GeoJSON style)
	if coords, ok := geom["coordinates"]; ok {
		if coordList, ok := coords.([]interface{}); ok {
			return coordList
		}
	}
	return nil
}
