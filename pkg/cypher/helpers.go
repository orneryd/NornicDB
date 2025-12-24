package cypher

import (
	"math"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// toFloat64 is a package-level alias to convert.ToFloat64 for internal use.
// This avoids updating 100+ call sites while still consolidating the implementation.
func toFloat64(v interface{}) (float64, bool) {
	return convert.ToFloat64(v)
}

// toFloat64Slice is a package-level alias to convert.ToFloat64Slice for internal use.
func toFloat64Slice(v interface{}) ([]float64, bool) {
	return convert.ToFloat64Slice(v)
}

// findKeywordIndex finds a keyword at word boundaries in the string.
// This prevents matching substrings like "RemoveReturn" when searching for "RETURN".
// Also prevents matching labels like ":Return" as keywords.
// Returns -1 if not found.
func findKeywordIndex(s, keyword string) int {
	return keywordIndex(s, keyword)
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
	if substr == "" {
		return false
	}
	first := substr[0]

	var (
		inSingleQuote  bool
		inDoubleQuote  bool
		inBacktick     bool
		inLineComment  bool
		inBlockComment bool
	)

	for i := 0; i < len(s); i++ {
		c := s[i]

		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if inSingleQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}
		if inBacktick {
			if c == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}

		if c == '/' && i+1 < len(s) {
			if s[i+1] == '/' {
				inLineComment = true
				i++
				continue
			}
			if s[i+1] == '*' {
				inBlockComment = true
				i++
				continue
			}
		}

		if c == '\'' {
			inSingleQuote = true
			continue
		}
		if c == '"' {
			inDoubleQuote = true
			continue
		}
		if c == '`' {
			inBacktick = true
			continue
		}

		if c != first {
			continue
		}
		if i+len(substr) > len(s) {
			return false
		}
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
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

// findMultiWordKeywordIndex finds a multi-word keyword with flexible whitespace.
// For example, "SHOW DATABASES" will match "SHOW DATABASES", "SHOW\tDATABASES", "SHOW\nDATABASES", etc.
// This is used for keywords like "SHOW DATABASES", "CREATE DATABASE", "DROP DATABASE", "OPTIONAL MATCH".
//
// Parameters:
//   - s: The string to search in
//   - firstWord: First word of the keyword (e.g., "SHOW")
//   - secondWord: Second word of the keyword (e.g., "DATABASES")
//
// Returns the position of the first word if the multi-word keyword is found, -1 otherwise.
func findMultiWordKeywordIndex(s, firstWord, secondWord string) int {
	opts := defaultKeywordScanOpts()

	firstStart, firstEnd := trimKeywordWSBounds(firstWord)
	secondStart, secondEnd := trimKeywordWSBounds(secondWord)
	if firstStart >= firstEnd || secondStart >= secondEnd {
		return -1
	}

	searchFrom := 0
	for {
		idx := keywordIndexFrom(s, firstWord[firstStart:firstEnd], searchFrom, opts)
		if idx == -1 {
			return -1
		}

		afterFirst, ok := keywordMatchAt(s, idx, firstWord, firstStart, firstEnd)
		if !ok {
			searchFrom = idx + 1
			continue
		}

		j := afterFirst
		if j >= len(s) || !isASCIISpace(s[j]) {
			searchFrom = idx + 1
			continue
		}
		for j < len(s) && isASCIISpace(s[j]) {
			j++
		}

		afterSecond, ok := keywordMatchAt(s, j, secondWord, secondStart, secondEnd)
		if !ok {
			searchFrom = idx + 1
			continue
		}

		if keywordRightBoundaryOK(s, afterSecond, opts.Boundary) {
			return idx
		}

		searchFrom = idx + 1
	}
}
