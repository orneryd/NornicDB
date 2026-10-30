// Package angle converts between degrees and radians the way Java's
// Math.toRadians / Math.toDegrees do: one multiplication by a constant. A
// division by π and a multiplication by 180 round twice, which differs from
// Neo4j in the last bit for about a quarter of inputs (#907); Cypher's
// radians() / degrees(), the geographic point functions and the APOC spatial
// procedures all convert through here.
package angle

const (
	// degreesToRadians is π / 180 rounded once (Java's DEGREES_TO_RADIANS).
	degreesToRadians = 0.017453292519943295
	// radiansToDegrees is 180 / π rounded once (Java's RADIANS_TO_DEGREES).
	radiansToDegrees = 57.29577951308232
)

// ToRadians converts an angle in degrees to radians.
func ToRadians(degrees float64) float64 { return degrees * degreesToRadians }

// ToDegrees converts an angle in radians to degrees.
func ToDegrees(radians float64) float64 { return radians * radiansToDegrees }
