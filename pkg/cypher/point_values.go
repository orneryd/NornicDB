package cypher

import (
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"reflect"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

// CypherPoint is a Cypher POINT value: a coordinate reference system,
// identified by its SRID, and two or three coordinates. For geographic
// points X is the longitude, Y the latitude and Z the height, as in Neo4j.
// Points are stored as properties (msgpack extension 47), sent over Bolt as
// Point2D / Point3D structures and over HTTP as GeoJSON-like maps (#817).
type CypherPoint struct {
	SRID    int
	X, Y, Z float64
}

// pointCRS describes one of the coordinate reference systems Neo4j supports.
type pointCRS struct {
	srid       int
	name       string
	geographic bool
	dimensions int
	href       string
}

var pointCRSs = []pointCRS{
	{srid: 7203, name: "cartesian", dimensions: 2, href: "https://spatialreference.org/ref/sr-org/7203/ogcwkt/"},
	{srid: 9157, name: "cartesian-3d", dimensions: 3, href: "https://spatialreference.org/ref/sr-org/9157/ogcwkt/"},
	{srid: 4326, name: "wgs-84", geographic: true, dimensions: 2, href: "https://spatialreference.org/ref/epsg/4326/ogcwkt/"},
	{srid: 4979, name: "wgs-84-3d", geographic: true, dimensions: 3, href: "https://spatialreference.org/ref/epsg/4979/ogcwkt/"},
}

func pointCRSBySRID(srid int) (pointCRS, bool) {
	for _, crs := range pointCRSs {
		if crs.srid == srid {
			return crs, true
		}
	}
	return pointCRS{}, false
}

func pointCRSByName(name string) (pointCRS, bool) {
	for _, crs := range pointCRSs {
		if strings.EqualFold(crs.name, name) {
			return crs, true
		}
	}
	return pointCRS{}, false
}

// crs returns the point's coordinate reference system; every CypherPoint is
// built with a known SRID.
func (p CypherPoint) crs() pointCRS {
	crs, _ := pointCRSBySRID(p.SRID)
	return crs
}

// Is3D reports whether the point has a third coordinate.
func (p CypherPoint) Is3D() bool { return p.crs().dimensions == 3 }

// CRSName is the coordinate reference system's name (cartesian, wgs-84-3d…).
func (p CypherPoint) CRSName() string { return p.crs().name }

// CRSHref is the spatialreference.org link Neo4j's HTTP results carry.
func (p CypherPoint) CRSHref() string { return p.crs().href }

// Coordinates are the point's coordinates in order (x, y[, z]).
func (p CypherPoint) Coordinates() []float64 {
	if p.Is3D() {
		return []float64{p.X, p.Y, p.Z}
	}
	return []float64{p.X, p.Y}
}

// String is the point's own text, as Neo4j shows the value:
// point({srid:7203, x:1.0, y:2.0}).
func (p CypherPoint) String() string {
	text := "point({srid:" + strconv.Itoa(p.SRID) + ", x:" + FormatFloat(p.X, 64) + ", y:" + FormatFloat(p.Y, 64)
	if p.Is3D() {
		text += ", z:" + FormatFloat(p.Z, 64)
	}
	return text + "})"
}

// cypherText is the text toString() gives a point:
// point({x: 1.0, y: 2.0, crs: 'cartesian'}).
func (p CypherPoint) cypherText() string {
	text := "point({x: " + FormatFloat(p.X, 64) + ", y: " + FormatFloat(p.Y, 64)
	if p.Is3D() {
		text += ", z: " + FormatFloat(p.Z, 64)
	}
	return text + ", crs: '" + p.CRSName() + "'})"
}

// PropertyValueKind identifies points as durable typed properties.
func (CypherPoint) PropertyValueKind() string { return "point" }

func pointArgumentError(message string) error {
	return newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", message)
}

// newPointFromMap builds a point from point()'s map argument as Neo4j does:
// x / y[ / z] make a cartesian point and latitude / longitude[ / height or z]
// a geographic one, unless crs or srid names the system. ok is false when a
// coordinate is null: point() is then null. Longitudes wrap into
// [-180, 180]; latitudes outside [-90, 90] are an error.
func newPointFromMap(fields map[string]interface{}) (point CypherPoint, ok bool, err error) {
	coordinate := func(key string) (float64, bool, bool, error) {
		value, present := fields[key]
		if !present {
			return 0, false, false, nil
		}
		if value == nil {
			return 0, true, true, nil
		}
		number, numeric := toFloat64(value)
		if !numeric {
			return 0, true, false, pointArgumentError(fmt.Sprintf("Cannot assign %s to field '%s'", neo4jValueDescription(value), fmt.Sprint(value)))
		}
		return number, true, false, nil
	}
	var crs pointCRS
	var coordinates []float64
	var null bool
	read := func(keys ...string) (bool, error) {
		values := make([]float64, 0, len(keys))
		for _, key := range keys {
			value, present, isNull, err := coordinate(key)
			if err != nil {
				return false, err
			}
			if !present {
				return false, nil
			}
			null = null || isNull
			values = append(values, value)
		}
		coordinates = values
		return true, nil
	}
	geographic := false
	if found, err := read("x", "y"); err != nil {
		return CypherPoint{}, false, err
	} else if found {
		if z, present, isNull, err := coordinate("z"); err != nil {
			return CypherPoint{}, false, err
		} else if present {
			null = null || isNull
			coordinates = append(coordinates, z)
		}
	} else if found, err := read("longitude", "latitude"); err != nil {
		return CypherPoint{}, false, err
	} else if found {
		geographic = true
		height, present, isNull, err := coordinate("height")
		if err != nil {
			return CypherPoint{}, false, err
		}
		if !present {
			height, present, isNull, err = coordinate("z")
			if err != nil {
				return CypherPoint{}, false, err
			}
		}
		if present {
			null = null || isNull
			coordinates = append(coordinates, height)
		}
	} else {
		// A map literal without coordinates fails when the statement compiles
		// (checkStaticLiteralArguments); a map value fails here.
		return CypherPoint{}, false, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidPoint",
			localization.CypherCorePointCoordinatesMissing())
	}
	if null {
		return CypherPoint{}, false, nil
	}

	crsValue, hasCRS := fields["crs"]
	sridValue, hasSRID := fields["srid"]
	switch {
	case hasCRS && hasSRID:
		return CypherPoint{}, false, pointArgumentError("Cannot specify both CRS and SRID")
	case hasCRS:
		name, _ := crsValue.(string)
		found, known := pointCRSByName(name)
		if !known {
			return CypherPoint{}, false, pointArgumentError("Unknown coordinate reference system: " + fmt.Sprint(crsValue))
		}
		crs = found
	case hasSRID:
		code, _ := toFloat64(sridValue)
		found, known := pointCRSBySRID(int(code))
		if !known {
			return CypherPoint{}, false, pointArgumentError("Unknown coordinate reference system: code=" + fmt.Sprint(sridValue))
		}
		crs = found
	default:
		for _, candidate := range pointCRSs {
			if candidate.geographic == geographic && candidate.dimensions == len(coordinates) {
				crs = candidate
			}
		}
	}
	if geographic && !crs.geographic {
		return CypherPoint{}, false, pointArgumentError("Geographic points does not support coordinate reference system: " + crs.name + ".This is set either in the csv header or the actual data column")
	}
	if crs.dimensions != len(coordinates) {
		if crs.dimensions == 2 {
			return CypherPoint{}, false, pointArgumentError("Cannot create point with 2D coordinate reference system and 3 coordinates. Please consider using equivalent 3D coordinate reference system")
		}
		return CypherPoint{}, false, pointArgumentError("Cannot create point with 3D coordinate reference system and 2 coordinates. Please consider using equivalent 2D coordinate reference system")
	}
	point = CypherPoint{SRID: crs.srid, X: coordinates[0], Y: coordinates[1]}
	if len(coordinates) == 3 {
		point.Z = coordinates[2]
	}
	if crs.geographic {
		if point.Y < -90 || point.Y > 90 {
			return CypherPoint{}, false, pointArgumentError(fmt.Sprintf("Cannot create WGS84 point with invalid coordinate: [%s, %s]. Valid range for Y coordinate is [-90, 90].", FormatFloat(point.X, 64), FormatFloat(point.Y, 64)))
		}
		point.X = wrapLongitude(point.X)
	}
	return point, true, nil
}

// wrapLongitude brings a longitude into [-180, 180], as Neo4j does
// (200 becomes -160).
func wrapLongitude(longitude float64) float64 {
	if longitude >= -180 && longitude <= 180 {
		return longitude
	}
	wrapped := math.Mod(longitude+180, 360)
	if wrapped < 0 {
		wrapped += 360
	}
	return wrapped - 180
}

// neo4jValueDescription names a value as Neo4j's error messages do:
// String("a"), Long(1), Double(1.5), Boolean(true).
func neo4jValueDescription(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return "String(" + strconv.Quote(typed) + ")"
	case bool:
		return "Boolean(" + strconv.FormatBool(typed) + ")"
	}
	if integer, ok := cypherIntegerValue(value); ok {
		return "Long(" + strconv.FormatInt(integer, 10) + ")"
	}
	if number, ok := toFloat64(value); ok {
		return "Double(" + FormatFloat(number, 64) + ")"
	}
	return fmt.Sprint(value)
}

// pointValue returns value as a point.
func pointValue(value interface{}) (CypherPoint, bool) {
	switch typed := value.(type) {
	case CypherPoint:
		return typed, true
	case *CypherPoint:
		if typed != nil {
			return *typed, true
		}
	}
	return CypherPoint{}, false
}

// evaluatePointProperty reads a point's field (x, y, z, latitude, longitude,
// height, crs, srid). The second result identifies points so callers do not
// read the field as a map key; an unknown or unavailable field is Neo4j's
// ArgumentError.
func evaluatePointProperty(value interface{}, property string) (interface{}, bool, error) {
	point, ok := pointValue(value)
	if !ok {
		return nil, false, nil
	}
	crs := point.crs()
	switch property {
	case "x":
		return point.X, true, nil
	case "y":
		return point.Y, true, nil
	case "z":
		if crs.dimensions == 3 {
			return point.Z, true, nil
		}
		return nil, true, pointArgumentError("Field: z is not available on point: " + point.cypherText())
	case "longitude", "latitude", "height":
		if !crs.geographic {
			return nil, true, pointArgumentError("Field: " + property + " is not available on cartesian point: " + point.cypherText())
		}
		switch property {
		case "longitude":
			return point.X, true, nil
		case "latitude":
			return point.Y, true, nil
		}
		if crs.dimensions == 3 {
			return point.Z, true, nil
		}
		return nil, true, pointArgumentError("Field: height is not available on point: " + point.cypherText())
	case "crs":
		return crs.name, true, nil
	case "srid":
		return int64(point.SRID), true, nil
	}
	return nil, true, pointArgumentError("No such field: " + property)
}

// comparePointValues is point equality: the same coordinate reference system
// and coordinates. The second result reports that either value is a point.
func comparePointValues(left, right interface{}) (bool, bool) {
	leftPoint, leftOK := pointValue(left)
	rightPoint, rightOK := pointValue(right)
	if !leftOK && !rightOK {
		return false, false
	}
	return leftOK && rightOK && leftPoint == rightPoint, true
}

// comparePointOrdering orders points for ORDER BY: by SRID, then x, y, z.
func comparePointOrdering(left, right interface{}) (int, bool) {
	leftPoint, leftOK := pointValue(left)
	rightPoint, rightOK := pointValue(right)
	if !leftOK || !rightOK {
		return 0, false
	}
	if leftPoint.SRID != rightPoint.SRID {
		return compareOrderedInts(leftPoint.SRID, rightPoint.SRID), true
	}
	for _, pair := range [][2]float64{{leftPoint.X, rightPoint.X}, {leftPoint.Y, rightPoint.Y}, {leftPoint.Z, rightPoint.Z}} {
		if pair[0] < pair[1] {
			return -1, true
		}
		if pair[0] > pair[1] {
			return 1, true
		}
	}
	return 0, true
}

// earthRadiusMeters is the radius Neo4j's WGS-84 distance uses.
const earthRadiusMeters = 6378140.0

// pointDistance is point.distance: Euclidean for cartesian points, the
// haversine great-circle distance for geographic points (for 3D, the arc at
// the points' average height combined with their height difference), and
// null for points of different systems.
func pointDistance(left, right CypherPoint) (float64, bool) {
	if left.SRID != right.SRID {
		return 0, false
	}
	if !left.crs().geographic {
		dx, dy, dz := left.X-right.X, left.Y-right.Y, left.Z-right.Z
		return math.Sqrt(dx*dx + dy*dy + dz*dz), true
	}
	if !left.Is3D() {
		return haversineDistance(left.Y, left.X, right.Y, right.X), true
	}
	// 3D: the arc at the points' average height, then the height
	// difference, as Neo4j measures it.
	arc := (earthRadiusMeters + (left.Z+right.Z)/2) * greatCircleAngle(left.Y, left.X, right.Y, right.X)
	dz := left.Z - right.Z
	return math.Sqrt(arc*arc + dz*dz), true
}

// pointWithinBBox is point.withinBBox: whether point lies in the box with the
// given lower-left and upper-right corners (all of one system); ok is false
// (null) for points of different systems.
func pointWithinBBox(point, lowerLeft, upperRight CypherPoint) (bool, bool) {
	if point.SRID != lowerLeft.SRID || point.SRID != upperRight.SRID {
		return false, false
	}
	within := func(value, low, high float64) bool { return value >= low && value <= high }
	inside := within(point.Y, lowerLeft.Y, upperRight.Y)
	if point.crs().geographic && lowerLeft.X > upperRight.X {
		// A geographic box that crosses the antimeridian.
		inside = inside && (point.X >= lowerLeft.X || point.X <= upperRight.X)
	} else {
		inside = inside && within(point.X, lowerLeft.X, upperRight.X)
	}
	if point.Is3D() {
		inside = inside && within(point.Z, lowerLeft.Z, upperRight.Z)
	}
	return inside, true
}

func init() {
	msgpack.RegisterExtEncoder(47, CypherPoint{}, func(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
		point := value.Interface().(CypherPoint)
		data := make([]byte, 4+8*3)
		binary.BigEndian.PutUint32(data, uint32(point.SRID))
		binary.BigEndian.PutUint64(data[4:], math.Float64bits(point.X))
		binary.BigEndian.PutUint64(data[12:], math.Float64bits(point.Y))
		binary.BigEndian.PutUint64(data[20:], math.Float64bits(point.Z))
		return data, nil
	})
	msgpack.RegisterExtDecoder(47, CypherPoint{}, func(decoder *msgpack.Decoder, value reflect.Value, length int) error {
		data := make([]byte, length)
		if err := decoder.ReadFull(data); err != nil {
			return err
		}
		if len(data) != 4+8*3 {
			return fmt.Errorf("point: stored value has %d bytes, want 28", len(data))
		}
		point := CypherPoint{
			SRID: int(binary.BigEndian.Uint32(data)),
			X:    math.Float64frombits(binary.BigEndian.Uint64(data[4:])),
			Y:    math.Float64frombits(binary.BigEndian.Uint64(data[12:])),
			Z:    math.Float64frombits(binary.BigEndian.Uint64(data[20:])),
		}
		if _, known := pointCRSBySRID(point.SRID); !known {
			return fmt.Errorf("point: stored value has unknown SRID %d", point.SRID)
		}
		value.Set(reflect.ValueOf(point))
		return nil
	})
}

var _ storage.TypedPropertyValue = CypherPoint{}

// fieldMap is the point's fields as a map (x, y[, z], srid, crs, and for
// geographic points latitude, longitude[, height]).
func (p CypherPoint) fieldMap() map[string]interface{} {
	crs := p.crs()
	fields := map[string]interface{}{"x": p.X, "y": p.Y, "srid": int64(p.SRID), "crs": crs.name}
	if crs.dimensions == 3 {
		fields["z"] = p.Z
	}
	if crs.geographic {
		fields["longitude"], fields["latitude"] = p.X, p.Y
		if crs.dimensions == 3 {
			fields["height"] = p.Z
		}
	}
	return fields
}

// spatialMap reads a point, or a map describing one, as a field map, for
// NornicDB's spatial extension functions (point.x(), polygon(), …).
func spatialMap(value interface{}) (map[string]interface{}, bool) {
	if point, ok := pointValue(value); ok {
		return point.fieldMap(), true
	}
	fields, ok := value.(map[string]interface{})
	return fields, ok
}

// NewCypherPoint builds a point from an SRID and its coordinates, as Bolt's
// Point2D / Point3D structures carry it. ok is false for an unknown SRID or
// a coordinate count that does not match its coordinate reference system.
func NewCypherPoint(srid int, coordinates ...float64) (CypherPoint, bool) {
	crs, known := pointCRSBySRID(srid)
	if !known || crs.dimensions != len(coordinates) {
		return CypherPoint{}, false
	}
	point := CypherPoint{SRID: srid, X: coordinates[0], Y: coordinates[1]}
	if len(coordinates) == 3 {
		point.Z = coordinates[2]
	}
	return point, true
}
