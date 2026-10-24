package storage

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// RemoteTypedValue is a temporal or spatial value a remote participant
// returned, in one representation for both transports. Type is Neo4j's type
// code. A date, time, localtime, datetime, localdatetime or duration carries
// its ISO 8601 Text; a point carries its SRID and Coordinates (x, y and, in
// three dimensions, z). Bolt delivers these values as driver values
// (normalizeBoltGraphValue) and HTTP as text typed by the row's metadata
// (decodeRemoteHTTPValue); the Cypher executor turns them into its own values.
type RemoteTypedValue struct {
	Type        string
	Text        string
	SRID        int
	Coordinates []float64
}

// remoteTypedFromBolt converts a driver temporal or point value. A zoned
// datetime keeps its zone ID; the driver names a fixed offset "Offset".
func remoteTypedFromBolt(value interface{}) (RemoteTypedValue, bool) {
	switch v := value.(type) {
	case dbtype.Date:
		return RemoteTypedValue{Type: "date", Text: time.Time(v).Format("2006-01-02")}, true
	case dbtype.LocalTime:
		return RemoteTypedValue{Type: "localtime", Text: time.Time(v).Format("15:04:05.999999999")}, true
	case dbtype.Time:
		return RemoteTypedValue{Type: "time", Text: time.Time(v).Format("15:04:05.999999999" + remoteOffsetLayout(time.Time(v)))}, true
	case dbtype.LocalDateTime:
		return RemoteTypedValue{Type: "localdatetime", Text: time.Time(v).Format("2006-01-02T15:04:05.999999999")}, true
	case time.Time:
		text := v.Format("2006-01-02T15:04:05.999999999" + remoteOffsetLayout(v))
		if zone := v.Location().String(); zone != "Offset" && zone != "Local" && zone != "" {
			text += "[" + zone + "]"
		}
		return RemoteTypedValue{Type: "datetime", Text: text}, true
	case dbtype.Duration:
		return RemoteTypedValue{Type: "duration", Text: v.String()}, true
	case dbtype.Point2D:
		return RemoteTypedValue{Type: "point", SRID: int(v.SpatialRefId), Coordinates: []float64{v.X, v.Y}}, true
	case dbtype.Point3D:
		return RemoteTypedValue{Type: "point", SRID: int(v.SpatialRefId), Coordinates: []float64{v.X, v.Y, v.Z}}, true
	}
	return RemoteTypedValue{}, false
}

// remoteOffsetLayout is the layout of t's zone offset as Neo4j writes it:
// with its seconds when they are not zero (+02:05:59, as a historical zone's
// local mean time can have), else hours and minutes.
func remoteOffsetLayout(t time.Time) string {
	if _, offset := t.Zone(); offset%60 != 0 {
		return "Z07:00:00"
	}
	return "Z07:00"
}

// remoteTypedFromHTTP converts a value that Neo4j's HTTP row metadata types
// as kind: temporal values are their text, a point is the map
// {"type": "Point", "coordinates": [...], "crs": {"srid": ...}}.
func remoteTypedFromHTTP(kind string, plain interface{}) (RemoteTypedValue, bool) {
	switch kind {
	case "date", "time", "localtime", "datetime", "localdatetime", "duration":
		text, ok := plain.(string)
		return RemoteTypedValue{Type: kind, Text: text}, ok
	case "point":
		point, ok := plain.(map[string]interface{})
		if !ok {
			return RemoteTypedValue{}, false
		}
		crs, _ := point["crs"].(map[string]interface{})
		srid, ok := crs["srid"].(int64)
		coordinates, _ := point["coordinates"].([]interface{})
		if !ok || len(coordinates) < 2 || len(coordinates) > 3 {
			return RemoteTypedValue{}, false
		}
		typed := RemoteTypedValue{Type: "point", SRID: int(srid), Coordinates: make([]float64, len(coordinates))}
		for i, coordinate := range coordinates {
			if typed.Coordinates[i], ok = remoteHTTPFloat64(coordinate); !ok {
				return RemoteTypedValue{}, false
			}
		}
		return typed, true
	}
	return RemoteTypedValue{}, false
}
