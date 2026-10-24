package cypher

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// Each temporal, spatial and duration value, the vectors and UUIDs, are
// storage.RemoteParameterValue: a remote participant receives them as their
// Cypher type, as the Neo4j Go driver sends the value RemoteParameterValue
// returns.

// RemoteParameterValue is the date as a driver Date.
func (v CypherDate) RemoteParameterValue() interface{} { return dbtype.Date(v.Time) }

// RemoteParameterValue is the time of day as a driver LocalTime.
func (v CypherLocalTime) RemoteParameterValue() interface{} { return dbtype.LocalTime(v.Time) }

// RemoteParameterValue is the time of day and offset as a driver Time.
func (v CypherTime) RemoteParameterValue() interface{} { return dbtype.Time(v.Time) }

// RemoteParameterValue is the date and time of day as a driver
// LocalDateTime.
func (v CypherLocalDateTime) RemoteParameterValue() interface{} {
	return dbtype.LocalDateTime(v.Time)
}

// RemoteParameterValue is the instant in its named zone, or at its offset
// in a zone named "Offset", which the driver sends as a fixed offset.
func (v CypherDateTime) RemoteParameterValue() interface{} {
	if v.ZoneID != "" {
		if v.Time.Location().String() == v.ZoneID {
			return v.Time
		}
		if location, err := time.LoadLocation(v.ZoneID); err == nil {
			return v.Time.In(location)
		}
	}
	_, offset := v.Time.Zone()
	return v.Time.In(time.FixedZone("Offset", offset))
}

// RemoteParameterValue is the duration as a driver Duration: months, days,
// and seconds with their nanoseconds.
func (d CypherDuration) RemoteParameterValue() interface{} {
	seconds := d.Hours*3600 + d.Minutes*60 + d.Seconds + d.Nanos/int64(time.Second)
	nanos := d.Nanos % int64(time.Second)
	if nanos < 0 && seconds > 0 {
		nanos += int64(time.Second)
		seconds--
	}
	return dbtype.Duration{Months: d.Years*12 + d.Months, Days: d.Days, Seconds: seconds, Nanos: int(nanos)}
}

// RemoteParameterValue is the point as a driver Point2D, or Point3D in a
// three-dimensional reference system.
func (p CypherPoint) RemoteParameterValue() interface{} {
	if p.SRID == 4979 || p.SRID == 9157 {
		return dbtype.Point3D{SpatialRefId: uint32(p.SRID), X: p.X, Y: p.Y, Z: p.Z}
	}
	return dbtype.Point2D{SpatialRefId: uint32(p.SRID), X: p.X, Y: p.Y}
}

// RemoteParameterValue is the vector's coordinates as a list.
func (v CypherVector) RemoteParameterValue() interface{} {
	if v.Floats != nil {
		return v.Floats
	}
	return v.Ints
}

// RemoteParameterValue is the UUID as its text.
func (u CypherUUID) RemoteParameterValue() interface{} { return u.String() }
