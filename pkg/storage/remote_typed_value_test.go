package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

// Both transports deliver temporal and point values as RemoteTypedValue,
// bare and nested, and leave every other value as it was.
func TestRemoteTypedValues(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	date := RemoteTypedValue{Type: "date", Text: "2024-02-29"}
	point := RemoteTypedValue{Type: "point", SRID: 7203, Coordinates: []float64{1.25, 2.5}}
	t.Run("bolt", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			value interface{}
			want  interface{}
		}{
			{"date", dbtype.Date(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)), date},
			{"localtime", dbtype.LocalTime(time.Date(0, 1, 1, 12, 34, 56, 123, time.UTC)), RemoteTypedValue{Type: "localtime", Text: "12:34:56.000000123"}},
			{"time", dbtype.Time(time.Date(0, 1, 1, 12, 34, 56, 0, time.FixedZone("Offset", 3600))), RemoteTypedValue{Type: "time", Text: "12:34:56+01:00"}},
			{"localdatetime", dbtype.LocalDateTime(time.Date(2024, 2, 29, 12, 34, 56, 500000000, time.UTC)), RemoteTypedValue{Type: "localdatetime", Text: "2024-02-29T12:34:56.5"}},
			{"datetime_offset", time.Date(2024, 2, 29, 12, 34, 56, 0, time.FixedZone("Offset", 3600)), RemoteTypedValue{Type: "datetime", Text: "2024-02-29T12:34:56+01:00"}},
			{"datetime_zone", time.Date(2024, 2, 29, 12, 34, 56, 0, berlin), RemoteTypedValue{Type: "datetime", Text: "2024-02-29T12:34:56+01:00[Europe/Berlin]"}},
			{"duration", dbtype.Duration{Months: 14, Days: 3, Seconds: 14706, Nanos: 500000000}, RemoteTypedValue{Type: "duration", Text: "P14M3DT14706.500000000S"}},
			{"point", dbtype.Point2D{X: 1.25, Y: 2.5, SpatialRefId: 7203}, point},
			{"point_3d", dbtype.Point3D{X: 1.25, Y: 2.5, Z: 3.75, SpatialRefId: 9157}, RemoteTypedValue{Type: "point", SRID: 9157, Coordinates: []float64{1.25, 2.5, 3.75}}},
			{"nested", map[string]interface{}{"a": []interface{}{dbtype.Point2D{X: 1.25, Y: 2.5, SpatialRefId: 7203}, int64(1)}}, map[string]interface{}{"a": []interface{}{point, int64(1)}}},
			{"plain", map[string]interface{}{"a": []interface{}{"x", int64(1)}}, map[string]interface{}{"a": []interface{}{"x", int64(1)}}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				require.Equal(t, tt.want, normalizeBoltValue(tt.value))
			})
		}
	})
	t.Run("http", func(t *testing.T) {
		pointJSON := `{"type":"Point","coordinates":[1.25,2.5],"crs":{"srid":7203,"name":"cartesian","type":"link","properties":{"href":"http://spatialreference.org/ref/sr-org/7203/ogcwkt/","type":"ogcwkt"}}}`
		node := RemoteEntityValue{"elementId": "4:db:n", "properties": map[string]interface{}{"a": int64(1)}}
		entities := map[string]interface{}{"4:db:n": map[string]interface{}(node)}
		nodeMeta := map[string]interface{}{"elementId": "4:db:n", "type": "node"}
		typed := func(kind string) map[string]interface{} { return map[string]interface{}{"type": kind} }
		for _, tt := range []struct {
			name, raw string
			metadata  []interface{}
			entities  map[string]interface{}
			want      interface{}
			consumed  int
		}{
			{"date", `"2024-02-29"`, []interface{}{typed("date")}, nil, date, 1},
			{"duration", `"P1M1DT2.5S"`, []interface{}{typed("duration")}, nil, RemoteTypedValue{Type: "duration", Text: "P1M1DT2.5S"}, 1},
			{"point", pointJSON, []interface{}{typed("point")}, nil, point, 1},
			// A list or map adds its elements' metadata; a plain value adds null.
			{"nested", `{"a":[` + pointJSON + `,1,"2024-02-29"]}`, []interface{}{typed("point"), nil, typed("date")}, nil, map[string]interface{}{"a": []interface{}{point, int64(1), date}}, 3},
			{"with_entity", `[{"a":1},"2024-02-29"]`, []interface{}{nodeMeta, typed("date")}, entities, []interface{}{node, date}, 2},
			{"untyped_text", `"2024-02-29"`, []interface{}{nil}, nil, "2024-02-29", 0},
		} {
			t.Run(tt.name, func(t *testing.T) {
				index := 0
				value, err := decodeRemoteHTTPValue(json.RawMessage(tt.raw), nil, tt.metadata, &index, tt.entities, remoteHTTPAuto)
				require.NoError(t, err)
				require.Equal(t, tt.want, value)
				require.Equal(t, tt.consumed, index)
			})
		}
	})
}

// A Bolt time or datetime keeps its offset's seconds (local mean time of a
// historical zone, or an explicit +02:05:59), as Neo4j writes them.
func TestRemoteTypedFromBoltOffsetSeconds(t *testing.T) {
	offset := time.FixedZone("", 2*3600+5*60+59)
	value, ok := remoteTypedFromBolt(dbtype.Time(time.Date(0, 1, 1, 12, 34, 56, 0, offset)))
	require.True(t, ok)
	require.Equal(t, "12:34:56+02:05:59", value.Text)
	value, ok = remoteTypedFromBolt(time.Date(2024, 1, 1, 12, 0, 0, 0, offset))
	require.True(t, ok)
	require.Equal(t, "2024-01-01T12:00:00+02:05:59", value.Text)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	value, ok = remoteTypedFromBolt(time.Date(1850, 1, 1, 12, 0, 0, 0, paris))
	require.True(t, ok)
	require.Equal(t, "1850-01-01T12:00:00+00:09:21[Europe/Paris]", value.Text)
	value, ok = remoteTypedFromBolt(time.Date(2024, 1, 1, 12, 0, 0, 0, paris))
	require.True(t, ok)
	require.Equal(t, "2024-01-01T12:00:00+01:00[Europe/Paris]", value.Text)
}
