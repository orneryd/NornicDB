package storage

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

// testParameterValue is a typed value as the Cypher executor's are.
type testParameterValue struct{ day time.Time }

func (v testParameterValue) RemoteParameterValue() interface{} { return dbtype.Date(v.day) }

// The Bolt transport sends each value as the Go driver does: a typed value as
// its driver value, a pointer as what it points to, and a nil pointer, list
// or map as null, at any depth. Plain parameters are sent as they are,
// without a copy.
func TestRemoteBoltParameters(t *testing.T) {
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	plain := map[string]interface{}{"i": int64(1), "s": "x", "l": []interface{}{int64(1), 2.5, "y"}, "m": map[string]interface{}{"k": true}, "b": []string{"a"}}
	require.Equal(t, 0.0, testing.AllocsPerRun(100, func() {
		if sent := remoteBoltParameters(plain); len(sent) != len(plain) {
			t.Fatal("changed")
		}
	}))
	sent := remoteBoltParameters(plain)
	sent["probe"] = true
	require.Contains(t, plain, "probe", "plain parameters are sent as they are")
	delete(plain, "probe")

	var nilTime *time.Time
	var nilValue *testParameterValue
	typed := testParameterValue{day: day}
	params := map[string]interface{}{
		"keep":      int64(7),
		"value":     typed,
		"pointer":   &typed,
		"nilPtr":    nilValue,
		"time":      &day,
		"nilTime":   nilTime,
		"nilList":   []interface{}(nil),
		"nilMap":    map[string]interface{}(nil),
		"nilString": []string(nil),
		"nested":    []interface{}{map[string]interface{}{"d": typed, "n": int64(1)}, nil},
	}
	original := params["nested"].([]interface{})[0].(map[string]interface{})
	sent = remoteBoltParameters(params)
	require.Equal(t, map[string]interface{}{
		"keep":      int64(7),
		"value":     dbtype.Date(day),
		"pointer":   dbtype.Date(day),
		"nilPtr":    nil,
		"time":      day,
		"nilTime":   nil,
		"nilList":   nil,
		"nilMap":    nil,
		"nilString": nil,
		"nested":    []interface{}{map[string]interface{}{"d": dbtype.Date(day), "n": int64(1)}, nil},
	}, sent)
	require.Equal(t, typed, original["d"], "the caller's values are not changed")
}

// The HTTP transport sends a value JSON can't hold as the JSON its Cypher
// constructor reads, and writes each reference to its parameter as that
// constructor over it: an atom that is valid wherever the parameter was.
func TestRemoteHTTPStatement(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	zoned := time.Date(2024, 2, 29, 12, 34, 56, 123456789, berlin)
	offset := time.Date(2024, 2, 29, 12, 34, 56, 0, time.FixedZone("Offset", 3600))
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name, statement string
		params          map[string]interface{}
		want            string
		sent            string
	}{
		{"plain", "RETURN $i, $l", map[string]interface{}{"i": int64(1), "l": []interface{}{1.0, "x"}}, "RETURN $i, $l", `{"i":1,"l":[1.0,"x"]}`},
		{"zoned", "RETURN $t.hour", map[string]interface{}{"t": zoned}, "RETURN datetime($t).hour", `{"t":"2024-02-29T12:34:56.123456789+01:00[Europe/Berlin]"}`},
		{"offset", "RETURN $t", map[string]interface{}{"t": offset}, "RETURN datetime($t)", `{"t":"2024-02-29T12:34:56+01:00"}`},
		{"lmt_offset_seconds", "RETURN $t", map[string]interface{}{"t": time.Date(1850, 1, 1, 12, 0, 0, 0, paris)}, "RETURN datetime($t)", `{"t":{"epochSeconds":` + strconv.FormatInt(time.Date(1850, 1, 1, 12, 0, 0, 0, paris).Unix(), 10) + `,"nanosecond":0,"timezone":"Europe/Paris"}}`},
		{"driver_kinds", "RETURN [$d, $lt, $ct, $ldt, $du, $p2, $p3]", map[string]interface{}{
			"d":   dbtype.Date(day),
			"lt":  dbtype.LocalTime(time.Date(0, 1, 1, 12, 34, 56, 5, time.UTC)),
			"ct":  dbtype.Time(time.Date(0, 1, 1, 0, 1, 2, 0, time.FixedZone("", -2670))),
			"ldt": dbtype.LocalDateTime(time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC)),
			"du":  dbtype.Duration{Months: 14, Days: 3, Seconds: 5, Nanos: 6},
			"p2":  dbtype.Point2D{SpatialRefId: 7203, X: 1, Y: 2.5},
			"p3":  dbtype.Point3D{SpatialRefId: 4979, X: 12.5, Y: 55.75, Z: 100},
		}, "RETURN [date($d), localtime($lt), time($ct), localdatetime($ldt), duration($du), point($p2), point($p3)]",
			`{"d":"2024-02-29","lt":"12:34:56.000000005","ct":{"hour":0,"minute":1,"second":2,"nanosecond":0,"timezone":"-00:44:30"},"ldt":"2024-02-29T12:00:00","du":{"months":14,"days":3,"seconds":5,"nanoseconds":6},"p2":{"srid":7203,"x":1.0,"y":2.5},"p3":{"srid":4979,"x":12.5,"y":55.75,"z":100.0}}`},
		{"typed_value_and_pointer", "RETURN $v, $p", map[string]interface{}{"v": testParameterValue{day: day}, "p": &testParameterValue{day: day}}, "RETURN date($v), date($p)", `{"v":"2024-02-29","p":"2024-02-29"}`},
		{"property_map", "CREATE (n:L $props)", map[string]interface{}{"props": map[string]interface{}{"run": "a", "t": zoned}}, "CREATE (n:L {`run`: $props.`run`, `t`: datetime($props.`t`)})", `{"props":{"run":"a","t":"2024-02-29T12:34:56.123456789+01:00[Europe/Berlin]"}}`},
		{"special_floats", "RETURN $n, $i, $f", map[string]interface{}{"n": math.NaN(), "i": math.Inf(-1), "f": []interface{}{1.5, math.Inf(1), nil}}, "RETURN toFloat($n), toFloat($i), [__element0 IN $f | toFloat(__element0)]", `{"n":"NaN","i":"-Infinity","f":[1.5,"Infinity",null]}`},
		{"nulls", "RETURN $a, $b, $c, $d", map[string]interface{}{"a": []interface{}(nil), "b": map[string]interface{}(nil), "c": (*time.Time)(nil), "d": []string(nil)}, "RETURN $a, $b, $c, $d", `{"a":null,"b":null,"c":null,"d":null}`},
		{"maps_with_nulls", "UNWIND $rows AS row RETURN row", map[string]interface{}{"rows": []interface{}{map[string]interface{}{"t": offset}, nil}}, "UNWIND [__element0 IN $rows | CASE WHEN __element0 IS NULL THEN null ELSE {`t`: datetime(__element0.`t`)} END] AS row RETURN row", `{"rows":[{"t":"2024-02-29T12:34:56+01:00"},null]}`},
		{"mixed_list", "RETURN $m", map[string]interface{}{"m": []interface{}{dbtype.Date(day), int64(1), offset}}, "RETURN [date($m[0]), $m[1], datetime($m[2])]", `{"m":["2024-02-29",1,"2024-02-29T12:34:56+01:00"]}`},
		{"untouched_text", "RETURN '$t', \"$t\", `$t`, /* $t */ $`t`, $t2", map[string]interface{}{"t": offset, "t2": int64(1)}, "RETURN '$t', \"$t\", `$t`, /* $t */ datetime($`t`), $t2", `{"t":"2024-02-29T12:34:56+01:00","t2":1}`},
		{"free_variable", "WITH 1 AS __element0 RETURN $l", map[string]interface{}{"l": []interface{}{offset}}, "WITH 1 AS __element0 RETURN [__element_10 IN $l | datetime(__element_10)]", `{"l":["2024-02-29T12:34:56+01:00"]}`},
		{"nested_lists", "RETURN $l", map[string]interface{}{"l": []interface{}{[]interface{}{offset}, []interface{}{offset, nil}}}, "RETURN [__element0 IN $l | [__element1 IN __element0 | datetime(__element1)]]", `{"l":[["2024-02-29T12:34:56+01:00"],["2024-02-29T12:34:56+01:00",null]]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			statement, sent := remoteHTTPRequest(t, tt.statement, tt.params)
			require.Equal(t, tt.want, statement)
			require.JSONEq(t, tt.sent, string(sent))
		})
	}
}

// remoteHTTPRequest is the statement and the parameters the HTTP transport
// writes for statement and params.
func remoteHTTPRequest(t *testing.T, statement string, params map[string]interface{}) (string, json.RawMessage) {
	t.Helper()
	raw, err := json.Marshal(remoteTxRequest{Statements: []remoteStatement{{Statement: statement, Parameters: params}}})
	require.NoError(t, err)
	var written struct {
		Statements []struct {
			Statement  string
			Parameters json.RawMessage
		}
	}
	require.NoError(t, json.Unmarshal(raw, &written))
	require.Len(t, written.Statements, 1)
	return written.Statements[0].Statement, written.Statements[0].Parameters
}

// A batch's statement text depends on its rows' types, never on how many
// there are, so the participant can reuse its plan.
func TestRemoteHTTPStatementBatchText(t *testing.T) {
	rows := func(n int) map[string]interface{} {
		list := make([]interface{}, n)
		for i := range list {
			list[i] = map[string]interface{}{"i": int64(i), "t": time.Date(2024, 2, 29, 12, 0, i%60, 0, time.FixedZone("Offset", 3600)), "f": float64(i)}
		}
		return map[string]interface{}{"rows": list}
	}
	one, _ := remoteHTTPRequest(t, "UNWIND $rows AS row RETURN row", rows(1))
	many, sent := remoteHTTPRequest(t, "UNWIND $rows AS row RETURN row", rows(2000))
	require.Equal(t, "UNWIND [__element0 IN $rows | {`f`: __element0.`f`, `i`: __element0.`i`, `t`: datetime(__element0.`t`)}] AS row RETURN row", one)
	require.Equal(t, one, many)
	var decoded struct{ Rows []map[string]interface{} }
	require.NoError(t, json.Unmarshal(sent, &decoded))
	require.Len(t, decoded.Rows, 2000)
	require.Equal(t, "2024-02-29T12:00:01+01:00", decoded.Rows[1]["t"])
	require.False(t, strings.Contains(many, "[1]"))
}

// Every other value is written as encoding/json writes it, and strings are
// read back as written, invalid UTF-8 as U+FFFD.
func TestRemoteHTTPWriterValues(t *testing.T) {
	type label string
	_, sent := remoteHTTPRequest(t, "RETURN 1", map[string]interface{}{
		"u": uint64(7), "n": label("x"), "a": [2]int{1, 2}, "m": map[string]int{"k": 1}, "s": struct{ A int }{1},
		"f32": float32(1.5), "b": true, "i8": int8(-3), "num": json.Number("12"), "bytes": []byte("x"),
	})
	require.JSONEq(t, `{"u":7,"n":"x","a":[1,2],"m":{"k":1},"s":{"A":1},"f32":1.5,"b":true,"i8":-3,"num":12,"bytes":"eA=="}`, string(sent))
	for _, s := range []string{"", "plain", "quote\" back\\slash", "ctl\x00\x01\n\r\t\x1f", "é€😀", "line\u2028sep\u2029", "<&>"} {
		var back string
		require.NoError(t, json.Unmarshal(jsontext.AppendString(nil, s), &back))
		require.Equal(t, s, back)
	}
	var back string
	require.NoError(t, json.Unmarshal(jsontext.AppendString(nil, "bad\xffbyte"), &back))
	require.Equal(t, "bad\ufffdbyte", back)
}

// Years outside Neo4j's text forms, offsets with seconds and every
// reflected kind are written so the constructor reads them exactly; lists
// whose elements need rebuilding differently are written element by
// element; a value JSON can't hold is an error.
func TestRemoteHTTPWriterEdges(t *testing.T) {
	type flag bool
	type ratio float32
	type raw []byte
	far := time.Date(10000, 1, 2, 3, 4, 5, 6, time.UTC)
	seconds := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("Offset", 3601))
	statement, sent := remoteHTTPRequest(t, "RETURN $d, $ldt, $dt, $s, $ct, $f, $r, $b", map[string]interface{}{
		"d":   dbtype.Date(far),
		"ldt": dbtype.LocalDateTime(far),
		"dt":  far,
		"s":   seconds,
		"ct":  dbtype.Time(time.Date(0, 1, 1, 1, 2, 3, 0, time.FixedZone("", 3600))),
		"f":   flag(true),
		"r":   ratio(0.5),
		"b":   raw("x"),
	})
	require.Equal(t, "RETURN date($d), localdatetime($ldt), datetime($dt), datetime($s), time($ct), $f, $r, $b", statement)
	require.JSONEq(t, `{"d":{"year":10000,"month":1,"day":2},"ldt":{"year":10000,"month":1,"day":2,"hour":3,"minute":4,"second":5,"nanosecond":6},`+
		`"dt":{"epochSeconds":`+strconv.FormatInt(far.Unix(), 10)+`,"nanosecond":6,"timezone":"UTC"},`+
		`"s":{"epochSeconds":`+strconv.FormatInt(seconds.Unix(), 10)+`,"nanosecond":0,"timezone":"+01:00:01"},"ct":"01:02:03+01:00","f":true,"r":0.5,"b":"eA=="}`, string(sent))

	for _, tt := range []struct {
		name  string
		value interface{}
		want  string
	}{
		{"different_keys", []interface{}{map[string]interface{}{"t": seconds}, map[string]interface{}{"u": seconds}}, "RETURN [{`t`: datetime($l[0].`t`)}, {`u`: datetime($l[1].`u`)}]"},
		{"missing_key", []interface{}{map[string]interface{}{"t": seconds, "n": 1}, map[string]interface{}{"t": seconds, "m": 1}}, "RETURN [{`n`: $l[0].`n`, `t`: datetime($l[0].`t`)}, {`m`: $l[1].`m`, `t`: datetime($l[1].`t`)}]"},
		{"field_typed_then_plain", []interface{}{map[string]interface{}{"t": seconds}, map[string]interface{}{"t": "text"}}, "RETURN [{`t`: datetime($l[0].`t`)}, $l[1]]"},
		{"plain_then_typed_field", []interface{}{map[string]interface{}{"t": seconds, "n": 1}, map[string]interface{}{"t": seconds, "n": seconds}}, "RETURN [{`n`: $l[0].`n`, `t`: datetime($l[0].`t`)}, {`n`: datetime($l[1].`n`), `t`: datetime($l[1].`t`)}]"},
		{"lists_of_lists", []interface{}{[]interface{}{seconds}, []interface{}{dbtype.Date(far)}}, "RETURN [[__element1 IN $l[0] | datetime(__element1)], [__element1 IN $l[1] | date(__element1)]]"},
		{"string_then_special_float", []interface{}{"x", 1.5, nil, []interface{}{1}}, "RETURN $l"},
		{"plain_before_typed", []interface{}{"x", seconds}, "RETURN [$l[0], datetime($l[1])]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			statement, _ := remoteHTTPRequest(t, "RETURN $l", map[string]interface{}{"l": tt.value})
			require.Equal(t, tt.want, statement)
		})
	}

	for _, value := range []interface{}{
		make(chan int),
		map[string]interface{}{"c": make(chan int)},
		[]interface{}{make(chan int)},
		[]interface{}{seconds, make(chan int)},
		[]interface{}{map[string]interface{}{"t": seconds}, map[string]interface{}{"t": make(chan int)}},
		[]interface{}{map[string]interface{}{"t": seconds, "n": 1}, map[string]interface{}{"t": seconds, "n": make(chan int)}},
	} {
		_, err := json.Marshal(remoteStatement{Statement: "RETURN $v", Parameters: map[string]interface{}{"v": value}})
		require.Error(t, err)
	}
}

// Neo4j writes an offset as ±hh:mm, with seconds when it has them.
func TestRemoteOffsetText(t *testing.T) {
	for offset, want := range map[int]string{0: "+00:00", 3600: "+01:00", -2670: "-00:44:30", 7559: "+02:05:59"} {
		require.Equal(t, want, remoteOffsetText(offset))
	}
}
