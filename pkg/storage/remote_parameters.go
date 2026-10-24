package storage

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// A remote participant receives every statement parameter as its Cypher
// type. The Bolt transport sends each value as the Neo4j Go driver does
// (remoteBoltParameters). Neo4j's HTTP API reads parameters as plain JSON,
// so the HTTP transport writes a temporal, spatial or duration value, or a
// float JSON can't hold (NaN, ±Infinity), as JSON its Cypher constructor
// reads, and writes each reference to that parameter in the statement as
// the constructor over it (remoteStatement.MarshalJSON). Both resolve a
// value the same way first (remoteParameterValue).

// RemoteParameterValue is a value of the Cypher executor that a remote
// participant receives as its Cypher type. RemoteParameterValue returns it
// as the Neo4j Go driver sends it over Bolt: time.Time, dbtype.Date,
// dbtype.LocalTime, dbtype.Time, dbtype.LocalDateTime, dbtype.Duration,
// dbtype.Point2D, dbtype.Point3D, or a list or string.
type RemoteParameterValue interface {
	RemoteParameterValue() interface{}
}

// remoteParameterValue is the value a transport sends for value, and whether
// it differs from value: a pointer the query entry keeps as a typed binding
// (*time.Time, a pointer RemoteParameterValue) is what it points to, a nil
// pointer, list or map is null, and a RemoteParameterValue is its driver
// value. Lists and maps are resolved by the transports themselves.
func remoteParameterValue(value interface{}) (interface{}, bool) {
	switch v := value.(type) {
	case nil, bool, string, int, int64, float64, time.Time:
		return value, false
	case []interface{}:
		if v == nil {
			return nil, true
		}
		return value, false
	case map[string]interface{}:
		if v == nil {
			return nil, true
		}
		return value, false
	case *time.Time:
		if v == nil {
			return nil, true
		}
		return *v, true
	case RemoteParameterValue:
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr && rv.IsNil() {
			return nil, true
		}
		return v.RemoteParameterValue(), true
	}
	switch rv := reflect.ValueOf(value); rv.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map:
		if rv.IsNil() {
			return nil, true
		}
	}
	return value, false
}

// remoteBoltParameters is params as the Bolt transport sends them
// (remoteBoltParameter), params itself when no value changes.
func remoteBoltParameters(params map[string]interface{}) map[string]interface{} {
	sent, _ := remoteBoltMap(params)
	return sent
}

// remoteBoltMap is a map with each value as the Bolt transport sends it, and
// whether one changed; the map is copied only then.
func remoteBoltMap(params map[string]interface{}) (map[string]interface{}, bool) {
	var out map[string]interface{}
	for key, value := range params {
		if remoteBoltPlain(value) {
			continue
		}
		sent, changed := remoteBoltParameter(value)
		if !changed {
			continue
		}
		if out == nil {
			out = make(map[string]interface{}, len(params))
			for k, original := range params {
				out[k] = original
			}
		}
		out[key] = sent
	}
	if out == nil {
		return params, false
	}
	return out, true
}

// remoteBoltPlain reports whether the driver sends value as it is without
// looking further: the common scalars.
func remoteBoltPlain(value interface{}) bool {
	switch value.(type) {
	case nil, bool, string, int64, int, float64, time.Time:
		return true
	}
	return false
}

// remoteBoltParameter is value as the Bolt transport sends it: resolved
// (remoteParameterValue) at any depth of lists and maps, which are copied
// only when an element changes.
func remoteBoltParameter(value interface{}) (interface{}, bool) {
	value, changed := remoteParameterValue(value)
	switch v := value.(type) {
	case []interface{}:
		var out []interface{}
		for i, item := range v {
			if remoteBoltPlain(item) {
				continue
			}
			sent, itemChanged := remoteBoltParameter(item)
			if !itemChanged {
				continue
			}
			if out == nil {
				out = append([]interface{}(nil), v...)
			}
			out[i] = sent
		}
		if out != nil {
			return out, true
		}
	case map[string]interface{}:
		if sent, mapChanged := remoteBoltMap(v); mapChanged {
			return sent, true
		}
	}
	return value, changed
}

// remoteHTTPShape is how an HTTP participant rebuilds a parameter value JSON
// can't carry: a constructor over its JSON argument (function), a map whose
// fields need rebuilding, or a list whose elements do, all of one shape
// (element) or each its own (items). It depends on the value's types, never
// on a list's length, unless the list mixes shapes.
type remoteHTTPShape struct {
	function string
	keys     []string
	fields   []*remoteHTTPShape
	list     bool
	element  *remoteHTTPShape
	nullable bool
	items    []*remoteHTTPShape
}

var (
	remoteHTTPDate          = &remoteHTTPShape{function: "date"}
	remoteHTTPLocalTime     = &remoteHTTPShape{function: "localtime"}
	remoteHTTPTime          = &remoteHTTPShape{function: "time"}
	remoteHTTPLocalDateTime = &remoteHTTPShape{function: "localdatetime"}
	remoteHTTPDateTime      = &remoteHTTPShape{function: "datetime"}
	remoteHTTPDuration      = &remoteHTTPShape{function: "duration"}
	remoteHTTPPoint         = &remoteHTTPShape{function: "point"}
	remoteHTTPSpecialFloat  = &remoteHTTPShape{function: "toFloat"}
)

// MarshalJSON writes the statement for Neo4j's HTTP API in one pass: each
// parameter as JSON (remoteHTTPWriter), an integer as a JSON integer and a
// float as a JSON float, and each reference in the statement to a parameter
// that needs rebuilding written as its rebuilding expression, an atom valid
// wherever the parameter was (a map literal, a list comprehension or a
// constructor call).
func (s remoteStatement) MarshalJSON() ([]byte, error) {
	w := remoteHTTPWriter{buf: make([]byte, 0, 64+len(s.Statement))}
	w.buf = append(w.buf, '{')
	var shapes map[string]*remoteHTTPShape
	if len(s.Parameters) > 0 {
		w.buf = append(w.buf, `"parameters":{`...)
		first := true
		for key, value := range s.Parameters {
			if !first {
				w.buf = append(w.buf, ',')
			}
			first = false
			w.buf = appendRemoteJSONString(w.buf, key)
			w.buf = append(w.buf, ':')
			shape, _, err := w.value(value)
			if err != nil {
				return nil, err
			}
			if shape != nil {
				if shapes == nil {
					shapes = make(map[string]*remoteHTTPShape)
				}
				shapes[key] = shape
			}
		}
		w.buf = append(w.buf, "},"...)
	}
	w.buf = append(w.buf, `"statement":`...)
	w.buf = appendRemoteJSONString(w.buf, remoteHTTPRewrite(s.Statement, shapes))
	if s.IncludeStats {
		w.buf = append(w.buf, `,"includeStats":true`...)
	}
	if len(s.ResultDataContents) > 0 {
		w.buf = append(w.buf, `,"resultDataContents":[`...)
		for i, content := range s.ResultDataContents {
			if i > 0 {
				w.buf = append(w.buf, ',')
			}
			w.buf = appendRemoteJSONString(w.buf, content)
		}
		w.buf = append(w.buf, ']')
	}
	w.buf = append(w.buf, '}')
	return w.buf, nil
}

// remoteHTTPRewrite is statement with each reference to a parameter shapes
// has written as the expression that rebuilds it.
func remoteHTTPRewrite(statement string, shapes map[string]*remoteHTTPShape) string {
	if shapes == nil {
		return statement
	}
	variable := remoteHTTPElementVariable(statement)
	var rewritten strings.Builder
	last := 0
	cyphertext.ParameterReferences(statement, func(name string, start, end int) {
		shape := shapes[name]
		if shape == nil {
			return
		}
		if last == 0 {
			rewritten.Grow(len(statement) + 64)
		}
		rewritten.WriteString(statement[last:start])
		shape.write(&rewritten, statement[start:end], variable, 0)
		last = end
	})
	if last == 0 {
		return statement
	}
	rewritten.WriteString(statement[last:])
	return rewritten.String()
}

// remoteHTTPElementVariable is the prefix of the list comprehension
// variables remoteHTTPShape writes, one that occurs nowhere in statement.
func remoteHTTPElementVariable(statement string) string {
	for n := 0; ; n++ {
		name := "__element"
		if n > 0 {
			name += "_" + strconv.Itoa(n)
		}
		if !strings.Contains(statement, name) {
			return name
		}
	}
}

// write writes the expression that rebuilds the value reference denotes.
func (s *remoteHTTPShape) write(b *strings.Builder, reference, variable string, depth int) {
	switch {
	case s.function != "":
		b.WriteString(s.function)
		b.WriteByte('(')
		b.WriteString(reference)
		b.WriteByte(')')
	case s.keys != nil:
		b.WriteByte('{')
		for i, key := range s.keys {
			if i > 0 {
				b.WriteString(", ")
			}
			quoted := remoteQuotedName(key)
			b.WriteString(quoted)
			b.WriteString(": ")
			field := reference + "." + quoted
			if s.fields[i] == nil {
				b.WriteString(field)
				continue
			}
			s.fields[i].write(b, field, variable, depth+1)
		}
		b.WriteByte('}')
	case s.element != nil:
		element := variable + strconv.Itoa(depth)
		b.WriteByte('[')
		b.WriteString(element)
		b.WriteString(" IN ")
		b.WriteString(reference)
		b.WriteString(" | ")
		if s.nullable && s.element.function == "" {
			b.WriteString("CASE WHEN ")
			b.WriteString(element)
			b.WriteString(" IS NULL THEN null ELSE ")
			s.element.write(b, element, variable, depth+1)
			b.WriteString(" END")
		} else {
			s.element.write(b, element, variable, depth+1)
		}
		b.WriteByte(']')
	default:
		b.WriteByte('[')
		for i, item := range s.items {
			if i > 0 {
				b.WriteString(", ")
			}
			indexed := reference + "[" + strconv.Itoa(i) + "]"
			if item == nil {
				b.WriteString(indexed)
				continue
			}
			item.write(b, indexed, variable, depth+1)
		}
		b.WriteByte(']')
	}
}

// remoteQuotedName is name as a backtick-quoted Cypher name.
func remoteQuotedName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// remoteHTTPKind is what remoteHTTPWriter.value wrote, for a list's shape.
type remoteHTTPKind uint8

const (
	remoteHTTPOther remoteHTTPKind = iota
	remoteHTTPNull
	remoteHTTPFloatKind
)

// remoteHTTPWriter writes parameter values as HTTP API JSON.
type remoteHTTPWriter struct {
	buf []byte
}

// value writes value as JSON and returns the shape that rebuilds it on the
// participant, nil when JSON carries it as it is. A float is written as a
// JSON float, so it stays a Float even when integral; a nil list or map is
// null; []byte is base64, as encoding/json writes it.
func (w *remoteHTTPWriter) value(value interface{}) (*remoteHTTPShape, remoteHTTPKind, error) {
	value, _ = remoteParameterValue(value)
	switch v := value.(type) {
	case nil:
		w.buf = append(w.buf, "null"...)
		return nil, remoteHTTPNull, nil
	case string:
		w.buf = appendRemoteJSONString(w.buf, v)
		return nil, remoteHTTPOther, nil
	case bool:
		w.buf = strconv.AppendBool(w.buf, v)
		return nil, remoteHTTPOther, nil
	case int64:
		w.buf = strconv.AppendInt(w.buf, v, 10)
		return nil, remoteHTTPOther, nil
	case int:
		w.buf = strconv.AppendInt(w.buf, int64(v), 10)
		return nil, remoteHTTPOther, nil
	case float64:
		return w.float(v, 64)
	case float32:
		return w.float(float64(v), 32)
	case json.Number:
		w.buf = append(w.buf, v...)
		return nil, remoteHTTPOther, nil
	case time.Time:
		w.dateTime(v)
		return remoteHTTPDateTime, remoteHTTPOther, nil
	case dbtype.Date:
		w.date(time.Time(v))
		return remoteHTTPDate, remoteHTTPOther, nil
	case dbtype.LocalTime:
		w.buf = append(w.buf, '"')
		w.buf = time.Time(v).AppendFormat(w.buf, "15:04:05.999999999")
		w.buf = append(w.buf, '"')
		return remoteHTTPLocalTime, remoteHTTPOther, nil
	case dbtype.Time:
		w.timeOfDay(time.Time(v))
		return remoteHTTPTime, remoteHTTPOther, nil
	case dbtype.LocalDateTime:
		w.localDateTime(time.Time(v))
		return remoteHTTPLocalDateTime, remoteHTTPOther, nil
	case dbtype.Duration:
		w.buf = append(w.buf, `{"months":`...)
		w.buf = strconv.AppendInt(w.buf, v.Months, 10)
		w.buf = append(w.buf, `,"days":`...)
		w.buf = strconv.AppendInt(w.buf, v.Days, 10)
		w.buf = append(w.buf, `,"seconds":`...)
		w.buf = strconv.AppendInt(w.buf, v.Seconds, 10)
		w.buf = append(w.buf, `,"nanoseconds":`...)
		w.buf = strconv.AppendInt(w.buf, int64(v.Nanos), 10)
		w.buf = append(w.buf, '}')
		return remoteHTTPDuration, remoteHTTPOther, nil
	case dbtype.Point2D:
		w.point(v.SpatialRefId, v.X, v.Y, 0, false)
		return remoteHTTPPoint, remoteHTTPOther, nil
	case dbtype.Point3D:
		w.point(v.SpatialRefId, v.X, v.Y, v.Z, true)
		return remoteHTTPPoint, remoteHTTPOther, nil
	case []byte:
		w.buf = append(w.buf, '"')
		w.buf = base64.StdEncoding.AppendEncode(w.buf, v)
		w.buf = append(w.buf, '"')
		return nil, remoteHTTPOther, nil
	case map[string]interface{}:
		shape, err := w.object(v)
		return shape, remoteHTTPOther, err
	case []interface{}:
		shape, err := w.list(v)
		return shape, remoteHTTPOther, err
	}
	switch rv := reflect.ValueOf(value); rv.Kind() {
	case reflect.Bool:
		w.buf = strconv.AppendBool(w.buf, rv.Bool())
		return nil, remoteHTTPOther, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		w.buf = strconv.AppendInt(w.buf, rv.Int(), 10)
		return nil, remoteHTTPOther, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		w.buf = strconv.AppendUint(w.buf, rv.Uint(), 10)
		return nil, remoteHTTPOther, nil
	case reflect.Float32, reflect.Float64:
		return w.float(rv.Float(), rv.Type().Bits())
	case reflect.String:
		w.buf = appendRemoteJSONString(w.buf, rv.String())
		return nil, remoteHTTPOther, nil
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 && rv.Kind() == reflect.Slice {
			w.buf = append(w.buf, '"')
			w.buf = base64.StdEncoding.AppendEncode(w.buf, rv.Bytes())
			w.buf = append(w.buf, '"')
			return nil, remoteHTTPOther, nil
		}
		items := make([]interface{}, rv.Len())
		for i := range items {
			items[i] = rv.Index(i).Interface()
		}
		shape, err := w.list(items)
		return shape, remoteHTTPOther, err
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			fields := make(map[string]interface{}, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				fields[iter.Key().String()] = iter.Value().Interface()
			}
			shape, err := w.object(fields)
			return shape, remoteHTTPOther, err
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, remoteHTTPOther, err
	}
	w.buf = append(w.buf, raw...)
	return nil, remoteHTTPOther, nil
}

// float writes a float as a JSON float, or for NaN and ±Infinity, which
// JSON has no number for, as the text toFloat reads.
func (w *remoteHTTPWriter) float(value float64, bitSize int) (*remoteHTTPShape, remoteHTTPKind, error) {
	switch {
	case math.IsNaN(value):
		w.buf = append(w.buf, `"NaN"`...)
		return remoteHTTPSpecialFloat, remoteHTTPOther, nil
	case math.IsInf(value, 1):
		w.buf = append(w.buf, `"Infinity"`...)
		return remoteHTTPSpecialFloat, remoteHTTPOther, nil
	case math.IsInf(value, -1):
		w.buf = append(w.buf, `"-Infinity"`...)
		return remoteHTTPSpecialFloat, remoteHTTPOther, nil
	}
	w.buf = appendRemoteJSONFloat(w.buf, value, bitSize)
	return nil, remoteHTTPFloatKind, nil
}

// object writes a map as a JSON object and returns the shape of the fields
// that need rebuilding.
func (w *remoteHTTPWriter) object(fields map[string]interface{}) (*remoteHTTPShape, error) {
	w.buf = append(w.buf, '{')
	var shapes map[string]*remoteHTTPShape
	first := true
	for key, item := range fields {
		if !first {
			w.buf = append(w.buf, ',')
		}
		first = false
		w.buf = appendRemoteJSONString(w.buf, key)
		w.buf = append(w.buf, ':')
		shape, _, err := w.value(item)
		if err != nil {
			return nil, err
		}
		if shape != nil {
			if shapes == nil {
				shapes = make(map[string]*remoteHTTPShape)
			}
			shapes[key] = shape
		}
	}
	w.buf = append(w.buf, '}')
	if shapes == nil {
		return nil, nil
	}
	shape := &remoteHTTPShape{keys: make([]string, 0, len(fields)), fields: make([]*remoteHTTPShape, len(fields))}
	for key := range fields {
		shape.keys = append(shape.keys, key)
	}
	sort.Strings(shape.keys)
	for i, key := range shape.keys {
		shape.fields[i] = shapes[key]
	}
	return shape, nil
}

// list writes a list as a JSON array and returns the shape that rebuilds
// its elements: one shape for every element when they share it, null
// elements and, for toFloat, plain floats included; otherwise one per
// element. An element after the first that needs rebuilding is checked
// against that shape while it is written, without building its own.
func (w *remoteHTTPWriter) list(items []interface{}) (*remoteHTTPShape, error) {
	w.buf = append(w.buf, '[')
	var (
		shapes   []*remoteHTTPShape
		common   *remoteHTTPShape
		mixed    bool
		nullable bool
		plain    bool // a non-null element JSON carries as it is
		floats   = true
	)
	for i, item := range items {
		if i > 0 {
			w.buf = append(w.buf, ',')
		}
		if common != nil && !mixed {
			start := len(w.buf)
			conforms, err := w.conforming(item, common)
			if err != nil {
				return nil, err
			}
			if conforms {
				if string(w.buf[start:]) == "null" {
					nullable = true
				} else {
					shapes[i] = common
				}
				continue
			}
			w.buf = w.buf[:start]
		}
		shape, kind, err := w.value(item)
		if err != nil {
			return nil, err
		}
		if shape == nil {
			switch kind {
			case remoteHTTPNull:
				nullable = true
			case remoteHTTPFloatKind:
				plain = true
			default:
				plain, floats = true, false
			}
			if common != nil && plain && !(common == remoteHTTPSpecialFloat && floats) {
				mixed = true
			}
			continue
		}
		if shapes == nil {
			shapes = make([]*remoteHTTPShape, len(items))
		}
		shapes[i] = shape
		switch {
		case common == nil:
			common = shape
			if plain && !(common == remoteHTTPSpecialFloat && floats) {
				mixed = true
			}
		case !sameRemoteHTTPShape(common, shape):
			mixed = true
		}
	}
	w.buf = append(w.buf, ']')
	switch {
	case common == nil:
		return nil, nil
	case mixed:
		return &remoteHTTPShape{list: true, items: shapes}, nil
	}
	return &remoteHTTPShape{list: true, element: common, nullable: nullable && common.function == ""}, nil
}

// conforming writes value when shape rebuilds it too: null, a value of
// shape's constructor, a plain float for toFloat, or a map with shape's keys
// whose fields conform. Otherwise it reports false; what it wrote is then
// the caller's to discard.
func (w *remoteHTTPWriter) conforming(value interface{}, shape *remoteHTTPShape) (bool, error) {
	if shape.function != "" {
		own, kind, err := w.value(value)
		if err != nil {
			return false, err
		}
		return own == shape || kind == remoteHTTPNull || own == nil && kind == remoteHTTPFloatKind && shape == remoteHTTPSpecialFloat, nil
	}
	resolved, _ := remoteParameterValue(value)
	if resolved == nil {
		w.buf = append(w.buf, "null"...)
		return true, nil
	}
	fields, ok := resolved.(map[string]interface{})
	if shape.keys == nil || !ok || len(fields) != len(shape.keys) {
		return false, nil
	}
	w.buf = append(w.buf, '{')
	for i, key := range shape.keys {
		item, present := fields[key]
		if !present {
			return false, nil
		}
		if i > 0 {
			w.buf = append(w.buf, ',')
		}
		w.buf = appendRemoteJSONString(w.buf, key)
		w.buf = append(w.buf, ':')
		if shape.fields[i] == nil {
			own, _, err := w.value(item)
			if err != nil || own != nil {
				return false, err
			}
			continue
		}
		if conforms, err := w.conforming(item, shape.fields[i]); err != nil || !conforms {
			return false, err
		}
	}
	w.buf = append(w.buf, '}')
	return true, nil
}

// sameRemoteHTTPShape reports whether a and b rebuild values alike.
func sameRemoteHTTPShape(a, b *remoteHTTPShape) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || a.function != b.function || a.list != b.list || a.nullable != b.nullable || len(a.keys) != len(b.keys) || len(a.items) != len(b.items) {
		return false
	}
	for i := range a.keys {
		if a.keys[i] != b.keys[i] || !sameRemoteHTTPShape(a.fields[i], b.fields[i]) {
			return false
		}
	}
	for i := range a.items {
		if !sameRemoteHTTPShape(a.items[i], b.items[i]) {
			return false
		}
	}
	return a.function != "" || a.keys != nil || sameRemoteHTTPShape(a.element, b.element)
}

// dateTime writes the JSON datetime() reads for t, as the Go driver sends it
// over Bolt: in t's named zone, or at its offset when the zone is named
// "Offset". Neo4j's text form takes neither an offset with seconds (a
// historical zone's local mean time) nor a year past 9999, so such a value
// is written as epoch seconds instead.
func (w *remoteHTTPWriter) dateTime(t time.Time) {
	name, offset := t.Zone()
	zone := t.Location().String()
	if name == "Offset" || zone == "" || zone == "Local" {
		zone = ""
	}
	if offset%60 != 0 || t.Year() < 1 || t.Year() > 9999 {
		if zone == "" {
			zone = remoteOffsetText(offset)
		}
		w.buf = append(w.buf, `{"epochSeconds":`...)
		w.buf = strconv.AppendInt(w.buf, t.Unix(), 10)
		w.buf = append(w.buf, `,"nanosecond":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Nanosecond()), 10)
		w.buf = append(w.buf, `,"timezone":`...)
		w.buf = appendRemoteJSONString(w.buf, zone)
		w.buf = append(w.buf, '}')
		return
	}
	w.buf = append(w.buf, '"')
	w.buf = t.AppendFormat(w.buf, "2006-01-02T15:04:05.999999999Z07:00")
	if zone != "" {
		w.buf = append(w.buf, '[')
		w.buf = append(w.buf, zone...)
		w.buf = append(w.buf, ']')
	}
	w.buf = append(w.buf, '"')
}

// timeOfDay writes the JSON time() reads for t's clock and offset.
func (w *remoteHTTPWriter) timeOfDay(t time.Time) {
	if _, offset := t.Zone(); offset%60 != 0 {
		w.buf = append(w.buf, `{"hour":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Hour()), 10)
		w.buf = append(w.buf, `,"minute":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Minute()), 10)
		w.buf = append(w.buf, `,"second":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Second()), 10)
		w.buf = append(w.buf, `,"nanosecond":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Nanosecond()), 10)
		w.buf = append(w.buf, `,"timezone":`...)
		w.buf = appendRemoteJSONString(w.buf, remoteOffsetText(offset))
		w.buf = append(w.buf, '}')
		return
	}
	w.buf = append(w.buf, '"')
	w.buf = t.AppendFormat(w.buf, "15:04:05.999999999Z07:00")
	w.buf = append(w.buf, '"')
}

// date writes the JSON date() reads for t's calendar date.
func (w *remoteHTTPWriter) date(t time.Time) {
	if t.Year() < 1 || t.Year() > 9999 {
		w.buf = append(w.buf, `{"year":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Year()), 10)
		w.buf = append(w.buf, `,"month":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Month()), 10)
		w.buf = append(w.buf, `,"day":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Day()), 10)
		w.buf = append(w.buf, '}')
		return
	}
	w.buf = append(w.buf, '"')
	w.buf = t.AppendFormat(w.buf, "2006-01-02")
	w.buf = append(w.buf, '"')
}

// localDateTime writes the JSON localdatetime() reads for t's date and
// clock.
func (w *remoteHTTPWriter) localDateTime(t time.Time) {
	if t.Year() < 1 || t.Year() > 9999 {
		w.buf = append(w.buf, `{"year":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Year()), 10)
		w.buf = append(w.buf, `,"month":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Month()), 10)
		w.buf = append(w.buf, `,"day":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Day()), 10)
		w.buf = append(w.buf, `,"hour":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Hour()), 10)
		w.buf = append(w.buf, `,"minute":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Minute()), 10)
		w.buf = append(w.buf, `,"second":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Second()), 10)
		w.buf = append(w.buf, `,"nanosecond":`...)
		w.buf = strconv.AppendInt(w.buf, int64(t.Nanosecond()), 10)
		w.buf = append(w.buf, '}')
		return
	}
	w.buf = append(w.buf, '"')
	w.buf = t.AppendFormat(w.buf, "2006-01-02T15:04:05.999999999")
	w.buf = append(w.buf, '"')
}

// point writes the JSON point() reads.
func (w *remoteHTTPWriter) point(srid uint32, x, y, z float64, threeD bool) {
	w.buf = append(w.buf, `{"srid":`...)
	w.buf = strconv.AppendUint(w.buf, uint64(srid), 10)
	w.buf = append(w.buf, `,"x":`...)
	w.buf = appendRemoteJSONFloat(w.buf, x, 64)
	w.buf = append(w.buf, `,"y":`...)
	w.buf = appendRemoteJSONFloat(w.buf, y, 64)
	if threeD {
		w.buf = append(w.buf, `,"z":`...)
		w.buf = appendRemoteJSONFloat(w.buf, z, 64)
	}
	w.buf = append(w.buf, '}')
}

// appendRemoteJSONFloat appends a finite float as a JSON float: with a
// fraction or an exponent, so the server reads a Float even when it is
// integral.
func appendRemoteJSONFloat(buf []byte, value float64, bitSize int) []byte {
	start := len(buf)
	buf = strconv.AppendFloat(buf, value, 'g', -1, bitSize)
	for _, c := range buf[start:] {
		if c == '.' || c == 'e' || c == 'E' {
			return buf
		}
	}
	return append(buf, ".0"...)
}

// appendRemoteJSONString appends s as a JSON string. Invalid UTF-8 is
// written as U+FFFD, as encoding/json writes it.
func appendRemoteJSONString(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch c {
			case '"', '\\':
				buf = append(buf, '\\', c)
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			buf = append(buf, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			buf = append(buf, s[start:i]...)
			buf = append(buf, '\\', 'u', '2', '0', '2', hex[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}

// remoteOffsetText is a UTC offset as Neo4j writes a timezone: +00:00 for
// zero, with seconds when it has them.
func remoteOffsetText(offset int) string {
	sign := byte('+')
	if offset < 0 {
		sign = '-'
		offset = -offset
	}
	text := []byte{sign}
	text = appendTwoDigits(text, offset/3600)
	text = append(text, ':')
	text = appendTwoDigits(text, offset/60%60)
	if offset%60 != 0 {
		text = append(text, ':')
		text = appendTwoDigits(text, offset%60)
	}
	return string(text)
}

func appendTwoDigits(text []byte, n int) []byte {
	return append(text, byte('0'+n/10), byte('0'+n%10))
}
