package cypher

import (
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// nornicdb.json.stringify, a NornicDB extension: a value as the exact JSON
// text another serializer writes, so a digest or name-based UUID of that
// text (apoc.util.sha256, uuid.v5) can be computed in the statement and
// equals the one computed outside it. apoc.convert.toJson writes APOC's
// JSON, whose number forms and spacing differ.
func init() {
	cypherfn.Register("nornicdb.json.stringify", fnJSONStringify)
}

// jsonProfile is how a profile writes JSON text.
type jsonProfile struct {
	name string
	// itemSeparator and keySeparator stand between list or map items and
	// between a key and its value.
	itemSeparator, keySeparator string
	// asciiOnly writes every character outside ' '..'~' as a \u escape.
	asciiOnly bool
	// nonFinite writes NaN and infinities as NaN, Infinity and -Infinity;
	// without it they are an error.
	nonFinite bool
	// stringDefault writes a temporal value or a byte array as the JSON
	// string of its text (stringDefaultText); without it they are an error.
	stringDefault bool
}

// jsonProfiles are the profiles, each Python's json.dumps with the given
// arguments, applied to the values the Neo4j Python driver (6.x) receives:
//
//   - python-canonical: json.dumps(value, sort_keys=True,
//     separators=(",", ":"), ensure_ascii=False, allow_nan=False);
//   - python-sorted: json.dumps(value, sort_keys=True, default=str).
var jsonProfiles = map[string]jsonProfile{
	"python-canonical": {name: "python-canonical", itemSeparator: ",", keySeparator: ":"},
	"python-sorted":    {name: "python-sorted", itemSeparator: ", ", keySeparator: ": ", asciiOnly: true, nonFinite: true, stringDefault: true},
}

// fnJSONStringify is nornicdb.json.stringify(value, config): value as the
// JSON text of config.profile (jsonProfiles). In both profiles map keys are
// sorted (by code point, as Python sorts them), integers are in decimal,
// floats are Python's repr (shortest round-trip digits: 1.0, 0.0001,
// 1e+16, 1.5e-05), a duration is the list [months, days, seconds,
// nanoseconds] and a point the list of its coordinates, as the driver's
// tuples are written. A null value is the text null.
func fnJSONStringify(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("nornicdb.json.stringify", "2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	config, isMap := values[1].(map[string]interface{})
	if !isMap && values[1] != nil {
		return nil, &cypherfn.TypeMismatchError{Function: "nornicdb.json.stringify", Expected: "Map", Value: values[1]}
	}
	name, _ := config["profile"].(string)
	profile, known := jsonProfiles[name]
	if !known {
		return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
			localization.CypherCoreJSONStringifyProfileUnknown(jsonStringifyProfileText(config["profile"])))
	}
	var out strings.Builder
	if err := profile.write(&out, values[0]); err != nil {
		return nil, err
	}
	return out.String(), nil
}

func jsonStringifyProfileText(value interface{}) string {
	if text, isText := value.(string); isText {
		return text
	}
	return cypherTypeName(value)
}

// write writes value as the profile's JSON text.
func (p jsonProfile) write(out *strings.Builder, value interface{}) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case string:
		p.writeString(out, v)
	case int64:
		out.WriteString(strconv.FormatInt(v, 10))
	case int:
		out.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		out.WriteString(strconv.FormatInt(int64(v), 10))
	case float64:
		return p.writeFloat(out, v)
	case float32:
		return p.writeFloat(out, float64(v))
	case []interface{}:
		out.WriteByte('[')
		for index, item := range v {
			if index > 0 {
				out.WriteString(p.itemSeparator)
			}
			if err := p.write(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		// Go orders strings by their UTF-8 bytes, which is code point order.
		sort.Strings(keys)
		out.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				out.WriteString(p.itemSeparator)
			}
			p.writeString(out, key)
			out.WriteString(p.keySeparator)
			if err := p.write(out, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case *CypherDuration:
		if v == nil {
			out.WriteString("null")
			return nil
		}
		return p.write(out, pythonDurationTuple(*v))
	case CypherDuration:
		return p.write(out, pythonDurationTuple(v))
	case CypherPoint:
		return p.write(out, pointTuple(v))
	case *CypherPoint:
		if v == nil {
			out.WriteString("null")
			return nil
		}
		return p.write(out, pointTuple(*v))
	default:
		if text, ok := stringDefaultText(value); ok {
			if !p.stringDefault {
				return p.unsupported(value)
			}
			p.writeString(out, text)
			return nil
		}
		return p.writeReflected(out, value)
	}
	return nil
}

// writeReflected writes the typed lists and string-keyed maps property
// values and parameters can be (a []int64, a map[string]string).
func (p jsonProfile) writeReflected(out *strings.Builder, value interface{}) error {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		items := make([]interface{}, reflected.Len())
		for index := range items {
			items[index] = reflected.Index(index).Interface()
		}
		return p.write(out, items)
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			entries := make(map[string]interface{}, reflected.Len())
			iterator := reflected.MapRange()
			for iterator.Next() {
				entries[iterator.Key().String()] = iterator.Value().Interface()
			}
			return p.write(out, entries)
		}
	case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		if reflected.Type().PkgPath() == "" {
			return p.write(out, reflected.Convert(reflect.TypeOf(int64(0))).Interface())
		}
	}
	return p.unsupported(value)
}

func (p jsonProfile) unsupported(value interface{}) error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCoreJSONStringifyValueUnsupported(p.name, cypherTypeName(value)))
}

// writeFloat writes value as Python's float repr: the shortest digits that
// read back as value, in plain notation (with .0 when whole) for an
// exponent from -4 to 15, otherwise as 1e+16, 1.5e-05.
func (p jsonProfile) writeFloat(out *strings.Builder, value float64) error {
	switch {
	case math.IsNaN(value) || math.IsInf(value, 0):
		if !p.nonFinite {
			return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
				localization.CypherCoreJSONStringifyValueUnsupported(p.name, pythonNonFiniteText(value)))
		}
		out.WriteString(pythonNonFiniteText(value))
		return nil
	case value == 0:
		if math.Signbit(value) {
			out.WriteString("-0.0")
		} else {
			out.WriteString("0.0")
		}
		return nil
	}
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	exponent, _ := strconv.Atoi(scientific[strings.IndexByte(scientific, 'e')+1:])
	if exponent < -4 || exponent >= 16 {
		// Go writes the exponent as Python does: signed, two digits at least.
		out.WriteString(scientific)
		return nil
	}
	plain := strconv.FormatFloat(value, 'f', -1, 64)
	out.WriteString(plain)
	if !strings.ContainsRune(plain, '.') {
		out.WriteString(".0")
	}
	return nil
}

func pythonNonFiniteText(value float64) string {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case value > 0:
		return "Infinity"
	}
	return "-Infinity"
}

const jsonHexDigits = "0123456789abcdef"

// writeString writes text as a JSON string as Python writes it: " and \
// escaped, \b \f \n \r \t, other characters below U+0020 as \u00xx; with
// asciiOnly, also every character outside ' '..'~' (U+007F included), one
// beyond U+FFFF as its UTF-16 surrogate pair. Invalid UTF-8 is U+FFFD.
func (p jsonProfile) writeString(out *strings.Builder, text string) {
	out.WriteByte('"')
	start := 0
	for index := 0; index < len(text); {
		r, size := rune(text[index]), 1
		if r >= utf8.RuneSelf {
			r, size = utf8.DecodeRuneInString(text[index:])
		}
		valid := r != utf8.RuneError || size > 1
		plain := r >= 0x20 && r != '"' && r != '\\' && (!p.asciiOnly || r < 0x7f)
		if plain && valid {
			index += size
			continue
		}
		out.WriteString(text[start:index])
		index += size
		start = index
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 || p.asciiOnly {
				writeJSONUnicodeEscape(out, r)
			} else {
				out.WriteRune(r) // U+FFFD for an invalid byte
			}
		}
	}
	out.WriteString(text[start:])
	out.WriteByte('"')
}

func writeJSONUnicodeEscape(out *strings.Builder, r rune) {
	if r > 0xffff {
		r -= 0x10000
		writeJSONUnicodeEscape(out, 0xd800+(r>>10))
		writeJSONUnicodeEscape(out, 0xdc00+(r&0x3ff))
		return
	}
	out.WriteString(`\u`)
	for shift := 12; shift >= 0; shift -= 4 {
		out.WriteByte(jsonHexDigits[(r>>uint(shift))&0xf])
	}
}

// pythonDurationTuple is the driver's Duration tuple of d as Bolt sends it:
// months, days, and the seconds and nanoseconds of its clock part, both
// with the sign of the whole (Python's symmetric divmod).
func pythonDurationTuple(d CypherDuration) []interface{} {
	seconds := d.Hours*3600 + d.Minutes*60 + d.Seconds + d.Nanos/1_000_000_000
	nanos := d.Nanos % 1_000_000_000
	switch {
	case seconds > 0 && nanos < 0:
		seconds, nanos = seconds-1, nanos+1_000_000_000
	case seconds < 0 && nanos > 0:
		seconds, nanos = seconds+1, nanos-1_000_000_000
	}
	return []interface{}{d.Years*12 + d.Months, d.Days, seconds, nanos}
}

// pointTuple is the driver's Point tuple of p: its coordinates.
func pointTuple(p CypherPoint) []interface{} {
	if crs, known := pointCRSBySRID(p.SRID); known && crs.dimensions == 3 {
		return []interface{}{p.X, p.Y, p.Z}
	}
	return []interface{}{p.X, p.Y}
}

// stringDefaultText is Python's str() of the driver's value for a temporal
// value or a byte array (json.dumps' default=str): dates as 2024-01-02,
// times with nine fraction digits and their offset (12:30:00.000000000+01:00),
// date-times joined by T. A zoned date-time (named zone) shows only its
// offset, which the driver takes from pytz, rounded to the minute, and
// writes with floor division, so -03:30 is -04:30; its clock is UTC plus
// that offset. A byte array is Python's bytes repr (b'\x00a').
func stringDefaultText(value interface{}) (string, bool) {
	switch v := value.(type) {
	case CypherDate:
		return pythonDateText(v.Time), true
	case CypherLocalTime:
		return pythonClockText(v.Time), true
	case CypherTime:
		_, offset := v.Time.Zone()
		return pythonClockText(v.Time) + pythonFixedOffsetText(offset), true
	case CypherLocalDateTime:
		return pythonDateText(v.Time) + "T" + pythonClockText(v.Time), true
	case CypherDateTime:
		if v.ZoneID != "" {
			return pythonNamedZoneDateTimeText(v.Time), true
		}
		return pythonOffsetDateTimeText(v.Time), true
	case time.Time:
		if strings.Contains(v.Location().String(), "/") {
			return pythonNamedZoneDateTimeText(v), true
		}
		return pythonOffsetDateTimeText(v), true
	case []byte:
		return pythonBytesRepr(v), true
	}
	return "", false
}

func pythonDateText(t time.Time) string {
	return jsonPad(t.Year(), 4) + "-" + jsonPad(int(t.Month()), 2) + "-" + jsonPad(t.Day(), 2)
}

func pythonClockText(t time.Time) string {
	return jsonPad(t.Hour(), 2) + ":" + jsonPad(t.Minute(), 2) + ":" + jsonPad(t.Second(), 2) + "." + jsonPad(t.Nanosecond(), 9)
}

// pythonFixedOffsetText is a fixed offset (pytz.FixedOffset of the whole
// minutes, floored) as the driver's Time writes it: sign, then hours and
// minutes of its magnitude.
func pythonFixedOffsetText(offsetSeconds int) string {
	minutes := jsonFloorDiv(offsetSeconds, 60)
	sign := "+"
	if minutes < 0 {
		sign, minutes = "-", -minutes
	}
	return sign + jsonPad(minutes/60, 2) + ":" + jsonPad(minutes%60, 2)
}

func pythonOffsetDateTimeText(t time.Time) string {
	_, offset := t.Zone()
	return pythonDateText(t) + "T" + pythonClockText(t) + pythonFixedOffsetText(offset)
}

func pythonNamedZoneDateTimeText(t time.Time) string {
	_, offset := t.Zone()
	minutes := jsonFloorDiv(offset+30, 60)
	wall := t.UTC().Add(time.Duration(minutes) * time.Minute)
	hours, rest := jsonFloorDiv(minutes, 60), minutes-jsonFloorDiv(minutes, 60)*60
	sign := "+"
	if hours < 0 {
		sign, hours = "-", -hours
	}
	return pythonDateText(wall) + "T" + pythonClockText(wall) + sign + jsonPad(hours, 2) + ":" + jsonPad(rest, 2)
}

// pythonBytesRepr is Python's repr of a bytes value: b'…' (b"…" when it
// holds ' and no "), with \\, the quote, \t, \n, \r escaped and any other
// byte outside ' '..'~' as \xhh.
func pythonBytesRepr(data []byte) string {
	quote := byte('\'')
	if strings.IndexByte(string(data), '\'') >= 0 && strings.IndexByte(string(data), '"') < 0 {
		quote = '"'
	}
	var out strings.Builder
	out.WriteString("b")
	out.WriteByte(quote)
	for _, c := range data {
		switch {
		case c == quote || c == '\\':
			out.WriteByte('\\')
			out.WriteByte(c)
		case c == '\t':
			out.WriteString(`\t`)
		case c == '\n':
			out.WriteString(`\n`)
		case c == '\r':
			out.WriteString(`\r`)
		case c < 0x20 || c >= 0x7f:
			out.WriteString(`\x`)
			out.WriteByte(jsonHexDigits[c>>4])
			out.WriteByte(jsonHexDigits[c&0xf])
		default:
			out.WriteByte(c)
		}
	}
	out.WriteByte(quote)
	return out.String()
}

func jsonFloorDiv(a, b int) int {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// jsonPad is n in decimal with leading zeros to width digits, a minus sign
// first, as Python's {n:0Wd} writes it.
func jsonPad(n, width int) string {
	text := strconv.Itoa(n)
	if n < 0 {
		return "-" + strings.Repeat("0", max(0, width-len(text))) + text[1:]
	}
	if len(text) >= width {
		return text
	}
	return strings.Repeat("0", width-len(text)) + text
}
