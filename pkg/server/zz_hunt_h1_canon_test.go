package server

// Hunt ground H1, protocol pair: one canonical form for Bolt and HTTP values
// (hunt DECISIONS item 17). The first protocol run compared Bolt values,
// written by huntBoltValue's text forms, against HTTP's ISO text and reported
// formatting differences as leads (H1 harness defect D1). Both sides are now
// brought to the same canonical form before the unchanged oracle (huntAgree)
// compares them:
//
//   - temporal values by their Neo4j ISO text, parsed and rewritten with
//     seconds always present, the fraction without trailing zeros, the offset
//     to the second (Z as +00:00:00) and the zone name kept when it is a named
//     zone;
//   - durations as Neo4j's months, days and total nanoseconds;
//   - points by SRID and coordinates;
//   - numbers by value; NaN and the infinities as the text HTTP writes;
//   - entities by their properties; a path as the alternating list of its
//     elements in traversal order, walked through the relationships' end
//     nodes (the Go driver's Path.Nodes need not be in traversal order);
//   - byte arrays as their signed byte values, as HTTP writes them.
//
// HTTP cannot carry a value's type, so a Bolt string that reads as ISO text
// and the temporal value it spells compare equal; that is the protocol's own
// limit, not a disagreement.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

var (
	huntCanonDateRE     = `([+-]?\d{4,})-(\d{2})-(\d{2})`
	huntCanonTimeRE     = `(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?`
	huntCanonOffsetRE   = `(Z|[+-]\d{2}(?::?\d{2}(?::?\d{2})?)?)`
	huntCanonZoneRE     = `(?:\[([^\]]+)\])?`
	huntCanonDateOnly   = regexp.MustCompile(`^` + huntCanonDateRE + `$`)
	huntCanonLocalTime  = regexp.MustCompile(`^` + huntCanonTimeRE + `$`)
	huntCanonTime       = regexp.MustCompile(`^` + huntCanonTimeRE + huntCanonOffsetRE + `$`)
	huntCanonLocalDT    = regexp.MustCompile(`^` + huntCanonDateRE + `T` + huntCanonTimeRE + `$`)
	huntCanonDateTime   = regexp.MustCompile(`^` + huntCanonDateRE + `T` + huntCanonTimeRE + huntCanonOffsetRE + huntCanonZoneRE + `$`)
	huntCanonDurationRE = regexp.MustCompile(`^(-)?P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+)(?:\.(\d{1,9}))?S)?)?$`)
)

// huntCanonClock writes hh:mm:ss[.fraction] with the fraction's trailing zeros removed.
func huntCanonClock(hh, mm, ss, frac string) string {
	if ss == "" {
		ss = "00"
	}
	out := hh + ":" + mm + ":" + ss
	if f := strings.TrimRight(frac, "0"); f != "" {
		out += "." + f
	}
	return out
}

// huntCanonOffset writes an offset as +hh:mm:ss (Z is +00:00:00).
func huntCanonOffset(offset string) string {
	if offset == "Z" {
		return "+00:00:00"
	}
	sign, digits := offset[:1], strings.ReplaceAll(offset[1:], ":", "")
	for len(digits) < 6 {
		digits += "0"
	}
	return sign + digits[0:2] + ":" + digits[2:4] + ":" + digits[4:6]
}

func huntCanonOffsetSeconds(seconds int) string {
	sign := "+"
	if seconds < 0 {
		sign, seconds = "-", -seconds
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, seconds/3600, seconds/60%60, seconds%60)
}

// huntCanonText rewrites ISO temporal text in canonical form; other text is returned unchanged.
func huntCanonText(text string) any {
	if m := huntCanonDurationRE.FindStringSubmatch(text); m != nil && text != "P" && text != "PT" {
		return huntCanonDurationParts(m)
	}
	if m := huntCanonDateTime.FindStringSubmatch(text); m != nil {
		zone := ""
		if m[9] != "" {
			zone = "[" + m[9] + "]"
		}
		return map[string]string{"DateTime": m[1] + "-" + m[2] + "-" + m[3] + "T" + huntCanonClock(m[4], m[5], m[6], m[7]) + huntCanonOffset(m[8]) + zone}
	}
	if m := huntCanonLocalDT.FindStringSubmatch(text); m != nil {
		return map[string]string{"LocalDateTime": m[1] + "-" + m[2] + "-" + m[3] + "T" + huntCanonClock(m[4], m[5], m[6], m[7])}
	}
	if m := huntCanonTime.FindStringSubmatch(text); m != nil {
		return map[string]string{"Time": huntCanonClock(m[1], m[2], m[3], m[4]) + huntCanonOffset(m[5])}
	}
	if m := huntCanonLocalTime.FindStringSubmatch(text); m != nil {
		return map[string]string{"LocalTime": huntCanonClock(m[1], m[2], m[3], m[4])}
	}
	if huntCanonDateOnly.MatchString(text) {
		return map[string]string{"Date": text}
	}
	return text
}

func huntCanonDurationParts(m []string) any {
	atoi := func(s string) *big.Int {
		v, _ := new(big.Int).SetString(strings.TrimPrefix(s, "+"), 10)
		if v == nil {
			return new(big.Int)
		}
		return v
	}
	months := new(big.Int).Add(new(big.Int).Mul(atoi(m[2]), big.NewInt(12)), atoi(m[3]))
	days := new(big.Int).Add(new(big.Int).Mul(atoi(m[4]), big.NewInt(7)), atoi(m[5]))
	nanos := new(big.Int).Mul(atoi(m[6]), big.NewInt(3600e9))
	nanos.Add(nanos, new(big.Int).Mul(atoi(m[7]), big.NewInt(60e9)))
	seconds := atoi(m[8])
	nanos.Add(nanos, new(big.Int).Mul(seconds, big.NewInt(1e9)))
	if m[9] != "" {
		fraction := m[9] + strings.Repeat("0", 9-len(m[9]))
		f := atoi(fraction)
		if strings.HasPrefix(m[8], "-") {
			f.Neg(f)
		}
		nanos.Add(nanos, f)
	}
	if m[1] == "-" {
		months.Neg(months)
		days.Neg(days)
		nanos.Neg(nanos)
	}
	return huntCanonDuration(months, days, nanos)
}

func huntCanonDuration(months, days, nanos *big.Int) map[string]string {
	return map[string]string{"Duration": months.String() + "M " + days.String() + "D " + nanos.String() + "N"}
}

func huntCanonNumber(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return map[string]string{"Number": strconv.FormatFloat(f, 'g', -1, 64)}
}

func huntCanonPoint(srid int64, coordinates ...float64) map[string]string {
	parts := []string{strconv.FormatInt(srid, 10)}
	for _, c := range coordinates {
		parts = append(parts, strconv.FormatFloat(c, 'g', -1, 64))
	}
	return map[string]string{"Point": strings.Join(parts, " ")}
}

func huntCanonFraction(t time.Time) string {
	if t.Nanosecond() == 0 {
		return ""
	}
	return fmt.Sprintf("%09d", t.Nanosecond())
}

// huntCanonDateText writes a date as Neo4j does (four-digit year, signed beyond 0000-9999).
func huntCanonDateText(t time.Time) string {
	year := t.Year()
	y := fmt.Sprintf("%04d", year)
	if year < 0 {
		y = fmt.Sprintf("-%04d", -year)
	} else if year > 9999 {
		y = fmt.Sprintf("+%d", year)
	}
	return fmt.Sprintf("%s-%02d-%02d", y, int(t.Month()), t.Day())
}

func huntCanonClockOf(t time.Time) string {
	return huntCanonClock(fmt.Sprintf("%02d", t.Hour()), fmt.Sprintf("%02d", t.Minute()), fmt.Sprintf("%02d", t.Second()), huntCanonFraction(t))
}

// huntCanonBolt brings a Go-driver value to the canonical form.
func huntCanonBolt(value any) any {
	switch typed := value.(type) {
	case nil, bool:
		return typed
	case string:
		return huntCanonText(typed)
	case int64:
		return huntCanonNumber(float64(typed))
	case float64:
		return huntCanonNumber(typed)
	case []byte:
		out := make([]any, len(typed))
		for i, b := range typed {
			out[i] = huntCanonNumber(float64(int8(b)))
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = huntCanonBolt(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, item := range typed {
			out[k] = huntCanonBolt(item)
		}
		return out
	case dbtype.Node:
		return huntCanonBolt(typed.Props)
	case dbtype.Relationship:
		return huntCanonBolt(typed.Props)
	case dbtype.Path:
		return huntCanonPath(typed)
	case dbtype.Date:
		return map[string]string{"Date": huntCanonDateText(typed.Time())}
	case dbtype.LocalTime:
		return map[string]string{"LocalTime": huntCanonClockOf(typed.Time())}
	case dbtype.Time:
		t := typed.Time()
		_, offset := t.Zone()
		return map[string]string{"Time": huntCanonClockOf(t) + huntCanonOffsetSeconds(offset)}
	case dbtype.LocalDateTime:
		t := typed.Time()
		return map[string]string{"LocalDateTime": huntCanonDateText(t) + "T" + huntCanonClockOf(t)}
	case time.Time:
		_, offset := typed.Zone()
		zone := ""
		if name := typed.Location().String(); name != "" && name != "Offset" && name != "Local" {
			zone = "[" + name + "]"
		}
		return map[string]string{"DateTime": huntCanonDateText(typed) + "T" + huntCanonClockOf(typed) + huntCanonOffsetSeconds(offset) + zone}
	case dbtype.Duration:
		nanos := new(big.Int).Mul(big.NewInt(typed.Seconds), big.NewInt(1e9))
		nanos.Add(nanos, big.NewInt(int64(typed.Nanos)))
		return huntCanonDuration(big.NewInt(typed.Months), big.NewInt(typed.Days), nanos)
	case dbtype.Point2D:
		return huntCanonPoint(int64(typed.SpatialRefId), typed.X, typed.Y)
	case dbtype.Point3D:
		return huntCanonPoint(int64(typed.SpatialRefId), typed.X, typed.Y, typed.Z)
	}
	return map[string]string{fmt.Sprintf("%T", value): fmt.Sprintf("%v", value)}
}

func huntCanonEntityKey(elementID string, id int64) string {
	if elementID != "" {
		return elementID
	}
	return strconv.FormatInt(id, 10)
}

// huntCanonPath writes a path as HTTP's row format does: node, relationship,
// node, ... in traversal order. The walk starts at Nodes[0] and follows each
// relationship to its other end; if an end is missing it falls back to the
// order of Nodes.
func huntCanonPath(path dbtype.Path) any {
	nodes := map[string]dbtype.Node{}
	for _, node := range path.Nodes {
		nodes[huntCanonEntityKey(node.ElementId, node.Id)] = node
	}
	var out []any
	if len(path.Nodes) == 0 {
		return out
	}
	current := path.Nodes[0]
	out = append(out, huntCanonBolt(current))
	for i, rel := range path.Relationships {
		start := huntCanonEntityKey(rel.StartElementId, rel.StartId)
		end := huntCanonEntityKey(rel.EndElementId, rel.EndId)
		here := huntCanonEntityKey(current.ElementId, current.Id)
		nextKey := end
		if here == end && here != start {
			nextKey = start
		}
		next, ok := nodes[nextKey]
		if !ok {
			if i+1 < len(path.Nodes) {
				next = path.Nodes[i+1]
			} else {
				next = current
			}
		}
		out = append(out, huntCanonBolt(rel), huntCanonBolt(next))
		current = next
	}
	return out
}

// huntCanonHTTP brings a value from huntNeutral (HTTP side) to the canonical form.
func huntCanonHTTP(value any) any {
	switch typed := value.(type) {
	case nil, bool:
		return typed
	case string:
		return huntCanonText(typed)
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return typed.String()
		}
		return huntCanonNumber(f)
	case map[string]string:
		if number, ok := typed["Number"]; ok && len(typed) == 1 {
			if f, err := strconv.ParseFloat(number, 64); err == nil {
				return huntCanonNumber(f)
			}
		}
		return typed
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = huntCanonHTTP(item)
		}
		return out
	case map[string]any:
		if point, ok := huntCanonHTTPPoint(typed); ok {
			return point
		}
		out := make(map[string]any, len(typed))
		for k, item := range typed {
			out[k] = huntCanonHTTP(item)
		}
		return out
	}
	return value
}

func huntCanonHTTPNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case map[string]string:
		f, err := strconv.ParseFloat(typed["Number"], 64)
		return f, err == nil
	case json.Number:
		f, err := typed.Float64()
		return f, err == nil
	case float64:
		return typed, true
	}
	return 0, false
}

// huntCanonHTTPPoint reads Neo4j's HTTP point map {"type":"Point","coordinates":[...],"crs":{"srid":...}}.
func huntCanonHTTPPoint(m map[string]any) (map[string]string, bool) {
	if kind, _ := m["type"].(string); kind != "Point" {
		return nil, false
	}
	coordinates, ok := m["coordinates"].([]any)
	crs, ok2 := m["crs"].(map[string]any)
	if !ok || !ok2 {
		return nil, false
	}
	srid, ok := huntCanonHTTPNumber(crs["srid"])
	if !ok {
		return nil, false
	}
	values := make([]float64, 0, len(coordinates))
	for _, c := range coordinates {
		f, ok := huntCanonHTTPNumber(c)
		if !ok {
			return nil, false
		}
		values = append(values, f)
	}
	return huntCanonPoint(int64(srid), values...), true
}

// huntCanonHTTPOutcome converts an HTTP outcome (huntServer.Run on routeHTTPCommit).
func huntCanonHTTPOutcome(outcome differential.Outcome) differential.Outcome {
	if outcome.Failed() {
		return outcome
	}
	out := differential.Outcome{Columns: outcome.Columns, Rows: make([][]any, len(outcome.Rows))}
	for i, row := range outcome.Rows {
		values := make([]any, len(row))
		for c, v := range row {
			values[c] = huntCanonHTTP(v)
		}
		out.Rows[i] = values
	}
	return out
}

// huntCanonBoltRun runs one statement through Bolt auto-commit and returns its
// outcome in the canonical form (the same steps as huntBoltRun, with
// huntCanonBolt in place of huntBoltValue).
func huntCanonBoltRun(h *huntServer, query string, params map[string]any, timeout time.Duration) (outcome differential.Outcome) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, query, params)
	if err == nil {
		var keys []string
		if keys, err = result.Keys(); err == nil {
			var records []*neo4jdriver.Record
			if records, err = result.Collect(ctx); err == nil {
				outcome.Columns = keys
				outcome.Rows = make([][]any, len(records))
				for i, record := range records {
					row := make([]any, len(record.Values))
					for c, v := range record.Values {
						row[c] = huntCanonBolt(v)
					}
					outcome.Rows[i] = row
				}
			}
		}
	}
	if err != nil {
		return huntErrorOutcome(err)
	}
	return outcome
}

// TestHuntH1CanonicalForms checks the canonical form on the shapes behind
// harness defect D1 (pure, no server).
func TestHuntH1CanonicalForms(t *testing.T) {
	offset := func(seconds int) *time.Location { return time.FixedZone("Offset", seconds) }
	same := func(name string, bolt any, httpText any) {
		t.Helper()
		a, b := fmt.Sprint(huntCanonBolt(bolt)), fmt.Sprint(huntCanonHTTP(httpText))
		if a != b {
			t.Errorf("%s: bolt %s, http %s", name, a, b)
		}
	}
	differ := func(name string, bolt any, httpText any) {
		t.Helper()
		if fmt.Sprint(huntCanonBolt(bolt)) == fmt.Sprint(huntCanonHTTP(httpText)) {
			t.Errorf("%s: expected a difference, both %v", name, huntCanonBolt(bolt))
		}
	}
	same("datetime offset minutes", time.Date(1984, 3, 7, 12, 31, 0, 0, offset(3600)), "1984-03-07T12:31+01:00")
	same("datetime Z nanos", time.Date(1984, 3, 7, 12, 31, 14, 645876123, offset(0)), "1984-03-07T12:31:14.645876123Z")
	same("datetime offset seconds", time.Date(1984, 10, 11, 12, 34, 56, 0, offset(2*3600+5*60+59)), "1984-10-11T12:34:56+02:05:59")
	if stockholm, err := time.LoadLocation("Europe/Stockholm"); err == nil {
		same("datetime named zone", time.Date(1818, 7, 21, 21, 40, 32, 142000000, stockholm), "1818-07-21T21:40:32.142+00:53:28[Europe/Stockholm]")
	} else {
		t.Logf("no tzdata for Europe/Stockholm: %v", err)
	}
	same("localdatetime", dbtype.LocalDateTime(time.Date(1984, 3, 7, 12, 31, 0, 0, time.UTC)), "1984-03-07T12:31")
	same("localtime", dbtype.LocalTime(time.Date(0, 1, 1, 10, 35, 0, 0, time.UTC)), "10:35")
	same("time", dbtype.Time(time.Date(0, 1, 1, 10, 35, 0, 0, offset(-8*3600))), "10:35-08:00")
	same("date", dbtype.Date(time.Date(1984, 10, 11, 0, 0, 0, 0, time.UTC)), "1984-10-11")
	same("duration", dbtype.Duration{Days: -27, Seconds: -78033, Nanos: 858000000}, "P-27DT-21H-40M-32.142S")
	same("duration years", dbtype.Duration{Months: 14, Days: 3, Seconds: 3600}, "P1Y2M3DT1H")
	same("duration zero", dbtype.Duration{}, "PT0S")
	same("nan", math.NaN(), "NaN")
	same("integer", int64(7), json.Number("7"))
	same("point", dbtype.Point2D{SpatialRefId: 7203, X: 1, Y: 2}, map[string]any{"type": "Point", "coordinates": []any{json.Number("1.0"), json.Number("2.0")}, "crs": map[string]any{"srid": json.Number("7203"), "name": "cartesian"}})
	n := dbtype.Node{Id: 1, ElementId: "n", Props: map[string]any{"name": "n"}}
	k := dbtype.Node{Id: 2, ElementId: "k", Props: map[string]any{"name": "k"}}
	r1 := dbtype.Relationship{Id: 10, ElementId: "r1", StartId: 2, StartElementId: "k", EndId: 1, EndElementId: "n", Props: map[string]any{"i": int64(1)}}
	r2 := dbtype.Relationship{Id: 11, ElementId: "r2", StartId: 1, StartElementId: "n", EndId: 2, EndElementId: "k", Props: map[string]any{"i": int64(2)}}
	r3 := dbtype.Relationship{Id: 12, ElementId: "r3", StartId: 2, StartElementId: "k", EndId: 1, EndElementId: "n", Props: map[string]any{"i": int64(3)}}
	httpPath := []any{map[string]any{"name": "n"}, map[string]any{"i": json.Number("1")}, map[string]any{"name": "k"}, map[string]any{"i": json.Number("2")}, map[string]any{"name": "n"}}
	same("path unique nodes", dbtype.Path{Nodes: []dbtype.Node{n, k}, Relationships: []dbtype.Relationship{r1, r3}}, []any{map[string]any{"name": "n"}, map[string]any{"i": json.Number("1")}, map[string]any{"name": "k"}, map[string]any{"i": json.Number("3")}, map[string]any{"name": "n"}})
	same("path ordered nodes", dbtype.Path{Nodes: []dbtype.Node{n, k, n}, Relationships: []dbtype.Relationship{r1, r2}}, httpPath)
	differ("datetime instant", time.Date(1984, 3, 7, 12, 31, 0, 0, offset(3600)), "1984-03-07T12:32+01:00")
	differ("offset seconds lost", time.Date(1984, 10, 11, 12, 34, 56, 0, offset(2*3600+5*60)), "1984-10-11T12:34:56+02:05:59")
	differ("duration value", dbtype.Duration{Days: -27, Seconds: -78032}, "P-27DT-21H-40M-32.142S")
	differ("path order", dbtype.Path{Nodes: []dbtype.Node{n, k}, Relationships: []dbtype.Relationship{r1, r2}}, []any{map[string]any{"name": "n"}, map[string]any{"i": json.Number("2")}, map[string]any{"name": "k"}, map[string]any{"i": json.Number("1")}, map[string]any{"name": "n"}})
}
