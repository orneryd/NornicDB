// Tests for temporal functions in NornicDB Cypher implementation.
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestTemporalMapConstructorsUseSharedComponentSemantics(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(baseEngine, "test"))
	ctx := context.Background()

	tests := []struct {
		expression string
		expected   string
	}{
		{"date({year: 1817, week: 1})", "1816-12-30"},
		{"date({year: 1984, quarter: 3, dayOfQuarter: 45})", "1984-08-14"},
		{"localtime({hour: 12, minute: 31, second: 14, millisecond: 123, microsecond: 456, nanosecond: 789})", "12:31:14.123456789"},
		{"time({hour: 12, minute: 34, second: 56, timezone: '+02:05:59'})", "12:34:56+02:05:59"},
		{"localdatetime({year: 1984, ordinalDay: 202, hour: 12})", "1984-07-20T12:00"},
		{"datetime({year: 1984, ordinalDay: 202, timezone: 'Europe/Stockholm'})", "1984-07-20T00:00+02:00[Europe/Stockholm]"},
		{"datetime.fromepoch(416779, 999999999)", "1970-01-05T19:46:19.999999999Z"},
		{"datetime.fromepochmillis(237821673987)", "1977-07-15T13:34:33.987Z"},
		{"duration({months: 5, days: 1.5})", "P5M1DT12H"},
		{"duration({months: 0.75})", "P22DT19H51M49.5S"},
	}

	for _, test := range tests {
		result, err := executor.Execute(ctx, "RETURN "+test.expression+" AS value", nil)
		if err != nil {
			t.Fatalf("%s failed: %v", test.expression, err)
		}
		got := fmt.Sprint(result.Rows[0][0])
		if value, ok := result.Rows[0][0].(time.Time); ok {
			zoneID := ""
			if strings.Contains(value.Location().String(), "/") {
				zoneID = value.Location().String()
			}
			got = formatTemporalDateTime(value, true, zoneID)
		}
		if got != test.expected {
			t.Fatalf("%s = %q, want %q", test.expression, got, test.expected)
		}
	}
}

func TestNamedTimezoneUsesNeo4jHistoricalRulesIndependentlyOfHostTZData(t *testing.T) {
	value, zoneID, ok := parseCypherDateTimeText("1818-07-21T21:40:32.142[Europe/Stockholm]")
	if !ok {
		t.Fatal("historical Stockholm datetime was not parsed")
	}
	if got, want := zoneID, "Europe/Stockholm"; got != want {
		t.Fatalf("zone ID = %q, want %q", got, want)
	}
	if got, want := formatTemporalDateTime(value, true, zoneID), "1818-07-21T21:40:32.142+00:53:28[Europe/Stockholm]"; got != want {
		t.Fatalf("historical datetime = %q, want %q", got, want)
	}
	if _, offset := value.Zone(); offset != 53*60+28 {
		t.Fatalf("historical offset = %d, want %d", offset, 53*60+28)
	}

	transition, _, ok := parseCypherDateTimeText("1893-04-01T00:00:00[Europe/Stockholm]")
	if !ok {
		t.Fatal("Stockholm transition datetime was not parsed")
	}
	if _, offset := transition.Zone(); offset != 60*60 {
		t.Fatalf("transition offset = %d, want %d", offset, 60*60)
	}
}

func TestTemporalWeekConstructionInheritsBaseDateWeekday(t *testing.T) {
	value, ok := buildTemporalValue("date", map[string]interface{}{
		"date": CypherDate{Time: time.Date(1816, 12, 31, 0, 0, 0, 0, time.UTC)},
		"week": int64(2),
	})
	if !ok {
		t.Fatal("week date was not constructed")
	}
	if got := value.(CypherDate).String(); got != "1817-01-07" {
		t.Fatalf("week date = %q, want %q", got, "1817-01-07")
	}
}

func TestTemporalProjectionUsesTypedIntermediateValues(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_projection"))
	initial, evaluated := executor.evaluateRowExpression("date({year: 1984, month: 11, day: 11})", pipelineRow{})
	if !evaluated {
		t.Fatal("initial date expression was not evaluated")
	}
	if _, ok := initial.(CypherDate); !ok {
		t.Fatalf("initial value type = %T, want CypherDate (value %#v)", initial, initial)
	}
	projectedValue, evaluated := executor.evaluateRowExpression("date({date: other, day: 28})", pipelineRow{"other": initial})
	if !evaluated {
		t.Fatal("projected date expression was not evaluated")
	}
	if _, ok := projectedValue.(CypherDate); !ok {
		t.Fatalf("direct projected value type = %T, want CypherDate (value %#v)", projectedValue, projectedValue)
	}
	result, err := executor.Execute(context.Background(), `
		WITH date({year: 1984, month: 11, day: 11}) AS other
		RETURN date({date: other, day: 28}) AS projected`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("unexpected result shape: %#v", result.Rows)
	}
	projected, ok := result.Rows[0][0].(CypherDate)
	if !ok {
		t.Fatalf("projected value type = %T, want CypherDate (value %#v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got := projected.String(); got != "1984-11-28" {
		t.Fatalf("projected date = %q, want %q", got, "1984-11-28")
	}
}

func TestTemporalPropertyAccessorsUseTypedValues(t *testing.T) {
	stockholm, err := time.LoadLocation("Europe/Stockholm")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		value    interface{}
		property string
		want     interface{}
	}{
		{name: "date year", value: CypherDate{Time: time.Date(1984, 10, 11, 0, 0, 0, 0, time.UTC)}, property: "year", want: int64(1984)},
		{name: "date ISO week", value: CypherDate{Time: time.Date(1984, 1, 1, 0, 0, 0, 0, time.UTC)}, property: "week", want: int64(52)},
		{name: "date ISO week year", value: CypherDate{Time: time.Date(1984, 1, 1, 0, 0, 0, 0, time.UTC)}, property: "weekYear", want: int64(1983)},
		{name: "date day of quarter", value: CypherDate{Time: time.Date(1984, 11, 11, 0, 0, 0, 0, time.UTC)}, property: "dayOfQuarter", want: int64(42)},
		{name: "local time microsecond", value: CypherLocalTime{Time: time.Date(1970, 1, 1, 12, 31, 14, 645876123, time.UTC)}, property: "microsecond", want: int64(645876)},
		{name: "zoned offset seconds", value: CypherTime{Time: time.Date(1970, 1, 1, 12, 31, 14, 645876123, time.FixedZone("+01:00", 3600))}, property: "offsetSeconds", want: int64(3600)},
		{name: "named timezone", value: time.Date(1984, 11, 11, 12, 31, 14, 645876123, stockholm), property: "timezone", want: "Europe/Stockholm"},
		{name: "epoch milliseconds", value: time.Date(1984, 11, 11, 12, 31, 14, 645876123, stockholm), property: "epochMillis", want: int64(469020674645)},
		{name: "duration total months", value: &CypherDuration{Years: 1, Months: 4, Days: 10, Hours: 1, Minutes: 1, Seconds: 1, Nanos: 111111111}, property: "months", want: int64(16)},
		{name: "duration total microseconds", value: &CypherDuration{Years: 1, Months: 4, Days: 10, Hours: 1, Minutes: 1, Seconds: 1, Nanos: 111111111}, property: "microseconds", want: int64(3661111111)},
		{name: "duration month within quarter", value: &CypherDuration{Years: 1, Months: 4}, property: "monthsOfQuarter", want: int64(1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := evaluateRowPropertyChain(test.value, test.property)
			if !ok {
				t.Fatalf("property %q was not evaluated", test.property)
			}
			if got != test.want {
				t.Fatalf("property %q = %#v, want %#v", test.property, got, test.want)
			}
		})
	}
}

func TestTemporalValuesRoundTripThroughStringInComputedRows(t *testing.T) {
	executor := &StorageExecutor{}
	tests := []struct {
		name       string
		expression string
		want       interface{}
	}{
		{name: "date text", expression: "toString(date({year: 1984, month: 10, day: 11}))", want: "1984-10-11"},
		{name: "fixed offset date time text", expression: "toString(datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123, timezone: '+01:00'}))", want: "1984-10-11T12:31:14.645876123+01:00"},
		{name: "normalized duration text", expression: "toString(duration({minutes: 12, seconds: -60}))", want: "PT11M"},
		{name: "negative fractional duration text", expression: "toString(duration({seconds: -2, milliseconds: 1}))", want: "PT-1.999S"},
		{name: "negative subsecond duration text", expression: "toString(duration({days: 1, milliseconds: -1}))", want: "P1DT-0.001S"},
		{name: "date round trip equality", expression: "date(toString(d)) = d", want: true},
		{name: "date time round trip equality", expression: "datetime(toString(dt)) = dt", want: true},
		{name: "duration round trip equality", expression: "duration(toString(duration({seconds: -2, milliseconds: -1}))) = duration({seconds: -2, milliseconds: -1})", want: true},
	}
	values := pipelineRow{
		"d":  CypherDate{Time: time.Date(1984, 10, 11, 0, 0, 0, 0, time.UTC)},
		"dt": time.Date(1984, 10, 11, 12, 31, 14, 645876123, time.FixedZone("+01:00", 3600)),
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := executor.evaluateRowExpression(test.expression, values)
			if !ok {
				t.Fatalf("expression %q was not evaluated", test.expression)
			}
			if got != test.want {
				t.Fatalf("expression %q = %#v, want %#v", test.expression, got, test.want)
			}
		})
	}
}

func TestTemporalComparisonUsesNeo4jValueOrdering(t *testing.T) {
	executor := &StorageExecutor{}
	tests := []struct {
		name       string
		expression string
		want       bool
	}{
		{
			name:       "zoned times compare by UTC clock value",
			expression: "time({hour: 10, minute: 0, timezone: '+01:00'}) < time({hour: 9, minute: 35, second: 14, nanosecond: 645876123, timezone: '+00:00'})",
			want:       true,
		},
		{
			name:       "duration clock seconds do not normalize into days",
			expression: "duration({days: 14, hours: 16, minutes: 13, seconds: 10}) = duration({days: 13, hours: 40, minutes: 13, seconds: 10})",
			want:       false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := executor.evaluateRowExpression(test.expression, pipelineRow{})
			if !ok {
				t.Fatalf("expression %q was not evaluated", test.expression)
			}
			if got != test.want {
				t.Fatalf("expression %q = %#v, want %v", test.expression, got, test.want)
			}
		})
	}
}

func TestTemporalArithmeticPreservesTypedValueSemantics(t *testing.T) {
	executor := &StorageExecutor{}
	baseDuration := buildDurationFromFields(map[string]interface{}{
		"years": int64(12), "months": int64(5), "days": int64(14),
		"hours": int64(16), "minutes": int64(12), "seconds": int64(70), "nanoseconds": int64(2),
	})
	values := pipelineRow{
		"dateValue":      CypherDate{Time: time.Date(1984, 10, 11, 0, 0, 0, 0, time.UTC)},
		"localTimeValue": CypherLocalTime{Time: time.Date(1970, 1, 1, 12, 31, 14, 1, time.UTC)},
		"dateTimeValue":  time.Date(1984, 10, 11, 12, 31, 14, 1, time.FixedZone("+01:00", 3600)),
		"durationValue":  baseDuration,
	}
	tests := []struct {
		expression string
		want       string
	}{
		{expression: "dateValue + durationValue", want: "1997-03-25"},
		{expression: "localTimeValue + durationValue", want: "04:44:24.000000003"},
		{expression: "dateTimeValue - durationValue", want: "1972-04-26T20:18:03.999999999+01:00"},
		{expression: "durationValue + durationValue", want: "P24Y10M28DT32H26M20.000000004S"},
		{expression: "durationValue / 2", want: "P6Y2M22DT13H21M8.000000001S"},
	}
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			got, ok := executor.evaluateRowExpression(test.expression, values)
			if !ok {
				t.Fatalf("expression %q was not evaluated", test.expression)
			}
			if text := formatCypherValueString(got); text != test.want {
				t.Fatalf("expression %q = %q, want %q (%T)", test.expression, text, test.want, got)
			}
		})
	}
}

func TestTemporalTruncationUsesOneTypedImplementation(t *testing.T) {
	executor := &StorageExecutor{}
	tests := []struct {
		expression string
		want       string
	}{
		{expression: "date.truncate('week', date({year: 1984, month: 10, day: 11}), {dayOfWeek: 2})", want: "1984-10-09"},
		{expression: "datetime.truncate('century', date({year: 2017, month: 10, day: 11}), {timezone: 'Europe/Stockholm'})", want: "2000-01-01T00:00+01:00[Europe/Stockholm]"},
		{expression: "localdatetime.truncate('millisecond', localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123}), {nanosecond: 2})", want: "1984-10-11T12:31:14.645000002"},
		{expression: "localtime.truncate('minute', time({hour: 12, minute: 31, second: 14, timezone: '+01:00'}), {})", want: "12:31"},
		{expression: "time.truncate('hour', localtime({hour: 12, minute: 31}), {timezone: '+01:00'})", want: "12:00+01:00"},
	}
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			got, ok := executor.evaluateRowExpression(test.expression, pipelineRow{})
			if !ok {
				t.Fatalf("expression %q was not evaluated", test.expression)
			}
			if text := formatCypherValueString(got); text != test.want {
				t.Fatalf("expression %q = %q, want %q (%T)", test.expression, text, test.want, got)
			}
		})
	}
}

func TestDurationBetweenFunctionsAlignTemporalTypes(t *testing.T) {
	executor := &StorageExecutor{}
	tests := []struct {
		expression string
		want       string
	}{
		{expression: "duration.between(localdatetime('2018-01-01T12:00'), localdatetime('2018-01-02T10:00'))", want: "PT22H"},
		{expression: "duration.between(date('1984-10-11'), date('2015-06-24'))", want: "P30Y8M13D"},
		{expression: "duration.inMonths(date('2018-03-11'), date('2016-06-24'))", want: "P-1Y-8M"},
		{expression: "duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4}))", want: "PT5H"},
		{expression: "duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:54.3'))", want: "PT-0.4S"},
		{expression: "duration.inSeconds(localdatetime('-999999999-01-01'), localdatetime('+999999999-12-31T23:59:59'))", want: "PT17531639991215H59M59S"},
	}
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			got, ok := executor.evaluateRowExpression(test.expression, pipelineRow{})
			if !ok {
				t.Fatalf("expression %q was not evaluated", test.expression)
			}
			if text := formatCypherValueString(got); text != test.want {
				t.Fatalf("expression %q = %q, want %q (%T)", test.expression, text, test.want, got)
			}
		})
	}
}

func TestTimestampFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := executor.Execute(ctx, "RETURN timestamp() AS ts", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	ts, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64 timestamp, got %T", result.Rows[0][0])
	}

	now := time.Now().UnixMilli()
	// Timestamp should be within 1 second of now
	if ts < now-1000 || ts > now+1000 {
		t.Errorf("Timestamp %d is not close to current time %d", ts, now)
	}
}

func TestDatetimeFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("datetime no args returns current datetime", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime() AS dt", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(time.Time)
		if !ok {
			t.Fatalf("Expected time.Time, got %T", result.Rows[0][0])
		}
		if got.IsZero() {
			t.Fatalf("Expected non-zero datetime")
		}
	})

	t.Run("datetime parses ISO string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime('2025-11-27T10:30:00') AS dt", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(time.Time)
		if !ok {
			t.Fatalf("Expected time.Time, got %T", result.Rows[0][0])
		}
		if got.UTC().Format(time.RFC3339) != "2025-11-27T10:30:00Z" {
			t.Errorf("Expected 2025-11-27T10:30:00Z, got %s", got.UTC().Format(time.RFC3339))
		}
	})
}

func TestDateFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("date no args returns current date", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date() AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(CypherDate)
		if !ok {
			t.Fatalf("Expected CypherDate, got %T", result.Rows[0][0])
		}
		if got.Time.IsZero() {
			t.Error("Expected non-zero date")
		}
	})

	t.Run("date parses ISO string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date('2025-11-27') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(CypherDate)
		if !ok {
			t.Fatalf("Expected CypherDate, got %T", result.Rows[0][0])
		}
		if got.String() != "2025-11-27" {
			t.Errorf("Expected 2025-11-27, got %s", got.String())
		}
	})
}

func TestTimeFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("time no args returns current time", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN time() AS t", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(CypherTime)
		if !ok {
			t.Fatalf("Expected CypherTime, got %T", result.Rows[0][0])
		}
		if got.Time.IsZero() {
			t.Error("Expected non-zero time")
		}
	})

	t.Run("time parses time string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN time('14:30:00') AS t", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(CypherTime)
		if !ok {
			t.Fatalf("Expected CypherTime, got %T", result.Rows[0][0])
		}
		if got.String() != "14:30Z" {
			t.Errorf("Expected 14:30Z, got %s", got.String())
		}
	})
}

func TestLocaldatetimeFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := executor.Execute(ctx, "RETURN localdatetime() AS ldt", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	got, ok := result.Rows[0][0].(CypherLocalDateTime)
	if !ok {
		t.Fatalf("Expected CypherLocalDateTime, got %T", result.Rows[0][0])
	}
	if got.Time.IsZero() {
		t.Error("Expected non-zero local datetime")
	}
}

func TestLocaltimeFunction(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := executor.Execute(ctx, "RETURN localtime() AS lt", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	got, ok := result.Rows[0][0].(CypherLocalTime)
	if !ok {
		t.Fatalf("Expected CypherLocalTime, got %T", result.Rows[0][0])
	}
	if got.Time.IsZero() {
		t.Error("Expected non-zero local time")
	}
}

func TestDateComponentFunctions(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("date.year extracts year", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date.year('2025-11-27') AS y", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(int64)
		if got != 2025 {
			t.Errorf("Expected 2025, got %d", got)
		}
	})

	t.Run("date.month extracts month", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date.month('2025-11-27') AS m", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(int64)
		if got != 11 {
			t.Errorf("Expected 11, got %d", got)
		}
	})

	t.Run("date.day extracts day", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date.day('2025-11-27') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(int64)
		if got != 27 {
			t.Errorf("Expected 27, got %d", got)
		}
	})

	t.Run("additional date components and truncation", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date.week('2025-11-27') AS w, date.quarter('2025-11-27') AS q, date.dayOfWeek('2025-11-27') AS dow, date.dayOfYear('2025-11-27') AS doy, date.ordinalDay('2025-11-27') AS od, date.weekYear('2025-11-27') AS wy", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row := result.Rows[0]
		if row[0].(int64) != 48 {
			t.Fatalf("date.week expected 48, got %v", row[0])
		}
		if row[1].(int64) != 4 {
			t.Fatalf("date.quarter expected 4, got %v", row[1])
		}
		if row[2].(int64) != 4 {
			t.Fatalf("date.dayOfWeek expected 4, got %v", row[2])
		}
		if row[3].(int64) != 331 || row[4].(int64) != 331 {
			t.Fatalf("date day-of-year mismatch: %v / %v", row[3], row[4])
		}
		if row[5].(int64) != 2025 {
			t.Fatalf("date.weekYear expected 2025, got %v", row[5])
		}

		result, err = executor.Execute(ctx, "RETURN date.truncate('year',date('2025-11-27')) AS y, date.truncate('quarter',date('2025-11-27')) AS q, date.truncate('month',date('2025-11-27')) AS m, date.truncate('week',date('2025-11-27')) AS w, date.truncate('day',date('2025-11-27')) AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row = result.Rows[0]
		if row[0].(CypherDate).String() != "2025-01-01" || row[1].(CypherDate).String() != "2025-10-01" || row[2].(CypherDate).String() != "2025-11-01" || row[3].(CypherDate).String() != "2025-11-24" || row[4].(CypherDate).String() != "2025-11-27" {
			t.Fatalf("unexpected date.truncate output: %#v", row)
		}
	})

	t.Run("datetime/time truncate and datetime components", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime.truncate('hour',datetime('2025-11-27T14:35:50Z')) AS h, datetime.truncate('minute',datetime('2025-11-27T14:35:50Z')) AS m, datetime.truncate('second',datetime('2025-11-27T14:35:50Z')) AS s, datetime.truncate('day',datetime('2025-11-27T14:35:50Z')) AS d, time.truncate('hour',time('14:35:50')) AS th, time.truncate('minute',time('14:35:50')) AS tm", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row := result.Rows[0]
		if row[0].(time.Time).Hour() != 14 || row[0].(time.Time).Minute() != 0 {
			t.Fatalf("unexpected datetime.truncate hour: %v", row[0])
		}
		if row[1].(time.Time).Hour() != 14 || row[1].(time.Time).Minute() != 35 || row[1].(time.Time).Second() != 0 {
			t.Fatalf("unexpected datetime.truncate minute: %v", row[1])
		}
		if row[2].(time.Time).Hour() != 14 || row[2].(time.Time).Minute() != 35 || row[2].(time.Time).Second() != 50 {
			t.Fatalf("unexpected datetime.truncate second: %v", row[2])
		}
		if row[3].(time.Time).Hour() != 0 {
			t.Fatalf("unexpected datetime.truncate day: %v", row[3])
		}
		if row[4].(CypherTime).Time.Hour() != 14 || row[4].(CypherTime).Time.Minute() != 0 || row[5].(CypherTime).Time.Hour() != 14 || row[5].(CypherTime).Time.Minute() != 35 {
			t.Fatalf("unexpected time.truncate outputs: %#v", row[4:6])
		}

		result, err = executor.Execute(ctx, "RETURN datetime.hour('2025-11-27T14:35:50Z') AS h, datetime.minute('2025-11-27T14:35:50Z') AS m, datetime.second('2025-11-27T14:35:50Z') AS s, datetime.year('2025-11-27T14:35:50Z') AS y, datetime.month('2025-11-27T14:35:50Z') AS mo, datetime.day('2025-11-27T14:35:50Z') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row = result.Rows[0]
		if row[0].(int64) != 14 || row[1].(int64) != 35 || row[2].(int64) != 50 || row[3].(int64) != 2025 || row[4].(int64) != 11 || row[5].(int64) != 27 {
			t.Fatalf("unexpected datetime component outputs: %#v", row)
		}
	})
}
