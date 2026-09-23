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
		got := result.Rows[0][0].(string)
		// Should be in YYYY-MM-DD format
		if _, err := time.Parse("2006-01-02", got); err != nil {
			t.Errorf("Invalid date format: %s", got)
		}
	})

	t.Run("date parses ISO string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date('2025-11-27') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if got != "2025-11-27" {
			t.Errorf("Expected 2025-11-27, got %s", got)
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
		got := result.Rows[0][0].(string)
		// Should be in HH:MM:SS format
		if _, err := time.Parse("15:04:05", got); err != nil {
			t.Errorf("Invalid time format: %s", got)
		}
	})

	t.Run("time parses time string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN time('14:30:00') AS t", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if got != "14:30:00" {
			t.Errorf("Expected 14:30:00, got %s", got)
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
	got := result.Rows[0][0].(string)
	// Should be in YYYY-MM-DDTHH:MM:SS format (no timezone)
	if _, err := time.Parse("2006-01-02T15:04:05", got); err != nil {
		t.Errorf("Invalid localdatetime format: %s", got)
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
	got := result.Rows[0][0].(string)
	// Should be in HH:MM:SS format
	if _, err := time.Parse("15:04:05", got); err != nil {
		t.Errorf("Invalid localtime format: %s", got)
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

		result, err = executor.Execute(ctx, "RETURN date.truncate('year','2025-11-27') AS y, date.truncate('quarter','2025-11-27') AS q, date.truncate('month','2025-11-27') AS m, date.truncate('week','2025-11-27') AS w, date.truncate('day','2025-11-27') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row = result.Rows[0]
		if row[0] != "2025-01-01" || row[1] != "2025-10-01" || row[2] != "2025-11-01" || row[3] != "2025-11-24" || row[4] != "2025-11-27" {
			t.Fatalf("unexpected date.truncate output: %#v", row)
		}
	})

	t.Run("datetime/time truncate and datetime components", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime.truncate('hour','2025-11-27T14:35:50Z') AS h, datetime.truncate('minute','2025-11-27T14:35:50Z') AS m, datetime.truncate('second','2025-11-27T14:35:50Z') AS s, datetime.truncate('day','2025-11-27T14:35:50Z') AS d, time.truncate('hour','14:35:50') AS th, time.truncate('minute','14:35:50') AS tm", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		row := result.Rows[0]
		if !strings.HasPrefix(row[0].(string), "2025-11-27T14:00:00") {
			t.Fatalf("unexpected datetime.truncate hour: %v", row[0])
		}
		if !strings.HasPrefix(row[1].(string), "2025-11-27T14:35:00") {
			t.Fatalf("unexpected datetime.truncate minute: %v", row[1])
		}
		if !strings.HasPrefix(row[2].(string), "2025-11-27T14:35:50") {
			t.Fatalf("unexpected datetime.truncate second: %v", row[2])
		}
		if !strings.HasPrefix(row[3].(string), "2025-11-27T00:00:00") {
			t.Fatalf("unexpected datetime.truncate day: %v", row[3])
		}
		if row[4] != "14:00:00" || row[5] != "14:35:00" {
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
