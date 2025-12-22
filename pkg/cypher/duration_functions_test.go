// Tests for duration and temporal arithmetic in NornicDB Cypher implementation.
package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *CypherDuration
	}{
		{
			name:  "simple days",
			input: "P5D",
			expected: &CypherDuration{
				Days: 5,
			},
		},
		{
			name:  "years months days",
			input: "P1Y2M3D",
			expected: &CypherDuration{
				Years:  1,
				Months: 2,
				Days:   3,
			},
		},
		{
			name:  "time components",
			input: "PT4H30M15S",
			expected: &CypherDuration{
				Hours:   4,
				Minutes: 30,
				Seconds: 15,
			},
		},
		{
			name:  "full duration",
			input: "P1Y2M3DT4H5M6S",
			expected: &CypherDuration{
				Years:   1,
				Months:  2,
				Days:    3,
				Hours:   4,
				Minutes: 5,
				Seconds: 6,
			},
		},
		{
			name:  "lowercase",
			input: "p1y2m3dt4h5m6s",
			expected: &CypherDuration{
				Years:   1,
				Months:  2,
				Days:    3,
				Hours:   4,
				Minutes: 5,
				Seconds: 6,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDuration(tt.input)
			if got == nil {
				t.Fatalf("parseDuration(%q) returned nil", tt.input)
			}
			if got.Years != tt.expected.Years ||
				got.Months != tt.expected.Months ||
				got.Days != tt.expected.Days ||
				got.Hours != tt.expected.Hours ||
				got.Minutes != tt.expected.Minutes ||
				got.Seconds != tt.expected.Seconds {
				t.Errorf("parseDuration(%q) = %+v, want %+v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDurationString(t *testing.T) {
	tests := []struct {
		name     string
		duration *CypherDuration
		expected string
	}{
		{
			name:     "days only",
			duration: &CypherDuration{Days: 5},
			expected: "P5D",
		},
		{
			name:     "full duration",
			duration: &CypherDuration{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6},
			expected: "P1Y2M3DT4H5M6S",
		},
		{
			name:     "time only",
			duration: &CypherDuration{Hours: 2, Minutes: 30},
			expected: "PT2H30M",
		},
		{
			name:     "empty duration",
			duration: &CypherDuration{},
			expected: "PT0S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.duration.String()
			if got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestDurationFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("duration parses ISO string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN duration('P5D') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(*CypherDuration)
		if !ok {
			t.Fatalf("Expected CypherDuration, got %T", result.Rows[0][0])
		}
		if got.Days != 5 {
			t.Errorf("Expected 5 days, got %d", got.Days)
		}
	})

	t.Run("duration with time components", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN duration('PT2H30M') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(*CypherDuration)
		if !ok {
			t.Fatalf("Expected CypherDuration, got %T", result.Rows[0][0])
		}
		if got.Hours != 2 || got.Minutes != 30 {
			t.Errorf("Expected 2h30m, got %dh%dm", got.Hours, got.Minutes)
		}
	})
}

func TestDurationBetween(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("duration between dates", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN duration.between('2025-01-01', '2025-01-06') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(*CypherDuration)
		if !ok {
			t.Fatalf("Expected CypherDuration, got %T", result.Rows[0][0])
		}
		if got.Days != 5 {
			t.Errorf("Expected 5 days, got %d", got.Days)
		}
	})

	t.Run("duration between datetimes", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN duration.between('2025-01-01T10:00:00', '2025-01-01T12:30:00') AS d", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(*CypherDuration)
		if !ok {
			t.Fatalf("Expected CypherDuration, got %T", result.Rows[0][0])
		}
		if got.Hours != 2 || got.Minutes != 30 {
			t.Errorf("Expected 2h30m, got %dh%dm", got.Hours, got.Minutes)
		}
	})
}

func TestDurationInDays(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := executor.Execute(ctx, "RETURN duration.inDays(duration('P10D')) AS d", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	got, ok := result.Rows[0][0].(float64)
	if !ok {
		t.Fatalf("Expected float64, got %T", result.Rows[0][0])
	}
	if got != 10.0 {
		t.Errorf("Expected 10 days, got %f", got)
	}
}

func TestDurationInSeconds(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	result, err := executor.Execute(ctx, "RETURN duration.inSeconds(duration('PT1H')) AS s", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	got, ok := result.Rows[0][0].(float64)
	if !ok {
		t.Fatalf("Expected float64, got %T", result.Rows[0][0])
	}
	if got != 3600.0 {
		t.Errorf("Expected 3600 seconds, got %f", got)
	}
}

func TestDurationTotalDays(t *testing.T) {
	d := &CypherDuration{Days: 5, Hours: 12}
	got := d.TotalDays()
	expected := 5.5
	if got != expected {
		t.Errorf("TotalDays() = %f, want %f", got, expected)
	}
}

func TestDurationTotalSeconds(t *testing.T) {
	d := &CypherDuration{Hours: 1, Minutes: 30}
	got := d.TotalSeconds()
	expected := 5400.0 // 1.5 hours = 5400 seconds
	if got != expected {
		t.Errorf("TotalSeconds() = %f, want %f", got, expected)
	}
}

func TestDurationFromMap(t *testing.T) {
	m := map[string]interface{}{
		"days":    5,
		"hours":   3,
		"minutes": 30,
	}
	got := durationFromMap(m)
	if got.Days != 5 || got.Hours != 3 || got.Minutes != 30 {
		t.Errorf("durationFromMap() = %+v, want {Days: 5, Hours: 3, Minutes: 30}", got)
	}
}

func TestAddDurationToDate(t *testing.T) {
	dur := &CypherDuration{Days: 5}
	result := addDurationToDate("2025-01-01", dur)
	if !strings.HasPrefix(result, "2025-01-06") {
		t.Errorf("addDurationToDate() = %s, want 2025-01-06*", result)
	}
}

func TestSubtractDurationFromDate(t *testing.T) {
	dur := &CypherDuration{Days: 5}
	result := subtractDurationFromDate("2025-01-10", dur)
	if !strings.HasPrefix(result, "2025-01-05") {
		t.Errorf("subtractDurationFromDate() = %s, want 2025-01-05*", result)
	}
}

func TestParseDateTime(t *testing.T) {
	tests := []struct {
		name  string
		input string
		year  int
		month int
		day   int
	}{
		{
			name:  "date only",
			input: "2025-11-27",
			year:  2025,
			month: 11,
			day:   27,
		},
		{
			name:  "datetime",
			input: "2025-11-27T10:30:00",
			year:  2025,
			month: 11,
			day:   27,
		},
		{
			name:  "RFC3339",
			input: "2025-11-27T10:30:00Z",
			year:  2025,
			month: 11,
			day:   27,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDateTime(tt.input)
			if got.IsZero() {
				t.Fatalf("parseDateTime(%q) returned zero time", tt.input)
			}
			if got.Year() != tt.year || int(got.Month()) != tt.month || got.Day() != tt.day {
				t.Errorf("parseDateTime(%q) = %v, want %d-%02d-%02d", tt.input, got, tt.year, tt.month, tt.day)
			}
		})
	}
}

// Test date arithmetic via Cypher queries
func TestDateArithmeticQueries(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("date + duration via query", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date('2025-01-01') + duration('P5D') AS newDate", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if !strings.HasPrefix(got, "2025-01-06") {
			t.Errorf("Expected 2025-01-06*, got %s", got)
		}
	})

	t.Run("date - duration via query", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date('2025-01-10') - duration('P3D') AS newDate", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if !strings.HasPrefix(got, "2025-01-07") {
			t.Errorf("Expected 2025-01-07*, got %s", got)
		}
	})

	t.Run("date - date returns duration", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN date('2025-01-10') - date('2025-01-01') AS diff", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got, ok := result.Rows[0][0].(*CypherDuration)
		if !ok {
			t.Fatalf("Expected CypherDuration, got %T: %v", result.Rows[0][0], result.Rows[0][0])
		}
		if got.Days != 9 {
			t.Errorf("Expected 9 days, got %d", got.Days)
		}
	})

	t.Run("datetime + duration with hours", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime('2025-01-01T10:00:00') + duration('PT3H') AS newDt", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if !strings.Contains(got, "13:00:00") {
			t.Errorf("Expected datetime with 13:00:00, got %s", got)
		}
	})

	t.Run("duration + date (commutative)", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN duration('P7D') + date('2025-01-01') AS newDate", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if !strings.HasPrefix(got, "2025-01-08") {
			t.Errorf("Expected 2025-01-08*, got %s", got)
		}
	})
}
