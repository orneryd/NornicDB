// Tests for temporal functions in NornicDB Cypher implementation.
package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestTimestampFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

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
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	defer engine.Close()
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	t.Run("datetime no args returns current datetime", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime() AS dt", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		// Should be in RFC3339 format
		if _, err := time.Parse(time.RFC3339, got); err != nil {
			t.Errorf("Invalid datetime format: %s", got)
		}
	})

	t.Run("datetime parses ISO string", func(t *testing.T) {
		result, err := executor.Execute(ctx, "RETURN datetime('2025-11-27T10:30:00') AS dt", nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		got := result.Rows[0][0].(string)
		if !strings.HasPrefix(got, "2025-11-27T10:30:00") {
			t.Errorf("Expected datetime to start with 2025-11-27T10:30:00, got %s", got)
		}
	})
}

func TestDateFunction(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

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
	baseEngine := storage.NewMemoryEngine()

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
	baseEngine := storage.NewMemoryEngine()

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
	baseEngine := storage.NewMemoryEngine()

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
	baseEngine := storage.NewMemoryEngine()

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
}
