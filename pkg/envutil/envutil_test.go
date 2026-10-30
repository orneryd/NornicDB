package envutil

import (
	"os"
	"testing"
	"time"
)

func TestGetAndNumericParsers(t *testing.T) {
	t.Setenv("ENVUTIL_STR", "value")
	if got := Get("ENVUTIL_STR", "fallback"); got != "value" {
		t.Fatalf("expected env value, got %q", got)
	}
	if got := Get("ENVUTIL_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for missing key, got %q", got)
	}

	t.Setenv("ENVUTIL_INT", "42")
	if got := GetInt("ENVUTIL_INT", 1); got != 42 {
		t.Fatalf("expected parsed int 42, got %d", got)
	}
	t.Setenv("ENVUTIL_INT", "bad")
	if got := GetInt("ENVUTIL_INT", 7); got != 7 {
		t.Fatalf("expected fallback int 7, got %d", got)
	}

	t.Setenv("ENVUTIL_FLOAT", "3.25")
	if got := GetFloat("ENVUTIL_FLOAT", 1.5); got != 3.25 {
		t.Fatalf("expected parsed float 3.25, got %f", got)
	}
	t.Setenv("ENVUTIL_FLOAT", "bad")
	if got := GetFloat("ENVUTIL_FLOAT", 1.5); got != 1.5 {
		t.Fatalf("expected fallback float 1.5, got %f", got)
	}
}

func TestBoolHelpers(t *testing.T) {
	t.Setenv("ENVUTIL_BOOL_STRICT", "true")
	if !GetBoolStrict("ENVUTIL_BOOL_STRICT", false) {
		t.Fatalf("expected strict bool true")
	}
	t.Setenv("ENVUTIL_BOOL_STRICT", "not-bool")
	if got := GetBoolStrict("ENVUTIL_BOOL_STRICT", true); !got {
		t.Fatalf("expected strict fallback true")
	}

	t.Setenv("ENVUTIL_BOOL_LOOSE", "YES")
	if !GetBoolLoose("ENVUTIL_BOOL_LOOSE", false) {
		t.Fatalf("expected loose bool true for YES")
	}
	t.Setenv("ENVUTIL_BOOL_LOOSE", "off")
	if got := GetBoolLoose("ENVUTIL_BOOL_LOOSE", true); got {
		t.Fatalf("expected loose bool false for off")
	}

	if val, ok := LookupBoolLoose("ENVUTIL_LOOKUP_MISSING"); ok || val {
		t.Fatalf("expected missing lookup to return false,false")
	}

	_ = os.Setenv("ENVUTIL_LOOKUP_EMPTY", "   ")
	defer os.Unsetenv("ENVUTIL_LOOKUP_EMPTY")
	if val, ok := LookupBoolLoose("ENVUTIL_LOOKUP_EMPTY"); ok || val {
		t.Fatalf("expected empty lookup to return false,false")
	}

	_ = os.Setenv("ENVUTIL_LOOKUP_TRUE", " On ")
	defer os.Unsetenv("ENVUTIL_LOOKUP_TRUE")
	if val, ok := LookupBoolLoose("ENVUTIL_LOOKUP_TRUE"); !ok || !val {
		t.Fatalf("expected trimmed lookup to return true,true")
	}
}

func TestDurationParsers(t *testing.T) {
	t.Setenv("ENVUTIL_DUR", "1500ms")
	if got := GetDuration("ENVUTIL_DUR", time.Second); got != 1500*time.Millisecond {
		t.Fatalf("expected parsed duration 1500ms, got %v", got)
	}
	t.Setenv("ENVUTIL_DUR", "bad")
	if got := GetDuration("ENVUTIL_DUR", 2*time.Second); got != 2*time.Second {
		t.Fatalf("expected fallback duration 2s, got %v", got)
	}

	t.Setenv("ENVUTIL_DUR_OR_SECS", "3s")
	if got := GetDurationOrSeconds("ENVUTIL_DUR_OR_SECS", time.Second); got != 3*time.Second {
		t.Fatalf("expected parsed duration 3s, got %v", got)
	}
	t.Setenv("ENVUTIL_DUR_OR_SECS", "5")
	if got := GetDurationOrSeconds("ENVUTIL_DUR_OR_SECS", time.Second); got != 5*time.Second {
		t.Fatalf("expected parsed seconds duration 5s, got %v", got)
	}
	t.Setenv("ENVUTIL_DUR_OR_SECS", "bad")
	if got := GetDurationOrSeconds("ENVUTIL_DUR_OR_SECS", 4*time.Second); got != 4*time.Second {
		t.Fatalf("expected fallback duration 4s, got %v", got)
	}
}
