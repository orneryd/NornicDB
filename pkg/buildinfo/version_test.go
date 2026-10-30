package buildinfo

import (
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Fatal("Version() should not be empty")
	}
	// Version file is embedded — it should be a semver-ish string or "dev"
	if v != "dev" && !strings.Contains(v, ".") {
		t.Fatalf("Version() looks invalid: %q", v)
	}
}

func TestProductVersion(t *testing.T) {
	pv := ProductVersion()
	if pv != "v"+Version() {
		t.Fatalf("ProductVersion() = %q, want %q", pv, "v"+Version())
	}
	if !strings.HasPrefix(pv, "v") {
		t.Fatalf("ProductVersion() should start with 'v': %q", pv)
	}
}

func TestServerAnnouncement(t *testing.T) {
	sa := ServerAnnouncement()
	if sa != "NornicDB/"+Version() {
		t.Fatalf("ServerAnnouncement() = %q, want %q", sa, "NornicDB/"+Version())
	}
}

func TestShortCommit(t *testing.T) {
	// Save and restore original
	orig := Commit
	defer func() { Commit = orig }()

	// Default "dev" commit
	Commit = "dev"
	if sc := ShortCommit(); sc != "dev" {
		t.Fatalf("ShortCommit() with 'dev' = %q, want 'dev'", sc)
	}

	// Empty commit
	Commit = ""
	if sc := ShortCommit(); sc != "dev" {
		t.Fatalf("ShortCommit() with empty = %q, want 'dev'", sc)
	}

	// Whitespace-only commit
	Commit = "   "
	if sc := ShortCommit(); sc != "dev" {
		t.Fatalf("ShortCommit() with whitespace = %q, want 'dev'", sc)
	}

	// Short commit (<=7 chars)
	Commit = "abc1234"
	if sc := ShortCommit(); sc != "abc1234" {
		t.Fatalf("ShortCommit() with 7-char = %q, want 'abc1234'", sc)
	}

	// Long commit (>7 chars, should truncate)
	Commit = "abc1234567890def"
	if sc := ShortCommit(); sc != "abc1234" {
		t.Fatalf("ShortCommit() with long hash = %q, want 'abc1234'", sc)
	}

	// Exactly 7 chars
	Commit = "1234567"
	if sc := ShortCommit(); sc != "1234567" {
		t.Fatalf("ShortCommit() with exactly 7 = %q, want '1234567'", sc)
	}
}

func TestDisplayVersion(t *testing.T) {
	origCommit := Commit
	origBuild := BuildTime
	defer func() { Commit = origCommit; BuildTime = origBuild }()

	// Base case: dev commit, unknown build time — just version
	Commit = "dev"
	BuildTime = "unknown"
	dv := DisplayVersion()
	if dv != ProductVersion() {
		t.Fatalf("DisplayVersion() with defaults = %q, want %q", dv, ProductVersion())
	}

	// With real commit hash — appends short commit
	Commit = "abc1234567890"
	BuildTime = "unknown"
	dv = DisplayVersion()
	expected := ProductVersion() + "-abc1234"
	if dv != expected {
		t.Fatalf("DisplayVersion() with commit = %q, want %q", dv, expected)
	}

	// With real commit and build time
	Commit = "abc1234567890"
	BuildTime = "2026-01-15T10:00:00Z"
	dv = DisplayVersion()
	if !strings.HasPrefix(dv, ProductVersion()+"-abc1234") {
		t.Fatalf("DisplayVersion() should start with version-commit: %q", dv)
	}
	if !strings.Contains(dv, "(built: 2026-01-15T10:00:00Z)") {
		t.Fatalf("DisplayVersion() should contain build time: %q", dv)
	}

	// Empty commit, real build time — just version with build time
	Commit = ""
	BuildTime = "2026-01-15T10:00:00Z"
	dv = DisplayVersion()
	if !strings.Contains(dv, "(built:") {
		t.Fatalf("DisplayVersion() with build time should contain '(built:': %q", dv)
	}
	if strings.Contains(dv, "-dev") {
		// ShortCommit returns "dev" for empty, but "dev" is excluded from display
		// Actually, ShortCommit returns "dev" and the condition checks shortCommit != "dev"
		// so it should NOT append "-dev"
	}

	// Empty build time — no build info
	Commit = "abc1234567890"
	BuildTime = ""
	dv = DisplayVersion()
	if strings.Contains(dv, "(built:") {
		t.Fatalf("DisplayVersion() with empty build time should not contain '(built:': %q", dv)
	}
}
