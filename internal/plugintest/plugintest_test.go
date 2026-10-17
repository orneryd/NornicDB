package plugintest

import (
	"runtime/debug"
	"slices"
	"testing"
)

// BuildFlags mirrors the running test binary's own -tags and -race settings.
func TestBuildFlagsMatchRunningBinary(t *testing.T) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build information")
	}
	var tags string
	race := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "-tags":
			tags = setting.Value
		case "-race":
			race = setting.Value == "true"
		}
	}
	flags := BuildFlags()
	if tags != "" {
		i := slices.Index(flags, "-tags")
		if i < 0 || i+1 >= len(flags) || flags[i+1] != tags {
			t.Fatalf("BuildFlags() = %v, want -tags %q", flags, tags)
		}
	} else if slices.Contains(flags, "-tags") {
		t.Fatalf("BuildFlags() = %v, want no -tags", flags)
	}
	if got := slices.Contains(flags, "-race"); got != race {
		t.Fatalf("BuildFlags() = %v, -race present = %v, want %v", flags, got, race)
	}
}
