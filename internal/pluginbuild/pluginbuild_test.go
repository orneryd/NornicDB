package pluginbuild

import (
	"os"
	"runtime/debug"
	"slices"
	"testing"
)

func TestFromBuildInfoRepeatsSettingsThatChangeSharedPackages(t *testing.T) {
	info := &debug.BuildInfo{
		GoVersion: "go1.26.9",
		Settings: []debug.BuildSetting{
			{Key: "-buildmode", Value: "exe"},
			{Key: "-compiler", Value: "gc"},
			{Key: "-ldflags", Value: "-X example.com/buildinfo.Commit=abc"},
			{Key: "-tags", Value: "localllm"},
			{Key: "-trimpath", Value: "true"},
			{Key: "-race", Value: "false"},
			{Key: "-gcflags", Value: "all=-N -l"},
			{Key: "CGO_ENABLED", Value: "1"},
			{Key: "CGO_CFLAGS", Value: ""},
			{Key: "GOARCH", Value: "amd64"},
			{Key: "GOOS", Value: "linux"},
			{Key: "GOAMD64", Value: "v3"},
			{Key: "vcs.revision", Value: "0376253d4"},
		},
	}
	cfg, err := FromBuildInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GoVersion != "go1.26.9" {
		t.Fatalf("GoVersion = %q", cfg.GoVersion)
	}
	wantFlags := []string{"-tags", "localllm", "-trimpath", "-gcflags=all=-N -l"}
	if !slices.Equal(cfg.Flags, wantFlags) {
		t.Fatalf("Flags = %q, want %q", cfg.Flags, wantFlags)
	}
	wantEnv := []string{
		"GOENV=off", "GOFLAGS=", "GOTOOLCHAIN=local", "GOEXPERIMENT=", "GOFIPS140=off",
		"CGO_ENABLED=1", "CGO_CFLAGS=", "GOARCH=amd64", "GOOS=linux", "GOAMD64=v3",
	}
	if !slices.Equal(cfg.Env, wantEnv) {
		t.Fatalf("Env = %q, want %q", cfg.Env, wantEnv)
	}
}

func TestFromBuildInfoRefusesSettingsAPluginCannotRepeat(t *testing.T) {
	for _, setting := range []debug.BuildSetting{
		{Key: "-compiler", Value: "gccgo"},
		{Key: "-pgo", Value: "/build/default.pgo"},
	} {
		if _, err := FromBuildInfo(&debug.BuildInfo{Settings: []debug.BuildSetting{setting}}); err == nil {
			t.Fatalf("FromBuildInfo accepted %s=%s", setting.Key, setting.Value)
		}
	}
}

// Running and FromBinary read the same settings from the running test binary.
func TestRunningMatchesFromBinary(t *testing.T) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build information")
	}
	running, err := Running()
	if err != nil {
		t.Fatal(err)
	}
	want, err := FromBuildInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(running.Flags, want.Flags) || !slices.Equal(running.Env, want.Env) {
		t.Fatalf("Running() = %+v, want %+v", running, want)
	}
	race := slices.ContainsFunc(info.Settings, func(s debug.BuildSetting) bool { return s.Key == "-race" && s.Value == "true" })
	if slices.Contains(running.Flags, "-race") != race {
		t.Fatalf("Running().Flags = %q, race build = %v", running.Flags, race)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fromBinary, err := FromBinary(executable)
	if err != nil {
		t.Fatal(err)
	}
	if fromBinary.GoVersion != running.GoVersion || !slices.Equal(fromBinary.Flags, running.Flags) || !slices.Equal(fromBinary.Env, running.Env) {
		t.Fatalf("FromBinary(%s) = %+v, want %+v", executable, fromBinary, running)
	}
	if _, err := FromBinary(executable + ".missing"); err == nil {
		t.Fatal("FromBinary accepted a missing file")
	}
}

// A recorded GOEXPERIMENT overrides the baseline default (later entries win in
// an exec environment).
func TestFromBuildInfoRecordedExperimentFollowsBaseline(t *testing.T) {
	cfg, err := FromBuildInfo(&debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "GOEXPERIMENT", Value: "arenas"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Env[len(cfg.Env)-1]; got != "GOEXPERIMENT=arenas" {
		t.Fatalf("last Env entry = %q", got)
	}
	if slices.Index(cfg.Env, "GOEXPERIMENT=") > slices.Index(cfg.Env, "GOEXPERIMENT=arenas") {
		t.Fatalf("baseline GOEXPERIMENT must come before the recorded one: %q", cfg.Env)
	}
}

func TestDifferencesExplainsSharedModulesAndSettings(t *testing.T) {
	target := &debug.BuildInfo{
		GoVersion: "go1.26.9",
		Settings: []debug.BuildSetting{
			{Key: "-trimpath", Value: "true"},
			{Key: "-ldflags", Value: "-X main.version=1"},
			{Key: "vcs.revision", Value: "abc"},
			{Key: "GOAMD64", Value: "v1"},
		},
		Deps: []*debug.Module{
			{Path: "golang.org/x/text", Version: "v0.42.0", Sum: "h1:new"},
			{Path: "example.com/same", Version: "v1.0.0", Sum: "h1:same"},
			{Path: "example.com/replaced", Version: "v1.0.0", Replace: &debug.Module{Path: "../fork"}},
			{Path: "example.com/target-only", Version: "v1.0.0"},
		},
	}
	plugin := &debug.BuildInfo{
		GoVersion: "go1.26.8",
		Settings: []debug.BuildSetting{
			{Key: "-buildmode", Value: "plugin"},
			{Key: "GOAMD64", Value: "v1"},
			{Key: "GOEXPERIMENT", Value: "arenas"},
		},
		Deps: []*debug.Module{
			{Path: "golang.org/x/text", Version: "v0.30.0"},
			{Path: "example.com/same", Version: "v1.0.0", Sum: "h1:same"},
			{Path: "example.com/replaced", Version: "v1.0.0"},
			{Path: "example.com/plugin-only", Version: "v1.0.0"},
		},
	}
	want := []string{
		"Go release: target go1.26.9, plugin go1.26.8",
		`build setting -trimpath: target "true", plugin (not set)`,
		`build setting GOEXPERIMENT: target (not set), plugin "arenas"`,
		"module golang.org/x/text: target v0.42.0 h1:new, plugin v0.30.0",
		"module example.com/replaced: target v1.0.0 => ../fork, plugin v1.0.0",
	}
	if got := Differences(target, plugin); !slices.Equal(got, want) {
		t.Fatalf("Differences =\n%q\nwant\n%q", got, want)
	}
	if got := Differences(target, target); len(got) != 0 {
		t.Fatalf("Differences(target, target) = %q", got)
	}
}

func TestFromBuildInfoGoVersionIsTheRelease(t *testing.T) {
	for recorded, want := range map[string]string{
		"go1.26.9":                 "go1.26.9",
		"go1.26.9-X:arenas":        "go1.26.9",
		"go1.26.9 X:arenas,jsonv2": "go1.26.9",
	} {
		cfg, err := FromBuildInfo(&debug.BuildInfo{GoVersion: recorded})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.GoVersion != want {
			t.Fatalf("GoVersion for %q = %q, want %q", recorded, cfg.GoVersion, want)
		}
	}
}
