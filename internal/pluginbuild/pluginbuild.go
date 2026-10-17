// Package pluginbuild derives a best-effort `go build` configuration for a Go
// plugin that a given binary should be able to load, and explains differences
// between two binaries' build information.
//
// plugin.Open refuses a plugin when any package linked into both the plugin
// and the host was compiled differently: "plugin was built with a different
// version of package ...". Many inputs change that: the toolchain, -trimpath,
// -race, GOEXPERIMENT, the architecture level, compiler flags, cgo flags and
// headers, and the source of every shared dependency. A binary's build
// information (`go version -m`) records only some of them, and some only
// partly (only the last of several -gcflags, no cgo flags under -trimpath, no
// content identity for vendored or replaced modules). So a Config makes a
// loadable build likely, never certain: whether the host accepts a plugin is
// decided only by the host's loader (`nornicdb stemmer check`).
package pluginbuild

import (
	"debug/buildinfo"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
)

// Config is the part of a binary's build configuration that a plugin for it
// repeats.
type Config struct {
	// GoVersion is the release of the toolchain that built the binary, e.g.
	// "go1.26.9", without the experiment suffix. The plugin must be built by
	// the same release.
	GoVersion string
	// Flags are `go build` flags, e.g. -trimpath, -race, -tags.
	Flags []string
	// Env are KEY=VALUE settings for the `go build` environment. They start
	// with defaults that keep the builder's own Go configuration out (GOENV=off,
	// empty GOFLAGS, default GOEXPERIMENT and GOFIPS140, GOTOOLCHAIN=local),
	// followed by the recorded settings, which override them.
	Env []string
}

// baselineEnv makes settings the binary does not record take their Go
// defaults rather than the builder's GOFLAGS, GOEXPERIMENT or `go env -w`
// values: build information omits false flags, empty tags and the default
// GOEXPERIMENT, so their absence means "default".
var baselineEnv = []string{
	"GOENV=off",
	"GOFLAGS=",
	"GOTOOLCHAIN=local",
	"GOEXPERIMENT=",
	"GOFIPS140=off",
}

// envSettings are the recorded environment settings that change how shared
// packages are compiled.
var envSettings = map[string]bool{
	"CGO_ENABLED":  true,
	"CGO_CFLAGS":   true,
	"CGO_CPPFLAGS": true,
	"CGO_CXXFLAGS": true,
	"CGO_FFLAGS":   true,
	"CGO_LDFLAGS":  true,
	"GOOS":         true,
	"GOARCH":       true,
	"GOEXPERIMENT": true,
	"GOFIPS140":    true,
	"GO386":        true,
	"GOAMD64":      true,
	"GOARM":        true,
	"GOARM64":      true,
	"GOMIPS":       true,
	"GOMIPS64":     true,
	"GOPPC64":      true,
	"GORISCV64":    true,
	"GOWASM":       true,
}

// FromBuildInfo returns the configuration recorded in info.
func FromBuildInfo(info *debug.BuildInfo) (Config, error) {
	cfg := Config{GoVersion: toolchainRelease(info.GoVersion), Env: append([]string(nil), baselineEnv...)}
	for _, setting := range info.Settings {
		key, value := setting.Key, setting.Value
		switch {
		case key == "-trimpath" || key == "-race" || key == "-msan" || key == "-asan":
			if value == "true" {
				cfg.Flags = append(cfg.Flags, key)
			}
		case key == "-tags":
			if tags := strings.TrimSpace(value); tags != "" {
				cfg.Flags = append(cfg.Flags, "-tags", tags)
			}
		case key == "-gcflags" || key == "-asmflags":
			// Only the last of several pattern flags is recorded; the
			// target's loader catches what this cannot repeat.
			cfg.Flags = append(cfg.Flags, key+"="+value)
		case key == "-compiler":
			if value != "gc" {
				return Config{}, fmt.Errorf("binary was built with the %s compiler; plugins need gc", value)
			}
		case key == "-pgo":
			// The profile path is the builder's; a plugin can't repeat it.
			return Config{}, fmt.Errorf("binary was built with the PGO profile %s, which a plugin build cannot repeat", value)
		case envSettings[key]:
			cfg.Env = append(cfg.Env, key+"="+value)
		}
	}
	return cfg, nil
}

// toolchainRelease strips the experiment suffix Go appends to a binary's
// recorded version when GOEXPERIMENT is not the default ("go1.26.9-X:arenas"
// or "go1.26.9 X:arenas"), leaving the release `go env GOVERSION` reports.
// The experiments themselves are repeated through the GOEXPERIMENT setting.
func toolchainRelease(version string) string {
	for _, marker := range []string{"-X:", " X:"} {
		if i := strings.Index(version, marker); i >= 0 {
			version = version[:i]
		}
	}
	return version
}

// FromBinary returns the configuration recorded in the binary at path.
func FromBinary(path string) (Config, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read build information of %s: %w", path, err)
	}
	return FromBuildInfo(info)
}

// Running returns the configuration of the running binary, e.g. a test binary
// that loads plugins it builds. It is empty if the binary carries no build
// information.
func Running() (Config, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return Config{}, nil
	}
	return FromBuildInfo(info)
}

// unrelatedSettings legitimately differ between a host and its plugins.
var unrelatedSettings = map[string]bool{
	"-buildmode":     true,
	"-ldflags":       true,
	"-buildvcs":      true,
	"DefaultGODEBUG": true,
	"vcs":            true,
	"vcs.revision":   true,
	"vcs.time":       true,
	"vcs.modified":   true,
}

// Differences lists the recorded differences between a target binary's and a
// plugin's build information that can make the target refuse the plugin: the
// toolchain, build settings, and modules both link with a different version,
// checksum or replacement. It explains a refusal; it never decides one, since
// build information does not record everything that matters.
func Differences(target, plugin *debug.BuildInfo) []string {
	var out []string
	if target.GoVersion != plugin.GoVersion {
		out = append(out, fmt.Sprintf("Go release: target %s, plugin %s", target.GoVersion, plugin.GoVersion))
	}
	targetSettings, pluginSettings := settingsMap(target), settingsMap(plugin)
	for _, key := range unionKeys(targetSettings, pluginSettings) {
		if unrelatedSettings[key] || targetSettings[key] == pluginSettings[key] {
			continue
		}
		out = append(out, fmt.Sprintf("build setting %s: target %s, plugin %s", key, settingText(targetSettings, key), settingText(pluginSettings, key)))
	}
	targetModules := make(map[string]string, len(target.Deps))
	for _, module := range target.Deps {
		targetModules[module.Path] = moduleText(module)
	}
	for _, module := range plugin.Deps {
		want, shared := targetModules[module.Path]
		if have := moduleText(module); shared && want != have {
			out = append(out, fmt.Sprintf("module %s: target %s, plugin %s", module.Path, want, have))
		}
	}
	return out
}

func settingsMap(info *debug.BuildInfo) map[string]string {
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	return settings
}

func unionKeys(a, b map[string]string) []string {
	keys := make([]string, 0, len(a)+len(b))
	for key := range a {
		keys = append(keys, key)
	}
	for key := range b {
		if _, ok := a[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func settingText(settings map[string]string, key string) string {
	value, ok := settings[key]
	if !ok {
		return "(not set)"
	}
	return fmt.Sprintf("%q", value)
}

func moduleText(module *debug.Module) string {
	text := strings.TrimSpace(module.Version + " " + module.Sum)
	if replace := module.Replace; replace != nil {
		text += " => " + strings.Join(strings.Fields(replace.Path+" "+replace.Version+" "+replace.Sum), " ")
	}
	return text
}
