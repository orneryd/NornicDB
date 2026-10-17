// Package plugintest helps tests build Go plugins that the running test
// binary can load.
package plugintest

import (
	"runtime/debug"
	"strings"
)

// BuildFlags returns the `go build` flags a plugin needs to be loadable by
// the running binary: the same build tags (-tags) and, when the binary is
// race-enabled, -race. plugin.Open rejects a plugin built with different
// flags ("plugin was built with a different version of package"), and a
// plugin built without the binary's tags can fail to build at all (without
// nolocalllm it links the llama library). The flags are read from the
// binary's own build information, so they follow whatever `go test` was
// invoked with.
func BuildFlags() []string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	var flags []string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "-tags":
			if tags := strings.TrimSpace(setting.Value); tags != "" {
				flags = append(flags, "-tags", tags)
			}
		case "-race":
			if setting.Value == "true" {
				flags = append(flags, "-race")
			}
		}
	}
	return flags
}
