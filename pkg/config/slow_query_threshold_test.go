package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestZeroSlowQueryThresholdDisablesWarnings (NOR-284, maintainer review of
// #1756): an explicit NORNICDB_SLOW_QUERY_THRESHOLD of zero disables
// slow-query reporting, the prolonged MERGE-wait warning included, instead of
// leaving the 5s default.
func TestZeroSlowQueryThresholdDisablesWarnings(t *testing.T) {
	for _, zero := range []string{"0", "0s"} {
		t.Setenv("NORNICDB_SLOW_QUERY_THRESHOLD", zero)
		cfg := LoadFromEnv()
		require.Zero(t, cfg.Logging.SlowQueryThreshold, "NORNICDB_SLOW_QUERY_THRESHOLD=%s", zero)
	}
}

// TestSlowQueryThresholdResolution (NOR-284): the effective slow-query
// threshold over every combination of its sources, through the public
// loaders. Precedence, lowest first: the 5s default, logging.slow_query_threshold,
// NORNICDB_SLOW_QUERY_THRESHOLD. An empty source is unset; a set one wins
// over every lower source, its zero (which disables reporting) included.
func TestSlowQueryThresholdResolution(t *testing.T) {
	parsed := map[string]time.Duration{
		"0s": 0, "0": 0, "200ms": 200 * time.Millisecond, "3": 3 * time.Second,
		"2s": 2 * time.Second, "7": 7 * time.Second,
	}
	for _, yamlValue := range []string{"", "0s", "200ms", "3"} {
		for _, envValue := range []string{"", "0", "0s", "2s", "7"} {
			t.Run(fmt.Sprintf("yaml=%q/env=%q", yamlValue, envValue), func(t *testing.T) {
				t.Setenv("NORNICDB_SLOW_QUERY_THRESHOLD", envValue)
				want := 5 * time.Second
				if yamlValue != "" {
					want = parsed[yamlValue]
				}
				if envValue != "" {
					want = parsed[envValue]
				}
				path := filepath.Join(t.TempDir(), "nornicdb.yaml")
				content := "logging:\n  level: INFO\n"
				if yamlValue != "" {
					content += fmt.Sprintf("  slow_query_threshold: %q\n", yamlValue)
				}
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
				cfg, err := LoadFromFile(path)
				require.NoError(t, err)
				require.Equal(t, want, cfg.Logging.SlowQueryThreshold, "LoadFromFile")

				want = 5 * time.Second
				if envValue != "" {
					want = parsed[envValue]
				}
				require.Equal(t, want, LoadFromEnv().Logging.SlowQueryThreshold, "LoadFromEnv")
				cfg = LoadDefaults()
				cfg.Logging.SlowQueryThreshold = time.Hour // a value set before the environment applies
				require.NoError(t, ApplyEnvVars(cfg))
				if envValue == "" {
					want = time.Hour
				}
				require.Equal(t, want, cfg.Logging.SlowQueryThreshold, "ApplyEnvVars")
			})
		}
	}
}

// TestSlowQueryThresholdRejectsInvalidValues (NOR-284): a negative or
// unparseable slow-query threshold in any source stops loading with an error
// naming the source, instead of silently keeping another value.
func TestSlowQueryThresholdRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name, yaml, env, source string
	}{
		{name: "negative yaml", yaml: "-1s", source: "logging.slow_query_threshold"},
		{name: "unparseable yaml", yaml: "soon", source: "logging.slow_query_threshold"},
		{name: "negative env", env: "-5s", source: "NORNICDB_SLOW_QUERY_THRESHOLD"},
		{name: "negative env seconds", env: "-5", source: "NORNICDB_SLOW_QUERY_THRESHOLD"},
		{name: "unparseable env", env: "abc", source: "NORNICDB_SLOW_QUERY_THRESHOLD"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("NORNICDB_SLOW_QUERY_THRESHOLD", test.env)
			path := filepath.Join(t.TempDir(), "nornicdb.yaml")
			content := "logging:\n  level: INFO\n"
			if test.yaml != "" {
				content += fmt.Sprintf("  slow_query_threshold: %q\n", test.yaml)
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			_, err := LoadFromFile(path)
			require.ErrorContains(t, err, test.source)
			if test.yaml == "" {
				require.ErrorContains(t, ApplyEnvVars(LoadDefaults()), test.source)
			}
		})
	}
}
