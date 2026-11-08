package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTransactionTimeoutResolution (NOR-284): the effective transaction
// timeout over every combination of its sources, through the public loaders.
// Precedence, lowest first: the 30s default, database.transaction_timeout,
// NEO4J_dbms_transaction_timeout, NORNICDB_TRANSACTION_TIMEOUT. An empty
// source is unset; a set one wins over every lower source, its zero (which
// disables the timeout) included, so a primary zero suppresses the legacy
// variable.
func TestTransactionTimeoutResolution(t *testing.T) {
	parsed := map[string]time.Duration{
		"0s": 0, "0": 0, "100ms": 100 * time.Millisecond, "45": 45 * time.Second,
		"2m": 2 * time.Minute, "90": 90 * time.Second, "75s": 75 * time.Second,
	}
	yamlValues := []string{"", "0s", "100ms", "45"}
	primaryValues := []string{"", "0", "0s", "2m", "90"}
	legacyValues := []string{"", "0s", "75s"}
	for _, yamlValue := range yamlValues {
		for _, primary := range primaryValues {
			for _, legacy := range legacyValues {
				name := fmt.Sprintf("yaml=%q/primary=%q/legacy=%q", yamlValue, primary, legacy)
				t.Run(name, func(t *testing.T) {
					t.Setenv(transactionTimeoutEnv, primary)
					t.Setenv(transactionTimeoutLegacyEnv, legacy)
					want := 30 * time.Second
					if yamlValue != "" {
						want = parsed[yamlValue]
					}
					if legacy != "" {
						want = parsed[legacy]
					}
					if primary != "" {
						want = parsed[primary]
					}
					path := filepath.Join(t.TempDir(), "nornicdb.yaml")
					content := "database:\n  read_only: false\n"
					if yamlValue != "" {
						content += fmt.Sprintf("  transaction_timeout: %q\n", yamlValue)
					}
					require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
					cfg, err := LoadFromFile(path)
					require.NoError(t, err)
					require.Equal(t, want, cfg.Database.TransactionTimeout, "LoadFromFile")

					// Without a file, the environment applies over the default.
					want = 30 * time.Second
					if legacy != "" {
						want = parsed[legacy]
					}
					if primary != "" {
						want = parsed[primary]
					}
					require.Equal(t, want, LoadFromEnv().Database.TransactionTimeout, "LoadFromEnv")
					cfg = LoadDefaults()
					cfg.Database.TransactionTimeout = time.Hour // a value set before the environment applies
					require.NoError(t, ApplyEnvVars(cfg))
					if primary == "" && legacy == "" {
						want = time.Hour
					}
					require.Equal(t, want, cfg.Database.TransactionTimeout, "ApplyEnvVars")
				})
			}
		}
	}
}

// TestTransactionTimeoutRejectsInvalidValues (NOR-284): a negative or
// unparseable transaction timeout in any source stops loading with an error
// naming the source, instead of silently falling back to another value. The
// legacy variable isn't read once the primary one is set.
func TestTransactionTimeoutRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name, yaml, primary, legacy, source string
	}{
		{name: "negative yaml", yaml: "-1s", source: transactionTimeoutYAMLKey},
		{name: "unparseable yaml", yaml: "soon", source: transactionTimeoutYAMLKey},
		{name: "negative primary", primary: "-5s", source: transactionTimeoutEnv},
		{name: "negative primary seconds", primary: "-5", source: transactionTimeoutEnv},
		{name: "unparseable primary", primary: "abc", source: transactionTimeoutEnv},
		{name: "unparseable legacy", legacy: "bad", source: transactionTimeoutLegacyEnv},
		{name: "negative legacy", legacy: "-2s", source: transactionTimeoutLegacyEnv},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(transactionTimeoutEnv, test.primary)
			t.Setenv(transactionTimeoutLegacyEnv, test.legacy)
			path := filepath.Join(t.TempDir(), "nornicdb.yaml")
			content := "database:\n  read_only: false\n"
			if test.yaml != "" {
				content += fmt.Sprintf("  transaction_timeout: %q\n", test.yaml)
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			_, err := LoadFromFile(path)
			require.ErrorContains(t, err, test.source)
			if test.yaml == "" {
				require.ErrorContains(t, ApplyEnvVars(LoadDefaults()), test.source)
			}
		})
	}

	t.Setenv(transactionTimeoutEnv, "10s")
	t.Setenv(transactionTimeoutLegacyEnv, "bad")
	cfg := LoadDefaults()
	require.NoError(t, ApplyEnvVars(cfg))
	require.Equal(t, 10*time.Second, cfg.Database.TransactionTimeout)
}

// TestSlowQueryThresholdReached (NOR-284): the one slow-query decision every
// reporter uses: a zero or negative threshold reports nothing, and a positive
// one reports what lasts at least that long.
func TestSlowQueryThresholdReached(t *testing.T) {
	for _, test := range []struct {
		threshold, duration time.Duration
		want                bool
	}{
		{0, time.Hour, false},
		{-time.Second, time.Hour, false},
		{5 * time.Second, 4 * time.Second, false},
		{5 * time.Second, 5 * time.Second, true},
		{5 * time.Second, 6 * time.Second, true},
	} {
		require.Equal(t, test.want, SlowQueryThresholdReached(test.threshold, test.duration), "%+v", test)
	}
}
