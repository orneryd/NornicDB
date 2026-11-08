package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// A disableable duration is a duration setting that 0 turns off: the
// server's transaction timeout and the slow-query threshold. Every source of
// one is read the same way (parseDisableableDuration), with one precedence:
// the built-in default, then its YAML key, then its environment variables.
// The MERGE key wait reads both resolved values (NOR-284).
type disableableDuration struct {
	// what names the setting in errors.
	what string
	// yamlKey is the setting's YAML key.
	yamlKey string
	// envs are its environment variables, highest precedence first: the
	// first one set applies, and the ones after it aren't read.
	envs []string
	// field is the setting in a Config.
	field func(*Config) *time.Duration
}

const (
	transactionTimeoutYAMLKey   = "database.transaction_timeout"
	transactionTimeoutEnv       = "NORNICDB_TRANSACTION_TIMEOUT"
	transactionTimeoutLegacyEnv = "NEO4J_dbms_transaction_timeout"
	slowQueryThresholdYAMLKey   = "logging.slow_query_threshold"
	slowQueryThresholdEnv       = "NORNICDB_SLOW_QUERY_THRESHOLD"
)

// transactionTimeoutSetting is Database.TransactionTimeout: the longest a
// MERGE waits for a node key another transaction holds. The legacy Neo4j
// variable applies only when NORNICDB_TRANSACTION_TIMEOUT is unset.
var transactionTimeoutSetting = disableableDuration{
	what:    "transaction timeout",
	yamlKey: transactionTimeoutYAMLKey,
	envs:    []string{transactionTimeoutEnv, transactionTimeoutLegacyEnv},
	field:   func(config *Config) *time.Duration { return &config.Database.TransactionTimeout },
}

// slowQueryThresholdSetting is Logging.SlowQueryThreshold: a statement, or a
// MERGE's wait for a key, that lasts this long is logged as slow
// (SlowQueryThresholdReached).
var slowQueryThresholdSetting = disableableDuration{
	what:    "slow-query threshold",
	yamlKey: slowQueryThresholdYAMLKey,
	envs:    []string{slowQueryThresholdEnv},
	field:   func(config *Config) *time.Duration { return &config.Logging.SlowQueryThreshold },
}

// parseDisableableDuration is the one reading of a source's value for
// setting. set is false when the source leaves the setting unset (an empty
// value), so a lower-precedence source applies. A set value is a Go duration
// ("30s", "2m") or whole seconds ("45"); 0 disables the setting and, being
// set, overrides every lower source. A negative or unparseable value is an
// error naming its source: like Neo4j, the server refuses an invalid setting
// instead of silently using another value.
func parseDisableableDuration(setting disableableDuration, source, value string) (duration time.Duration, set bool, err error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false, nil
	}
	if d, parseErr := time.ParseDuration(value); parseErr == nil {
		duration = d
	} else if seconds, atoiErr := strconv.Atoi(value); atoiErr == nil {
		duration = time.Duration(seconds) * time.Second
	} else {
		return 0, false, fmt.Errorf("invalid %s %q: want a duration such as 30s, whole seconds, or 0 to disable the %s", source, value, setting.what)
	}
	if duration < 0 {
		return 0, false, fmt.Errorf("invalid %s %q: the %s cannot be negative; 0 disables it", source, value, setting.what)
	}
	return duration, true, nil
}

// apply sets config's setting from source's value when that value sets it.
func (setting disableableDuration) apply(config *Config, source, value string) error {
	duration, set, err := parseDisableableDuration(setting, source, value)
	if err != nil {
		return err
	}
	if set {
		*setting.field(config) = duration
	}
	return nil
}

// applyYAML applies the setting's YAML value.
func (setting disableableDuration) applyYAML(config *Config, value string) error {
	return setting.apply(config, setting.yamlKey, value)
}

// applyEnv applies the first of the setting's environment variables that is
// set, 0 included; the ones after it aren't read.
func (setting disableableDuration) applyEnv(config *Config) error {
	for _, name := range setting.envs {
		if value := os.Getenv(name); strings.TrimSpace(value) != "" {
			return setting.apply(config, name, value)
		}
	}
	return nil
}

// SlowQueryThresholdReached reports whether something that took duration is
// slow under threshold (Logging.SlowQueryThreshold): a zero or negative
// threshold disables slow-query reporting.
func SlowQueryThresholdReached(threshold, duration time.Duration) bool {
	return threshold > 0 && duration >= threshold
}
