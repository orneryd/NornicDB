// Package envutil provides shared helpers for environment variable parsing.
package envutil

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Get returns the env var value or fallback when unset/empty.
func Get(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// GetInt returns the parsed integer env var or fallback on missing/invalid values.
func GetInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

// GetFloat returns the parsed float env var or fallback on missing/invalid values.
func GetFloat(key string, fallback float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return fallback
}

// GetBoolStrict parses bool values using strconv.ParseBool semantics.
func GetBoolStrict(key string, fallback bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}
	return b
}

// GetBoolLoose parses common bool strings (true/1/yes/on) and uses fallback when unset.
func GetBoolLoose(key string, fallback bool) bool {
	if val := os.Getenv(key); val != "" {
		val = strings.ToLower(val)
		return val == "true" || val == "1" || val == "yes" || val == "on"
	}
	return fallback
}

// LookupBoolLoose looks up a bool env var and reports whether a non-empty value was present.
func LookupBoolLoose(key string) (bool, bool) {
	val, exists := os.LookupEnv(key)
	if !exists {
		return false, false
	}
	trimmed := strings.TrimSpace(strings.ToLower(val))
	if trimmed == "" {
		return false, false
	}
	return trimmed == "true" || trimmed == "1" || trimmed == "yes" || trimmed == "on", true
}

// GetDuration parses a duration env var, returning fallback when unset/invalid.
func GetDuration(key string, fallback time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return fallback
}

// GetDurationOrSeconds parses duration first; if that fails, parses integer seconds.
func GetDurationOrSeconds(key string, fallback time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
		if secs, err := strconv.Atoi(val); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return fallback
}
