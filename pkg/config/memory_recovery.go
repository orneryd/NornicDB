package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func parseMemoryRecoveryMS(name, raw string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 1 || value > int64((1<<63-1)/time.Millisecond) {
		return 0, fmt.Errorf("invalid %s value %q: must be an integer >= 1 ms within time.Duration range", name, raw)
	}
	return value, nil
}

const (
	// DefaultMemoryRecoveryIntervalMS is the minimum spacing between reclamation starts.
	DefaultMemoryRecoveryIntervalMS int64 = 5000
	// DefaultMemoryRecoveryRearmMS rearms recovery without a released lease.
	DefaultMemoryRecoveryRearmMS int64 = 30000
)

// ValidateMemoryRecoveryTimings validates startup configuration before creating a policy.
func ValidateMemoryRecoveryTimings(intervalMS, rearmMS int64) error {
	const maxMS = int64((1<<63 - 1) / time.Millisecond)
	if intervalMS < 1 || intervalMS > maxMS {
		return fmt.Errorf("memory.recovery_interval_ms (NORNICDB_MEMORY_RECOVERY_INTERVAL_MS) must be an integer >= 1 ms within time.Duration range")
	}
	if rearmMS < 1 || rearmMS > maxMS {
		return fmt.Errorf("memory.recovery_rearm_ms (NORNICDB_MEMORY_RECOVERY_REARM_MS) must be an integer >= 1 ms within time.Duration range")
	}
	if rearmMS < intervalMS {
		return fmt.Errorf("memory.recovery_rearm_ms (NORNICDB_MEMORY_RECOVERY_REARM_MS) must be >= memory.recovery_interval_ms (NORNICDB_MEMORY_RECOVERY_INTERVAL_MS)")
	}
	return nil
}
