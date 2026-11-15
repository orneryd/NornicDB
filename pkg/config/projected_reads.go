package config

import (
	"os"
	"strings"
	"sync/atomic"
)

// EnvProjectedReads enables format-independent candidate projection and lazy
// property-cache coverage. It never selects an encoder or rewrites stored data.
const EnvProjectedReads = "NORNICDB_PROJECTED_READS_ENABLED"

var projectedReadsEnabled atomic.Bool

func init() {
	value := strings.TrimSpace(os.Getenv(EnvProjectedReads))
	projectedReadsEnabled.Store(value == "1" || strings.EqualFold(value, "true"))
}

func IsProjectedReadsEnabled() bool { return projectedReadsEnabled.Load() }

func SetProjectedReadsEnabled(enabled bool) { projectedReadsEnabled.Store(enabled) }
