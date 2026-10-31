// NOR-160 lane (Claude), 30 October 2026: lifecycle status is a caller copy.
package lifecycle

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionLifecycleLastRun(t *testing.T) {
	metrics := NewLifecycleMetrics()
	metrics.RecordPruneRun(ApplyResult{KeysProcessed: 3, VersionsDeleted: 2}, time.Second)
	want := readownership.Capture(metrics.ToMap(nil)["last_run"])
	for pass := 0; pass < 3; pass++ {
		got := metrics.ToMap(nil)["last_run"]
		require.Equal(t, want, readownership.Capture(got), "pass %d", pass)
		readownership.Mutate(got)
	}
}
