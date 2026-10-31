// NOR-160 lane (Claude), 30 October 2026: cached metrics are caller copies.
package heimdall

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionMetricsCache(t *testing.T) {
	collector := NewMetricsCollector(nil, nil)
	want := readownership.Capture(collector.Collect())
	for pass := 0; pass < 3; pass++ {
		got := collector.Collect()
		require.Equal(t, want, readownership.Capture(got), "pass %d", pass)
		readownership.Mutate(got)
	}
}
