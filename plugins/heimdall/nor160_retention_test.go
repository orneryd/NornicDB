// NOR-160 lane (Claude), 30 October 2026: recent events are caller copies.
package heimdall

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionRecentEvents(t *testing.T) {
	plugin := &WatcherPlugin{}
	plugin.addEvent("t", "m", map[string]interface{}{"detail": map[string]interface{}{"ids": []interface{}{"a"}}})
	want := readownership.Capture(plugin.RecentEvents(10))
	for pass := 0; pass < 3; pass++ {
		got := plugin.RecentEvents(10)
		require.Equal(t, want, readownership.Capture(got), "pass %d", pass)
		readownership.Mutate(got)
	}
}
