package observability

import (
	"context"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRegistry_RegistersGoAndProcessCollectors verifies MET-17 prep:
// stdlib runtime + process collectors register against our registry.
func TestNewRegistry_RegistersGoAndProcessCollectors(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "test"}
	reg, mp, err := newRegistry(info)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(t, mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var sawGo, sawProcess bool
	for _, mf := range mfs {
		name := mf.GetName()
		if strings.HasPrefix(name, "go_") {
			sawGo = true
		}
		if strings.HasPrefix(name, "process_") {
			sawProcess = true
		}
	}
	assert.True(t, sawGo, "expected at least one go_* metric")
	assert.True(t, sawProcess, "expected at least one process_* metric")
}

// TestNewRegistry_OTelBridgeNamespace verifies Pattern 2 wiring: bridge metrics
// are namespaced under nornicdb_otel_* and WithoutUnits suppresses auto-suffix.
func TestNewRegistry_OTelBridgeNamespace(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "test"}
	reg, mp, err := newRegistry(info)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	// Emit a counter via the OTel meter API.
	c, err := mp.Meter("test").Int64Counter("foo")
	require.NoError(t, err)
	c.Add(context.Background(), 1)

	// Force the OTel→Prom bridge to flush by gathering.
	mfs, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		// With WithoutUnits + WithNamespace("nornicdb_otel"), the counter "foo"
		// should appear as nornicdb_otel_foo_total (Prometheus convention adds
		// _total suffix for Counter type).
		name := mf.GetName()
		if name == "nornicdb_otel_foo_total" || name == "nornicdb_otel_foo" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected nornicdb_otel_foo_total or nornicdb_otel_foo in registry; got: %v", metricNames(mfs))
}

// TestNewRegistry_NoDefaultRegistererPoison asserts that the bridge does NOT
// register against prometheus.DefaultRegisterer (TEST-01 anti-pattern guard).
func TestNewRegistry_NoDefaultRegistererPoison(t *testing.T) {
	// Snapshot the default registerer before.
	defaultBefore := testutil.CollectAndCount(prometheus.NewGoCollector())

	info := ServiceInfo{Name: "nornicdb", Version: "test"}
	reg, mp, err := newRegistry(info)
	require.NoError(t, err)
	require.NotNil(t, reg)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	// If newRegistry leaked into DefaultRegisterer, gather would return non-zero
	// metrics that did NOT come from the explicit registry. We use a roundabout
	// check: the default global gatherer should not have any nornicdb_otel_* metrics.
	defaultGatherer := prometheus.DefaultGatherer
	mfs, err := defaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		assert.NotContains(t, mf.GetName(), "nornicdb_otel",
			"DefaultGatherer should not contain nornicdb_otel_* metrics")
	}
	// Sanity: just ensure the default-go-collector path still works.
	_ = defaultBefore
}

// TestNewRegistry_BSPGaugesDeclared verifies that the Phase-1 BSP placeholder
// metrics are registered (TRC-02 foundation; populated in Phase 6).
func TestNewRegistry_BSPGaugesDeclared(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "test"}
	reg, mp, err := newRegistry(info)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	mfs, err := reg.Gather()
	require.NoError(t, err)

	wantQueueDepth := "nornicdb_otel_bsp_queue_depth"
	wantDropped := "nornicdb_otel_bsp_dropped_spans_total"

	var sawQueue, sawDropped bool
	for _, mf := range mfs {
		switch mf.GetName() {
		case wantQueueDepth:
			sawQueue = true
		case wantDropped:
			sawDropped = true
		}
	}
	assert.True(t, sawQueue, "expected %s in registry", wantQueueDepth)
	assert.True(t, sawDropped, "expected %s in registry", wantDropped)
}

func metricNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	return names
}
