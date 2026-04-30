package observability

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestNewTestEnv_IsolatesRegistry — two TestEnvs in the same test must not
// share metrics. A counter registered on env1.Registry must NOT be visible
// from env2.Registry.
func TestNewTestEnv_IsolatesRegistry(t *testing.T) {
	t.Run("subtest A registers a counter", func(t *testing.T) {
		envA := NewTestEnv(t)
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: "envA_counter_total", Help: "test"})
		require.NoError(t, envA.Registry.Register(c))
		c.Inc()

		got, err := envA.Registry.Gather()
		require.NoError(t, err)
		var found bool
		for _, mf := range got {
			if mf.GetName() == "envA_counter_total" {
				found = true
			}
		}
		require.True(t, found, "envA counter must appear in envA Registry")
	})

	t.Run("subtest B does not see subtest A's counter", func(t *testing.T) {
		envB := NewTestEnv(t)
		got, err := envB.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range got {
			require.NotEqual(t, "envA_counter_total", mf.GetName(),
				"envB Registry must be isolated from envA")
		}
	})
}

// TestNewTestEnv_DefaultRegistererUntouched — TEST-01 anti-pattern guard:
// NewTestEnv must NOT register anything on prometheus.DefaultRegisterer.
func TestNewTestEnv_DefaultRegistererUntouched(t *testing.T) {
	// Snapshot the global registerer. We cast DefaultGatherer (which is the
	// same underlying registry as DefaultRegisterer per client_golang).
	gatherer, ok := prometheus.DefaultGatherer.(*prometheus.Registry)
	require.True(t, ok, "DefaultGatherer must be a *Registry to snapshot")

	preNames := gatheredMetricNames(t, gatherer)

	_ = NewTestEnv(t)

	postNames := gatheredMetricNames(t, gatherer)
	for n := range postNames {
		// Only fail if a NEW nornicdb_* series appeared as a result of NewTestEnv.
		if !preNames[n] && strings.HasPrefix(n, "nornicdb") {
			t.Errorf("NewTestEnv must NOT register %q on DefaultGatherer", n)
		}
	}
}

func gatheredMetricNames(t *testing.T, g prometheus.Gatherer) map[string]bool {
	t.Helper()
	got, err := g.Gather()
	require.NoError(t, err)
	out := map[string]bool{}
	for _, mf := range got {
		out[mf.GetName()] = true
	}
	return out
}

// TestNewTestEnv_ProvidesAllFields — sanity check that every TestEnv field is
// populated and usable by callers. This is the contract Phase 3+ tests
// depend on.
func TestNewTestEnv_ProvidesAllFields(t *testing.T) {
	env := NewTestEnv(t)
	require.NotNil(t, env.Registry)
	require.NotNil(t, env.Exporter)
	require.NotNil(t, env.Logger)
	require.NotNil(t, env.Provider)
	require.NotNil(t, env.Health)

	// Provider's Registry should be the SAME instance as env.Registry (no
	// double-construction).
	require.Same(t, env.Registry, env.Provider.Registry(),
		"TestEnv.Registry and Provider.Registry() must be the same instance")
}
