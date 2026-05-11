package observability

import (
	"strings"
	"sync"
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

// TestTestEnv_RecordCapture — CaptureRecords() rewires the logger to a
// JSON handler; LoggedRecords() returns parsed map records (D-12).
func TestTestEnv_RecordCapture(t *testing.T) {
	te := NewTestEnv(t)
	te.CaptureRecords()
	te.Logger.Info("hello", "k", "v")

	recs := te.LoggedRecords()
	require.Len(t, recs, 1)
	require.Equal(t, "hello", recs[0]["msg"])
	require.Equal(t, "v", recs[0]["k"])
}

// TestTestEnv_RecordCapture_Race — concurrent loggers must be race-clean
// under -race -count=10.
func TestTestEnv_RecordCapture_Race(t *testing.T) {
	te := NewTestEnv(t)
	te.CaptureRecords()

	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(i int) {
			defer wg.Done()
			te.Logger.Info("concurrent", "i", i)
		}(i)
	}
	wg.Wait()

	recs := te.LoggedRecords()
	require.GreaterOrEqual(t, len(recs), 50)
}

// TestTestEnv_RecordCapture_Idempotent — calling CaptureRecords twice within
// the same test is a no-op (does not reset the buffer).
func TestTestEnv_RecordCapture_Idempotent(t *testing.T) {
	te := NewTestEnv(t)
	te.CaptureRecords()
	te.Logger.Info("first")
	te.CaptureRecords() // idempotent
	te.Logger.Info("second")

	recs := te.LoggedRecords()
	require.Len(t, recs, 2)
	require.Equal(t, "first", recs[0]["msg"])
	require.Equal(t, "second", recs[1]["msg"])
}
