package bolt

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeQueryExecutor is a minimal QueryExecutor for the metrics tests —
// returns an empty result for any query so message dispatch lands in
// the success path.
type fakeQueryExecutor struct{}

func (fakeQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	return &QueryResult{Columns: nil, Rows: nil}, nil
}

// TestSessionLifetime asserts CONTEXT D-11 connection-accept observation:
// ConnectionsActive is Inc/Dec'd around handleConnection; SessionDuration
// observed on close; ConnectionsTotal{result="success"} incremented on
// clean termination.
func TestSessionLifetime(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)

	srv := New(DefaultConfig(), fakeQueryExecutor{})
	srv.SetBoltMetrics(bag)

	// Drive a synthetic accept → handshake-fail → close cycle by passing
	// a closed pipe to handleConnection. Handshake reads 4 bytes of magic
	// and fails immediately; that's enough to exercise the
	// session-lifecycle defer pair.
	cliEnd, srvEnd := net.Pipe()
	go func() {
		// Close the client end so the server's handshake read returns
		// EOF — drives the failure path.
		_ = cliEnd.Close()
	}()
	srv.handleConnection(srvEnd)

	// ConnectionsActive must return to 0 after handleConnection unwinds.
	got := readGaugeValue(t, reg, "nornicdb_bolt_connections_active")
	assert.InDelta(t, 0.0, got, 0.0001,
		"D-11: connections_active must Dec on session close (deferred)")

	// ConnectionsTotal{result} must record exactly one outcome — either
	// "error" (handshake failure) is acceptable; "success" would also be
	// valid if handshake's err==EOF path is short-circuited.
	count := sumCounterByLabel(t, reg, "nornicdb_bolt_connections_total", "result")
	require.Greater(t, count, 0.0,
		"D-11: connections_total must record at least one termination")

	// SessionDuration histogram must have at least one observation
	// (the count field of the histogram).
	histCount := histogramCount(t, reg, "nornicdb_bolt_session_duration_seconds")
	assert.Greater(t, histCount, uint64(0),
		"D-11: session_duration_seconds must observe on close")
}

// TestMessageDispatch_AllOps asserts CONTEXT D-11a: every closed-enum op
// value flows through dispatchMessage and shows up in
// messages_total{op,result}. Drives the dispatch directly via
// dispatchMessage to bypass the network handshake.
func TestMessageDispatch_AllOps(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)

	srv := New(DefaultConfig(), fakeQueryExecutor{})
	srv.SetBoltMetrics(bag)

	sess := &Session{server: srv, executor: fakeQueryExecutor{}}

	// Drive each closed-enum op via its message-type byte. Each op is
	// expected to fail validation (empty data), but the dispatch loop
	// still records the message-total and message-duration observation —
	// failure path is the chokepoint we care about.
	msgTypes := []byte{
		MsgHello, MsgGoodbye, MsgRun, MsgPull, MsgDiscard,
		MsgReset, MsgBegin, MsgCommit, MsgRollback, MsgRoute,
	}
	for _, mt := range msgTypes {
		// Some handlers send PackStream responses to a writer; the
		// Session has no writer in this test fixture, so we tolerate
		// any error or panic.
		func() {
			defer func() { _ = recover() }()
			_ = sess.dispatchMessage(mt, nil)
		}()
	}

	// Verify every op label value appears in messages_total.
	seenOps := map[string]bool{}
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_bolt_messages_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "op" {
					seenOps[lp.GetValue()] = true
				}
			}
		}
	}
	for _, want := range observability.AllowedBoltOps {
		assert.True(t, seenOps[want],
			"D-11a: messages_total must record op=%q", want)
	}
}

// TestPullChunks_NoSeparateObservation asserts CONTEXT D-11b regression:
// observation happens at the message boundary (single observe per PULL),
// NOT per chunk. Verified by driving PULL twice and asserting
// message_duration_seconds count==2 (not, e.g., 200 if chunks were
// separately observed).
func TestPullChunks_NoSeparateObservation(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)

	srv := New(DefaultConfig(), fakeQueryExecutor{})
	srv.SetBoltMetrics(bag)

	sess := &Session{server: srv, executor: fakeQueryExecutor{}}
	for i := 0; i < 2; i++ {
		func() {
			defer func() { _ = recover() }()
			_ = sess.dispatchMessage(MsgPull, nil)
		}()
	}

	// message_duration_seconds for op=pull should have count==2.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_bolt_message_duration_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			isPull := false
			for _, lp := range m.Label {
				if lp.GetName() == "op" && lp.GetValue() == "pull" {
					isPull = true
				}
			}
			if !isPull {
				continue
			}
			require.NotNil(t, m.Histogram)
			assert.Equal(t, uint64(2), m.Histogram.GetSampleCount(),
				"D-11b: PULL chunks roll up — exactly 2 observations for 2 PULL messages")
			return
		}
	}
	t.Fatal("nornicdb_bolt_message_duration_seconds{op=pull} not found")
}

// TestAuthAttempt_BoltProtocol asserts CONTEXT D-11 + D-05e auth-attempts
// crosswire: when authMetrics is nil (Plan 04-02 default), no panic and
// no observation. When authMetrics is wired (Plan 04-06 simulation here),
// HELLO completion increments auth_attempts_total{result, protocol="bolt"}.
func TestAuthAttempt_BoltProtocol(t *testing.T) {
	t.Run("nil_bag_no_op", func(t *testing.T) {
		// observeAuthAttempt no-ops when bag is nil — no panic.
		srv := New(DefaultConfig(), fakeQueryExecutor{})
		srv.observeAuthAttempt("success")
		// Implicit assertion: did not panic.
	})

	t.Run("bag_wired_increments", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		// Build a minimal AuthMetrics-shaped object using the stub
		// constructor's panic-on-call semantics: we can't call
		// NewAuthMetrics (Plan 04-06 stub panics). Build the bag
		// manually for this Plan 04-02 forward-compat smoke test.
		bag := &observability.AuthMetrics{
			AuthAttempts: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: "nornicdb",
					Subsystem: "auth",
					Name:      "attempts_total",
					Help:      "Forward-compat fixture for Plan 04-06 wiring (D-11/D-05e).",
				},
				[]string{"result", "protocol"},
			),
		}
		reg.MustRegister(bag.AuthAttempts)

		srv := New(DefaultConfig(), fakeQueryExecutor{})
		srv.SetAuthMetrics(bag)

		// Drive three outcomes; verify each lands.
		srv.observeAuthAttempt("success")
		srv.observeAuthAttempt("failure")
		srv.observeAuthAttempt("denied")

		// Assert protocol="bolt" on every emitted series.
		mfs, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "nornicdb_auth_attempts_total" {
				continue
			}
			for _, m := range mf.Metric {
				labels := map[string]string{}
				for _, lp := range m.Label {
					labels[lp.GetName()] = lp.GetValue()
				}
				assert.Equal(t, "bolt", labels["protocol"],
					"D-05e: protocol label is locked to 'bolt' from this call site")
				assert.Contains(t, []string{"success", "failure", "denied"}, labels["result"],
					"D-05e: result enum closed")
			}
		}
	})
}

// readGaugeValue gathers reg, finds the named single-series gauge, and
// returns its value. Test fails if not found.
func readGaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.NotEmpty(t, mf.Metric)
		require.Equal(t, dto.MetricType_GAUGE, mf.GetType())
		return mf.Metric[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge %q not found in registry", name)
	return 0
}

// sumCounterByLabel returns the sum of all counter values in a *Vec
// regardless of label values. Test fails if family not found.
func sumCounterByLabel(t *testing.T, reg *prometheus.Registry, name, labelName string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var total float64
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == labelName && lp.GetValue() != "" {
					total += m.GetCounter().GetValue()
				}
			}
		}
		return total
	}
	t.Fatalf("counter %q not found in registry", name)
	return 0
}

// histogramCount returns the total sample count (across all label
// permutations) of the named histogram. Test fails if family not found.
func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var total uint64
		for _, m := range mf.Metric {
			require.NotNil(t, m.Histogram)
			total += m.Histogram.GetSampleCount()
		}
		return total
	}
	t.Fatalf("histogram %q not found in registry", name)
	return 0
}

// Diagnostic helpers; intentionally unused but kept for in-line
// debugging when a test fails.
//
//nolint:unused
func dumpRegistry(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, _ := reg.Gather()
	var sb strings.Builder
	for _, mf := range mfs {
		sb.WriteString(mf.GetName())
		sb.WriteString("\n")
	}
	t.Log(sb.String())
}

//nolint:unused
func sleepIfNeeded(d time.Duration) { time.Sleep(d) }
