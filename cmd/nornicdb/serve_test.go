package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// Phase-success integration tests for OBS-07, OBS-08, and ROADMAP Phase 1
// success criteria 1, 2, 3, 5. These exercise the lifecycle/observability
// integration directly — they intentionally bypass nornicdb.Open + full
// HTTP/Bolt server boot (which would require disk, embeddings, search
// indexes, etc.) because the targets are the SUPERVISED LIFECYCLE seam,
// not the business-logic boot path. The latter is exercised via
// pkg/nornicdb tests (Plan 01-04 added TestDB_HealthCheck_*) and the
// existing pkg/server, pkg/bolt test suites.
//
// VALIDATION.md rows:
//   01-05-01 → TestServe_TelemetryEndpointsLive  (Phase-success-1)
//   01-05-02 → TestServe_PprofOptIn              (Phase-success-2)
//   01-05-03 → TestServe_DrainOrder              (Phase-success-3)
//   01-05-05 → TestServe_OTLPCollectorDownStaysUp (Phase-success-5)

// recorderComponent wraps any lifecycle.Component to record start/shutdown
// timestamps. Used by TestServe_DrainOrder to prove the OBS-08 reverse
// drain order at runtime.
type recorderComponent struct {
	inner      lifecycle.Component
	startedAt  atomic.Int64
	shutdownAt atomic.Int64
}

func (r *recorderComponent) Name() string { return r.inner.Name() }
func (r *recorderComponent) Start(ctx context.Context) error {
	r.startedAt.Store(time.Now().UnixNano())
	return r.inner.Start(ctx)
}
func (r *recorderComponent) Shutdown(ctx context.Context) error {
	r.shutdownAt.Store(time.Now().UnixNano())
	return r.inner.Shutdown(ctx)
}

// stubComponent is a minimal lifecycle.Component for ordering tests where
// the real HTTP/Bolt servers are too heavy. It blocks on ctx until
// Shutdown is called, which is the same shape as workersAdapter.
type stubComponent struct {
	name string
	// optional: inject extra delay in Shutdown to make timestamp ordering
	// reliable on fast machines.
	shutdownDelay time.Duration
}

func (s *stubComponent) Name() string { return s.name }
func (s *stubComponent) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
func (s *stubComponent) Shutdown(ctx context.Context) error {
	if s.shutdownDelay > 0 {
		time.Sleep(s.shutdownDelay)
	}
	return nil
}

// waitForListen polls the address until a TCP dial succeeds or the
// timeout elapses. Avoids race-prone time.Sleep waits.
func waitForListen(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitForListen: %s did not accept connections within %s", addr, timeout)
}

// telemetryAddr returns the actual ":port" the listener bound to. The
// telemetry listener constructor opens net.Listen synchronously, so we
// can read the address before the supervisor is started — but we'd need
// access to the underlying ln. Easiest: caller specifies a fixed
// 127.0.0.1:0 in cfg, then we resolve via a probe-and-retry helper.
//
// Since pkg/observability/listener.go does NOT expose the bound address
// publicly, we use a small workaround: bind a probe listener to find a
// free port BEFORE constructing the telemetry listener, then close the
// probe and feed that port into cfg. There's a tiny TOCTOU window but
// for ephemeral test ports this is the standard Go idiom.
func reservePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reservePort: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// runComponentsInBackground starts lifecycle.Run in a goroutine and
// returns a cancel func + a channel that delivers the Run result.
func runComponentsInBackground(parent context.Context, components ...lifecycle.Component) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() {
		done <- lifecycle.Run(ctx, components...)
	}()
	return cancel, done
}

// ----------------------------------------------------------------------
// TestServe_TelemetryEndpointsLive — Phase-success-1.
// ----------------------------------------------------------------------

func TestServe_TelemetryEndpointsLive(t *testing.T) {
	telemetryAddr := reservePort(t)

	cfg := observability.DefaultConfig()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Listen = telemetryAddr
	cfg.Tracing.Enabled = false // Phase-success-1 doesn't require OTLP.
	cfg.Pprof.Enabled = false

	obs, err := observability.New(context.Background(), cfg, observability.ServiceInfo{
		Name:    "nornicdb-test",
		Version: "0.0.0-test",
		NodeID:  "test-instance-live",
	}, nil, nil)
	if err != nil {
		t.Fatalf("observability.New: %v", err)
	}

	health := observability.NewHealth()
	// Storage check stub — passes (open path).
	health.Register("storage", func(ctx context.Context) error { return nil })

	telemetry, err := observability.NewTelemetryListener(obs, health)
	if err != nil {
		t.Fatalf("NewTelemetryListener: %v", err)
	}

	cancel, done := runComponentsInBackground(context.Background(), telemetry)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("lifecycle.Run did not return within 5s of cancel")
		}
	})

	waitForListen(t, telemetryAddr, 3*time.Second)
	base := "http://" + telemetryAddr

	t.Run("livez_200", func(t *testing.T) {
		resp, err := http.Get(base + "/livez")
		if err != nil {
			t.Fatalf("GET /livez: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/livez status: got %d, want 200", resp.StatusCode)
		}
	})

	t.Run("readyz_200_when_storage_check_passes", func(t *testing.T) {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			t.Fatalf("GET /readyz: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("/readyz status: got %d, want 200; body=%s", resp.StatusCode, body)
		}
		var result observability.ReadyResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("decode /readyz JSON: %v", err)
		}
		if !result.OK {
			t.Fatalf("/readyz result.OK = false; want true; checks=%v", result.Checks)
		}
		if _, ok := result.Checks["storage"]; !ok {
			t.Fatalf("/readyz body missing 'storage' check; got %v", result.Checks)
		}
	})

	t.Run("version_json_has_required_keys", func(t *testing.T) {
		resp, err := http.Get(base + "/version")
		if err != nil {
			t.Fatalf("GET /version: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/version status: got %d, want 200", resp.StatusCode)
		}
		var v map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			t.Fatalf("decode /version JSON: %v", err)
		}
		for _, key := range []string{"version", "commit", "go", "build_date", "service_instance_id"} {
			if _, ok := v[key]; !ok {
				t.Fatalf("/version missing required key %q; body=%v", key, v)
			}
		}
		if got := v["service_instance_id"]; got != "test-instance-live" {
			t.Fatalf("service_instance_id: got %v, want test-instance-live", got)
		}
	})

	t.Run("metrics_exposes_go_and_process_collectors", func(t *testing.T) {
		resp, err := http.Get(base + "/metrics")
		if err != nil {
			t.Fatalf("GET /metrics: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/metrics status: got %d, want 200", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read /metrics body: %v", err)
		}
		text := string(body)
		if !strings.Contains(text, "go_") {
			t.Fatalf("/metrics body missing go_* series; first 200 chars: %s", truncate(text, 200))
		}
		if !strings.Contains(text, "process_") {
			t.Fatalf("/metrics body missing process_* series; first 200 chars: %s", truncate(text, 200))
		}
	})
}

// ----------------------------------------------------------------------
// TestServe_PprofOptIn — Phase-success-2.
// ----------------------------------------------------------------------

func TestServe_PprofOptIn(t *testing.T) {
	t.Run("disabled_by_default_returns_nil_listener", func(t *testing.T) {
		cfg := observability.PprofConfig{Enabled: false}
		ln, err := observability.NewPprofListener(cfg)
		if err != nil {
			t.Fatalf("NewPprofListener disabled: %v", err)
		}
		if ln != nil {
			t.Fatalf("NewPprofListener returned non-nil when disabled — want nil so main.go skips registration")
		}
	})

	t.Run("enabled_binds_loopback_and_serves_pprof_index", func(t *testing.T) {
		addr := reservePort(t)
		cfg := observability.PprofConfig{Enabled: true, Listen: addr}
		ln, err := observability.NewPprofListener(cfg)
		if err != nil {
			t.Fatalf("NewPprofListener enabled: %v", err)
		}
		if ln == nil {
			t.Fatalf("NewPprofListener returned nil when enabled — want a Component")
		}

		// Verify the address is loopback (defense-in-depth — ADR §A9 default).
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			t.Fatalf("split test addr: %v", splitErr)
		}
		if host != "127.0.0.1" {
			t.Fatalf("test addr host: got %s, want 127.0.0.1 (defaults must be loopback)", host)
		}

		cancel, done := runComponentsInBackground(context.Background(), ln)
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("lifecycle.Run did not return within 5s of cancel")
			}
		})

		waitForListen(t, addr, 3*time.Second)

		resp, err := http.Get("http://" + addr + "/debug/pprof/")
		if err != nil {
			t.Fatalf("GET /debug/pprof/: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/debug/pprof/ status: got %d, want 200", resp.StatusCode)
		}
	})
}

// ----------------------------------------------------------------------
// TestServe_DrainOrder — Phase-success-3 + OBS-08.
// ----------------------------------------------------------------------
//
// Asserts that drain timestamps satisfy the OBS-08 contract:
//   http.shutdownAt < bolt.shutdownAt < workers.shutdownAt
//     < pprof.shutdownAt < telemetry.shutdownAt
//
// Telemetry must drain LAST — that's the operational property kubelet
// depends on (continuous /metrics scrape during graceful drain). HTTP
// drains FIRST so we stop accepting new requests immediately.
//
// Uses stubComponents (with a tiny shutdownDelay to make ordering
// deterministic on fast machines) wrapped in recorderComponents. This
// proves the supervisor's reverse-iteration drain — the same code path
// that production main.go exercises.

func TestServe_DrainOrder(t *testing.T) {
	// Build stubs in the SAME registration order main.go uses:
	// telemetry → pprof → workers → bolt → http.
	//
	// shutdownDelay is small but non-zero so timestamps order
	// deterministically even when each Shutdown is otherwise instant.
	stubs := []struct {
		name  string
		delay time.Duration
	}{
		{"telemetry", 5 * time.Millisecond},
		{"pprof", 5 * time.Millisecond},
		{"embed-workers", 5 * time.Millisecond},
		{"bolt", 5 * time.Millisecond},
		{"http", 5 * time.Millisecond},
	}

	recorders := make([]*recorderComponent, len(stubs))
	components := make([]lifecycle.Component, len(stubs))
	for i, s := range stubs {
		r := &recorderComponent{inner: &stubComponent{name: s.name, shutdownDelay: s.delay}}
		recorders[i] = r
		components[i] = r
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- lifecycle.Run(ctx, components...)
	}()

	// Wait until every component has recorded its startedAt timestamp before
	// triggering shutdown. A poll loop with a generous deadline replaces the
	// unconditional time.Sleep(50ms) which is flaky under high CI load — if
	// cancel() fires before a goroutine reaches startedAt.Store the subsequent
	// shutdownAt ordering assertions become unreliable.
	for _, r := range recorders {
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if r.startedAt.Load() != 0 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		if r.startedAt.Load() == 0 {
			t.Fatalf("component %q did not record startedAt within 500ms", r.Name())
		}
	}

	// Trigger shutdown.
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("lifecycle.Run returned error: %v", err)
		}
	case <-time.After(lifecycle.ShutdownTimeout + time.Second):
		t.Fatalf("lifecycle.Run did not return within ShutdownTimeout+1s")
	}

	// Build a name → shutdownAt map and assert order.
	shutdownAt := make(map[string]int64, len(recorders))
	for _, r := range recorders {
		ts := r.shutdownAt.Load()
		if ts == 0 {
			t.Fatalf("component %q never had Shutdown called", r.Name())
		}
		shutdownAt[r.Name()] = ts
	}

	expectedOrder := []string{"http", "bolt", "embed-workers", "pprof", "telemetry"}
	for i := 0; i < len(expectedOrder)-1; i++ {
		earlier := expectedOrder[i]
		later := expectedOrder[i+1]
		if shutdownAt[earlier] >= shutdownAt[later] {
			t.Errorf("OBS-08 drain order violation: %q (Shutdown=%d) should be drained BEFORE %q (Shutdown=%d)",
				earlier, shutdownAt[earlier], later, shutdownAt[later])
		}
	}

	// Belt-and-suspenders: telemetry must be the LAST shutdown overall.
	for _, r := range recorders {
		if r.Name() == "telemetry" {
			continue
		}
		if r.shutdownAt.Load() > shutdownAt["telemetry"] {
			t.Errorf("OBS-08 violation: %q drained AFTER telemetry — telemetry must be last", r.Name())
		}
	}
}

// ----------------------------------------------------------------------
// TestServe_OTLPCollectorDownStaysUp — Phase-success-5 + OBS-11.
// ----------------------------------------------------------------------

func TestServe_OTLPCollectorDownStaysUp(t *testing.T) {
	// Configure a deliberately unreachable OTLP endpoint. We use a
	// reserved test port that we then close before passing it to
	// observability.New — the dial will fail with connection refused.
	unreachableAddr := reservePort(t)

	telemetryAddr := reservePort(t)

	cfg := observability.DefaultConfig()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Listen = telemetryAddr
	cfg.Tracing.Enabled = true
	cfg.Tracing.Endpoint = unreachableAddr
	cfg.Tracing.Insecure = true
	cfg.Tracing.Timeout = 200 * time.Millisecond
	cfg.Pprof.Enabled = false

	// observability.New must NOT return an error — OBS-11 contract.
	// Process startup is unconditionally robust against OTLP failure.
	obs, err := observability.New(context.Background(), cfg, observability.ServiceInfo{
		Name:    "nornicdb-test",
		Version: "0.0.0-test",
		NodeID:  "test-instance-otlp-down",
	}, nil, nil)
	if err != nil {
		t.Fatalf("observability.New must not fail when OTLP collector is down (OBS-11); got %v", err)
	}

	health := observability.NewHealth()
	telemetry, err := observability.NewTelemetryListener(obs, health)
	if err != nil {
		t.Fatalf("NewTelemetryListener: %v", err)
	}

	cancel, done := runComponentsInBackground(context.Background(), telemetry)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("lifecycle.Run did not return within 5s of cancel")
		}
	})

	waitForListen(t, telemetryAddr, 3*time.Second)

	// /livez must still be 200 — the process is up despite OTLP being
	// unreachable. This is the integration counterpart to Plan 02's unit
	// test; same property, different layer.
	resp, err := http.Get("http://" + telemetryAddr + "/livez")
	if err != nil {
		t.Fatalf("GET /livez with OTLP down: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/livez status with OTLP down: got %d, want 200 (OBS-11 / Phase-success-5)", resp.StatusCode)
	}
}

// Test_TenantLabelsResolved_Override_Precedence asserts at the cmd-package
// boundary that observability.ResolveTenantLabels enforces D-02a precedence
// (explicit YAML > autodetect > default). This is a thin cmd-level smoke
// test on top of pkg/observability's TestResolveTenantLabels_Precedence —
// it provides extra confidence that cmd/nornicdb is reading the correct
// pkg/observability symbols (the ones the startup hook in main.go relies
// on).
//
// MET-22 coverage: precedence chain.
func Test_TenantLabelsResolved_Override_Precedence(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	// Use the production probe; on a non-K8s test host this returns
	// (false, ReasonServiceHostAbsent). The cases below assert the EXPLICIT
	// precedence path which short-circuits the probe — independent of host.
	probe := observability.DefaultK8sProbe()

	cases := []struct {
		name       string
		explicit   *bool
		wantResult bool
		wantSource string
	}{
		{name: "explicit_true_overrides_host_default", explicit: boolPtr(true), wantResult: true, wantSource: observability.ReasonExplicitYAML},
		{name: "explicit_false_overrides_host_default", explicit: boolPtr(false), wantResult: false, wantSource: observability.ReasonExplicitYAML},
		// Note: the nil-explicit case yields whatever the host autodetect
		// returns (on non-K8s CI: ReasonServiceHostAbsent); covered by the
		// pkg/observability matrix test, not duplicated here.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved, source := observability.ResolveTenantLabels(tc.explicit, probe)
			if resolved != tc.wantResult {
				t.Fatalf("resolved: got %v, want %v", resolved, tc.wantResult)
			}
			if source != tc.wantSource {
				t.Fatalf("source: got %q, want %q", source, tc.wantSource)
			}
		})
	}
}

// Test_TenantLabelsResolved_LogsViaInjectedSlog exercises the exact helper
// (observability.ResolveAndLogTenantLabels) that cmd/nornicdb/main.go calls
// at startup. It captures the slog output via a bytes.Buffer-backed JSON
// handler, calls the helper with explicit=nil (defers to autodetect on a
// non-K8s test host → resolved=false, reason=ReasonServiceHostAbsent), and
// asserts:
//
//   - exactly ONE log record with msg="resolved tenant labels enabled" is
//     emitted (MET-22: logged once at startup).
//   - all four canonical fields (enabled, reason, service_host_present,
//     token_file_present) are present and well-typed.
//
// LOG-09 compliance: the helper uses the injected logger only — never
// slog.Default. This test verifies that contract.
func Test_TenantLabelsResolved_LogsViaInjectedSlog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Call the same helper main.go calls. Resolved bool is host-dependent
	// (false on non-K8s CI, true if the test host happens to be a real
	// pod) — we don't assert on it. We assert on the LOG SHAPE.
	_ = observability.ResolveAndLogTenantLabels(nil, logger)

	var found int
	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("invalid JSON log line: %s err=%v", line, err)
		}
		msg, _ := rec["msg"].(string)
		if msg != "resolved tenant labels enabled" {
			continue
		}
		found++
		// All four canonical fields must be present.
		if _, ok := rec["enabled"]; !ok {
			t.Errorf("log record missing 'enabled' field: %v", rec)
		}
		if _, ok := rec["reason"]; !ok {
			t.Errorf("log record missing 'reason' field: %v", rec)
		}
		if _, ok := rec["service_host_present"]; !ok {
			t.Errorf("log record missing 'service_host_present' field: %v", rec)
		}
		if _, ok := rec["token_file_present"]; !ok {
			t.Errorf("log record missing 'token_file_present' field: %v", rec)
		}
		// Type checks: reason is string, enabled is bool, *_present are bools.
		if _, ok := rec["reason"].(string); !ok {
			t.Errorf("'reason' field must be string, got %T", rec["reason"])
		}
		if _, ok := rec["enabled"].(bool); !ok {
			t.Errorf("'enabled' field must be bool, got %T", rec["enabled"])
		}
		if _, ok := rec["service_host_present"].(bool); !ok {
			t.Errorf("'service_host_present' field must be bool, got %T", rec["service_host_present"])
		}
		if _, ok := rec["token_file_present"].(bool); !ok {
			t.Errorf("'token_file_present' field must be bool, got %T", rec["token_file_present"])
		}
	}
	if found != 1 {
		t.Fatalf("MET-22: resolution log line must be emitted exactly once, got %d (buf=%s)", found, buf.String())
	}
}

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Compile-time guards to keep imports honest. unused but keeps the
// linter from complaining if a refactor removes the last reference.
var (
	_ = errors.New
	_ = fmt.Errorf
	_ = sync.Mutex{}
)
