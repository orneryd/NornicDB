package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
)

// CheckFunc is the readiness probe contract.
//
// Callers (storage, search, replication, ...) inject implementations from the
// composition root (cmd/nornicdb/main.go). This keeps pkg/observability a leaf
// in the import graph (OBS-01) — the registry holds func values, never types
// from business packages.
//
// Implementations MUST be safe for concurrent use and MUST honor ctx
// cancellation; the registry runs checks in parallel and bounds each one with
// a per-check timeout.
type CheckFunc func(ctx context.Context) error

// CheckOpts configures a single registered check.
type CheckOpts struct {
	// Required: when true, a failing check flips ReadyResult.OK to false (and
	// the /readyz handler returns 503). When false (the default), the failure
	// still appears in the JSON response but the overall status stays OK —
	// useful for informational probes (downstream service health, warmup
	// progress, etc.) that should not block kubelet rollouts.
	Required bool
	// Timeout is the per-check budget. Default 1s. A check that exceeds its
	// budget reports the deadline error and Ready returns promptly.
	Timeout time.Duration
}

type registeredCheck struct {
	fn   CheckFunc
	opts CheckOpts
}

// Health is the readiness check registry served by /readyz.
//
// Lookup is read-heavy (every kubelet probe — every few seconds in production)
// and registration is one-shot at startup (or once per t.Cleanup in tests), so
// sync.RWMutex matches the access pattern. Tests stress concurrent
// register/deregister/Ready under -race.
type Health struct {
	mu     sync.RWMutex
	checks map[string]registeredCheck
}

// NewHealth constructs an empty registry.
func NewHealth() *Health {
	return &Health{checks: map[string]registeredCheck{}}
}

// Register adds (or replaces) a check by name.
//
// Re-registering the same name OVERWRITES the previous entry — this is the
// idiomatic behavior for t.Cleanup re-registration in tests and for hot-reload
// scenarios in long-running daemons. Concurrent-safe.
//
// If opts is omitted (variadic empty), the zero value is used: Required=false,
// Timeout=1s default.
func (h *Health) Register(name string, fn CheckFunc, opts ...CheckOpts) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var o CheckOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Timeout <= 0 {
		o.Timeout = time.Second
	}
	h.checks[name] = registeredCheck{fn: fn, opts: o}
}

// Deregister removes a check by name. Deregistering an unknown name is a
// no-op (idempotent — supports test teardown via t.Cleanup without coupling
// to registration order).
func (h *Health) Deregister(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.checks, name)
}

// ReadyResult is the JSON response body for /readyz.
//
// The shape is locked by D-03 (CONTEXT.md): top-level {ok, checks} where
// `checks` is a map keyed by check name. Phase 9 (K8S-06) will add an
// optional `progress` field to CheckStatus as an additive non-breaking
// change — Phase 1 must NOT emit it.
type ReadyResult struct {
	OK     bool                   `json:"ok"`
	Checks map[string]CheckStatus `json:"checks"`
}

// CheckStatus is one entry in ReadyResult.Checks.
//
// The JSON contract is intentionally narrow: ok, latency_ms, and an optional
// error string. Phase 1 deliberately omits the `progress` field that K8S-06
// will add — operators reading Phase-1 /readyz must not see it; tests
// (TestHealth_RenderJSON_HasNoProgressFieldInPhase1) enforce the omission.
type CheckStatus struct {
	OK      bool   `json:"ok"`
	Latency int64  `json:"latency_ms"`
	Error   string `json:"error,omitempty"`
}

// Ready runs every registered check in parallel and aggregates results.
//
// Per-check timeout (CheckOpts.Timeout, default 1s) is enforced via a
// child context derived from ctx. The aggregate `OK` is true iff every
// REQUIRED check passed; informational (Required=false) failures appear
// in the Checks map but don't flip OK.
//
// Implementation note: a registry snapshot under RLock, then run checks
// without holding the lock so a concurrent Register/Deregister can't be
// blocked by a slow check. This matches the access-pattern documentation
// on Health and is safe under -race.
func (h *Health) Ready(ctx context.Context) ReadyResult {
	h.mu.RLock()
	snapshot := make(map[string]registeredCheck, len(h.checks))
	for k, v := range h.checks {
		snapshot[k] = v
	}
	h.mu.RUnlock()

	type pair struct {
		name string
		st   CheckStatus
		req  bool
	}
	results := make(chan pair, len(snapshot))
	var wg sync.WaitGroup
	for name, rc := range snapshot {
		wg.Add(1)
		go func(name string, rc registeredCheck) {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(ctx, rc.opts.Timeout)
			defer cancel()
			start := time.Now()
			err := rc.fn(checkCtx)
			latency := time.Since(start).Milliseconds()
			st := CheckStatus{OK: err == nil, Latency: latency}
			if err != nil {
				st.Error = err.Error()
			}
			results <- pair{name: name, st: st, req: rc.opts.Required}
		}(name, rc)
	}
	wg.Wait()
	close(results)

	out := ReadyResult{OK: true, Checks: map[string]CheckStatus{}}
	for p := range results {
		out.Checks[p.name] = p.st
		if !p.st.OK && p.req {
			out.OK = false
		}
	}
	return out
}

// handleLivez is the OBS-05 / D-03a unconditional 200 stub.
//
// /livez signals "the process is past package init and the kernel can route
// to me". It deliberately runs ZERO registered checks — container
// orchestrators restart the container if /livez fails, and we never want a
// transient downstream failure to trigger a livez restart loop. /readyz is
// the right place for "ready to serve" semantics.
func handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleReadyz is the registered /readyz handler. It runs all checks,
// encodes the result as JSON, and chooses a status code:
//   - 200 OK if all REQUIRED checks pass (informational failures still
//     appear in the body).
//   - 503 Service Unavailable if any required check fails.
//
// The body is JSON in BOTH cases — operators read this directly during
// incidents; an empty 503 body is hostile.
func (h *Health) handleReadyz(w http.ResponseWriter, r *http.Request) {
	result := h.Ready(r.Context())
	w.Header().Set("Content-Type", "application/json")
	if !result.OK {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(result)
}

// versionResponse is the D-03b /version JSON payload. The five keys are
// locked: version, commit, go, build_date, service_instance_id. Operators
// correlate `service_instance_id` here with span resource attributes on the
// same pod when debugging mismatched binaries on a node.
type versionResponse struct {
	Version           string `json:"version"`
	Commit            string `json:"commit"`
	Go                string `json:"go"`
	BuildDate         string `json:"build_date"`
	ServiceInstanceID string `json:"service_instance_id"`
}

// handleVersion returns a /version handler bound to a specific
// service.instance.id (resolved by the Provider per OBS-10).
//
// We return a closure rather than a method on Provider/Health so the
// listener wiring can pass `prov.InstanceID()` once at construction and
// avoid a method-call indirection on every probe.
func handleVersion(instanceID string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		resp := versionResponse{
			Version:           buildinfo.Version(),
			Commit:            buildinfo.ShortCommit(),
			Go:                runtime.Version(),
			BuildDate:         buildinfo.BuildTime,
			ServiceInstanceID: instanceID,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
