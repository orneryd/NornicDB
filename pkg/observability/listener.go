package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// telemetryListener serves the :9090 telemetry mux and implements
// lifecycle.Component. It exposes the four OBS-05 endpoints (/metrics,
// /livez, /readyz, /version) and orchestrates the OQ4 shutdown ordering
// (Provider.Shutdown → HTTP listener close).
type telemetryListener struct {
	srv    *http.Server
	ln     net.Listener
	prov   *Provider
	health *Health
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*telemetryListener)(nil)

// NewTelemetryListener builds the :9090 listener.
//
// The net.Listener is opened in this constructor so that bind failures
// (EADDRINUSE) surface during observability.New rather than asynchronously
// inside the supervisor goroutine — matching Pattern 6 / Plan-02 idioms.
//
// /metrics is registered ONLY when prov.MetricsEnabled() && prov.Registry()
// is non-nil (OBS-04). When metrics are disabled, the route is not
// registered and the mux falls through to a 404 — operators can still hit
// /livez, /readyz, /version.
func NewTelemetryListener(prov *Provider, health *Health) (*telemetryListener, error) {
	if prov == nil {
		return nil, errors.New("observability: telemetry listener requires a non-nil Provider")
	}
	if health == nil {
		return nil, errors.New("observability: telemetry listener requires a non-nil Health")
	}
	addr := prov.cfg.Metrics.Listen
	if addr == "" {
		addr = ":9090"
	}
	if isNumericPort(addr) {
		addr = ":" + addr
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("observability: listen on telemetry %s: %w", addr, err)
	}

	mux := http.NewServeMux()

	// OBS-04: register /metrics only when metrics are enabled.
	if prov.MetricsEnabled() && prov.Registry() != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(prov.Registry(), promhttp.HandlerOpts{
			// EnableOpenMetrics negotiates application/openmetrics-text — Phase 3
			// needs it for exemplars. Free in Phase 1; locked in for forward-compat.
			EnableOpenMetrics: true,
			// Self-instrument /metrics responses against the same registry.
			Registry: prov.Registry(),
		}))
	}

	mux.HandleFunc("/livez", handleLivez)
	mux.HandleFunc("/readyz", health.handleReadyz)
	mux.HandleFunc("/version", handleVersion(prov.InstanceID()))

	return &telemetryListener{
		srv: &http.Server{
			Handler: mux,
			// Slowloris guard — :9090 is unauthenticated by design (it's
			// behind a NetworkPolicy in production), so a non-zero
			// ReadHeaderTimeout is a free hardening per Pattern 6 note 3.
			ReadHeaderTimeout: 5 * time.Second,
		},
		ln:     ln,
		prov:   prov,
		health: health,
	}, nil
}

func isNumericPort(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// Name returns "telemetry" for supervisor logging.
func (l *telemetryListener) Name() string { return "telemetry" }

// Start serves :9090 until ctx is cancelled or Serve returns a fatal
// error. On ctx cancellation, Start initiates the OQ4 drain itself
// (Provider.Shutdown then srv.Shutdown) so srv.Serve returns and the
// supervisor's errgroup unblocks.
//
// Why initiate the drain here instead of relying on the supervisor's
// reverse-iteration drain phase: lifecycle.Run only calls Shutdown
// AFTER g.Wait() returns. g.Wait() blocks until every Component's
// Start returns. Therefore Start MUST observe ctx and trigger its own
// graceful shutdown — otherwise the supervisor would deadlock on
// SIGTERM (Plan 04 integration regression caught this latent issue).
//
// Idempotency of Shutdown lets lifecycle.Run's drain phase call
// Shutdown(ctx) a second time without harm: Provider.Shutdown is
// sync.Once-wrapped, and net/http.Server.Shutdown is itself idempotent.
//
// http.ErrServerClosed is filtered to nil so the supervisor's errgroup
// does not interpret a clean shutdown as a runtime failure (Pitfall 5).
func (l *telemetryListener) Start(ctx context.Context) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- l.srv.Serve(l.ln)
	}()
	select {
	case <-ctx.Done():
		// OQ4 drain: Provider.Shutdown FIRST (BSP flushes while listener
		// is still serving), then srv.Shutdown.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), lifecycle.ShutdownTimeout)
		defer cancel()
		_ = l.Shutdown(shutdownCtx)
		err := <-serveErr
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Shutdown drains the telemetry surface in this strict order (OQ4
// resolution from RESEARCH.md):
//
//  1. Provider.Shutdown(ctx) — flushes the BSP and shuts down the meter
//     provider WHILE the HTTP listener is still serving. Kubelet can keep
//     scraping /metrics during the BSP flush window, and any in-flight
//     spans get exported before the process exits.
//  2. srv.Shutdown(ctx) — stops accepting new requests and drains
//     in-flight ones with the supervisor-provided 30s budget.
//
// This keeps :9090 reachable for the LONGEST possible window during
// graceful drain, satisfying OBS-08 (telemetry drains last across all
// components) AND ensuring the BSP gets flushed (OBS-11 / TRC-02
// foundation). Reversing the order would cause kubelet to record a 503
// scrape during drain.
func (l *telemetryListener) Shutdown(ctx context.Context) error {
	var errs error
	if err := l.prov.Shutdown(ctx); err != nil {
		errs = errors.Join(errs, fmt.Errorf("provider shutdown: %w", err))
	}
	if err := l.srv.Shutdown(ctx); err != nil {
		errs = errors.Join(errs, fmt.Errorf("telemetry listener shutdown: %w", err))
	}
	return errs
}
