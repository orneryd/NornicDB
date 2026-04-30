package observability

// Plan 03 Task 01 compile-only stub. Replaced by the full implementation in
// Plan 03 Task 02 — see acceptance criteria.

import (
	"context"
	"net"
	"net/http"
)

type telemetryListener struct {
	srv    *http.Server
	ln     net.Listener
	prov   *Provider
	health *Health
}

// NewTelemetryListener is a placeholder satisfied by Task 02.
func NewTelemetryListener(prov *Provider, h *Health) (*telemetryListener, error) {
	return nil, nil
}

func (l *telemetryListener) Name() string                       { return "telemetry" }
func (l *telemetryListener) Start(ctx context.Context) error    { return nil }
func (l *telemetryListener) Shutdown(ctx context.Context) error { return nil }
