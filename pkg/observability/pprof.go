package observability

// Plan 03 Task 01 compile-only stub. Replaced by the full implementation in
// Plan 03 Task 02 — see acceptance criteria.

import (
	"context"
	"net"
	"net/http"
)

type pprofListener struct {
	srv *http.Server
	ln  net.Listener
}

// NewPprofListener is a placeholder satisfied by Task 02.
func NewPprofListener(cfg PprofConfig) (*pprofListener, error) {
	return nil, nil
}

// resolvePprofListen is a placeholder satisfied by Task 02.
func resolvePprofListen(cfg PprofConfig) string {
	return ""
}

func (l *pprofListener) Name() string                       { return "pprof" }
func (l *pprofListener) Start(ctx context.Context) error    { return nil }
func (l *pprofListener) Shutdown(ctx context.Context) error { return nil }
