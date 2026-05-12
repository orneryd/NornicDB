package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// pprofListener serves the optional :9091 debug surface and implements
// lifecycle.Component. It is opt-in (cfg.Enabled gates construction) and
// binds to 127.0.0.1:9091 by default per ADR §A9 — pprof exposes
// goroutine stacks and heap state, so a non-loopback default would be a
// credential-exfiltration surface.
type pprofListener struct {
	srv *http.Server
	ln  net.Listener
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*pprofListener)(nil)

// resolvePprofListen returns the bind address the pprof listener WILL use
// for the given config, without actually binding. Exposed for tests that
// assert the OBS-06 / ADR §A9 default of "127.0.0.1:9091".
func resolvePprofListen(cfg PprofConfig) string {
	if cfg.Listen != "" {
		return cfg.Listen
	}
	return "127.0.0.1:9091"
}

// NewPprofListener returns the optional :9091 listener.
//
// Returns (nil, nil) — NOT an error — when cfg.Enabled is false. The
// caller (Plan 04 main.go) skips registration as a Component in that case,
// so pprof imposes zero runtime cost when disabled.
//
// Default Listen is "127.0.0.1:9091" per ADR §A9; ":9091" or
// "0.0.0.0:9091" only if the operator explicitly overrides via
// NORNICDB_PPROF_LISTEN. The /debug/pprof/* handlers expose goroutine
// stacks and heap data; a non-loopback default would be a security bug.
//
// Handlers are registered EXPLICITLY on a custom mux. We do NOT use the
// blank/underscore side-effect import form because that registers on
// http.DefaultServeMux, which we deliberately avoid for isolation. A
// dedicated mux also prevents accidental cross-pollution if some unrelated
// import elsewhere ever registers something on DefaultServeMux.
func NewPprofListener(cfg PprofConfig) (*pprofListener, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	listen := resolvePprofListen(cfg)
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("observability: listen on pprof %s: %w", listen, err)
	}
	// Enable mutex and block profiling when pprof is explicitly opted in.
	// These are opt-in because each adds runtime overhead. Kept modest:
	// MutexProfileFraction=1 records every contention event; BlockProfileRate
	// = 1_000_000 ns (1ms) records every block longer than 1ms. Operators
	// gathering /debug/pprof/{mutex,block} will see meaningful data
	// without a full-detail tax on every goroutine switch.
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1_000_000)

	mux := http.NewServeMux()
	// Explicit registration — see godoc above for the rationale.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// Named profiles (mutex, block, goroutine, heap, allocs, threadcreate)
	// are all routed through pprof.Index, but we register explicit handles
	// so operators can query them directly with short paths.
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	return &pprofListener{
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		ln: ln,
	}, nil
}

// Name returns "pprof" for supervisor logging.
func (l *pprofListener) Name() string { return "pprof" }

// Start serves :9091 until ctx is cancelled or Serve returns a fatal
// error. On ctx cancellation, Start triggers srv.Shutdown internally
// so Serve returns and the supervisor's errgroup unblocks. Same shape
// as telemetryListener.Start — see the longer rationale in
// listener.go for why Start MUST observe ctx (Plan 04 integration).
//
// http.ErrServerClosed is filtered to nil so errgroup does not
// interpret a clean shutdown as a fault.
func (l *pprofListener) Start(ctx context.Context) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- l.srv.Serve(l.ln)
	}()
	select {
	case <-ctx.Done():
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

// Shutdown drains in-flight requests with the supervisor-provided budget.
// pprof has no analog of the OQ4 BSP-flush ordering — there's nothing to
// flush, so a plain srv.Shutdown is the entire drain.
func (l *pprofListener) Shutdown(ctx context.Context) error {
	return l.srv.Shutdown(ctx)
}
