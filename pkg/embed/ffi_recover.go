// Package embed — D-09 FFI recovery wrapper.
//
// The recoverFFI function is the deferred-recover idiom for every CGo /
// purego call site that drives llama.cpp. A panic from inside C-land
// (segfault wrapped by Go runtime, Metal/CUDA driver fault, etc.) is
// recovered, counted as nornicdb_embed_ffi_panics_total{mode=<backend>},
// and converted into a Go error. The server stays up — Phase 4 hardens
// availability per CONTEXT T-04-08.
//
// Per AGENTS.md §8 (Separation of Concerns) the FFI semantics live in
// pkg/embed; pkg/observability remains the leaf-package authority for
// metric label-set discipline. The counter handle is injected via the
// EmbedMetrics bag (Plan 04-05-02) so this file only depends on the bag
// shape, not on prometheus internals.
//
// Usage:
//
//	func (e *LocalGGUFEmbedder) Embed(ctx context.Context, text string) (vec []float32, err error) {
//	    defer recoverFFI(e.metrics, e.Backend(), &err)
//	    // ... C / purego call that may panic
//	}
//
// The closed `mode` enum (gpu, cpu, cuda, metal, vulkan) is enforced by
// the call site itself — passing the embedder's Backend() return value
// guarantees a value in observability.AllowedEmbedBackends. Adding a new
// backend = update both enums (pkg/embed Backend() implementer +
// observability.AllowedEmbedBackends) plus an ADR §2.3 amendment.
package embed

import (
	"fmt"
	"runtime"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// recoverFFI is the deferred-recover function for FFI call sites.
//
// Behavior:
//   - On no panic: no-op (errp is left untouched; the caller's normal
//     return value flows through).
//   - On panic: increments observability.EmbedMetrics.FFIPanicTotal
//     {mode=mode} (skipped if metrics is nil — defensive for embedded
//     callers without an observability bag wired), wraps the recovered
//     value as a Go error, and assigns it to *errp.
//
// The runtime stack is captured into a small buffer for diagnostic
// inclusion in the error message — operators get the panic site without
// the panic killing the whole server. Stack capture is cheap (a single
// runtime.Stack call into a 4 KiB buffer); no heap escape beyond what the
// error formatting already produces.
//
// errp MUST be a pointer to the named return value. Go's defer semantics
// require the named return; the wrapper modifies it in place so the
// caller's signature `(vec []float32, err error)` flows the recovered
// error back to the caller without any conversion glue.
func recoverFFI(metrics *observability.EmbedMetrics, mode string, errp *error) {
	r := recover()
	if r == nil {
		return
	}

	if metrics != nil && metrics.FFIPanicTotal != nil {
		metrics.FFIPanicTotal.WithLabelValues(mode).Inc()
	}

	// Capture a small stack snapshot for the error message — operators
	// debugging an embed.FFI panic want the call site, not just "panic
	// recovered". 4 KiB is enough for the leaf frames; deeper traces
	// would need more buffer but are rarely useful in CGo crashes.
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	*errp = fmt.Errorf("ffi panic in %s mode (recovered): %v\nstack:\n%s",
		mode, r, string(buf[:n]))
}
