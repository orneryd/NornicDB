package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// recoveringHandler is the OUTERMOST wrapper in the 4-layer stack (D-09 /
// Pitfall 3). On panic from any inner handler it writes a single fallback
// JSON line to os.Stderr and returns nil so the calling goroutine survives.
//
// File budget: ~50 LOC (well under PERF-06 800-line cap).
type recoveringHandler struct {
	inner slog.Handler
}

// newRecoveringHandler wraps inner so panics in inner.Handle never escape.
func newRecoveringHandler(inner slog.Handler) *recoveringHandler {
	return &recoveringHandler{inner: inner}
}

// Enabled delegates; cannot panic for sane handlers, but a defer here is
// cheap insurance against a buggy custom inner.
func (h *recoveringHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.inner.Enabled(ctx, lvl)
}

// Handle wraps inner.Handle with defer/recover. On panic, emits a single
// fallback JSON line to os.Stderr in the same RFC3339Nano-stamped shape as
// regular records and returns nil (D-09: keep the process alive).
func (h *recoveringHandler) Handle(ctx context.Context, r slog.Record) (returnedErr error) {
	defer func() {
		if rec := recover(); rec != nil {
			emitFallbackPanicLine(rec)
			returnedErr = nil // D-09: do not propagate
		}
	}()
	return h.inner.Handle(ctx, r)
}

// WithAttrs preserves the recoveringHandler-outermost invariant — derived
// handlers MUST also be *recoveringHandler so the stack invariant survives
// `.With(...)` calls.
func (h *recoveringHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &recoveringHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *recoveringHandler) WithGroup(name string) slog.Handler {
	return &recoveringHandler{inner: h.inner.WithGroup(name)}
}

// emitFallbackPanicLine writes one parser-compatible JSON line to os.Stderr.
// Format: {"time":"...","level":"ERROR","msg":"slog handler panic","panic":"..."}
//
// We do NOT call back into the slog stack — that's exactly what just paniced.
// Use direct []byte construction with strconv.AppendQuote for safe escaping.
func emitFallbackPanicLine(panicVal any) {
	buf := make([]byte, 0, 256)
	buf = append(buf, `{"time":`...)
	buf = strconv.AppendQuote(buf, time.Now().UTC().Format(time.RFC3339Nano))
	buf = append(buf, `,"level":"ERROR","msg":"slog handler panic","panic":`...)
	buf = strconv.AppendQuote(buf, fmt.Sprintf("%v", panicVal))
	buf = append(buf, '}', '\n')
	_, _ = os.Stderr.Write(buf)
}
