package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// mandatoryFieldsHandler injects the LOG-04 mandatory fields (service,
// version, node_id) into every record, plus trace_id/span_id when ctx
// carries an active span (D-05). When the SpanContext is invalid (Phase-1
// noop default) the trace-correlation fields are OMITTED entirely — the
// IsValid() guard prevents the 32-char TraceID().String() allocation
// (Pitfall 4).
//
// File budget: ~80 LOC (PERF-06 cap is 800).
type mandatoryFieldsHandler struct {
	inner   slog.Handler
	service string
	version string
	nodeID  string
}

// newMandatoryFieldsHandler captures service/version/node_id ONCE at
// construction time so per-record cost is bounded to the slog.String calls.
//
// info.Name → "service" attr; info.Version → "version"; info.NodeID resolved
// via the same OBS-10 chain as resolveInstanceID (config → POD_NAME →
// hostname → "standalone").
func newMandatoryFieldsHandler(inner slog.Handler, info ServiceInfo) *mandatoryFieldsHandler {
	resolvedNodeID, _ := resolveInstanceID(info.NodeID)
	return &mandatoryFieldsHandler{
		inner:   inner,
		service: info.Name,
		version: info.Version,
		nodeID:  resolvedNodeID,
	}
}

// Enabled delegates.
func (h *mandatoryFieldsHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.inner.Enabled(ctx, lvl)
}

// Handle clones the record (Pitfall 6 — never mutate the source Record),
// injects mandatory fields + trace context if valid, and forwards.
func (h *mandatoryFieldsHandler) Handle(ctx context.Context, r slog.Record) error {
	r2 := r.Clone()
	r2.AddAttrs(
		slog.String("service", h.service),
		slog.String("version", h.version),
		slog.String("node_id", h.nodeID),
	)

	// D-05 + Pitfall 4: only resolve trace IDs when SpanContext is valid.
	// On Phase 1 noop ctx, sc.IsValid() short-circuits to false and we
	// skip the TraceID().String() / SpanID().String() allocations entirely.
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r2.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}

	return h.inner.Handle(ctx, r2)
}

// WithAttrs / WithGroup preserve the mandatoryFieldsHandler wrapper so the
// captured service/version/node_id flow through derived loggers.
func (h *mandatoryFieldsHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &mandatoryFieldsHandler{
		inner:   h.inner.WithAttrs(attrs),
		service: h.service,
		version: h.version,
		nodeID:  h.nodeID,
	}
}

func (h *mandatoryFieldsHandler) WithGroup(name string) slog.Handler {
	return &mandatoryFieldsHandler{
		inner:   h.inner.WithGroup(name),
		service: h.service,
		version: h.version,
		nodeID:  h.nodeID,
	}
}
