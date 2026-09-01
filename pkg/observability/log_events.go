package observability

import (
	"context"
	"fmt"
	"log"
	"log/slog"
)

type bootstrapLogEvent struct {
	id     string
	level  string
	format string
	attrs  []string
}

var (
	bootstrapInstanceIDResolved     = bootstrapLogEvent{"observability.resource.instance_id_resolved", "INFO", "INFO observability: service.instance.id=%q (resolved from %s)", []string{"service_instance_id", "source"}}
	bootstrapOTLPPlaintextRejected  = bootstrapLogEvent{"observability.tracing.otlp_plaintext_rejected", "WARN", "WARN observability: OTLP endpoint %q is plaintext but NORNICDB_OTLP_INSECURE is not set; installing noop tracer provider (TRC-09)", []string{"endpoint"}}
	bootstrapSpanExporterInitFailed = bootstrapLogEvent{"observability.tracing.span_exporter_init_failed", "WARN", "WARN observability: span exporter init failed: %v; installing noop tracer provider — process continues", []string{"error"}}
	bootstrapSamplerModeInvalid     = bootstrapLogEvent{"observability.tracing.sampler_mode_invalid", "WARN", "WARN observability: %v; falling back to default sampler mode (TRC-05)", []string{"error"}}
	bootstrapParentStrictUnbounded  = bootstrapLogEvent{"observability.tracing.parent_strict_unbounded", "WARN", "WARN observability: parent_strict sampler honors upstream decisions unconditionally; trace volume is unbounded (TRC-07)", []string{"sampler"}}
)

func logBootstrapEvent(logger *log.Logger, event bootstrapLogEvent, args ...any) {
	if logger == nil {
		logger = log.Default()
	}
	message := fmt.Sprintf(event.format, args...)
	fields := fmt.Sprintf(" event_id=%q component=%q", event.id, "observability")
	for index, name := range event.attrs {
		if index < len(args) {
			fields += fmt.Sprintf(" %s=%q", name, fmt.Sprint(args[index]))
		}
	}
	logger.Print(message + fields)
}

func logSlogBootstrapEvent(logger *slog.Logger, event bootstrapLogEvent, args ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := []slog.Attr{slog.String("event_id", event.id), slog.String("component", "observability")}
	for index, name := range event.attrs {
		if index < len(args) {
			attrs = append(attrs, slog.Any(name, args[index]))
		}
	}
	level := slog.LevelInfo
	if event.level == "WARN" {
		level = slog.LevelWarn
	}
	logger.LogAttrs(context.Background(), level, fmt.Sprintf(event.format, args...), attrs...)
}
