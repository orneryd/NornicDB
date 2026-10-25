package observability

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// bootstrapLogEvent describes one startup diagnostic emitted during
// observability bootstrap. The human-readable prose lives in pkg/localization
// (Message constructors + runtime-log catalogs) so the text is not duplicated
// as a bare English string here.
type bootstrapLogEvent struct {
	id      string
	level   string
	message func(args ...any) localization.Message
	attrs   []string
}

var (
	bootstrapInstanceIDResolved = bootstrapLogEvent{
		id:    "observability.resource.instance_id_resolved",
		level: "INFO",
		message: func(args ...any) localization.Message {
			return localization.ObservabilityLogInstanceIDResolved(sprintBootstrapArg(args, 0), sprintBootstrapArg(args, 1))
		},
		attrs: []string{"service_instance_id", "source"},
	}
	bootstrapOTLPPlaintextRejected = bootstrapLogEvent{
		id:    "observability.tracing.otlp_plaintext_rejected",
		level: "WARN",
		message: func(args ...any) localization.Message {
			return localization.ObservabilityLogOTLPPlaintextRejected(sprintBootstrapArg(args, 0))
		},
		attrs: []string{"endpoint"},
	}
	bootstrapSpanExporterInitFailed = bootstrapLogEvent{
		id:    "observability.tracing.span_exporter_init_failed",
		level: "WARN",
		message: func(args ...any) localization.Message {
			return localization.ObservabilityLogSpanExporterInitFailed(bootstrapErrorArg(args, 0))
		},
		attrs: []string{"error"},
	}
	bootstrapSamplerModeInvalid = bootstrapLogEvent{
		id:    "observability.tracing.sampler_mode_invalid",
		level: "WARN",
		message: func(args ...any) localization.Message {
			return localization.ObservabilityLogSamplerModeInvalid(bootstrapErrorArg(args, 0))
		},
		attrs: []string{"error"},
	}
	bootstrapParentStrictUnbounded = bootstrapLogEvent{
		id:    "observability.tracing.parent_strict_unbounded",
		level: "WARN",
		message: func(args ...any) localization.Message {
			return localization.ObservabilityLogParentStrictUnbounded()
		},
		attrs: []string{"sampler"},
	}
)

// logSlogBootstrapEvent emits one startup diagnostic through the provided
// slog logger (falling back to slog.Default when nil). The level prefix lives
// only in the handler's "level" field — the message text must not embed
// "INFO "/"WARN " or the level would be duplicated in the output.
func logSlogBootstrapEvent(logger *slog.Logger, event bootstrapLogEvent, args ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := []slog.Attr{slog.String("event_id", event.id), slog.String("component", "observability")}
	for index, name := range event.attrs {
		if index < len(args) {
			value := args[index]
			switch item := value.(type) {
			case string:
				value = DisplayValue(name, item)
			case error:
				value = RedactError(item)
			}
			attrs = append(attrs, slog.Any(name, value))
		}
	}
	level := slog.LevelInfo
	if event.level == "WARN" {
		level = slog.LevelWarn
	}
	logger.LogAttrs(context.Background(), level, RedactText(event.message(args...).Fallback), attrs...)
}

func sprintBootstrapArg(args []any, index int) string {
	if index < len(args) {
		return fmt.Sprint(args[index])
	}
	return ""
}

func bootstrapErrorArg(args []any, index int) error {
	if index < len(args) {
		if err, ok := args[index].(error); ok {
			return err
		}
	}
	return nil
}
