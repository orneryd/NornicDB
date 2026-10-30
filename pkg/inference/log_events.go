package inference

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func logInferenceEvent(ctx context.Context, logger *slog.Logger, localizer *localization.Manager, level slog.Level, event localization.LogEvent) {
	if logger == nil {
		logger = slog.Default()
	}
	if localizer != nil {
		localizer.Log(ctx, logger, level, event)
		return
	}
	attrs := append([]slog.Attr{slog.String("event_id", string(event.ID))}, event.Attrs...)
	logger.LogAttrs(ctx, level, event.Message.Fallback, attrs...)
}
