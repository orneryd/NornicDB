package cypher

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type logEventLocalizer interface {
	Log(context.Context, *slog.Logger, slog.Level, localization.LogEvent)
}

func (e *StorageExecutor) logEvent(level slog.Level, event localization.LogEvent) {
	if e == nil {
		return
	}
	ctx := context.Background()
	if localizer, ok := e.localizationRenderer.(logEventLocalizer); ok {
		localizer.Log(ctx, e.logger(), level, event)
		return
	}

	attrs := make([]slog.Attr, 0, len(event.Attrs)+1)
	attrs = append(attrs, slog.String("event_id", string(event.ID)))
	for _, attr := range event.Attrs {
		if attr.Key != "event_id" {
			attrs = append(attrs, attr)
		}
	}
	e.logger().LogAttrs(ctx, level, event.Message.Fallback, attrs...)
}
