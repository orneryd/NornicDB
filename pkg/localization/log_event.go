package localization

import (
	"context"
	"log/slog"
)

// EventID is a stable, language-independent log event identifier.
type EventID string

// LogEvent describes localized log prose and its language-independent fields.
type LogEvent struct {
	ID      EventID
	Message Message
	Attrs   []slog.Attr
}

// Log renders an event message for ctx and emits its stable identity and fields.
func (m *Manager) Log(ctx context.Context, logger *slog.Logger, level slog.Level, event LogEvent) {
	if logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	text, _, err := m.Render(ctx, event.Message)
	if err != nil {
		text = event.Message.Fallback
		if text == "" {
			text = string(event.Message.ID)
		}
	}

	attrs := make([]slog.Attr, 0, len(event.Attrs)+1)
	attrs = append(attrs, slog.String("event_id", string(event.ID)))
	for _, attr := range event.Attrs {
		if attr.Key != "event_id" {
			attrs = append(attrs, attr)
		}
	}
	logger.LogAttrs(ctx, level, text, attrs...)
}
